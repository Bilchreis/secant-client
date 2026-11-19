defmodule SECNodeStatemTest do
  use ExUnit.Case, async: false
  import Mox

  setup :set_mox_global
  setup :verify_on_exit!

  # Each test gets a unique port to avoid Registry key collisions.
  setup do
    port = rem(:erlang.unique_integer([:positive, :monotonic]), 50_000) + 10_000
    host = ~c"localhost"
    node_id = {host, port}
    NodeTable.start(node_id)
    stub(MockTcpConnection, :is_connected, fn _id -> false end)
    stub(MockTcpConnection, :send_message, fn _id, _msg -> :ok end)
    stub(MockTcpConnection, :disconnect, fn _id -> :ok end)
    {:ok, node_id: node_id, host: host, port: port}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp start_statem(ctx) do
    start_supervised!({SEC_Node_Statem, [host: ctx.host, port: ctx.port, restart: :temporary]})
  end

  defp inject(node_id, msg) do
    Registry.dispatch(Registry.SEC_Node_Statem, node_id, fn entries ->
      Enum.each(entries, fn {pid, _} -> send(pid, msg) end)
    end)
  end

  defp wait_for_state(node_id, expected, timeout \\ 1_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(node_id, expected, deadline)
  end

  defp do_wait(node_id, expected, deadline) do
    with [{pid, _}] <- Registry.lookup(Registry.SEC_Node_Statem, node_id),
         {:ok, %{state: ^expected}} <- SEC_Node_Statem.get_state(pid) do
      :ok
    else
      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(10)
          do_wait(node_id, expected, deadline)
        end
    end
  end

  defp get_state_data(node_id) do
    [{pid, _}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    {:ok, state} = SEC_Node_Statem.get_state(pid)
    state
  end

  defp minimal_description do
    %{properties: %{equipment_id: "test-device"}, modules: %{}}
  end

  # Auto-respond to the common SECoP messages a test device would send back.
  # Called from within the gen_statem process (self() == statem pid).
  defp auto_respond(charlist) do
    case to_string(charlist) do
      "*IDN?\n" ->
        send(
          self(),
          {:identification,
           %SECoP_IDN{
             manufacturer: "ISSE",
             product: "SECoP",
             draft_date: "",
             version: "V2024-09-19"
           }}
        )

      "describe .\n" ->
        send(self(), {:describe, ".", minimal_description(), %{}})

      "activate\n" ->
        send(self(), {:active})

      "deactivate\n" ->
        send(self(), {:inactive})

      <<"ping ", rest::binary>> ->
        id = String.trim(rest)
        send(self(), {:pong, id, %{}})

      _ ->
        :ok
    end
  end

  # Drive the state machine from :connecting all the way to :initialized by
  # responding automatically to describe, activate, and inactivity pings.
  defp drive_to_initialized(ctx) do
    stub(MockTcpConnection, :is_connected, fn _id -> true end)

    stub(MockTcpConnection, :send_message, fn _id, cl ->
      auto_respond(cl)
      :ok
    end)

    start_statem(ctx)
    :ok = wait_for_state(ctx.node_id, :initialized, 1_000)
  end

  # ---------------------------------------------------------------------------
  # State transitions (happy path)
  # ---------------------------------------------------------------------------

  describe "state transitions" do
    test "goes to :disconnected when TcpConnection is not ready at startup", ctx do
      # Default stub: is_connected = false
      start_statem(ctx)
      assert wait_for_state(ctx.node_id, :disconnected, 500) == :ok
    end

    test "transitions to :initialized when initially connected", ctx do
      drive_to_initialized(ctx)
      assert get_state_data(ctx.node_id).state == :initialized
    end

    test "transitions from :disconnected to :initialized via :socket_connected", ctx do
      start_statem(ctx)
      assert wait_for_state(ctx.node_id, :disconnected, 500) == :ok

      stub(MockTcpConnection, :send_message, fn _id, cl ->
        auto_respond(cl)
        :ok
      end)

      inject(ctx.node_id, :socket_connected)

      assert wait_for_state(ctx.node_id, :initialized, 1_000) == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Commands while not initialized
  # ---------------------------------------------------------------------------

  describe "commands while not initialized" do
    test "all calls return {:error, :disconnected} when disconnected", ctx do
      start_statem(ctx)
      assert wait_for_state(ctx.node_id, :disconnected, 500) == :ok

      assert SEC_Node_Statem.read(ctx.node_id, "M", "p") == {:error, :disconnected}
      assert SEC_Node_Statem.change(ctx.node_id, "M", "p", "1") == {:error, :disconnected}
      assert SEC_Node_Statem.ping(ctx.node_id) == {:error, :disconnected}
      assert SEC_Node_Statem.execute_command(ctx.node_id, "M", "cmd") == {:error, :disconnected}
    end
  end

  # ---------------------------------------------------------------------------
  # describe / activate / deactivate
  # ---------------------------------------------------------------------------

  describe "describe / activate / deactivate" do
    test "describe/1 returns the current device description", ctx do
      drive_to_initialized(ctx)

      expect(MockTcpConnection, :send_message, fn _id, ~c"describe .\n" ->
        send(self(), {:describe, ".", minimal_description(), %{}})
        :ok
      end)

      assert SEC_Node_Statem.describe(ctx.node_id) == {:describing, minimal_description()}
    end

    test "activate/1 sends activate and returns {:active}", ctx do
      drive_to_initialized(ctx)

      expect(MockTcpConnection, :send_message, fn _id, ~c"activate\n" ->
        send(self(), {:active})
        :ok
      end)

      assert SEC_Node_Statem.activate(ctx.node_id) == {:active}
    end

    test "deactivate/1 sends deactivate and returns {:inactive}", ctx do
      drive_to_initialized(ctx)

      expect(MockTcpConnection, :send_message, fn _id, ~c"deactivate\n" ->
        send(self(), {:inactive})
        :ok
      end)

      assert SEC_Node_Statem.deactivate(ctx.node_id) == {:inactive}
    end
  end

  # ---------------------------------------------------------------------------
  # read / change / execute_command
  # ---------------------------------------------------------------------------

  describe "read / change / execute_command" do
    test "read/3 sends the correct SECoP message and returns the reply", ctx do
      drive_to_initialized(ctx)

      expect(MockTcpConnection, :send_message, fn _id, charlist ->
        assert to_string(charlist) == "read T:value\n"
        send(self(), {:reply, "T", "value", [42.0, %{t: 1.0}]})
        :ok
      end)

      assert SEC_Node_Statem.read(ctx.node_id, "T", "value") ==
               {:reply, "T", "value", [42.0, %{t: 1.0}]}
    end

    test "change/4 sends the correct SECoP message and returns the changed value", ctx do
      drive_to_initialized(ctx)

      expect(MockTcpConnection, :send_message, fn _id, charlist ->
        assert to_string(charlist) == "change T:target 300.0\n"
        send(self(), {:changed, "T", "target", [300.0, %{t: 1.0}]})
        :ok
      end)

      assert SEC_Node_Statem.change(ctx.node_id, "T", "target", "300.0") ==
               {:changed, "T", "target", [300.0, %{t: 1.0}]}
    end

    test "execute_command/3 sends the correct SECoP message and returns done", ctx do
      drive_to_initialized(ctx)

      expect(MockTcpConnection, :send_message, fn _id, charlist ->
        assert to_string(charlist) == "do T:stop null\n"
        send(self(), {:done, "T", "stop", nil})
        :ok
      end)

      assert SEC_Node_Statem.execute_command(ctx.node_id, "T", "stop") ==
               {:done, "T", "stop", nil}
    end

    test "ping/1 sends a ping and returns the pong", ctx do
      drive_to_initialized(ctx)

      expect(MockTcpConnection, :send_message, fn _id, charlist ->
        assert String.starts_with?(to_string(charlist), "ping ")
        send(self(), {:pong, 12_345, %{}})
        :ok
      end)

      assert SEC_Node_Statem.ping(ctx.node_id) == {:pong, 12_345, %{}}
    end
  end

  # ---------------------------------------------------------------------------
  # Inactivity check
  # ---------------------------------------------------------------------------

  describe "inactivity check" do
    test "sends a ping when no NodeTable update is seen since last check", ctx do
      drive_to_initialized(ctx)

      # Override stub: notify test process when a ping is sent, then respond
      # with the pong so the state machine stays alive for further checks.
      test_pid = self()

      stub(MockTcpConnection, :send_message, fn _id, charlist ->
        if String.starts_with?(to_string(charlist), "ping ") do
          send(test_pid, :ping_detected)
          [_, id] = to_string(charlist) |> String.trim() |> String.split(" ")
          send(self(), {:pong, id, %{}})
        end

        :ok
      end)

      # Wait longer than inactivity_timeout (100 ms) so the check fires.
      Process.sleep(250)

      assert_received :ping_detected
    end

    test "stays in :initialized after a successful pong response", ctx do
      drive_to_initialized(ctx)
      # Default stub auto-responds to pings — allow several inactivity cycles.
      Process.sleep(400)
      assert get_state_data(ctx.node_id).state == :initialized
    end

    test "triggers reconnect when no pong received within call_timeout", ctx do
      drive_to_initialized(ctx)

      # Ping gets no pong. On timeout, maybe_force_disconnect calls disconnect/1
      # immediately (no :socket_disconnected in mailbox). Simulate TcpConnection
      # by injecting :socket_disconnected from the disconnect stub.
      stub(MockTcpConnection, :send_message, fn _id, _msg -> :ok end)
      stub(MockTcpConnection, :is_connected, fn _id -> false end)

      stub(MockTcpConnection, :disconnect, fn _id ->
        send(self(), :socket_disconnected)
        :ok
      end)

      # inactivity_timeout (100 ms) + call_timeout (200 ms) + buffer
      Process.sleep(500)

      assert get_state_data(ctx.node_id).state == :disconnected
    end

    test "stale socket messages queued during ping receive do not cause a second reconnect cycle",
         ctx do
      drive_to_initialized(ctx)

      describe_count = :counters.new(1, [])

      stub(MockTcpConnection, :send_message, fn _id, charlist ->
        case to_string(charlist) do
          "*IDN?\n" ->
            send(
              self(),
              {:identification,
               %SECoP_IDN{
                 manufacturer: "ISSE",
                 product: "SECoP",
                 draft_date: "",
                 version: "V2024-09-19"
               }}
            )

          <<"ping ", rest::binary>> ->
            if :counters.get(describe_count, 1) == 0 do
              # First inactivity ping: simulate drop+reconnect while the
              # receive is blocking. No pong — let it time out.
              send(self(), :socket_disconnected)
              send(self(), :socket_connected)
            else
              # After the first recovery, respond normally so no further
              # timeout-driven recoveries happen within the test window.
              id = String.trim(rest)
              send(self(), {:pong, id, %{}})
            end

          "describe .\n" ->
            :counters.add(describe_count, 1, 1)
            send(self(), {:describe, ".", minimal_description(), %{}})

          "activate\n" ->
            send(self(), {:active})

          _ ->
            :ok
        end

        :ok
      end)

      # TcpConnection already reconnected when the statem polls after the ping timeout.
      stub(MockTcpConnection, :is_connected, fn _id -> true end)

      # inactivity_timeout (100 ms) + call_timeout (200 ms) + recovery buffer
      Process.sleep(450)

      assert get_state_data(ctx.node_id).state == :initialized
      # Exactly one handshake cycle: the :socket_disconnected queued during the
      # blocked receive drives recovery once. If the statem had manually
      # transitioned to :disconnected on timeout, the stale :socket_disconnected
      # would have fired a second time and describe would have been called twice.
      assert :counters.get(describe_count, 1) == 1
    end

    test "skips ping and updates timestamp when fresh NodeTable updates exist", ctx do
      drive_to_initialized(ctx)

      # Simulate incoming data by updating the timestamp in NodeTable.
      NodeTable.insert(ctx.node_id, :last_update_timestamp, System.monotonic_time())

      # Wait for one inactivity cycle.
      Process.sleep(300)

      state = get_state_data(ctx.node_id)
      assert state.state == :initialized
      # The inactivity check updated last_seen_update_ts to reflect the new value.
      assert state.last_seen_update_ts != nil
    end
  end

  # ---------------------------------------------------------------------------
  # Network recovery
  # ---------------------------------------------------------------------------

  describe "network recovery" do
    test "transitions from :initialized to :disconnected on socket_disconnected", ctx do
      drive_to_initialized(ctx)
      # Prevent immediate reconnect via internal :connect attempt.
      stub(MockTcpConnection, :is_connected, fn _id -> false end)

      inject(ctx.node_id, :socket_disconnected)

      assert wait_for_state(ctx.node_id, :disconnected, 500) == :ok
    end

    test "fully re-initializes after socket_disconnected + socket_connected", ctx do
      drive_to_initialized(ctx)
      stub(MockTcpConnection, :is_connected, fn _id -> false end)

      inject(ctx.node_id, :socket_disconnected)
      assert wait_for_state(ctx.node_id, :disconnected, 500) == :ok

      inject(ctx.node_id, :socket_connected)

      assert wait_for_state(ctx.node_id, :initialized, 1_000) == :ok
    end

    test "retries connection automatically after reconnect_timeout", ctx do
      # Start disconnected (default stub).
      start_statem(ctx)
      assert wait_for_state(ctx.node_id, :disconnected, 500) == :ok

      # Make TCP available and handle the handshake.
      stub(MockTcpConnection, :is_connected, fn _id -> true end)

      stub(MockTcpConnection, :send_message, fn _id, cl ->
        auto_respond(cl)
        :ok
      end)

      # After reconnect_timeout (50 ms) the timer fires, is_connected returns
      # true, and the handshake completes.
      assert wait_for_state(ctx.node_id, :initialized, 2_000) == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # IDN handshake
  # ---------------------------------------------------------------------------

  describe "IDN handshake" do
    test "sends *IDN? as first handshake message and stores parsed struct in state", ctx do
      stub(MockTcpConnection, :is_connected, fn _id -> true end)
      test_pid = self()

      stub(MockTcpConnection, :send_message, fn _id, cl ->
        if to_string(cl) == "*IDN?\n", do: send(test_pid, :idn_sent)
        auto_respond(cl)
        :ok
      end)

      start_statem(ctx)
      assert_receive :idn_sent, 1_000
      :ok = wait_for_state(ctx.node_id, :initialized, 1_000)

      assert get_state_data(ctx.node_id).idn == %SECoP_IDN{
               manufacturer: "ISSE",
               product: "SECoP",
               draft_date: "",
               version: "V2024-09-19"
             }
    end

    test "transitions to :could_not_initialize when IDN times out", ctx do
      stub(MockTcpConnection, :is_connected, fn _id -> true end)
      # send_message responds to nothing — IDN will time out
      stub(MockTcpConnection, :send_message, fn _id, _msg -> :ok end)

      start_statem(ctx)
      # handshake_timeout in test config is 500 ms
      assert wait_for_state(ctx.node_id, :could_not_initialize, 2_000) == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Description changes (issue #13)
  # ---------------------------------------------------------------------------

  defp statem_pid(node_id) do
    [{pid, _}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    pid
  end

  defp description_with(modules) do
    %{properties: %{equipment_id: "test-device"}, modules: modules}
  end

  defp module_with(parameters) do
    %{
      properties: %{},
      parameters: Map.new(parameters, &{&1, %{value: nil}}),
      commands: %{}
    }
  end

  describe "description changes" do
    test "async describe with a new description updates state and broadcasts it", ctx do
      drive_to_initialized(ctx)
      pid = statem_pid(ctx.node_id)
      old = get_state_data(ctx.node_id)
      Phoenix.PubSub.subscribe(:secant_client_pubsub, "descriptive_data_change")

      new_description = description_with(%{T: module_with([:value])})
      inject(ctx.node_id, {:describe, ".", new_description, %{raw: true}})

      topic = "#{ctx.host}:#{ctx.port}"
      assert_receive {:description_change, ^topic, announced}, 1_000
      assert announced.description == new_description
      assert announced.raw_description == %{raw: true}
      assert announced.uuid != old.uuid

      assert statem_pid(ctx.node_id) == pid
      state = get_state_data(ctx.node_id)
      assert state.uuid == announced.uuid
      assert state.description == new_description
    end

    test "async describe with an unchanged description keeps the uuid", ctx do
      drive_to_initialized(ctx)
      pid = statem_pid(ctx.node_id)
      old = get_state_data(ctx.node_id)
      Phoenix.PubSub.subscribe(:secant_client_pubsub, "descriptive_data_change")

      inject(ctx.node_id, {:describe, ".", minimal_description(), %{}})

      refute_receive {:description_change, _, _}, 200
      assert statem_pid(ctx.node_id) == pid
      assert get_state_data(ctx.node_id).uuid == old.uuid
    end

    test "a description change drops values of removed modules and parameters", ctx do
      drive_to_initialized(ctx)

      NodeTable.insert(ctx.node_id, {:data_report, :T, :value}, [1.0, %{}])
      NodeTable.insert(ctx.node_id, {:data_report, :T, :removed}, [2.0, %{}])
      NodeTable.insert(ctx.node_id, {:data_report, :Gone, :value}, [3.0, %{}])
      NodeTable.insert(ctx.node_id, :last_update_timestamp, 123)
      old_uuid = get_state_data(ctx.node_id).uuid

      inject(ctx.node_id, {:describe, ".", description_with(%{T: module_with([:value])}), %{}})
      :ok = wait_for_uuid_change(ctx.node_id, old_uuid)

      assert NodeTable.lookup(ctx.node_id, {:data_report, :T, :value}) == {:ok, [1.0, %{}]}
      assert NodeTable.lookup(ctx.node_id, {:data_report, :T, :removed}) == {:error, :notfound}
      assert NodeTable.lookup(ctx.node_id, {:data_report, :Gone, :value}) == {:error, :notfound}
      assert NodeTable.lookup(ctx.node_id, :last_update_timestamp) == {:ok, 123}
    end
  end

  defp wait_for_uuid_change(node_id, uuid) do
    Enum.reduce_while(1..100, {:error, :timeout}, fn _, acc ->
      if get_state_data(node_id).uuid != uuid do
        {:halt, :ok}
      else
        Process.sleep(10)
        {:cont, acc}
      end
    end)
  end

  describe "unexpected messages" do
    test ":socket_disconnected while :disconnected does not crash", ctx do
      start_statem(ctx)
      :ok = wait_for_state(ctx.node_id, :disconnected, 500)
      pid = statem_pid(ctx.node_id)

      inject(ctx.node_id, :socket_disconnected)

      assert get_state_data(ctx.node_id).state == :disconnected
      assert statem_pid(ctx.node_id) == pid
    end

    test ":socket_disconnected while :could_not_initialize retries the connection", ctx do
      stub(MockTcpConnection, :is_connected, fn _id -> true end)
      start_statem(ctx)
      :ok = wait_for_state(ctx.node_id, :could_not_initialize, 2_000)
      pid = statem_pid(ctx.node_id)

      stub(MockTcpConnection, :is_connected, fn _id -> false end)
      inject(ctx.node_id, :socket_disconnected)

      assert wait_for_state(ctx.node_id, :disconnected, 500) == :ok
      assert statem_pid(ctx.node_id) == pid
    end

    test "unknown info messages are dropped without crashing", ctx do
      drive_to_initialized(ctx)
      pid = statem_pid(ctx.node_id)

      inject(ctx.node_id, {:something_unexpected, 1})

      assert get_state_data(ctx.node_id).state == :initialized
      assert statem_pid(ctx.node_id) == pid
    end
  end

  describe "review follow-ups" do
    test "a late handshake reply in :could_not_initialize retries the handshake", ctx do
      stub(MockTcpConnection, :is_connected, fn _id -> true end)
      start_statem(ctx)
      :ok = wait_for_state(ctx.node_id, :could_not_initialize, 2_000)
      pid = statem_pid(ctx.node_id)

      stub(MockTcpConnection, :send_message, fn _id, cl ->
        auto_respond(cl)
        :ok
      end)

      inject(ctx.node_id, {:describe, ".", minimal_description(), %{}})

      assert wait_for_state(ctx.node_id, :initialized, 1_000) == :ok
      assert statem_pid(ctx.node_id) == pid
    end

    test "a late reply with a data report does not crash the statem", ctx do
      drive_to_initialized(ctx)
      pid = statem_pid(ctx.node_id)

      inject(ctx.node_id, {:reply, "T", "value", [1.0, %{t: 1.0}]})
      inject(ctx.node_id, {:changed, "T", "target", [1.0, %{t: 1.0}]})
      inject(ctx.node_id, {:done, "T", "stop", [nil, %{}]})

      assert get_state_data(ctx.node_id).state == :initialized
      assert statem_pid(ctx.node_id) == pid
    end

    test "the node table exists when the statem starts while TCP is down", ctx do
      :ets.delete(:node_table_lookup, ctx.node_id)
      start_statem(ctx)
      :ok = wait_for_state(ctx.node_id, :disconnected, 500)

      assert NodeTable.insert(ctx.node_id, :probe, 1) == {:ok, :inserted}
    end
  end
end
