defmodule SecantClient.TcpConnectionTest do
  alias SecantClient.{NodeTable, Parser, TcpConnection}
  use ExUnit.Case, async: false

  setup do
    port = rem(:erlang.unique_integer([:positive, :monotonic]), 50_000) + 10_000
    node_id = {~c"localhost", port}
    {:ok, _table} = NodeTable.start(node_id)
    # Atoms for the module and parameter, as the description would create them
    _ = [:tc_mod, :value]
    Phoenix.PubSub.subscribe(:secant_client_pubsub, "value_update:localhost:#{port}")
    {:ok, node_id: node_id}
  end

  defp update(value), do: ~s(update tc_mod:value [#{value},{"t":1.0}]\n)

  describe "handle_data/3 (issue #18)" do
    test "parses every complete line in order and keeps the rest", %{node_id: node_id} do
      data = update(1) <> update(2) <> update(3) <> "update tc_mod:va"

      assert TcpConnection.handle_data("", data, node_id) == "update tc_mod:va"

      for value <- [1, 2, 3] do
        assert_received {:value_update, "tc_mod", "value", [^value, %{t: 1.0}]}
      end

      assert NodeTable.lookup(node_id, {:data_report, :tc_mod, :value}) ==
               {:ok, [3, %{t: 1.0}]}
    end

    test "joins a line split over several packets", %{node_id: node_id} do
      rest = TcpConnection.handle_data("", "update tc_mod:value [4,", node_id)
      rest = TcpConnection.handle_data(rest, ~s({"t":2.0}), node_id)
      refute_received {:value_update, _, _, _}

      assert TcpConnection.handle_data(rest, "]\r\n", node_id) == ""
      assert_received {:value_update, "tc_mod", "value", [4, %{t: 2.0}]}
    end

    test "a malformed line does not stop the lines after it", %{node_id: node_id} do
      data = "update tc_mod:value [not json\n" <> update(5)

      assert TcpConnection.handle_data("", data, node_id) == ""
      assert_received {:value_update, "tc_mod", "value", [5, %{t: 1.0}]}
    end
  end

  describe "Parser data reports (issue #18)" do
    test "unknown keys in a data report stay strings and create no atoms", %{node_id: node_id} do
      key = "never_an_atom_#{System.unique_integer([:positive])}"

      Parser.parse(node_id, ~s(update tc_mod:value [{"#{key}":1},{"t":1.0}]))

      assert_received {:value_update, "tc_mod", "value", [%{^key => 1}, %{t: 1.0}]}
      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    end

    test "an update for an undescribed accessible is dropped", %{node_id: node_id} do
      name = "undescribed_#{System.unique_integer([:positive])}"

      Parser.parse(node_id, ~s(update #{name}:value [1,{}]))

      refute_received {:value_update, _, _, _}
    end

    test "an update stores the report and the timestamp", %{node_id: node_id} do
      Parser.parse(node_id, String.trim(update(6)))

      assert {:ok, [6, _]} = NodeTable.lookup(node_id, {:data_report, :tc_mod, :value})
      assert {:ok, ts} = NodeTable.lookup(node_id, :last_update_timestamp)
      assert is_integer(ts)
    end
  end
end

defmodule SecantClient.TcpConnectionSocketTest do
  alias SecantClient.TcpConnection
  use ExUnit.Case, async: false

  # A real listener standing in for the node.
  setup do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    node_id = {~c"localhost", port}
    {:ok, _table} = SecantClient.NodeTable.start(node_id)
    _ = [:tc_mod, :value]
    Phoenix.PubSub.subscribe(:secant_client_pubsub, "value_update:localhost:#{port}")

    pid =
      start_supervised!({TcpConnection, [host: ~c"localhost", port: port, reconnect_backoff: 20]})

    {:ok, server} = :gen_tcp.accept(listen, 1_000)
    on_exit(fn -> :gen_tcp.close(listen) end)
    {:ok, listen: listen, server: server, node_id: node_id, pid: pid}
  end

  test "lines sent over the socket are parsed in order", ctx do
    lines = for value <- 1..300, into: "", do: ~s(update tc_mod:value [#{value},{}]\n)
    :ok = :gen_tcp.send(ctx.server, lines)

    # More messages than {:active, N} allows, so the socket is re-armed.
    for value <- 1..300 do
      assert_receive {:value_update, "tc_mod", "value", [^value, %{}]}, 1_000
    end
  end

  test "reconnects and drops the partial line of the old socket", ctx do
    :ok = :gen_tcp.send(ctx.server, "update tc_mod:value [1")
    wait_until(fn -> TcpConnection.connected?(ctx.node_id) end)
    :gen_tcp.close(ctx.server)

    {:ok, server} = :gen_tcp.accept(ctx.listen, 1_000)
    :ok = :gen_tcp.send(server, ~s(update tc_mod:value [2,{}]\n))

    assert_receive {:value_update, "tc_mod", "value", [2, %{}]}, 1_000
    refute_received {:value_update, _, _, _}
  end

  test "messages of another socket are ignored", ctx do
    {:ok, other} = :gen_tcp.listen(0, [])
    send(ctx.pid, {:tcp, other, ~s(update tc_mod:value [9,{}]\n)})
    send(ctx.pid, {:tcp_closed, other})
    :gen_tcp.close(other)

    :ok = :gen_tcp.send(ctx.server, ~s(update tc_mod:value [3,{}]\n))
    assert_receive {:value_update, "tc_mod", "value", [3, %{}]}, 1_000
    refute_received {:value_update, "tc_mod", "value", [9, _]}
    assert TcpConnection.connected?(ctx.node_id)
  end

  test "helpers return errors for an unknown node" do
    node_id = {~c"nowhere", 1}
    refute TcpConnection.connected?(node_id)
    assert TcpConnection.send_message(node_id, "ping\n") == {:error, :not_found}
    assert TcpConnection.disconnect(node_id) == {:error, :not_found}
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition not met")

      true ->
        Process.sleep(5)
        wait_until(fun, attempts - 1)
    end
  end
end
