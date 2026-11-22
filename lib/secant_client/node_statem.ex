defmodule SecantClient.NodeStatem do
  @moduledoc """
  The state machine of one SEC node: it waits for the connection, runs the
  handshake (`*IDN?`, `describe`, `activate`) and sends requests to the node.

  Requests (`read/3`, `change/4`, `execute_command/4`, `ping/1`, `describe/1`,
  `activate/1`, `deactivate/1`) do not block the state machine. Each waits in
  the state under a key of its kind and specifier, and the node's answer goes
  to the oldest request with the same key, so an answer never goes to a
  request for another specifier.

  A request that times out keeps its place for `stale_reply_ttl`, so its late
  answer is dropped instead of answering a retry of the same request. SECoP
  replies carry no request id, though, so a lost answer and a late one look
  the same. To keep one lost answer from making every retry time out, a retry
  whose answer may have been dropped this way does not keep its place when it
  times out in turn. A node that always answers later than the timeout can
  therefore still have a late answer taken by the next retry of the same
  request.

  State changes are published on `:secant_client_pubsub`.
  """

  alias SecantClient.{IDN, NodeServices, NodeTable, TcpConnection}
  require Logger

  @behaviour :gen_statem

  @initial_state :connecting
  @tcp_module Application.compile_env(:secant_client, :tcp_connection_module, TcpConnection)
  @inactivity_timeout Application.compile_env(:secant_client, :inactivity_timeout, 15_000)
  @reconnect_timeout Application.compile_env(:secant_client, :reconnect_timeout, 5_000)
  @call_timeout Application.compile_env(:secant_client, :call_timeout, 5_000)
  @handshake_timeout Application.compile_env(:secant_client, :handshake_timeout, 15_000)
  # How long a timed-out request still takes its late answer
  @stale_reply_ttl Application.compile_env(:secant_client, :stale_reply_ttl, @call_timeout)

  # Error replies and the request they answer.
  @error_requests %{
    error_change: :change,
    error_read: :read,
    error_do: :do,
    error_ping: :ping,
    error_activate: :activate,
    error_deactivate: :deactivate,
    error_describe: :describe
  }

  # Public
  def start_link(opts) do
    :gen_statem.start_link(
      {:via, Registry, {SecantClient.Registry.NodeStatem, {opts[:host], opts[:port]}}},
      __MODULE__,
      opts,
      []
    )
  end

  def child_spec(opts) do
    %{
      id: {opts[:host], opts[:port]},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: opts[:restart] || :permanent,
      shutdown: 500
    }
  end

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  def get_state(pid) do
    :gen_statem.call(pid, :get_state)
  end

  def describe(node_id), do: call(node_id, :describe)

  def activate(node_id), do: call(node_id, :activate)

  def deactivate(node_id), do: call(node_id, :deactivate)

  def change(node_id, module, parameter, value),
    do: call(node_id, {:change, module, parameter, value})

  def read(node_id, module, parameter), do: call(node_id, {:read, module, parameter})

  def ping(node_id), do: call(node_id, :ping)

  def execute_command(node_id, module, command, value \\ "null"),
    do: call(node_id, {:do, module, command, value})

  # The statem answers a request within its request timeout. Wait a little
  # longer, so the caller gets {:error, :timeout} instead of an exit.
  @call_margin 1_000

  defp call(node_id, request) do
    case Registry.lookup(SecantClient.Registry.NodeStatem, node_id) do
      [{pid, _value}] ->
        {_key, _line, timeout} = request_spec(request)

        try do
          :gen_statem.call(pid, request, timeout + @call_margin)
        catch
          # Busy in the handshake, or stopped while the request waited
          :exit, {:timeout, _} -> {:error, :timeout}
          :exit, {_reason, _} -> {:error, :not_found}
        end

      [] ->
        {:error, :not_found}
    end
  end

  # callbacks
  @impl true
  def init(opts) do
    state = %{
      host: opts[:host],
      port: opts[:port],
      node_id: {opts[:host], opts[:port]},
      pubsub_topic: "#{opts[:host]}:#{opts[:port]}",
      equipment_id: nil,
      reconnect_backoff: opts[:reconnect_backoff] || 5000,
      description: nil,
      raw_description: nil,
      # Hash of the description the uuid stands for. See store_identity/1.
      description_hash: nil,
      active: false,
      uuid: generate_uuid(),
      error: false,
      state: :disconnected,
      manual: opts[:manual] || false,
      last_seen_update_ts: nil,
      idn: nil,
      # Requests waiting for the node's answer: key => [{timer_ref, from}],
      # oldest first. See send_request/4.
      pending: %{},
      # Refs of requests whose answer a stale entry may have taken. They do
      # not go stale when they time out, so one lost answer cannot make
      # every retry of the same request time out.
      unsure: MapSet.new()
    }

    Logger.info("Starting SEC Node State Machine for: #{opts[:host]}:#{opts[:port]}")

    # After a restart, pick up the identity the previous instance stored, so an
    # unchanged description keeps its uuid. The table is owned by
    # NodeTable.Owner and survives a crash of this process.
    state = restore_identity(state)

    # No need to manually start TCP connection - supervisor handles it
    {:ok, @initial_state, state, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def handle_event(:internal, :connect, :connecting, %{node_id: node_id, manual: manual} = state) do
    case @tcp_module.connected?(node_id) do
      true ->
        updated_state = %{state | state: :connected}
        publish_new_node(state, state.pubsub_topic)
        {:next_state, :connected, updated_state, {:next_event, :internal, :handshake}}

      false ->
        if manual do
          Logger.warning(
            "Manual connection failed!! stopping all services for: #{inspect(node_id)}"
          )

          publish_new_node(:connection_failed, state.pubsub_topic)
          NodeServices.stop(node_id)
          {:stop, :normal}
        else
          updated_state = %{state | state: :disconnected}
          publish_statechange(updated_state, state.pubsub_topic)

          {:next_state, :disconnected, updated_state,
           {{:timeout, :reconnect}, @reconnect_timeout, nil}}
        end
    end
  end

  def handle_event(:internal, :connect, :disconnected, %{node_id: node_id} = state) do
    case @tcp_module.connected?(node_id) do
      true ->
        updated_state = %{state | state: :connected}
        publish_statechange(updated_state, state.pubsub_topic)
        {:next_state, :connected, updated_state, {:next_event, :internal, :handshake}}

      false ->
        {:keep_state, state, {{:timeout, :reconnect}, @reconnect_timeout, nil}}
    end
  end

  # periodically check if socket is connected
  def handle_event({:timeout, :reconnect}, nil, :disconnected, state) do
    {:keep_state, state, {:next_event, :internal, :connect}}
  end

  # The timer can outlive :disconnected (the socket came back, or a handshake
  # ended in :could_not_initialize). Ignore it in every other state.
  def handle_event({:timeout, :reconnect}, nil, _state_name, _state) do
    {:keep_state_and_data, []}
  end

  def handle_event(:info, :socket_disconnected, :initialized, state) do
    {updated_state, actions} =
      fail_pending(%{state | state: :disconnected}, {:error, :disconnected})

    publish_statechange(updated_state, state.pubsub_topic)
    {:next_state, :disconnected, updated_state, [{:next_event, :internal, :connect} | actions]}
  end

  def handle_event(:info, :socket_disconnected, :connected, state) do
    updated_state = %{state | state: :disconnected}
    publish_statechange(updated_state, state.pubsub_topic)
    {:next_state, :disconnected, updated_state, {:next_event, :internal, :connect}}
  end

  def handle_event(:info, :socket_connected, :disconnected, state) do
    updated_state = %{state | state: :connected}
    publish_statechange(updated_state, state.pubsub_topic)
    {:next_state, :connected, updated_state, {:next_event, :internal, :handshake}}
  end

  def handle_event(:info, :socket_connected, :connected, _state) do
    {:keep_state_and_data, []}
  end

  def handle_event(:info, :socket_connected, :initialized, _state) do
    {:keep_state_and_data, []}
  end

  def handle_event({:call, from}, message, :disconnected, state) do
    reply =
      case message do
        :get_state ->
          {:ok, state}

        _ ->
          Logger.warning("Node call while disconnected: #{inspect(message)}")
          {:error, :disconnected}
      end

    {:keep_state_and_data, {:reply, from, reply}}
  end

  def handle_event({:call, from}, message, :connected, state) do
    reply =
      case message do
        :get_state ->
          {:ok, state}

        _ ->
          Logger.warning("Node call before initialization: #{inspect(message)}")
          {:error, :uninitialized}
      end

    {:keep_state_and_data, {:reply, from, reply}}
  end

  def handle_event(
        :internal,
        :handshake,
        :connected,
        %{node_id: node_id} = state
      ) do
    case send_idn_message(node_id) do
      {:ok, idn} ->
        Logger.info("IDN handshake successful: #{idn.version}")
        state = %{state | idn: idn}

        Logger.info("Initial Describe Message sent")

        case send_describe_message(node_id) do
          {:ok, _specifier, parsed_description, raw_description} ->
            case same_description?(state, parsed_description) do
              # Nothing changed, probably just a prior network disconnect
              true ->
                Logger.info("Descriptive data constant --> connected & initialized")

                updated_state =
                  keep_description(
                    %{state | state: :initialized},
                    parsed_description,
                    raw_description
                  )

                publish_statechange(updated_state, state.pubsub_topic)

                {:next_state, :initialized, updated_state,
                 [
                   {:next_event, :internal, :activate},
                   {{:timeout, :inactivity_check}, @inactivity_timeout, nil}
                 ]}

              # Description changed update uuid, equipment_id and description
              false ->
                Logger.info(
                  "Descriptive data changed issuing new uuid --> connected & initialized"
                )

                updated_state_descr =
                  apply_description_change(
                    %{state | state: :initialized},
                    parsed_description,
                    raw_description
                  )

                publish_statechange(updated_state_descr, state.pubsub_topic)

                {:next_state, :initialized, updated_state_descr,
                 [
                   {:next_event, :internal, :activate},
                   {{:timeout, :inactivity_check}, @inactivity_timeout, nil}
                 ]}
            end

          {:error, _} ->
            Logger.warning(
              "NO answer on describe message for #{elem(node_id, 0)}:#{elem(node_id, 1)}, going into ERROR state"
            )

            updated_state = %{state | state: :could_not_initialize}
            publish_statechange(updated_state, state.pubsub_topic)
            {:next_state, :could_not_initialize, updated_state}
        end

      {:error, :timeout} ->
        Logger.warning(
          "IDN timed out for #{elem(node_id, 0)}:#{elem(node_id, 1)}, going into ERROR state"
        )

        updated_state = %{state | state: :could_not_initialize}
        publish_statechange(updated_state, state.pubsub_topic)
        {:next_state, :could_not_initialize, updated_state}
    end
  end

  def handle_event(:internal, :activate, :initialized, state) do
    case send_request(state, :activate, :internal) do
      {:ok, state, action} -> {:keep_state, state, action}
      {:error, _reason} -> :keep_state_and_data
    end
  end

  def handle_event({:call, from}, :get_state, state_name, state)
      when state_name in [
             :initialized,
             :connected,
             :disconnected,
             :connecting,
             :could_not_initialize
           ] do
    {:keep_state_and_data, {:reply, from, {:ok, state}}}
  end

  # Requests to the node. The answer arrives later as an info message and is
  # matched to the request by its key (see handle_reply/3), so the statem
  # keeps handling other events while it waits.
  def handle_event({:call, from}, request, :initialized, state) when request != :get_state do
    Logger.info("#{inspect(request)} sent to #{state.pubsub_topic}")

    case send_request(state, request, from) do
      {:ok, state, action} -> {:keep_state, state, action}
      {:error, reason} -> {:keep_state_and_data, {:reply, from, {:error, reason}}}
    end
  end

  def handle_event({:call, from}, request, :could_not_initialize, _state)
      when request != :get_state do
    Logger.warning("Node call before initialization: #{inspect(request)}")
    {:keep_state_and_data, {:reply, from, {:error, :uninitialized}}}
  end

  # No answer within the request timeout. The entry stays in its place in the
  # queue as :stale for @stale_reply_ttl, so a late answer is taken by it and
  # cannot answer a retry of the same request. A request whose answer a stale
  # entry may already have taken is removed instead, which ends the chain.
  def handle_event({:timeout, {:request, ref}}, key, _state_name, state) do
    case List.keyfind(Map.get(state.pending, key, []), ref, 0) do
      nil ->
        :keep_state_and_data

      {^ref, :stale} ->
        {_from, state} = pop_pending(state, key, ref)
        {:keep_state, state}

      {^ref, from} ->
        if MapSet.member?(state.unsure, ref) do
          {from, state} = pop_pending(state, key, ref)
          {:keep_state, state, timeout_actions(from, key, state)}
        else
          mark_stale(state, key, ref, from)
        end
    end
  end

  def handle_event(
        {:timeout, :inactivity_check},
        nil,
        :initialized,
        %{node_id: node_id, last_seen_update_ts: last_seen} = state
      ) do
    current_ts =
      case NodeTable.lookup(node_id, :last_update_timestamp) do
        {:ok, ts} -> ts
        {:error, _} -> nil
      end

    if current_ts == last_seen do
      Logger.debug("Inactivity check: sending ping to #{inspect(node_id)}")

      case send_request(state, :ping, :inactivity) do
        {:ok, state, action} -> {:keep_state, state, action}
        {:error, _reason} -> {:keep_state_and_data, inactivity_timer()}
      end
    else
      {:keep_state, %{state | last_seen_update_ts: current_ts}, inactivity_timer()}
    end
  end

  def handle_event({:timeout, :inactivity_check}, nil, _state_name, _state) do
    {:keep_state_and_data, []}
  end

  def handle_event(:info, {:active}, :initialized, state) do
    state |> set_active(true) |> handle_reply(:activate, {:active})
  end

  def handle_event(:info, {:inactive}, :initialized, state) do
    state |> set_active(false) |> handle_reply(:deactivate, {:inactive})
  end

  def handle_event(:info, {:pong, id, data}, :initialized, state) do
    handle_reply(state, {:ping, id}, {:pong, id, data})
  end

  def handle_event(
        :info,
        {:describe, _specifier, parsed_description, raw_description},
        :initialized,
        state
      ) do
    state =
      case same_description?(state, parsed_description) do
        # Nothing changed, probably just a network disconnect
        true ->
          Logger.info("Descriptive data constant")
          state

        # Description changed update uuid, equipment_id and description
        false ->
          Logger.info("Descriptive data changed issuing new uuid")
          apply_description_change(state, parsed_description, raw_description)
      end

    handle_reply(state, :describe, {:describing, parsed_description})
  end

  def handle_event(:info, {:done, module, command, data_report}, :initialized, state) do
    handle_reply(state, {:do, module, command}, {:done, module, command, data_report})
  end

  def handle_event(:info, {:reply, module, parameter, data_report}, :initialized, state) do
    handle_reply(state, {:read, module, parameter}, {:reply, module, parameter, data_report})
  end

  def handle_event(:info, {:changed, module, parameter, data_report}, :initialized, state) do
    handle_reply(
      state,
      {:change, module, parameter},
      {:changed, module, parameter, data_report}
    )
  end

  def handle_event(
        :info,
        {error_code, specifier, error_class, error_text, error_dict},
        :initialized,
        state
      )
      when is_map_key(@error_requests, error_code) do
    request = Map.fetch!(@error_requests, error_code)
    Logger.error("Error on #{request} request #{specifier}: #{inspect(error_text)}")

    handle_reply(
      state,
      error_key(request, specifier),
      {:error, request, specifier, error_class, error_text, error_dict}
    )
  end

  # The socket can also drop before the handshake has finished. Retry the
  # connection instead of staying in :could_not_initialize.
  def handle_event(:info, :socket_disconnected, :could_not_initialize, state) do
    updated_state = %{state | state: :disconnected}
    publish_statechange(updated_state, state.pubsub_topic)
    {:next_state, :disconnected, updated_state, {:next_event, :internal, :connect}}
  end

  # A handshake reply that arrives after the handshake timed out, or a fresh
  # connection, means the node may answer now: run the handshake again.
  def handle_event(:info, message, :could_not_initialize, state)
      when message == :socket_connected or
             (is_tuple(message) and elem(message, 0) in [:identification, :describe]) do
    Logger.info(
      "Retrying handshake for #{state.pubsub_topic} after #{inspect(message, limit: 3)}"
    )

    updated_state = %{state | state: :disconnected}
    publish_statechange(updated_state, state.pubsub_topic)
    {:next_state, :disconnected, updated_state, {:next_event, :internal, :connect}}
  end

  # Already disconnected (or about to check the connection): nothing to do.
  def handle_event(:info, :socket_disconnected, state_name, _state)
      when state_name in [:connecting, :disconnected] do
    :keep_state_and_data
  end

  # Log and drop any other message instead of crashing the state machine
  # (and getting a new uuid on restart).
  def handle_event(:info, message, state_name, _state) do
    Logger.warning("Unexpected message in state #{state_name}: #{inspect(message, limit: 5)}")
    :keep_state_and_data
  end

  # Store a changed description under a new uuid, drop values of modules and
  # parameters that are no longer described, and announce the change.
  defp apply_description_change(state, parsed_description, raw_description) do
    updated_state = %{
      state
      | description: parsed_description,
        raw_description: raw_description,
        description_hash: description_hash(parsed_description),
        uuid: generate_uuid(),
        equipment_id: parsed_description[:properties][:equipment_id]
    }

    NodeTable.prune(state.node_id, parsed_description)
    store_identity(updated_state)
    publish_descriptive_data_change(updated_state, state.pubsub_topic)
    updated_state
  end

  # Whether `description` is the one the uuid stands for. After a restart the
  # state may have only the hash of that description.
  defp same_description?(%{description: nil, description_hash: hash}, description)
       when hash != nil,
       do: hash == description_hash(description)

  defp same_description?(%{description: current}, description), do: current === description

  defp keep_description(state, parsed_description, raw_description),
    do: %{state | description: parsed_description, raw_description: raw_description}

  defp description_hash(description),
    do: :crypto.hash(:sha256, :erlang.term_to_binary(description, [:deterministic]))

  @identity_fields [:uuid, :equipment_id, :description_hash]

  # The description itself is not stored a second time: the node's table has
  # it already (Parser stores it), and only its hash is needed to tell
  # whether the next description is still the one this uuid stands for.
  defp store_identity(state) do
    NodeTable.insert(state.node_id, :statem_identity, Map.take(state, @identity_fields))
  rescue
    error -> Logger.warning("Could not store node identity: #{inspect(error)}")
  end

  defp restore_identity(state) do
    case NodeTable.lookup(state.node_id, :statem_identity) do
      {:ok, identity} ->
        Logger.info("Restored identity #{identity.uuid} for #{state.pubsub_topic}")
        state |> Map.merge(identity) |> restore_description()

      {:error, _} ->
        state
    end
  end

  # Parser may have stored a description this process has not seen yet, so
  # the stored one is taken only when it is the one the uuid stands for.
  # Otherwise the description stays empty until the handshake. Parser writes
  # :description before :raw_description, so they are read in the opposite
  # order: a raw description newer than the hashed one cannot be taken.
  defp restore_description(state) do
    with {:ok, raw_description} <- NodeTable.lookup(state.node_id, :raw_description),
         {:ok, description} <- NodeTable.lookup(state.node_id, :description),
         true <- description_hash(description) == state.description_hash do
      %{state | description: description, raw_description: raw_description}
    else
      _ -> state
    end
  end

  defp send_idn_message(node_id) do
    @tcp_module.send_message(node_id, ~c"*IDN?\n")

    receive do
      {:identification, %IDN{} = idn} -> {:ok, idn}
    after
      @handshake_timeout -> {:error, :timeout}
    end
  end

  defp send_describe_message(node_id) do
    @tcp_module.send_message(node_id, ~c"describe .\n")

    receive do
      {:describe, specifier, parsed_description, raw_description} ->
        Logger.info("Description received")
        {:ok, specifier, parsed_description, raw_description}

      {:error_describe, specifier, error_class, error_text, error_dict} ->
        Logger.error("Error on describe request: #{inspect(error_text)}")
        {:error, {specifier, error_class, error_text, error_dict}}
    after
      @handshake_timeout -> {:error, :timeout}
    end
  end

  # -- Requests to the node ---------------------------------------------------

  # {key, line, timeout} of a request. A reply is matched to the oldest
  # pending request with the same key, so a late reply cannot answer a
  # request for another specifier.
  defp request_spec({:change, module, parameter, value}),
    do:
      {{:change, to_string(module), to_string(parameter)},
       "change #{module}:#{parameter} #{value}\n", @call_timeout}

  defp request_spec({:read, module, parameter}),
    do:
      {{:read, to_string(module), to_string(parameter)}, "read #{module}:#{parameter}\n",
       @call_timeout}

  defp request_spec({:do, module, command, value}),
    do:
      {{:do, to_string(module), to_string(command)}, "do #{module}:#{command} #{value}\n",
       @call_timeout}

  defp request_spec(:ping) do
    id = Integer.to_string(:rand.uniform(100_000))
    {{:ping, id}, "ping #{id}\n", @call_timeout}
  end

  defp request_spec(:activate), do: {:activate, "activate\n", @handshake_timeout}
  defp request_spec(:deactivate), do: {:deactivate, "deactivate\n", @call_timeout}
  defp request_spec(:describe), do: {:describe, "describe .\n", @handshake_timeout}

  defp error_key(request, specifier) when request in [:change, :read, :do] do
    case String.split(specifier, ":", parts: 2) do
      [module, accessible] -> {request, module, accessible}
      _ -> {request, specifier}
    end
  end

  defp error_key(:ping, id), do: {:ping, id}
  defp error_key(request, _specifier), do: request

  # Sends a request and records `from` until the matching reply or the
  # request timeout. `from` is a gen_statem caller, :internal (activate after
  # the handshake) or :inactivity (the inactivity ping).
  defp send_request(state, request, from) do
    {key, line, timeout} = request_spec(request)

    case @tcp_module.send_message(state.node_id, to_charlist(line)) do
      :ok ->
        ref = make_ref()
        entry = {ref, from}
        pending = Map.update(state.pending, key, [entry], &(&1 ++ [entry]))
        {:ok, %{state | pending: pending}, {{:timeout, {:request, ref}}, timeout, key}}

      {:error, reason} ->
        Logger.warning("Could not send #{inspect(request)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Answers the oldest pending request with `key`. A reply without one is
  # late (its request timed out) or unsolicited and is dropped.
  defp handle_reply(state, key, reply) do
    case Map.get(state.pending, key, []) do
      [] ->
        Logger.debug("Dropped reply without a pending request: #{inspect(reply, limit: 5)}")
        {:keep_state, state}

      # The answer may be the late one of the stale request, or the answer to
      # a newer request: drop it, and keep the newer ones from going stale.
      [{ref, :stale} | rest] ->
        newer = for {newer_ref, from} <- rest, from != :stale, do: newer_ref

        state = %{
          state
          | pending: put_pending(state.pending, key, rest),
            unsure: MapSet.union(state.unsure, MapSet.new(newer))
        }

        {:keep_state, state,
         [{{:timeout, {:request, ref}}, :cancel} | reply_actions(:stale, reply)]}

      [{ref, from} | rest] ->
        pending = put_pending(state.pending, key, rest)
        state = %{state | pending: pending, unsure: MapSet.delete(state.unsure, ref)}

        {:keep_state, state,
         [{{:timeout, {:request, ref}}, :cancel} | reply_actions(from, reply)]}
    end
  end

  defp reply_actions(:stale, reply) do
    Logger.debug("Dropped late reply to a timed-out request: #{inspect(reply, limit: 5)}")
    []
  end

  defp reply_actions(:internal, _reply), do: []
  defp reply_actions(:inactivity, _reply), do: [inactivity_timer()]
  defp reply_actions(from, reply), do: [{:reply, from, reply}]

  defp timeout_actions(:internal, key, _state) do
    Logger.warning("No answer to #{inspect(key)}")
    []
  end

  defp timeout_actions(:inactivity, _key, state) do
    Logger.warning(
      "Inactivity check timed out for #{inspect(state.node_id)}, forcing TCP disconnect"
    )

    @tcp_module.disconnect(state.node_id)
    [inactivity_timer()]
  end

  defp timeout_actions(from, _key, _state), do: [{:reply, from, {:error, :timeout}}]

  # Keeps the timed-out entry in its place as :stale for @stale_reply_ttl.
  defp mark_stale(state, key, ref, from) do
    pending = Map.update!(state.pending, key, &List.keyreplace(&1, ref, 0, {ref, :stale}))
    state = %{state | pending: pending}

    {:keep_state, state,
     [{{:timeout, {:request, ref}}, @stale_reply_ttl, key} | timeout_actions(from, key, state)]}
  end

  # Removes the entry `ref` and returns who waited for it.
  defp pop_pending(state, key, ref) do
    entries = Map.get(state.pending, key, [])

    case List.keytake(entries, ref, 0) do
      {{^ref, from}, rest} ->
        pending = put_pending(state.pending, key, rest)
        {from, %{state | pending: pending, unsure: MapSet.delete(state.unsure, ref)}}

      nil ->
        {nil, state}
    end
  end

  defp put_pending(pending, key, []), do: Map.delete(pending, key)
  defp put_pending(pending, key, entries), do: Map.put(pending, key, entries)

  # Answers every pending request with `reply` and cancels their timers.
  defp fail_pending(state, reply) do
    actions =
      for {_key, entries} <- state.pending,
          {ref, from} <- entries,
          action <- [{{:timeout, {:request, ref}}, :cancel} | failure_actions(from, reply)],
          do: action

    {%{state | pending: %{}, unsure: MapSet.new()}, actions}
  end

  defp failure_actions(from, _reply) when from in [:internal, :inactivity, :stale], do: []
  defp failure_actions(from, reply), do: [{:reply, from, reply}]

  defp inactivity_timer, do: {{:timeout, :inactivity_check}, @inactivity_timeout, nil}

  defp set_active(%{active: active} = state, active), do: state

  defp set_active(state, active) do
    publish_secop_conn_state(active, state.pubsub_topic)
    %{state | active: active}
  end

  # A random (version 4) UUID.
  defp generate_uuid do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)
    hex = Base.encode16(<<a::48, 4::4, b::12, 2::2, c::62>>, case: :lower)
    <<p1::binary-8, p2::binary-4, p3::binary-4, p4::binary-4, p5::binary-12>> = hex
    Enum.join([p1, p2, p3, p4, p5], "-")
  end

  defp publish_descriptive_data_change(state, pubsub_topic) do
    Logger.debug("publish descriptive data change for #{pubsub_topic}")

    Phoenix.PubSub.broadcast(
      :secant_client_pubsub,
      "descriptive_data_change",
      {:description_change, pubsub_topic, state}
    )
  end

  defp publish_secop_conn_state(active, pubsub_topic) do
    Logger.debug("publish conn state change for #{pubsub_topic} connection is active: #{active}")

    Phoenix.PubSub.broadcast(
      :secant_client_pubsub,
      "secop_conn_state",
      {:conn_state, pubsub_topic, active}
    )
  end

  defp publish_statechange(state, pubsub_topic) do
    Logger.debug("publish statechange for #{pubsub_topic} --> #{state.state}")

    Phoenix.PubSub.broadcast(
      :secant_client_pubsub,
      "state_change",
      {:state_change, pubsub_topic, state}
    )
  end

  defp publish_new_node(state, pubsub_topic) do
    Logger.debug("publish new node added at: #{pubsub_topic}")

    Phoenix.PubSub.broadcast(
      :secant_client_pubsub,
      "new_node",
      {:new_node, pubsub_topic, state}
    )
  end
end
