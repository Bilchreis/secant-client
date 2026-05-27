defmodule SEC_Node_Statem do
  require Logger
  alias NodeTable
  alias Ecto
  alias TcpConnection

  @behaviour :gen_statem

  @initial_state :connecting
  @tcp_module Application.compile_env(:secant_client, :tcp_connection_module, TcpConnection)
  @inactivity_timeout Application.compile_env(:secant_client, :inactivity_timeout, 15_000)
  @reconnect_timeout Application.compile_env(:secant_client, :reconnect_timeout, 5_000)
  @call_timeout Application.compile_env(:secant_client, :call_timeout, 5_000)
  @handshake_timeout Application.compile_env(:secant_client, :handshake_timeout, 15_000)

  # Public
  def start_link(opts) do
    :gen_statem.start_link(
      {:via, Registry, {Registry.SEC_Node_Statem, {opts[:host], opts[:port]}}},
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

  def describe(node_id) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, :describe)
  end

  def activate(node_id) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, :activate)
  end

  def deactivate(node_id) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, :deactivate)
  end

  def change(node_id, module, parameter, value) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, {:change, module, parameter, value})
  end

  def read(node_id, module, parameter) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, {:read, module, parameter})
  end

  def ping(node_id) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, :ping)
  end

  def execute_command(node_id, module, command, value) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, {:do, module, command, value})
  end

  def execute_command(node_id, module, command) do
    [{sec_node_pid, _value}] = Registry.lookup(Registry.SEC_Node_Statem, node_id)
    :gen_statem.call(sec_node_pid, {:do, module, command, "null"})
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
      active: false,
      uuid: Ecto.UUID.generate(),
      error: false,
      state: :disconnected,
      manual: opts[:manual] || false,
      last_seen_update_ts: nil,
      idn: nil
    }

    Logger.info("Starting SEC Node State Machine for: #{opts[:host]}:#{opts[:port]}")

    # No need to manually start TCP connection - supervisor handles it
    {:ok, @initial_state, state, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def handle_event(:internal, :connect, :connecting, %{node_id: node_id, manual: manual} = state) do
    case @tcp_module.is_connected(node_id) do
      true ->
        updated_state = %{state | state: :connected}
        NodeTable.start(state.node_id)
        publish_new_node(state, state.pubsub_topic)
        {:next_state, :connected, updated_state, {:next_event, :internal, :handshake}}

      false ->
        if manual do
          Logger.warning(
            "Manual connection failed!! stopping all services for: #{inspect(node_id)}"
          )

          publish_new_node(:connection_failed, state.pubsub_topic)
          SEC_Node_Services.stop(node_id)
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
    case @tcp_module.is_connected(node_id) do
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

  # Do noting if Connection was already established again
  def handle_event({:timeout, :reconnect}, nil, :initialized, _state) do
    {:keep_state_and_data, []}
  end

  # Do noting if Connection was already established again
  def handle_event({:timeout, :reconnect}, nil, :connected, _state) do
    {:keep_state_and_data, []}
  end

  def handle_event(:info, :socket_disconnected, :initialized, state) do
    updated_state = %{state | state: :disconnected}
    publish_statechange(updated_state, state.pubsub_topic)
    {:next_state, :disconnected, updated_state, {:next_event, :internal, :connect}}
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
        %{node_id: node_id, description: description} = state
      ) do
    case send_idn_message(node_id) do
      {:ok, idn} ->
        Logger.info("IDN handshake successful: #{idn.version}")
        state = %{state | idn: idn}

        Logger.info("Initial Describe Message sent")

        case send_describe_message(node_id) do
          {:ok, _specifier, parsed_description, raw_description} ->
            equipment_id = parsed_description[:properties][:equipment_id]

            case MapDiff.diff(parsed_description, description) do
              # Nothing changed, probably just a prior network disconnect
              %{changed: :equal, value: _} ->
                Logger.info("Descriptive data constant --> connected & initialized")
                updated_state = %{state | state: :initialized}
                publish_statechange(updated_state, state.pubsub_topic)

                {:next_state, :initialized, updated_state,
                 [
                   {:next_event, :internal, :activate},
                   {{:timeout, :inactivity_check}, @inactivity_timeout, nil}
                 ]}

              # Description changed update uuid, equipment_id and description
              _ ->
                Logger.info(
                  "Descriptive data changed issuing new uuid --> connected & initialized"
                )

                updated_state_descr = %{
                  state
                  | description: parsed_description,
                    raw_description: raw_description,
                    uuid: Ecto.UUID.generate(),
                    equipment_id: equipment_id,
                    state: :initialized
                }

                updated_state_descr = %{updated_state_descr | state: :initialized}
                publish_descriptive_data_change(updated_state_descr, state.pubsub_topic)
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

  def handle_event(:internal, :activate, :initialized, %{node_id: node_id} = state) do
    case send_activate_message(node_id) do
      {:ok, :active} ->
        publish_secop_conn_state(true, state.pubsub_topic)
        updated_state = %{state | active: true}
        {:keep_state, updated_state}

      {:error, _} ->
        {:keep_state_and_data, []}
    end
  end

  def handle_event({:call, from}, :get_state, state_name, state)
      when state_name in [:initialized, :connected, :disconnected, :connecting, :could_not_initialize] do
    {:keep_state_and_data, {:reply, from, {:ok, state}}}
  end

  def handle_event(
        {:call, from},
        :describe,
        :initialized,
        %{node_id: node_id, description: description} = state
      ) do
    Logger.info("Describe Message sent")

    case send_describe_message(node_id) do
      {:ok, _specifier, parsed_description, raw_description} ->
        equipment_id = parsed_description[:properties][:equipment_id]

        case MapDiff.diff(parsed_description, description) do
          # Notihng changed, probably just a network disconnect
          %{changed: :equal, value: _} ->
            Logger.info("Descriptive data constant")
            {:keep_state_and_data, {:reply, from, {:describing, parsed_description}}}

          # Description changed update uuid, equipment_id and description
          _ ->
            Logger.info("Descriptive data changed issuing new uuid")

            updated_state_descr = %{
              state
              | description: parsed_description,
                raw_description: raw_description,
                uuid: Ecto.UUID.generate(),
                equipment_id: equipment_id
            }

            publish_descriptive_data_change(updated_state_descr, state.pubsub_topic)
            {:keep_state, updated_state_descr, {:reply, from, {:describing, parsed_description}}}
        end

      {:error, :timeout} ->
        Logger.warning(
          "NO answer on describe message for #{elem(node_id, 0)}:#{elem(node_id, 1)}, going into ERROR state"
        )

        {:keep_state_and_data, {:reply, from, {:error, :timeout}}}

      {:error, {specifier, error_class, error_text, error_dict}} ->
        Logger.error("Error on describe request: #{error_text}")

        {:keep_state_and_data,
         {:reply, from, {:error, :describe, specifier, error_class, error_text, error_dict}}}
    end
  end

  def handle_event({:call, from}, :activate, :initialized, %{node_id: node_id} = state) do
    Logger.info("Activate Message sent")

    case send_activate_message(node_id) do
      {:ok, :active} ->
        updated_state = %{state | active: true}
        publish_secop_conn_state(true, state.pubsub_topic)
        {:keep_state, updated_state, {:reply, from, {:active}}}

      {:error, :timeout} ->
        {:keep_state_and_data, {:reply, from, {:error, :timeout}}}

      {:error, {specifier, error_class, error_text, error_dict}} ->
        Logger.error("Error on activate request: #{error_text}")

        {:keep_state_and_data,
         {:reply, from, {:error, :activate, specifier, error_class, error_text, error_dict}}}
    end
  end

  def handle_event({:call, from}, :deactivate, :initialized, %{node_id: node_id} = state) do
    Logger.info("Deactivate Message sent")
    @tcp_module.send_message(node_id, ~c"deactivate\n")

    receive do
      {:inactive} ->
        Logger.info("Node Inactive")
        updated_state = %{state | active: false}
        publish_secop_conn_state(false, state.pubsub_topic)
        {:keep_state, updated_state, {:reply, from, {:inactive}}}

      {:error_deactivate, specifier, error_class, error_text, error_dict} ->
        Logger.error("Error on deactivate request: #{error_text}")

        {:keep_state_and_data,
         {:reply, from, {:error, :deactivate, specifier, error_class, error_text, error_dict}}}
    after
      @call_timeout -> {:keep_state_and_data, {:reply, from, {:error, :timeout}}}
    end
  end

  def handle_event(
        {:call, from},
        {:change, module, parameter, value},
        :initialized,
        %{node_id: node_id} = _state
      ) do
    message = "change #{module}:#{parameter} #{value}\n"

    Logger.info("Change Message '#{String.trim_trailing(message)}' sent")

    @tcp_module.send_message(node_id, String.to_charlist(message))

    receive do
      {:changed, r_module, r_parameter, data_report} ->
        Logger.info("Value of #{r_module}:#{r_module} changed to #{Jason.encode!(data_report)}")
        {:keep_state_and_data, {:reply, from, {:changed, r_module, r_parameter, data_report}}}

      {:error_change, specifier, error_class, error_text, error_dict} ->
        Logger.error("Error on change request: #{error_text}")

        {:keep_state_and_data,
         {:reply, from, {:error, :change, specifier, error_class, error_text, error_dict}}}
    after
      @call_timeout -> {:keep_state_and_data, {:reply, from, {:error, :timeout}}}
    end
  end

  def handle_event(
        {:call, from},
        {:do, module, command, value},
        :initialized,
        %{node_id: node_id} = _state
      ) do
    message = "do #{module}:#{command} #{value}\n"

    Logger.info("Do Message '#{String.trim_trailing(message)}' sent")
    @tcp_module.send_message(node_id, String.to_charlist(message))

    receive do
      {:done, r_module, r_command, data_report} ->
        Logger.info(
          "Command #{module}:#{command} executed and returned: #{Jason.encode!(data_report)}"
        )

        {:keep_state_and_data, {:reply, from, {:done, r_module, r_command, data_report}}}

      {:error_do, specifier, error_class, error_text, error_dict} ->
        Logger.error("Error on do request: #{error_text}")

        {:keep_state_and_data,
         {:reply, from, {:error, :do, specifier, error_class, error_text, error_dict}}}
    after
      @call_timeout -> {:keep_state_and_data, {:reply, from, {:error, :timeout}}}
    end
  end

  def handle_event(
        {:call, from},
        {:read, module, parameter},
        :initialized,
        %{node_id: node_id} = _state
      ) do
    message = "read #{module}:#{parameter}\n"

    Logger.info("Read Message '#{String.trim_trailing(message)}' sent")
    @tcp_module.send_message(node_id, String.to_charlist(message))

    receive do
      {:reply, r_module, r_parameter, data_report} ->
        Logger.info(
          "Read request #{module}:#{parameter} sent and returned: #{Jason.encode!(data_report)}"
        )

        {:keep_state_and_data, {:reply, from, {:reply, r_module, r_parameter, data_report}}}

      {:error_read, specifier, error_class, error_text, error_dict} ->
        Logger.error("Error on read request: #{error_text}")

        {:keep_state_and_data,
         {:reply, from, {:error, :read, specifier, error_class, error_text, error_dict}}}
    after
      @call_timeout -> {:keep_state_and_data, {:reply, from, {:error, :timeout}}}
    end
  end

  def handle_event({:call, from}, :ping, :initialized, %{node_id: node_id} = _state) do
    # generate random id
    id = Enum.random(1..100_000)

    message = "ping #{id}\n"

    Logger.info("Ping Message '#{String.trim_trailing(message)}' sent")
    @tcp_module.send_message(node_id, String.to_charlist(message))

    receive do
      {:pong, id, data} ->
        Logger.info("Corresponding Pong received: #{Jason.encode!(data)}")
        {:keep_state_and_data, {:reply, from, {:pong, id, data}}}

      {:error_ping, specifier, error_class, error_text, error_dict} ->
        Logger.error("Error on ping request: #{error_text}")

        {:keep_state_and_data,
         {:reply, from, {:error, :ping, specifier, error_class, error_text, error_dict}}}
    after
      @call_timeout -> {:keep_state_and_data, {:reply, from, {:error, :timeout}}}
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
      id = Integer.to_string(:rand.uniform(100_000))
      message = "ping #{id}\n"
      Logger.debug("Inactivity check: sending ping #{id} to #{inspect(node_id)}")
      @tcp_module.send_message(node_id, String.to_charlist(message))

      receive do
        {:pong, ^id, _data} ->
          Logger.debug("Inactivity check: pong #{id} received from #{inspect(node_id)}")
          {:keep_state_and_data, {{:timeout, :inactivity_check}, @inactivity_timeout, nil}}

        {:error_ping, _specifier, _error_class, error_text, _error_dict} ->
          Logger.warning("Inactivity check ping error for #{inspect(node_id)}: #{error_text}")
          {:keep_state_and_data, {{:timeout, :inactivity_check}, @inactivity_timeout, nil}}
      after
        @call_timeout ->
          Logger.warning(
            "Inactivity check timed out for #{inspect(node_id)}, checking for queued socket events"
          )

          maybe_force_disconnect(node_id)
          {:keep_state_and_data, {{:timeout, :inactivity_check}, @inactivity_timeout, nil}}
      end
    else
      {:keep_state, %{state | last_seen_update_ts: current_ts},
       {{:timeout, :inactivity_check}, @inactivity_timeout, nil}}
    end
  end

  def handle_event({:timeout, :inactivity_check}, nil, _state_name, _state) do
    {:keep_state_and_data, []}
  end

  def handle_event(:info, {:active}, :initialized, state) do
    Logger.warning("received async ACTIVE message")
    updated_state = %{state | active: true}
    {:keep_state, updated_state}
  end

  def handle_event(:info, {:inactive}, :initialized, state) do
    Logger.warning("received async INACTIVE message")
    updated_state = %{state | active: false}
    {:keep_state, updated_state}
  end

  def handle_event(:info, {:pong, id, data}, :initialized, _state) do
    Logger.warning("received async PONG message id:#{id}, data:#{data}")
    :keep_state_and_data
  end

  def handle_event(
        :info,
        {:describe, _specifier, parsed_description},
        :initialized,
        %{description: description} = state
      ) do
    equipment_id = parsed_description[:properties][:equipment_id]

    case MapDiff.diff(parsed_description, description) do
      # Notihng changed, probably just a network disconnect
      %{changed: :equal, value: _} ->
        Logger.warning("received async Description: data constant")
        :keep_state_and_data

      # Description changed update uuid, equipment_id and description
      _ ->
        Logger.warning("received async Description: data changed issuing new uuid")

        updated_state_descr = %{
          state
          | description: parsed_description,
            uuid: Ecto.UUID.generate(),
            equipment_id: equipment_id
        }

        {:keep_state, updated_state_descr}
    end
  end

  def handle_event(:info, {:done, module, command, data_report}, :initialized, _state) do
    Logger.warning("received async DONE message #{module}:#{command} data: #{data_report}")
    :keep_state_and_data
  end

  def handle_event(:info, {:reply, module, parameter, data_report}, :initialized, _state) do
    Logger.warning("received async REPLY message #{module}:#{parameter} data: #{data_report}")
    :keep_state_and_data
  end

  def handle_event(:info, {:changed, module, parameter, data_report}, :initialized, _state) do
    Logger.warning("received async CHANGED message #{module}:#{parameter} data: #{data_report}")
    :keep_state_and_data
  end

  defp maybe_force_disconnect(node_id) do
    {:messages, msgs} = :erlang.process_info(self(), :messages)

    if :socket_disconnected not in msgs do
      Logger.warning(
        "No socket event after inactivity timeout for #{inspect(node_id)}, forcing TCP disconnect"
      )

      @tcp_module.disconnect(node_id)
    end
  end

  defp send_idn_message(node_id) do
    @tcp_module.send_message(node_id, ~c"*IDN?\n")

    receive do
      {:identification, %SECoP_IDN{} = idn} -> {:ok, idn}
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
        Logger.error("Error on describe request: #{error_text}")
        {:error, {specifier, error_class, error_text, error_dict}}
    after
      @handshake_timeout -> {:error, :timeout}
    end
  end

  defp send_activate_message(node_id) do
    @tcp_module.send_message(node_id, ~c"activate\n")

    receive do
      {:active} ->
        Logger.info("Node Activated")
        {:ok, :active}

      {:error_activate, specifier, error_class, error_text, error_dict} ->
        Logger.error("Error on activate request: #{error_text}")
        {:error, {specifier, error_class, error_text, error_dict}}
    after
      @handshake_timeout -> {:error, :timeout}
    end
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

defmodule SEC_Node_Services do
  use Supervisor
  require Logger

  def start_link(opts) do
    node_id = {opts[:host], opts[:port]}

    Supervisor.start_link(__MODULE__, opts,
      name: {:via, Registry, {Registry.SEC_Node_Services, node_id}}
    )
  end

  def stop(node_id) do
    case Registry.lookup(Registry.SEC_Node_Services, node_id) do
      [{supervisor_pid, _}] ->
        Supervisor.stop(supervisor_pid, :normal)

      [] ->
        {:error, :not_found}
    end
  end

  @impl true
  def init(opts) do
    node_id = {opts[:host], opts[:port]}

    children = [
      # Buffer process for this specific node
      {Buffer, node_id},
      # TCP connection for this specific node
      {TcpConnection, opts},
      # Main state machine for this node
      {SEC_Node_Statem, opts}
    ]

    # Use :rest_for_one strategy - if SEC_Node_Statem crashes,
    # restart it but keep TCP connection and buffer
    # If TCP connection crashes, restart it and SEC_Node_Statem
    Supervisor.init(children, strategy: :rest_for_one)
  end

  def child_spec(opts) do
    %{
      id: {opts[:host], opts[:port]},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :transient,
      shutdown: :infinity
    }
  end
end

defmodule SEC_Node_Supervisor do
  # Automatically defines child_spec/1
  require Logger
  alias Jason
  use DynamicSupervisor
  @reconnect_backoff 5000

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_child(opts) do
    DynamicSupervisor.start_child(
      __MODULE__,
      {SEC_Node_Services, Map.put(opts, :restart, :transient)}
    )
  end

  def get_active_nodes() do
    Registry.select(Registry.SEC_Node_Statem, [
      {{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}
    ])
    |> Enum.reduce(%{}, fn {_id, pid, _value}, acc ->
      case SEC_Node_Statem.get_state(pid) do
        {:ok, state} ->
          node_id = state.node_id

          Map.put(acc, node_id, state)

        _ ->
          acc
      end
    end)
  end

  def start_child_from_discovery(ip, _port, discovery_message) do
    discover_map = Jason.decode!(discovery_message)

    chl_ip = ip |> Tuple.to_list() |> Enum.join(".") |> String.to_charlist()

    node_port = Map.get(discover_map, "port")

    opts = %{
      host: chl_ip,
      port: node_port,
      reconnect_backoff: @reconnect_backoff
    }

    case Registry.lookup(Registry.SEC_Node_Statem, {chl_ip, node_port}) do
      [] -> DynamicSupervisor.start_child(__MODULE__, {SEC_Node_Services, opts})
      _ -> {:ok, :node_already_running}
    end
  end
end

defmodule NodeTable do
  require Logger

  @lookup_table :node_table_lookup

  def init_lookup_table do
    :ets.new(@lookup_table, [:set, :public, :named_table])
    :ok
  rescue
    ArgumentError ->
      :ok
  end

  def start(node_id) do
    case :ets.lookup(@lookup_table, node_id) do
      [{_, table}] ->
        if :ets.info(table) != :undefined do
          {:ok, table}
        else
          # Owner process crashed and ETS deleted the table; remove stale entry and recreate.
          :ets.delete(@lookup_table, node_id)
          start(node_id)
        end

      [] ->
        table = :ets.new(:ets_table, [:set, :public])

        case :ets.insert_new(@lookup_table, {node_id, table}) do
          true ->
            {:ok, table}

          false ->
            :ets.delete(table)
            get_table(node_id)
        end
    end
  end

  def insert(node_id, key, value) do
    {:ok, table} = get_table(node_id)

    true = :ets.insert(table, {key, value})

    {:ok, :inserted}
  end

  def lookup(node_id, key) do
    try do
      {:ok, table} = get_table(node_id)

      case :ets.lookup(table, key) do
        [{_, value}] -> {:ok, value}
        [] -> {:error, :notfound}
      end
    rescue
      ArgumentError -> {:error, :table_not_found}
    end
  end

  defp get_table(node_id) do
    case :ets.lookup(@lookup_table, node_id) do
      [{_, table}] ->
        {:ok, table}

      [] ->
        Logger.error("Lookup for node_id #{inspect(node_id)} failed - no table found")
        {:error, :notfound}
    end
  end
end
