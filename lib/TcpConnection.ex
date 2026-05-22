defmodule TcpConnection do
  alias Buffer
  alias BufferSupervisor

  require Logger

  @behaviour :gen_statem

  @initial_state :disconnected

  ## Public API

  def start_link(opts) do
    :gen_statem.start_link(
      {:via, Registry, {Registry.TcpConnection, {opts[:host], opts[:port]}}},
      __MODULE__,
      opts,
      []
    )
  end

  ## Callbacks

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :transient,
      shutdown: 500
    }
  end

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  @impl :gen_statem
  def init(opts) do
    state = %{
      host: opts[:host],
      port: opts[:port],
      socket: nil,
      reconnect_backoff: opts[:reconnect_backoff] || 500
    }

    {:ok, @initial_state, state, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def handle_event(:internal, :connect, :disconnected, %{host: host, port: port} = state) do
    case :gen_tcp.connect(state.host, state.port, [:binary, active: true, keepalive: true]) do
      {:ok, socket} ->
        # TCP_KEEPIDLE=30s, TCP_KEEPINTVL=10s, TCP_KEEPCNT=3 (Linux: SOL_TCP=6, opts 4/5/6)
        # Dead connection detected within ~60s; setopts returns {:error,_} on non-Linux, safe to ignore
        :inet.setopts(socket, [{:raw, 6, 4, <<30::native-32>>}])
        :inet.setopts(socket, [{:raw, 6, 5, <<10::native-32>>}])
        :inet.setopts(socket, [{:raw, 6, 6, <<3::native-32>>}])

        Registry.dispatch(Registry.SEC_Node_Statem, {host, port}, fn entries ->
          for {pid, _} <- entries, do: send(pid, :socket_connected)
        end)

        Logger.info("Connected to #{state.host}:#{state.port}")
        {:next_state, :connected, %{state | socket: socket}}

      {:error, reason} ->
        Logger.error("Failed to connect: #{inspect(reason)}")
        {:keep_state, state, {{:timeout, :reconnect}, state.reconnect_backoff, nil}}
    end
  end

  def handle_event({:timeout, :reconnect}, nil, :disconnected, state) do
    {:keep_state, state, {:next_event, :internal, :connect}}
  end

  def handle_event(:info, {:tcp, _socket, data}, :connected, %{host: host, port: port} = state) do
    [{buffer_pid, _value}] = Registry.lookup(Registry.Buffer, {host, port})
    Buffer.receive(buffer_pid, data)
    {:keep_state, state}
  end

  def handle_event(:info, {:tcp_closed, _socket}, :connected, %{host: host, port: port} = state) do
    Logger.warning("Connection closed by server")

    Registry.dispatch(Registry.SEC_Node_Statem, {host, port}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :socket_disconnected)
    end)

    {:next_state, :disconnected, %{state | socket: nil}, {:next_event, :internal, :connect}}
  end

  def handle_event(
        :info,
        {:tcp_error, _socket, reason},
        :connected,
        %{host: host, port: port} = state
      ) do
    Logger.error("TCP error: #{inspect(reason)}")

    Registry.dispatch(Registry.SEC_Node_Statem, {host, port}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :socket_disconnected)
    end)

    {:next_state, :disconnected, %{state | socket: nil}, {:next_event, :internal, :connect}}
  end

  def handle_event({:call, from}, {:send, data}, :connected, state) do
    # Logger.info("sending data: #{data}")

    case :gen_tcp.send(state.socket, data) do
      :ok ->
        :gen_statem.reply(from, :ok)

      {:error, reason} ->
        :gen_statem.reply(from, {:error, reason})
    end

    {:keep_state, state}
  end

  # Handle send attempts while disconnected
  def handle_event({:call, from}, {:send, _message}, :disconnected, _state) do
    {:keep_state_and_data, {:reply, from, {:error, :not_connected}}}
  end

  def handle_event({:call, from}, :is_connected, :connected, state) do
    :gen_statem.reply(from, true)
    {:keep_state, state}
  end

  def handle_event({:call, from}, :is_connected, :disconnected, state) do
    :gen_statem.reply(from, false)
    {:keep_state, state}
  end

  def handle_event({:call, from}, :stop, _state, %{host: host, port: port} = state) do
    # Close socket if connected
    if state.socket, do: :gen_tcp.close(state.socket)

    # Notify any listeners
    Registry.dispatch(Registry.SEC_Node_Statem, {host, port}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :socket_disconnected)
    end)

    :gen_statem.reply(from, :ok)
    {:stop, :normal, state}
  end

  ## Helpers
  def is_connected(node_id) do
    [{conn_pid, _value}] = Registry.lookup(Registry.TcpConnection, node_id)
    :gen_statem.call(conn_pid, :is_connected)
  end

  def send_message(node_id, data) do
    [{conn_pid, _value}] = Registry.lookup(Registry.TcpConnection, node_id)
    :gen_statem.call(conn_pid, {:send, data})
  end

  def stop(node_id) do
    case Registry.lookup(Registry.TcpConnection, node_id) do
      [{conn_pid, _}] ->
        :gen_statem.call(conn_pid, :stop)
      [] ->
        {:error, :not_found}
    end
  end
end


defmodule Buffer do
  @moduledoc "Buffer the data received to parse out statements"

  use GenServer
  require Logger
  alias SECoP_Parser

  @eol <<10>>
  @initial_state ""

  def start_link(buffer_identifier \\ nil) do
    state = %{
      buffer: @initial_state,
      node_id: buffer_identifier
    }

    GenServer.start_link(__MODULE__, state,
      name: {:via, Registry, {Registry.Buffer, buffer_identifier}}
    )
  end

  def receive(pid \\ __MODULE__, data) do
    GenServer.cast(pid, {:receive, data})
  end

  @impl true
  def init(buffer) do
    {:ok, buffer}
  end

  @impl true
  def handle_cast({:receive, data}, state) do
    updated_state = state |> append(data) |> process

    {:noreply, updated_state}
  end

  defp append(state, ""), do: state

  defp append(%{buffer: buffer} = state, data) do
    updated_buffer = buffer <> data
    %{state | buffer: updated_buffer}
  end

  defp process(%{buffer: buffer, node_id: node_id} = state) do
    case extract(buffer) do
      {:statement, updated_buffer, statement} ->
        Task.start(fn -> SECoP_Parser.parse(node_id, statement) end)
        process(%{state | buffer: updated_buffer})

      {:nothing, updated_buffer} ->
        %{state | buffer: updated_buffer}
    end
  end

  defp extract(buffer) do
    case String.split(buffer, @eol, parts: 2) do
      [match, rest] -> {:statement, rest, match}
      [rest] -> {:nothing, rest}
    end
  end



end
