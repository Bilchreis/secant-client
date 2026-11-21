defmodule SecantClient.TcpConnection do
  alias SecantClient.{Parser, TcpConnectionBehaviour}

  require Logger

  @behaviour :gen_statem
  @behaviour TcpConnectionBehaviour

  @initial_state :disconnected

  ## Public API

  def start_link(opts) do
    :gen_statem.start_link(
      {:via, Registry, {SecantClient.Registry.TcpConnection, {opts[:host], opts[:port]}}},
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
      # Bytes received after the last complete line
      buffer: "",
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

        Registry.dispatch(SecantClient.Registry.NodeStatem, {host, port}, fn entries ->
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

  # Lines are parsed here, in the order they arrive, so updates of the same
  # parameter are applied in order.
  def handle_event(:info, {:tcp, _socket, data}, :connected, state) do
    {:keep_state, %{state | buffer: handle_data(state.buffer, data, {state.host, state.port})}}
  end

  def handle_event(:info, {:tcp_closed, _socket}, :connected, %{host: host, port: port} = state) do
    Logger.warning("Connection closed by server")

    Registry.dispatch(SecantClient.Registry.NodeStatem, {host, port}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :socket_disconnected)
    end)

    {:next_state, :disconnected, %{state | socket: nil, buffer: ""},
     {:next_event, :internal, :connect}}
  end

  def handle_event(
        :info,
        {:tcp_error, _socket, reason},
        :connected,
        %{host: host, port: port} = state
      ) do
    Logger.error("TCP error: #{inspect(reason)}")

    Registry.dispatch(SecantClient.Registry.NodeStatem, {host, port}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :socket_disconnected)
    end)

    {:next_state, :disconnected, %{state | socket: nil, buffer: ""},
     {:next_event, :internal, :connect}}
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

  def handle_event({:call, from}, :disconnect, :connected, %{host: host, port: port} = state) do
    :gen_tcp.close(state.socket)

    Registry.dispatch(SecantClient.Registry.NodeStatem, {host, port}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :socket_disconnected)
    end)

    :gen_statem.reply(from, :ok)

    {:next_state, :disconnected, %{state | socket: nil, buffer: ""},
     {:next_event, :internal, :connect}}
  end

  def handle_event({:call, from}, :disconnect, :disconnected, state) do
    {:keep_state, state, {:reply, from, :ok}}
  end

  def handle_event({:call, from}, :stop, _state, %{host: host, port: port} = state) do
    # Close socket if connected
    if state.socket, do: :gen_tcp.close(state.socket)

    # Notify any listeners
    Registry.dispatch(SecantClient.Registry.NodeStatem, {host, port}, fn entries ->
      for {pid, _} <- entries, do: send(pid, :socket_disconnected)
    end)

    :gen_statem.reply(from, :ok)
    {:stop, :normal, state}
  end

  ## Helpers

  @doc false
  # Appends `data` to `buffer`, parses every complete line and returns the
  # rest. Only `data` is searched for a newline, so a long message arriving in
  # many packets is not rescanned for each one.
  def handle_data(buffer, data, node_id) do
    case :binary.split(data, "\n", [:global]) do
      [incomplete] ->
        buffer <> incomplete

      [first | rest] ->
        parse_line(buffer <> first, node_id)
        {lines, [incomplete]} = Enum.split(rest, -1)
        Enum.each(lines, &parse_line(&1, node_id))
        incomplete
    end
  end

  defp parse_line("", _node_id), do: :ok

  # A malformed line must not take down the connection.
  defp parse_line(line, node_id) do
    Parser.parse(node_id, line)
  rescue
    error ->
      Logger.error(
        "Could not parse message from #{inspect(node_id)}: #{Exception.message(error)} " <>
          "line: #{inspect(line, printable_limit: 200)}"
      )
  end

  @impl TcpConnectionBehaviour
  def is_connected(node_id) do
    [{conn_pid, _value}] = Registry.lookup(SecantClient.Registry.TcpConnection, node_id)
    :gen_statem.call(conn_pid, :is_connected)
  end

  @impl TcpConnectionBehaviour
  def send_message(node_id, data) do
    [{conn_pid, _value}] = Registry.lookup(SecantClient.Registry.TcpConnection, node_id)
    :gen_statem.call(conn_pid, {:send, data})
  end

  @impl TcpConnectionBehaviour
  def disconnect(node_id) do
    [{conn_pid, _value}] = Registry.lookup(SecantClient.Registry.TcpConnection, node_id)
    :gen_statem.call(conn_pid, :disconnect)
  end

  def stop(node_id) do
    case Registry.lookup(SecantClient.Registry.TcpConnection, node_id) do
      [{conn_pid, _}] ->
        :gen_statem.call(conn_pid, :stop)

      [] ->
        {:error, :not_found}
    end
  end
end
