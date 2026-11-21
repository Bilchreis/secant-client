defmodule SecantClient.TcpConnection do
  @moduledoc """
  Owns the TCP socket of one SEC node and reconnects when it drops.

  Received data is split into lines here and each line is handed to
  `SecantClient.Parser` in arrival order. The socket runs in `{:active, N}`
  mode, so a node that floods the connection cannot grow the mailbox without
  bound: after N messages the socket pauses until they have been parsed.

  The node's `SecantClient.NodeStatem` is told about the connection with
  `:socket_connected` and `:socket_disconnected` messages.
  """

  alias SecantClient.{Parser, TcpConnectionBehaviour}

  require Logger

  @behaviour :gen_statem
  @behaviour TcpConnectionBehaviour

  @initial_state :disconnected
  # Messages delivered before the socket pauses for the parser to catch up
  @active_n 100
  @connect_timeout 5_000
  @call_timeout 5_000

  ## Public API

  def start_link(opts) do
    :gen_statem.start_link(
      {:via, Registry, {SecantClient.Registry.TcpConnection, {opts[:host], opts[:port]}}},
      __MODULE__,
      opts,
      []
    )
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      shutdown: 500
    }
  end

  @impl TcpConnectionBehaviour
  def connected?(node_id), do: call(node_id, :connected?) == true

  @impl TcpConnectionBehaviour
  def send_message(node_id, data), do: call(node_id, {:send, data})

  @impl TcpConnectionBehaviour
  def disconnect(node_id), do: call(node_id, :disconnect)

  def stop(node_id), do: call(node_id, :stop)

  defp call(node_id, request) do
    case Registry.lookup(SecantClient.Registry.TcpConnection, node_id) do
      [{pid, _value}] ->
        try do
          :gen_statem.call(pid, request, @call_timeout)
        catch
          :exit, {:timeout, _} -> {:error, :timeout}
          :exit, {reason, _} when reason in [:noproc, :normal, :shutdown] -> {:error, :not_found}
        end

      [] ->
        {:error, :not_found}
    end
  end

  ## Callbacks

  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  @impl :gen_statem
  def init(opts) do
    state = %{
      host: opts[:host],
      port: opts[:port],
      node_id: {opts[:host], opts[:port]},
      socket: nil,
      # Bytes received after the last complete line
      buffer: "",
      reconnect_backoff: opts[:reconnect_backoff] || 500
    }

    {:ok, @initial_state, state, {:next_event, :internal, :connect}}
  end

  @impl :gen_statem
  def handle_event(:internal, :connect, :disconnected, state) do
    options = [:binary, active: @active_n, keepalive: true]

    case :gen_tcp.connect(state.host, state.port, options, @connect_timeout) do
      {:ok, socket} ->
        # TCP_KEEPIDLE=30s, TCP_KEEPINTVL=10s, TCP_KEEPCNT=3 (Linux: SOL_TCP=6, opts 4/5/6)
        # Dead connection detected within ~60s; setopts returns {:error,_} on non-Linux, safe to ignore
        :inet.setopts(socket, [{:raw, 6, 4, <<30::native-32>>}])
        :inet.setopts(socket, [{:raw, 6, 5, <<10::native-32>>}])
        :inet.setopts(socket, [{:raw, 6, 6, <<3::native-32>>}])

        notify_statem(state.node_id, :socket_connected)
        Logger.info("Connected to #{state.host}:#{state.port}")
        {:next_state, :connected, %{state | socket: socket, buffer: ""}}

      {:error, reason} ->
        Logger.error("Failed to connect to #{state.host}:#{state.port}: #{inspect(reason)}")
        {:keep_state_and_data, {{:timeout, :reconnect}, state.reconnect_backoff, nil}}
    end
  end

  def handle_event({:timeout, :reconnect}, nil, :disconnected, _state) do
    {:keep_state_and_data, {:next_event, :internal, :connect}}
  end

  def handle_event({:timeout, :reconnect}, nil, _state_name, _state), do: :keep_state_and_data

  # Lines are parsed here, in the order they arrive, so updates of the same
  # parameter are applied in order. Only messages of the current socket are
  # handled; a closed socket can still have messages in the mailbox.
  def handle_event(:info, {:tcp, socket, data}, :connected, %{socket: socket} = state) do
    {:keep_state, %{state | buffer: handle_data(state.buffer, data, state.node_id)}}
  end

  def handle_event(:info, {:tcp_passive, socket}, :connected, %{socket: socket}) do
    :ok = :inet.setopts(socket, active: @active_n)
    :keep_state_and_data
  end

  def handle_event(:info, {:tcp_closed, socket}, :connected, %{socket: socket} = state) do
    Logger.warning("Connection to #{state.host}:#{state.port} closed by server")
    close(state)
  end

  def handle_event(:info, {:tcp_error, socket, reason}, :connected, %{socket: socket} = state) do
    Logger.error("TCP error on #{state.host}:#{state.port}: #{inspect(reason)}")
    close(state)
  end

  def handle_event(:info, message, _state_name, _state) do
    Logger.debug("Dropped message of a previous socket: #{inspect(message, limit: 3)}")
    :keep_state_and_data
  end

  def handle_event({:call, from}, {:send, data}, :connected, state) do
    {:keep_state_and_data, {:reply, from, :gen_tcp.send(state.socket, data)}}
  end

  def handle_event({:call, from}, {:send, _data}, :disconnected, _state) do
    {:keep_state_and_data, {:reply, from, {:error, :not_connected}}}
  end

  def handle_event({:call, from}, :connected?, state_name, _state) do
    {:keep_state_and_data, {:reply, from, state_name == :connected}}
  end

  def handle_event({:call, from}, :disconnect, :connected, state) do
    {:next_state, :disconnected, data, actions} = close(state)
    {:next_state, :disconnected, data, [{:reply, from, :ok} | List.wrap(actions)]}
  end

  def handle_event({:call, from}, :disconnect, :disconnected, _state) do
    {:keep_state_and_data, {:reply, from, :ok}}
  end

  def handle_event({:call, from}, :stop, _state_name, state) do
    if state.socket, do: :gen_tcp.close(state.socket)
    notify_statem(state.node_id, :socket_disconnected)
    {:stop_and_reply, :normal, {:reply, from, :ok}}
  end

  defp close(state) do
    :gen_tcp.close(state.socket)
    notify_statem(state.node_id, :socket_disconnected)

    {:next_state, :disconnected, %{state | socket: nil, buffer: ""},
     {:next_event, :internal, :connect}}
  end

  defp notify_statem(node_id, message) do
    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _} <- entries, do: send(pid, message)
    end)
  end

  ## Line framing

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
end
