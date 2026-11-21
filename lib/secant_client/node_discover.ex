defmodule SecantClient.SocketStruct do
  @moduledoc false
  defstruct socket: nil, callback: nil
end

defmodule SecantClient.NodeDiscover do
  @moduledoc """
  Finds SEC nodes on the local network with the SECoP UDP discovery broadcast,
  at start and every minute, and calls the callback for every node that
  answers with `callback.(ip, port, message)`.
  """

  alias SecantClient.SocketStruct
  use GenServer
  @message "{\"SECoP\": \"discover\"}"
  # Broadcast address
  @broadcast_address {255, 255, 255, 255}
  @port 10767
  @interval 60 * 1000
  require Logger

  # Starts the GenServer with a given UDP port and callback function
  def start_link(callback_function) do
    GenServer.start_link(
      __MODULE__,
      %SocketStruct{socket: nil, callback: callback_function},
      name: __MODULE__
    )
  end

  def scan() do
    GenServer.cast(__MODULE__, :scan)
  end

  @impl true
  def handle_cast(:scan, socketstruct) do
    # Send the message to the broadcast address and port
    Logger.info("Manually triggered Scan for new Nodes")
    send_discover(socketstruct)
    {:noreply, socketstruct}
  end

  @impl true
  def init(socketstruct) do
    # Open a UDP socket
    Logger.info("Opening discovery port")
    {:ok, socket} = :gen_udp.open(0, [{:broadcast, true}, {:ip, {0, 0, 0, 0}}, {:active, true}])

    # Send the message to the broadcast address and port
    Logger.info("Initial Scan...")
    :gen_udp.send(socket, @broadcast_address, @port, @message)
    schedule_scan()

    {:ok, %SocketStruct{socket: socket, callback: socketstruct.callback}}
  end

  @impl true
  def handle_info({:udp, _socket, ip, port, message}, socketstruct) do
    # Any host can send a datagram to this port: ignore what is not a node's
    # answer instead of crashing.
    case Jason.decode(message) do
      {:ok, %{"port" => node_port}} when is_integer(node_port) ->
        Logger.info("Node Discovered at #{:inet.ntoa(ip)}:#{node_port}")
        socketstruct.callback.(ip, port, message)

      _ ->
        Logger.debug("Ignored discovery datagram from #{:inet.ntoa(ip)}:#{port}")
    end

    {:noreply, socketstruct}
  end

  @impl true
  def handle_info(:work, socketstruct) do
    Logger.info("Scheduled Node scan...")
    # Do the desired work here
    send_discover(socketstruct)

    # Reschedule once more
    schedule_scan()

    {:noreply, socketstruct}
  end

  defp schedule_scan do
    # We schedule the work to happen in 2 hours (written in milliseconds).
    # Alternatively, one might write :timer.hours(2)
    Process.send_after(self(), :work, @interval)
  end

  @impl true
  def terminate(_reason, %{socket: socket}) do
    # Close the socket on termination
    :gen_udp.close(socket)
    :ok
  end

  defp send_discover(socketstruct) do
    :gen_udp.send(socketstruct.socket, @broadcast_address, @port, @message)
  end
end
