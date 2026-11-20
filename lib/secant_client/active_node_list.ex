defmodule SecantClient.ActiveNodeList do
  use GenServer
  alias Jason

  def start_link(nodemap) do
    GenServer.start_link(__MODULE__, nodemap, name: __MODULE__)
  end

  def add_node(ip, port, equipment_id) do
    GenServer.cast(__MODULE__, {:add_node, ip, port, equipment_id})
  end

  def add_node_from_discovery(ip, port, message) do
    GenServer.cast(__MODULE__, {:add_node_from_discovery, ip, port, message})
  end

  def pop(ip, port) do
    GenServer.call(__MODULE__, {:pop, ip, port})
  end

  @impl true
  def handle_call({:pop, ip, port}, _from, nodemap) do
    {to_caller, updated_nodemap} = Map.pop(nodemap, {ip, port})
    {:reply, to_caller, updated_nodemap}
  end

  @impl true
  def handle_cast({:add_node, ip, port, equipment_id}, nodemap) do
    updated_nodemap = Map.put(nodemap, {ip, port}, equipment_id)
    {:noreply, updated_nodemap}
  end

  @impl true
  def handle_cast({:add_node_from_discovery, ip, _port, message}, nodemap) do
    discover_map = Jason.decode!(message)

    equipment_id = Map.get(discover_map, "equipment_id")
    node_port = Map.get(discover_map, "port")

    updated_nodemap = Map.put(nodemap, {ip, node_port}, equipment_id)

    {:noreply, updated_nodemap}
  end

  @impl true
  def init(nodemap) do
    {:ok, nodemap}
  end
end
