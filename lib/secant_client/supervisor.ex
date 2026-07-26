defmodule SecantClient.Supervisor do
  # alias SecantClient.UdpBroadcaster
  alias NodeDiscover
  alias NodeTable
  alias TcpConnection
  alias Buffer
  alias SEC_Node_Statem
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    :ok = NodeTable.init_lookup_table()

    base_children = [
      {Phoenix.PubSub, name: :secant_client_pubsub},
      {Registry, keys: :unique, name: Registry.Buffer},
      {Registry, keys: :unique, name: Registry.TcpConnection},
      {Registry, keys: :unique, name: Registry.SEC_Node_Statem},
      {Registry, keys: :unique, name: Registry.SecNodePublisher},
      {Registry, keys: :unique, name: Registry.SEC_Node_Services},
      {SEC_Node_Supervisor, []}
    ]

    children =
      if Application.get_env(:secant_client, :start_node_discover, true) do
        base_children ++ [{NodeDiscover, &SEC_Node_Supervisor.start_child_from_discovery/3}]
      else
        base_children
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
