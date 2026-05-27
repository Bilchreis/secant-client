defmodule SecantClient do
  # alias SecantClient.UdpBroadcaster
  alias NodeDiscover
  alias NodeTable
  alias TcpConnection
  alias Buffer
  alias SEC_Node_Statem
  use Application

  def start(_type, _args) do
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

    opts = [strategy: :one_for_one, name: SecantClient.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
