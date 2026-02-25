defmodule SecopClient do
  # alias SecopClient.UdpBroadcaster
  alias NodeDiscover
  alias NodeTable
  alias TcpConnection
  alias Buffer
  alias SEC_Node_Statem
  use Application

  def start(_type, _args) do
    :ok = NodeTable.init_lookup_table()

    children = [
      {Phoenix.PubSub, name: :secop_client_pubsub},
      {Registry, keys: :unique, name: Registry.Buffer},
      {Registry, keys: :unique, name: Registry.TcpConnection},
      {Registry, keys: :unique, name: Registry.SEC_Node_Statem},
      {Registry, keys: :unique, name: Registry.SecNodePublisher},
      {Registry, keys: :unique, name: Registry.SEC_Node_Services},
      {SEC_Node_Supervisor, []},
      {NodeDiscover, &SEC_Node_Supervisor.start_child_from_discovery/3}
    ]

    opts = [strategy: :one_for_one, name: SecopClient.Supervisor]

    Supervisor.start_link(children, opts)
  end
end
