defmodule SecantClient.Supervisor do
  @moduledoc """
  The top of the secant_client supervision tree. Start it in the host
  application's supervision tree.
  """

  alias SecantClient.{NodeDiscover, NodeSupervisor, NodeTable}
  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(_opts) do
    :ok = NodeTable.init_lookup_table()

    base_children = [
      {Phoenix.PubSub, name: :secant_client_pubsub},
      {Registry, keys: :unique, name: SecantClient.Registry.TcpConnection},
      {Registry, keys: :unique, name: SecantClient.Registry.NodeStatem},
      {Registry, keys: :unique, name: SecantClient.Registry.NodeServices},
      {NodeSupervisor, []}
    ]

    children =
      if Application.get_env(:secant_client, :start_node_discover, true) do
        base_children ++ [{NodeDiscover, &NodeSupervisor.start_child_from_discovery/3}]
      else
        base_children
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
