defmodule SecantClient.NodeSupervisor do
  @moduledoc """
  Starts and lists the nodes, one `SecantClient.NodeServices` per node.
  """

  alias SecantClient.{NodeServices, NodeStatem}
  # Automatically defines child_spec/1
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
      {NodeServices, Map.put(opts, :restart, :transient)}
    )
  end

  def get_active_nodes() do
    Registry.select(SecantClient.Registry.NodeStatem, [
      {{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}
    ])
    |> Enum.reduce(%{}, fn {_id, pid, _value}, acc ->
      case NodeStatem.get_state(pid) do
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

    case Registry.lookup(SecantClient.Registry.NodeStatem, {chl_ip, node_port}) do
      [] -> DynamicSupervisor.start_child(__MODULE__, {NodeServices, opts})
      _ -> {:ok, :node_already_running}
    end
  end
end
