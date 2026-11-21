defmodule SecantClient.NodeTable.Owner do
  @moduledoc """
  Owns a node's ETS table. It runs as the first child of NodeServices,
  so the table outlives restarts of the TCP connection and the state machine,
  and is deleted when the node is stopped.
  """

  alias SecantClient.NodeTable
  use GenServer

  def start_link(node_id), do: GenServer.start_link(__MODULE__, node_id)

  def child_spec(node_id) do
    %{id: {__MODULE__, node_id}, start: {__MODULE__, :start_link, [node_id]}}
  end

  @impl true
  def init(node_id) do
    {:ok, _table} = NodeTable.start(node_id)
    {:ok, node_id}
  end
end
