defmodule SecantClient.NodeServices do
  @moduledoc """
  Supervises the processes of one node: its `SecantClient.NodeTable.Owner`,
  `SecantClient.TcpConnection` and `SecantClient.NodeStatem`, `:rest_for_one`.
  """

  alias SecantClient.{NodeStatem, NodeTable, TcpConnection}
  use Supervisor
  require Logger

  def start_link(opts) do
    node_id = {opts[:host], opts[:port]}

    Supervisor.start_link(__MODULE__, opts,
      name: {:via, Registry, {SecantClient.Registry.NodeServices, node_id}}
    )
  end

  def stop(node_id) do
    case Registry.lookup(SecantClient.Registry.NodeServices, node_id) do
      [{supervisor_pid, _}] ->
        Supervisor.stop(supervisor_pid, :normal)

      [] ->
        {:error, :not_found}
    end
  end

  @impl true
  def init(opts) do
    node_id = {opts[:host], opts[:port]}

    children = [
      # Owns the node's ETS table, so values and identity survive restarts
      # of the processes below
      {NodeTable.Owner, node_id},
      # TCP connection for this specific node; it also parses the lines it receives
      {TcpConnection, opts},
      # Main state machine for this node
      {NodeStatem, opts}
    ]

    # Use :rest_for_one strategy - if NodeStatem crashes,
    # restart it but keep the TCP connection
    # If TCP connection crashes, restart it and NodeStatem
    Supervisor.init(children, strategy: :rest_for_one)
  end

  def child_spec(opts) do
    %{
      id: {opts[:host], opts[:port]},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :transient,
      shutdown: :infinity
    }
  end
end
