defmodule TcpConnectionBehaviour do
  @callback is_connected(node_id :: term()) :: boolean()
  @callback send_message(node_id :: term(), data :: term()) :: :ok | {:error, term()}
  @callback disconnect(node_id :: term()) :: :ok
end
