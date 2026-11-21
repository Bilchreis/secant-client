defmodule SecantClient.TcpConnectionBehaviour do
  @moduledoc false
  # The part of `SecantClient.TcpConnection` that `SecantClient.NodeStatem`
  # uses, so tests can replace the connection with a mock.

  @callback connected?(node_id :: term()) :: boolean()
  @callback send_message(node_id :: term(), data :: iodata()) :: :ok | {:error, term()}
  @callback disconnect(node_id :: term()) :: :ok | {:error, term()}
end
