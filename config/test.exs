import Config

config :secant_client,
  tcp_connection_module: MockTcpConnection,
  inactivity_timeout: 100,
  reconnect_timeout: 50,
  handshake_timeout: 500,
  call_timeout: 200,
  start_node_discover: false
