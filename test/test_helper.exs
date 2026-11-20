# secant_client has no application callback, so start its supervision tree
# (registries, PubSub, NodeSupervisor) for the test run.
{:ok, _pid} = SecantClient.Supervisor.start_link()

ExUnit.start()
