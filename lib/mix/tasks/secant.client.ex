defmodule Mix.Tasks.Secant.Client do
  @moduledoc "Starts `SecantClient.Supervisor` and runs until stopped."
  use Mix.Task

  @shortdoc "Starts SecantClient.Supervisor and runs indefinitely"

  def run(_args) do
    Mix.Task.run("app.start")
    {:ok, _pid} = SecantClient.Supervisor.start_link()

    # opts = %{
    #   host: "192.168.178.75" |> String.to_charlist(),
    #   port: 10900,
    #   reconnect_backoff: 5000,
    #   manual: true
    # }

    # NodeSupervisor.start_child(opts)

    # Process.sleep(3000)

    # node_map = NodeSupervisor.get_active_nodes()

    # all_entries = Registry.select(Registry.PlotPublisher,[{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}])
    # IO.inspect(all_entries, label: "All entries (key, PID pairs)")

    # Registry.PlotPublisher

    # parameter_id = {~c"192.168.178.52", 10800,:massflow_contr1,:value}

    # IO.inspect(PlotPublisher.get_data(parameter_id))

    # Keep the application running indefinitely
    :timer.sleep(:infinity)
  end
end
