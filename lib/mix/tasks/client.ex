defmodule Mix.Tasks.Client do
  use Mix.Task
  alias SecantClient
  alias SEC_Node_Supervisor

  @shortdoc "Starts the SecantClient application and runs indefinitely"

  def run(_args) do
    Mix.Task.run("app.start")

    # opts = %{
    #   host: "192.168.178.75" |> String.to_charlist(),
    #   port: 10900,
    #   reconnect_backoff: 5000,
    #   manual: true
    # }

    # SEC_Node_Supervisor.start_child(opts)

    # Process.sleep(3000)

    # node_map = SEC_Node_Supervisor.get_active_nodes()

    # all_entries = Registry.select(Registry.PlotPublisher,[{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}])
    # IO.inspect(all_entries, label: "All entries (key, PID pairs)")

    # Registry.PlotPublisher

    # parameter_id = {~c"192.168.178.52", 10800,:massflow_contr1,:value}

    # IO.inspect(PlotPublisher.get_data(parameter_id))

    # Keep the application running indefinitely
    :timer.sleep(:infinity)
  end
end
