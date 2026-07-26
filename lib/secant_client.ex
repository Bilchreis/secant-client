defmodule SecantClient do
  use Application

  def start(_type, _args) do
    SecantClient.Supervisor.start_link(name: SecantClient.Supervisor)
  end
end
