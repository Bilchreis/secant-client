defmodule SecantClient.IDN do
  @moduledoc "The node's answer to `*IDN?`."
  defstruct [:manufacturer, :product, :draft_date, :version]
end
