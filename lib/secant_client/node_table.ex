defmodule SecantClient.NodeTable do
  @moduledoc """
  The ETS table of each node, holding its latest data reports
  (`{:data_report, module, accessible}`), its description and its identity.
  Tables are found by node id in the named table `:node_table_lookup`.
  """

  require Logger

  @lookup_table :node_table_lookup

  def init_lookup_table do
    :ets.new(@lookup_table, [:set, :public, :named_table])
    :ok
  rescue
    ArgumentError ->
      :ok
  end

  def start(node_id) do
    case :ets.lookup(@lookup_table, node_id) do
      [{_, table}] ->
        if :ets.info(table) != :undefined do
          {:ok, table}
        else
          # Owner process crashed and ETS deleted the table; remove stale entry and recreate.
          :ets.delete(@lookup_table, node_id)
          start(node_id)
        end

      [] ->
        table = :ets.new(:ets_table, [:set, :public])

        case :ets.insert_new(@lookup_table, {node_id, table}) do
          true ->
            {:ok, table}

          false ->
            :ets.delete(table)
            get_table(node_id)
        end
    end
  end

  def insert(node_id, key, value), do: insert(node_id, [{key, value}])

  # Insert several {key, value} entries with one table lookup.
  def insert(node_id, entries) when is_list(entries) do
    {:ok, table} = get_table(node_id)

    true = :ets.insert(table, entries)

    {:ok, :inserted}
  end

  def lookup(node_id, key) do
    with {:ok, table} <- get_table(node_id) do
      case :ets.lookup(table, key) do
        [{_, value}] -> {:ok, value}
        [] -> {:error, :notfound}
      end
    end
  rescue
    ArgumentError -> {:error, :table_not_found}
  end

  # Delete data reports of modules and parameters that are not in the description.
  def prune(node_id, description) do
    modules = description[:modules] || %{}

    with {:ok, table} <- get_table(node_id) do
      table
      |> :ets.match({{:data_report, :"$1", :"$2"}, :_})
      |> Enum.reject(fn [module, parameter] ->
        Map.has_key?(get_in(modules, [module, :parameters]) || %{}, parameter)
      end)
      |> Enum.each(fn [module, parameter] ->
        :ets.delete(table, {:data_report, module, parameter})
      end)

      :ok
    end
  rescue
    ArgumentError -> {:error, :table_not_found}
  end

  defp get_table(node_id) do
    case :ets.lookup(@lookup_table, node_id) do
      [{_, table}] ->
        {:ok, table}

      [] ->
        Logger.error("Lookup for node_id #{inspect(node_id)} failed - no table found")
        {:error, :notfound}
    end
  end
end
