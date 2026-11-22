defmodule SecantClient.Parser do
  @moduledoc """
  Parses SECoP messages, one line at a time.

  Data reports are stored in the node's `SecantClient.NodeTable` and published
  on `:secant_client_pubsub` as `{:value_update, module, accessible, report}`
  on `"value_update:<host>:<port>"` (errors as `{:error_update, ...}` on
  `"error_update:<host>:<port>"`). Replies to requests are sent to the node's
  `SecantClient.NodeStatem`.
  """
  alias SecantClient.{IDN, NodeTable}
  require Logger

  defmodule Context do
    @moduledoc false
    # What every line of one node needs, resolved once per connection instead
    # of once per line.
    defstruct [:node_id, :table, :value_topic, :error_topic]
  end

  @doc false
  # `table` is nil when the node has no table yet; it is then looked up for
  # each insert, as without a context.
  def context(%Context{} = context), do: context

  def context({host, port} = node_id) do
    table =
      case NodeTable.fetch_table(node_id) do
        {:ok, table} -> table
        :error -> nil
      end

    %Context{
      node_id: node_id,
      table: table,
      value_topic: "value_update:#{host}:#{port}",
      error_topic: "error_update:#{host}:#{port}"
    }
  end

  # Parses one line. `node` is the node id, or the node's context/1 when many
  # lines of the same node are parsed.
  #
  # Updates are nearly all the traffic of an active node, so they skip the
  # general split in parse_trimmed/2. A line they cannot handle goes there.
  def parse(node, "update " <> rest = message) do
    context = context(node)

    with [specifier, data] <- :binary.split(rest, " "),
         {:ok, data_report} <- decode_report(data) do
      store_report(context, specifier, data_report, update_entries())
    else
      _ -> parse_trimmed(context, message)
    end
  end

  def parse(node, message), do: parse_trimmed(context(node), message)

  defp parse_trimmed(%Context{node_id: node_id} = context, message) do
    trimmed = String.trim(message)

    # SECoP message are of the form
    # messge_code specifier data
    split_message = String.split(trimmed, " ", parts: 3)

    case split_message do
      ["update", specifier, data] ->
        update(context, specifier, data)

      ["describing", specifier, data] ->
        describe(node_id, specifier, data)

      ["inactive"] ->
        inactive(node_id)

      ["pong", id, data] ->
        pong(node_id, id, data)

      ["active"] ->
        active(node_id)

      ["reply", specifier, data] ->
        reply(context, specifier, data)

      ["changed", specifier, data] ->
        changed(context, specifier, data)

      ["done", specifier, data] ->
        done(node_id, specifier, data)

      ["error_update", specifier, data] ->
        error_update(context, specifier, data)

      ["error_describe", specifier, data] ->
        error_response(:error_describe, node_id, specifier, data)

      ["error_deactivate", specifier, data] ->
        error_response(:error_deactivate, node_id, specifier, data)

      ["error_ping", specifier, data] ->
        error_response(:error_ping, node_id, specifier, data)

      ["error_activate", specifier, data] ->
        error_response(:error_activate, node_id, specifier, data)

      ["error_read", specifier, data] ->
        error_response(:error_read, node_id, specifier, data)

      ["error_change", specifier, data] ->
        error_response(:error_change, node_id, specifier, data)

      ["error_do", specifier, data] ->
        error_response(:error_do, node_id, specifier, data)

      ["ISSE,SECoP," <> _ = idn] ->
        idn(node_id, idn)

      ["ISSE&SINE2020,SECoP," <> _ = idn] ->
        idn(node_id, idn)

      _ ->
        Logger.warning("Unknown message received: #{trimmed}")
    end
  end

  defp idn(node_id, idn_string) do
    Logger.debug("IDN response received: #{idn_string}")
    [manufacturer, product, draft_date, version] = String.split(idn_string, ",", parts: 4)

    struct = %IDN{
      manufacturer: manufacturer,
      product: product,
      draft_date: draft_date,
      version: version
    }

    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _value} <- entries, do: send(pid, {:identification, struct})
    end)
  end

  defp split_specifier(specifier) do
    case :binary.split(specifier, ":") do
      [module, parameter] -> {:ok, module, parameter}
      _ -> {:error, :no_match}
    end
  end

  # SECoP qualifiers. Listed so their atoms exist before the first data
  # report is decoded.
  @qualifiers [:t, :e]
  @doc false
  def qualifiers, do: @qualifiers

  # Data and error reports come from the node, so their keys must not create
  # atoms: keys that are already atoms (qualifiers, struct members named in the
  # description) become atoms, any other key stays a string.
  defp decode_report(data), do: Jason.decode(data, keys: &existing_atom_or_string/1)

  defp existing_atom_or_string(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp existing_atom(string) do
    {:ok, String.to_existing_atom(string)}
  rescue
    ArgumentError -> :error
  end

  defp data_to_ets(context, specifier, data, extra_entries \\ []) do
    {:ok, data_report} = decode_report(data)
    store_report(context, specifier, data_report, extra_entries)
  end

  defp store_report(context, specifier, data_report, extra_entries) do
    {:ok, module, accessible} = split_specifier(specifier)

    with {:ok, module_atom} <- existing_atom(module),
         {:ok, accessible_atom} <- existing_atom(accessible) do
      {:ok, :inserted} =
        insert(context, [
          {{:data_report, module_atom, accessible_atom}, data_report} | extra_entries
        ])

      Phoenix.PubSub.local_broadcast(
        :secant_client_pubsub,
        context.value_topic,
        {:value_update, module, accessible, data_report}
      )
    else
      :error ->
        Logger.debug("Data report for undescribed accessible #{specifier} dropped")
    end

    {:ok, module, accessible, data_report}
  end

  defp insert(%Context{table: nil, node_id: node_id}, entries),
    do: NodeTable.insert(node_id, entries)

  defp insert(%Context{table: table}, entries), do: NodeTable.insert_into(table, entries)

  defp update_entries, do: [{:last_update_timestamp, System.monotonic_time(:millisecond)}]

  def update(node, specifier, data) do
    data_to_ets(context(node), specifier, data, update_entries())
  end

  def describe(node_id, specifier, data) do
    # Logger.debug("Describe message received. Specifier: #{specifier}, Data: #{data}")
    {:ok, description} = Jason.decode(data, keys: :atoms)

    opts = %{
      host: elem(node_id, 0),
      port: elem(node_id, 1)
    }

    parsed_description = parse_node_description(description, opts)

    {:ok, :inserted} = NodeTable.insert(node_id, :description, parsed_description)
    {:ok, :inserted} = NodeTable.insert(node_id, :raw_description, description)

    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:describe, specifier, parsed_description, description})
      end
    end)
  end

  def inactive(node_id) do
    Logger.debug("Deactivated update messages received.")

    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:inactive})
      end
    end)
  end

  def pong(node_id, id, data) do
    Logger.debug("Pong received. ID: #{id}, Data: #{data}")

    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:pong, id, data})
      end
    end)
  end

  def active(node_id) do
    Logger.debug("Active message received.")

    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:active})
      end
    end)
  end

  def reply(node, specifier, data) do
    Logger.debug("Read reply received. Specifier: #{specifier}, Data: #{data}")
    context = context(node)
    {:ok, module, parameter, data_report} = data_to_ets(context, specifier, data)

    Registry.dispatch(SecantClient.Registry.NodeStatem, context.node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:reply, module, parameter, data_report})
      end
    end)
  end

  def changed(node, specifier, data) do
    Logger.debug("Parameter successfully changed. Specifier: #{specifier}, Data: #{data}")
    context = context(node)
    {:ok, module, parameter, data_report} = data_to_ets(context, specifier, data)

    Registry.dispatch(SecantClient.Registry.NodeStatem, context.node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:changed, module, parameter, data_report})
      end
    end)
  end

  def done(node_id, specifier, data) do
    Logger.debug("Command executed. Specifier: #{specifier}, Data: #{data}")

    {:ok, data_report} = decode_report(data)

    {:ok, module, command} = split_specifier(specifier)

    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:done, module, command, data_report})
      end
    end)
  end

  def get_empty_values_map(parsed_description) do
    modules = parsed_description[:modules]

    empty_values_map =
      Enum.reduce(modules, %{}, fn {module_name, module_data}, acc ->
        parameteter_map = module_data[:parameters]

        parameters =
          Enum.reduce(parameteter_map, %{}, fn {param_name, _param_data}, param_acc ->
            Map.put(param_acc, param_name, nil)
          end)

        Map.put(acc, module_name, parameters)
      end)

    {:ok, empty_values_map}
  end

  def parse_node_description(description, opts) do
    node_descripttion = %{
      properties: Map.drop(description, [:modules])
    }

    # add all run parse_module_description for each module in desctription[:modules] and put result in a map
    modules =
      Enum.reduce(description[:modules], %{}, fn {module_name, module_description}, acc ->
        module_opts = Map.put(opts, :module, module_name)
        parsed_module_description = parse_module_description(module_description, module_opts)
        Map.put(acc, module_name, parsed_module_description)
      end)

    node_descripttion = Map.put(node_descripttion, :modules, modules)

    node_descripttion
  end

  def parse_module_description(module_description, _opts) do
    {parameters, commands} =
      Enum.reduce(module_description[:accessibles], {%{}, %{}}, fn {accessible_name,
                                                                    accessible_data},
                                                                   {param_acc, cmd_acc} ->
        if accessible_data[:datainfo][:type] != "command" do
          accessible_data = Map.put(accessible_data, :value, nil)
          {Map.put(param_acc, accessible_name, accessible_data), cmd_acc}
        else
          {param_acc, Map.put(cmd_acc, accessible_name, accessible_data)}
        end
      end)

    parsed_module_description = %{
      properties: Map.drop(module_description, [:accessibles]),
      parameters: parameters,
      commands: commands
    }

    parsed_module_description
  end

  def error_update(node, specifier, data) do
    context = context(node)
    {:ok, error_report} = decode_report(data)

    Logger.debug(fn ->
      "Error update message received. Specifier: #{specifier}, Data: #{inspect(error_report)}"
    end)

    {:ok, module, accessible} = split_specifier(specifier)

    with {:ok, module_atom} <- existing_atom(module),
         {:ok, accessible_atom} <- existing_atom(accessible) do
      {:ok, :inserted} =
        insert(context, [
          {{:data_report, module_atom, accessible_atom}, {:error_report, error_report}}
        ])

      Phoenix.PubSub.local_broadcast(
        :secant_client_pubsub,
        context.error_topic,
        {:error_update, module, accessible, error_report}
      )
    else
      :error -> Logger.debug("Error report for undescribed accessible #{specifier} dropped")
    end
  end

  def error_response(error_code, node_id, specifier, data) do
    {:ok, error_report} = decode_report(data)
    error_class = Enum.at(error_report, 0)
    error_text = Enum.at(error_report, 1)
    error_dict = Enum.at(error_report, 2)

    Registry.dispatch(SecantClient.Registry.NodeStatem, node_id, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {error_code, specifier, error_class, error_text, error_dict})
      end
    end)
  end
end
