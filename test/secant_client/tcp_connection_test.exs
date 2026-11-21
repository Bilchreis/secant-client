defmodule SecantClient.TcpConnectionTest do
  alias SecantClient.{NodeTable, Parser, TcpConnection}
  use ExUnit.Case, async: false

  setup do
    port = rem(:erlang.unique_integer([:positive, :monotonic]), 50_000) + 10_000
    node_id = {~c"localhost", port}
    {:ok, _table} = NodeTable.start(node_id)
    # Atoms for the module and parameter, as the description would create them
    _ = [:tc_mod, :value]
    Phoenix.PubSub.subscribe(:secant_client_pubsub, "value_update:localhost:#{port}")
    {:ok, node_id: node_id}
  end

  defp update(value), do: ~s(update tc_mod:value [#{value},{"t":1.0}]\n)

  describe "handle_data/3 (issue #18)" do
    test "parses every complete line in order and keeps the rest", %{node_id: node_id} do
      data = update(1) <> update(2) <> update(3) <> "update tc_mod:va"

      assert TcpConnection.handle_data("", data, node_id) == "update tc_mod:va"

      for value <- [1, 2, 3] do
        assert_received {:value_update, "tc_mod", "value", [^value, %{t: 1.0}]}
      end

      assert NodeTable.lookup(node_id, {:data_report, :tc_mod, :value}) ==
               {:ok, [3, %{t: 1.0}]}
    end

    test "joins a line split over several packets", %{node_id: node_id} do
      rest = TcpConnection.handle_data("", "update tc_mod:value [4,", node_id)
      rest = TcpConnection.handle_data(rest, ~s({"t":2.0}), node_id)
      refute_received {:value_update, _, _, _}

      assert TcpConnection.handle_data(rest, "]\r\n", node_id) == ""
      assert_received {:value_update, "tc_mod", "value", [4, %{t: 2.0}]}
    end

    test "a malformed line does not stop the lines after it", %{node_id: node_id} do
      data = "update tc_mod:value [not json\n" <> update(5)

      assert TcpConnection.handle_data("", data, node_id) == ""
      assert_received {:value_update, "tc_mod", "value", [5, %{t: 1.0}]}
    end
  end

  describe "Parser data reports (issue #18)" do
    test "unknown keys in a data report stay strings and create no atoms", %{node_id: node_id} do
      key = "never_an_atom_#{System.unique_integer([:positive])}"

      Parser.parse(node_id, ~s(update tc_mod:value [{"#{key}":1},{"t":1.0}]))

      assert_received {:value_update, "tc_mod", "value", [%{^key => 1}, %{t: 1.0}]}
      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    end

    test "an update for an undescribed accessible is dropped", %{node_id: node_id} do
      name = "undescribed_#{System.unique_integer([:positive])}"

      Parser.parse(node_id, ~s(update #{name}:value [1,{}]))

      refute_received {:value_update, _, _, _}
    end

    test "an update stores the report and the timestamp", %{node_id: node_id} do
      Parser.parse(node_id, String.trim(update(6)))

      assert {:ok, [6, _]} = NodeTable.lookup(node_id, {:data_report, :tc_mod, :value})
      assert {:ok, ts} = NodeTable.lookup(node_id, :last_update_timestamp)
      assert is_integer(ts)
    end
  end
end
