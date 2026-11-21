defmodule SecantClient.NodeDiscoverTest do
  alias SecantClient.{NodeDiscover, SocketStruct}
  use ExUnit.Case, async: true

  defp receive_datagram(message) do
    test_pid = self()

    state = %SocketStruct{
      callback: fn ip, port, msg -> send(test_pid, {:found, ip, port, msg}) end
    }

    {:noreply, ^state} =
      NodeDiscover.handle_info({:udp, nil, {10, 0, 0, 1}, 5_000, message}, state)
  end

  test "a node's answer calls the callback" do
    message = ~s({"SECoP": "node", "port": 10767, "equipment_id": "x"})
    receive_datagram(message)
    assert_received {:found, {10, 0, 0, 1}, 5_000, ^message}
  end

  test "other datagrams are ignored without crashing" do
    for message <- [
          "not json",
          ~s({"SECoP": "discover"}),
          ~s({"SECoP": "node"}),
          ~s({"SECoP": "node", "port": 70000}),
          ~s({"SECoP": "node", "port": "10767"}),
          ~s({"port": 10767})
        ] do
      receive_datagram(message)
    end

    refute_received {:found, _, _, _}
  end
end
