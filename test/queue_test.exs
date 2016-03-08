defmodule Verk.QueueTest do
  use ExUnit.Case
  import Verk.Queue

  @queue     "default"
  @queue_key "queue:default"

  setup do
    { :ok, pid } = Application.fetch_env(:verk, :redis_url)
                    |> elem(1)
                    |> Redix.start_link
    Redix.command!(pid, ~w(DEL #{@queue_key}))
    on_exit fn ->
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}
    end
    { :ok, redis: pid }
  end

  test "count empty queue", %{ redis: redis } do
    assert count(@queue, redis) == 0
  end

  test "count", %{ redis: redis } do
    Redix.command!(redis, ~w(LPUSH #{@queue_key} 1 2 3))

    assert count(@queue, redis) == 3
  end

  test "clear", %{ redis: redis } do
    Redix.command!(redis, ~w(LPUSH #{@queue_key} 1 2 3))

    assert clear(@queue, redis)

    assert Redix.command!(redis, ~w(GET #{@queue_key})) == nil
  end

  test "range", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    Redix.command!(redis, ~w(LPUSH #{@queue_key} #{json}))

    assert range(@queue, redis) == [%{ job | original_json: json }]
  end

  test "range with no items", %{ redis: redis } do
    assert range(@queue, redis) == []
  end

  test "delete_job having job with original_json", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    Redix.command!(redis, ~w(LPUSH #{@queue_key} #{json}))

    job = %{ job | original_json: json}

    assert delete_job(@queue, job, redis) == true
  end

  test "delete_job with original_json", %{ redis: redis } do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

    Redix.command!(redis, ~w(LPUSH #{@queue_key} #{json}))

    assert delete_job(@queue, json, redis) == true
  end
end
