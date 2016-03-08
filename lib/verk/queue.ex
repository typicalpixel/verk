defmodule Verk.Queue do
  @moduledoc """
  This module interacts with a queue
  """
  alias Verk.Job

  @doc """
  Counts how many jobs are enqueued on a queue
  """
  @spec count(String.t, GenServer.server) :: integer
  def count(queue, redis \\ Verk.Redis) do
    Redix.command!(redis, ["LLEN", queue_name(queue)])
  end

  @doc """
  Clears the `queue`
  """
  @spec clear(String.t, GenServer.server) :: boolean
  def clear(queue, redis \\ Verk.Redis) do
    Redix.command!(redis, ["DEL", queue_name(queue)]) == 1
  end

  @doc """
  Lists enqueued jobs from `start` to `stop`
  """
  @spec range(String.t, integer, integer, GenServer.server) :: [Verk.Job.T]
  def range(queue, start \\ 0, stop \\ -1, redis) do
    for job <- Redix.command!(redis, ["LRANGE", queue_name(queue), start, stop]) do
      Job.decode!(job)
    end
  end

  @doc """
  Deletes the job from the queue
  """
  @spec delete_job(String.t, %Job{} | String.t, GenServer.server) :: boolean
  def delete_job(queue, %Job{ original_json: original_json }, redis) do
    Redix.command!(redis, ["LREM", queue_name(queue), 1, original_json]) == 1
  end
  def delete_job(queue, original_json, redis) do
    Redix.command!(redis, ["LREM", queue_name(queue), 1, original_json]) == 1
  end

  defp queue_name(queue), do: "queue:#{queue}"
end
