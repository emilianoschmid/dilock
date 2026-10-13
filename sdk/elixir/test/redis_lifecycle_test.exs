defmodule Dilock.RedisLifecycleTest do
  use ExUnit.Case, async: true

  test "late positive SET acknowledgement is cleaned and classified by its acknowledgement" do
    cleanup_calls = :counters.new(1, [:atomics])

    cleanup = fn ->
      :counters.add(cleanup_calls, 1, 1)
      :released
    end

    assert {:error, :acquisition_timeout} =
             Dilock.Redis.late_wait_cleanup(true, 100, 100, cleanup)

    assert :counters.get(cleanup_calls, 1) == 1
  end

  test "late positive SET acknowledgement with unconfirmed cleanup is acquisition unknown" do
    assert {:error, {:acquisition_unknown, :late_acquisition_cleanup_unconfirmed}} =
             Dilock.Redis.late_wait_cleanup(true, 100, 101, fn -> :unknown end)
  end

  test "expired lease budget with acknowledged cleanup is a timeout, not an unknown outcome" do
    assert {:error, :acquisition_timeout} =
             Dilock.Redis.late_acquisition_cleanup(fn -> :already_not_owned end)
  end

  test "expired lease budget with unconfirmed cleanup remains acquisition unknown" do
    assert {:error, {:acquisition_unknown, :late_acquisition_cleanup_unconfirmed}} =
             Dilock.Redis.late_acquisition_cleanup(fn -> :unknown end)
  end

  test "one-shot acquisition does not apply the caller wait deadline" do
    assert :continue =
             Dilock.Redis.late_wait_cleanup(false, 100, 101, fn ->
               flunk("cleanup is not needed")
             end)
  end
end
