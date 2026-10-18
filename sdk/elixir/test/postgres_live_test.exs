defmodule Dilock.PostgresLiveTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  test "Elixir PostgreSQL route contends, renews and hands off" do
    dsn = System.get_env("DILOCK_POSTGRES_DSN")

    if is_nil(dsn) or dsn == "" do
      :ok
    else
      namespace = "elixir-postgres-live"
      epoch = "elixir-postgres-live-epoch"
      options = [connection_string: dsn, namespace: namespace, epoch: epoch]
      assert :ok = Dilock.Postgres.provision_namespace(options)

      {:ok, first_provider} = Dilock.Postgres.start_link(options)
      {:ok, second_provider} = Dilock.Postgres.start_link(options)

      on_exit(fn ->
        if Process.alive?(first_provider), do: GenServer.stop(first_provider)
        if Process.alive?(second_provider), do: GenServer.stop(second_provider)
      end)

      {:ok, first_lock} = Dilock.lock(first_provider, namespace, "shared")
      {:ok, second_lock} = Dilock.lock(second_provider, namespace, "shared")
      {:ok, other_lock} = Dilock.lock(second_provider, namespace, "other")
      assert first_lock.resource_id == second_lock.resource_id
      acquire_options = %{wait_ms: 500, lease_ms: 30_000}

      assert {:ok, first_lease} = Dilock.Lock.acquire(first_lock, acquire_options)
      assert %{connection: first_connection} = Dilock.Lease.ownership(first_lease)
      assert Process.alive?(first_connection)
      assert :busy = Dilock.Lock.try_acquire(second_lock, %{lease_ms: 30_000})
      assert {:ok, other_lease} = Dilock.Lock.acquire(other_lock, acquire_options)
      assert :ok = Dilock.Lease.renew(first_lease)
      assert :released = Dilock.Lease.release(other_lease)
      assert :released = Dilock.Lease.release(first_lease)
      assert {:ok, successor} = Dilock.Lock.acquire(second_lock, acquire_options)
      assert :released = Dilock.Lease.release(successor)
    end
  end
end
