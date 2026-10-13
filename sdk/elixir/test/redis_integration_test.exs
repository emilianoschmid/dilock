if System.get_env("DILOCK_REDIS_URL") do
  defmodule Dilock.RedisIntegrationTest do
    use ExUnit.Case, async: false
    @moduletag :integration

    test "independent providers contend, hand off, renew and preserve a successor" do
      namespace = "elixir-test-#{System.unique_integer([:positive])}"

      options = [
        endpoint: System.fetch_env!("DILOCK_REDIS_URL"),
        namespace: namespace,
        epoch: "test-#{System.unique_integer([:positive])}",
        rpc_timeout_ms: 2_000,
        cleanup_timeout_ms: 2_000
      ]

      assert :ok = Dilock.Redis.provision_namespace(options)
      {:ok, first_provider} = Dilock.Redis.start_link(options)
      {:ok, second_provider} = Dilock.Redis.start_link(options)

      on_exit(fn ->
        if Process.alive?(first_provider), do: GenServer.stop(first_provider)
        if Process.alive?(second_provider), do: GenServer.stop(second_provider)
        cleanup_namespace(options)
      end)

      resource = "redis-integration/#{System.unique_integer([:positive])}"
      {:ok, first_lock} = Dilock.lock(first_provider, namespace, resource)
      {:ok, second_lock} = Dilock.lock(second_provider, namespace, resource)

      assert {:ok, first_lease} =
               Dilock.Lock.try_acquire(first_lock, %{lease_ms: 2_000})

      assert :busy = Dilock.Lock.try_acquire(second_lock, %{lease_ms: 2_000})
      assert :released = Dilock.Lease.release(first_lease)
      assert :released = Dilock.Lease.release(first_lease)
      assert :ok = Dilock.Lease.close(first_lease)

      assert {:ok, value} =
               Dilock.with_lease(first_lock, %{wait_ms: 1_000, lease_ms: 2_000}, fn _lease ->
                 :callback_result
               end)

      assert value == :callback_result

      assert {:ok, expiring_lease} =
               Dilock.Lock.try_acquire(first_lock, %{lease_ms: 250})

      Process.sleep(350)

      assert {:ok, successor} =
               Dilock.Lock.try_acquire(second_lock, %{lease_ms: 2_000})

      assert :already_not_owned = Dilock.Lease.release(expiring_lease)
      assert :busy = Dilock.Lock.try_acquire(first_lock, %{lease_ms: 2_000})
      assert :released = Dilock.Lease.release(successor)
      assert :ok = Dilock.Lease.close(expiring_lease)
      assert :ok = Dilock.Lease.close(successor)
    end

    test "auto-renew extends the real Redis TTL beyond its original lease" do
      namespace = "elixir-renew-#{System.unique_integer([:positive])}"

      options = [
        endpoint: System.fetch_env!("DILOCK_REDIS_URL"),
        namespace: namespace,
        epoch: "test-#{System.unique_integer([:positive])}"
      ]

      assert :ok = Dilock.Redis.provision_namespace(options)
      {:ok, owner_provider} = Dilock.Redis.start_link(options)
      {:ok, contender_provider} = Dilock.Redis.start_link(options)

      on_exit(fn ->
        if Process.alive?(owner_provider), do: GenServer.stop(owner_provider)
        if Process.alive?(contender_provider), do: GenServer.stop(contender_provider)
        cleanup_namespace(options)
      end)

      {:ok, owner_lock} = Dilock.lock(owner_provider, namespace, "renewed-resource")
      {:ok, contender_lock} = Dilock.lock(contender_provider, namespace, "renewed-resource")

      assert {:ok, lease} =
               Dilock.Lock.try_acquire(owner_lock, %{lease_ms: 300})

      assert :ok = Dilock.Lease.renew(lease, 1_500)
      Process.sleep(500)

      assert :busy = Dilock.Lock.try_acquire(contender_lock, %{lease_ms: 500})
      assert :released = Dilock.Lease.release(lease)
      assert :ok = Dilock.Lease.close(lease)
    end

    test "scope reports lease loss when the real Redis lease expires during work" do
      namespace = "elixir-scope-expiry-#{System.unique_integer([:positive])}"

      options = [
        endpoint: System.fetch_env!("DILOCK_REDIS_URL"),
        namespace: namespace,
        epoch: "test-#{System.unique_integer([:positive])}"
      ]

      assert :ok = Dilock.Redis.provision_namespace(options)
      {:ok, provider} = Dilock.Redis.start_link(options)

      on_exit(fn ->
        if Process.alive?(provider), do: GenServer.stop(provider)
        cleanup_namespace(options)
      end)

      {:ok, lock} = Dilock.lock(provider, namespace, "expires-during-scope")

      assert {:error, {:lease_lost, :already_not_owned, :work_result}} =
               Dilock.with_lease(lock, %{wait_ms: 1_000, lease_ms: 250}, fn _lease ->
                 Process.sleep(400)
                 :work_result
               end)
    end

    defp cleanup_namespace(options) do
      case Redix.start_link(options[:endpoint], sync_connect: true) do
        {:ok, client} ->
          try do
            _ =
              Redix.command(client, ["DEL", "dilock:namespace:#{options[:namespace]}:meta"],
                timeout: 2_000
              )
          after
            GenServer.stop(client)
          end

        {:error, _reason} ->
          :ok
      end
    end
  end
end
