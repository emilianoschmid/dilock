if System.get_env("DILOCK_CONSUL_URL") do
  defmodule Dilock.ConsulIntegrationTest do
    use ExUnit.Case, async: false
    @moduletag :integration

    test "independent providers contend, hand off and advance the retained-key fence" do
      namespace = System.get_env("DILOCK_NAMESPACE", "default")
      options = consul_options(namespace)
      assert :ok = Dilock.Consul.provision_namespace(options)

      {:ok, first_provider} = Dilock.Consul.start_link(options)
      {:ok, second_provider} = Dilock.Consul.start_link(options)

      on_exit(fn ->
        if Process.alive?(first_provider), do: GenServer.stop(first_provider)
        if Process.alive?(second_provider), do: GenServer.stop(second_provider)
      end)

      resource = "elixir-consul/#{System.unique_integer([:positive])}"
      {:ok, first_lock} = Dilock.lock(first_provider, namespace, resource)
      {:ok, second_lock} = Dilock.lock(second_provider, namespace, resource)
      {:ok, first_lease} = Dilock.Lock.acquire(first_lock, %{wait_ms: 1_000, lease_ms: 10_000})

      assert {:ok, %Dilock.Consul.Fence{epoch: epoch, lock_index: first_index}} =
               Dilock.Consul.fence(Dilock.Lease.ownership(first_lease))

      assert epoch == options[:epoch]
      assert first_index > 0
      assert :busy = Dilock.Lock.try_acquire(second_lock, %{lease_ms: 10_000})
      assert :released = Dilock.Lease.release(first_lease)

      {:ok, next_lease} = Dilock.Lock.acquire(second_lock, %{wait_ms: 2_000, lease_ms: 10_000})
      assert {:ok, %Dilock.Consul.Fence{epoch: ^epoch, lock_index: next_index}} =
               Dilock.Consul.fence(Dilock.Lease.ownership(next_lease))
      assert next_index > first_index
      assert :released = Dilock.Lease.release(next_lease)
      assert :ok = Dilock.Lease.close(first_lease)
      assert :ok = Dilock.Lease.close(next_lease)
    end

    defp consul_options(namespace) do
      [
        endpoint: System.fetch_env!("DILOCK_CONSUL_URL"),
        namespace: namespace,
        epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH"),
        token: System.get_env("DILOCK_CONSUL_TOKEN")
      ]
      |> Keyword.reject(fn {_key, value} -> is_nil(value) end)
    end
  end
end
