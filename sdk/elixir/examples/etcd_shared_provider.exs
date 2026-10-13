namespace = System.get_env("DILOCK_NAMESPACE", "default")

{:ok, provider} =
  Dilock.Etcd.start_link(
    endpoint: System.get_env("ETCD_URL", "http://127.0.0.1:2379"),
    namespace: namespace,
    epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH"),
    rpc_timeout_ms: 1_000,
    cleanup_timeout_ms: 1_000
  )

try do
  # Handles are independent resources, but share the provider's gRPC channel.
  {:ok, first_lock} = Dilock.lock(provider, namespace, "invoice/123")
  {:ok, second_lock} = Dilock.lock(provider, namespace, "invoice/124")

  for {lock, resource, policy} <- [
        {first_lock, "invoice/123", :defaults},
        {second_lock, "invoice/124", %{wait_ms: 5_000, lease_ms: 30_000}}
      ] do
    operation = fn lease ->
      {:ok, %Dilock.Etcd.Fence{} = fence} =
        Dilock.Etcd.fence(Dilock.Lease.ownership(lease))

      IO.inspect(fence, label: "verified etcd fence for #{resource}")
    end

    result =
      case policy do
        :defaults -> Dilock.with_lease(lock, operation)
        options -> Dilock.with_lease(lock, options, operation)
      end

    case result do
      {:ok, _value} -> :ok
      {:error, reason} -> raise "Dilock failure for #{resource}: #{inspect(reason)}"
    end
  end
after
  GenServer.stop(provider)
end
