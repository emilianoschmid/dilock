options = [
  endpoint: System.fetch_env!("ETCD_URL"),
  namespace: System.get_env("DILOCK_NAMESPACE", "default"),
  epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH")
]

{:ok, provider} = Dilock.Etcd.start_link(options)
try do
  {:ok, lock} = Dilock.lock(provider, options[:namespace], "invoice/123")
  case Dilock.with_lease(lock, fn lease ->
         {:ok, %Dilock.Etcd.Fence{} = fence} = Dilock.Etcd.fence(Dilock.Lease.ownership(lease))
         IO.inspect(fence, label: "epoch-scoped etcd creation revision")
         :process_invoice
       end) do
    {:ok, :process_invoice} -> IO.puts("lease released")
    {:error, reason} -> raise "Dilock failure: #{inspect(reason)}"
  end
after
  GenServer.stop(provider)
end
