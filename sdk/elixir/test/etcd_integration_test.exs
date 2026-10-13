if System.get_env("DILOCK_ETCD_URL") do
  defmodule Dilock.EtcdIntegrationTest do
    use ExUnit.Case, async: false
    @moduletag :integration

    test "independent providers queue, hand off, advance fences and release" do
      namespace = System.get_env("DILOCK_NAMESPACE", "default")

      options = [
        endpoint: System.fetch_env!("DILOCK_ETCD_URL"),
        namespace: namespace,
        epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH")
      ]

      {:ok, first_provider} = Dilock.Etcd.start_link(options)
      {:ok, second_provider} = Dilock.Etcd.start_link(options)

      on_exit(fn ->
        if Process.alive?(first_provider), do: GenServer.stop(first_provider)
        if Process.alive?(second_provider), do: GenServer.stop(second_provider)
      end)

      resource = "elixir-integration/#{System.unique_integer([:positive])}"

      {:ok, first_lock} =
        Dilock.lock(
          first_provider,
          namespace,
          resource
        )

      {:ok, second_lock} = Dilock.lock(second_provider, namespace, resource)

      assert {:error, :unsupported_capability} =
               Dilock.Lock.try_acquire(second_lock, %{lease_ms: 5_000})

      {:ok, first_lease} =
        Dilock.Lock.acquire(first_lock, %{wait_ms: 2_000, lease_ms: 5_000})

      assert {:ok, %Dilock.Etcd.Fence{epoch: epoch, revision: revision}} =
               Dilock.Etcd.fence(Dilock.Lease.ownership(first_lease))

      assert epoch == options[:epoch]
      assert revision > 0

      test_process = self()

      queued =
        Task.async(fn ->
          {:ok, lease} = Dilock.Lock.acquire(second_lock, %{wait_ms: 5_000, lease_ms: 5_000})
          send(test_process, {:second_lease_acquired, lease})
          receive do: (:keep_owner_alive -> :ok)
        end)

      assert Task.yield(queued, 250) == nil
      assert :released = Dilock.Lease.release(first_lease)

      assert_receive {:second_lease_acquired, second_lease}, 5_000

      assert {:ok, %Dilock.Etcd.Fence{epoch: ^epoch, revision: next_revision}} =
               Dilock.Etcd.fence(Dilock.Lease.ownership(second_lease))

      assert next_revision > revision
      assert :released = Dilock.Lease.release(second_lease)
      assert :ok = Dilock.Lease.close(first_lease)
      assert :ok = Dilock.Lease.close(second_lease)
      send(queued.pid, :keep_owner_alive)
      assert :ok = Task.await(queued, 5_000)
    end

    test "renewal preserves ownership beyond the original etcd lease TTL" do
      namespace = System.get_env("DILOCK_NAMESPACE", "default")

      options =
        Keyword.merge(etcd_options(namespace), rpc_timeout_ms: 500, cleanup_timeout_ms: 500)

      {:ok, owner_provider} = Dilock.Etcd.start_link(options)
      {:ok, contender_provider} = Dilock.Etcd.start_link(options)

      on_exit(fn ->
        if Process.alive?(owner_provider), do: GenServer.stop(owner_provider)
        if Process.alive?(contender_provider), do: GenServer.stop(contender_provider)
      end)

      resource = "elixir-renewal/#{System.unique_integer([:positive])}"
      {:ok, owner_lock} = Dilock.lock(owner_provider, namespace, resource)
      {:ok, contender_lock} = Dilock.lock(contender_provider, namespace, resource)
      {:ok, lease} = Dilock.Lock.acquire(owner_lock, %{wait_ms: 1_000, lease_ms: 3_000})

      assert :ok = Dilock.Lease.renew(lease, 3_000)
      # The explicit renewal extends ownership past the original ~2s TTL;
      # keep the assertion within the renewed 3s server TTL.
      Process.sleep(1_500)

      assert {:error, {:acquisition_unknown, _reason, :ok}} =
               Dilock.Lock.acquire(contender_lock, %{wait_ms: 100, lease_ms: 3_000})

      ownership = Dilock.Lease.ownership(lease)

      assert {:ok, %{kvs: [%{lease: owner_lease_id}]}} =
               Etcdserverpb.KV.Stub.range(
                 ownership.backend_state.channel,
                 %Etcdserverpb.RangeRequest{key: ownership.opaque_key},
                 timeout: 500
               )

      assert owner_lease_id == ownership.lease_id

      assert :released = Dilock.Lease.release(lease)

      assert {:ok, %{kvs: []}} =
               Etcdserverpb.KV.Stub.range(
                 ownership.backend_state.channel,
                 %Etcdserverpb.RangeRequest{key: ownership.opaque_key},
                 timeout: 500
               )

      assert :ok = Dilock.Lease.close(lease)
    end

    test "scope reports lease loss when the real etcd lease expires during work" do
      namespace = System.get_env("DILOCK_NAMESPACE", "default")

      options =
        Keyword.merge(etcd_options(namespace), rpc_timeout_ms: 500, cleanup_timeout_ms: 500)

      {:ok, provider} = Dilock.Etcd.start_link(options)

      on_exit(fn ->
        if Process.alive?(provider), do: GenServer.stop(provider)
      end)

      {:ok, lock} =
        Dilock.lock(
          provider,
          namespace,
          "elixir-scope-expiry/#{System.unique_integer([:positive])}"
        )

      assert {:error, {:lease_lost, :already_not_owned, :work_result}} =
               Dilock.with_lease(lock, %{wait_ms: 1_000, lease_ms: 1_000}, fn lease ->
                 ownership = Dilock.Lease.ownership(lease)
                 Process.sleep(ownership.effective_lease_ms + 10_000)

                 assert System.monotonic_time(:millisecond) > ownership.deadline_ms

                 assert {:ok, %{kvs: []}} =
                          Etcdserverpb.KV.Stub.range(
                            ownership.backend_state.channel,
                            %Etcdserverpb.RangeRequest{key: ownership.opaque_key},
                            timeout: 500
                          )

                 :work_result
               end)
    end

    test "acknowledged revoke releases this acquisition when exact-key preflight is uncertain" do
      namespace = System.get_env("DILOCK_NAMESPACE", "default")

      options =
        Keyword.merge(etcd_options(namespace), rpc_timeout_ms: 500, cleanup_timeout_ms: 500)

      {:ok, provider} = Dilock.Etcd.start_link(options)

      on_exit(fn ->
        if Process.alive?(provider), do: GenServer.stop(provider)
      end)

      {:ok, lock} =
        Dilock.lock(
          provider,
          namespace,
          "elixir-revoke-ack/#{System.unique_integer([:positive])}"
        )

      {:ok, lease} = Dilock.Lock.acquire(lock, %{wait_ms: 1_000, lease_ms: 3_000})
      ownership = Dilock.Lease.ownership(lease)
      state = ownership.backend_state

      outcome =
        Dilock.Etcd.release_with_budget(
          500,
          fn -> System.monotonic_time(:millisecond) end,
          fn _remaining -> :unknown end,
          fn _remaining -> flunk("Unlock must not run after uncertain ownership preflight") end,
          fn remaining ->
            case Etcdserverpb.Lease.Stub.lease_revoke(
                   state.channel,
                   %Etcdserverpb.LeaseRevokeRequest{ID: ownership.lease_id},
                   timeout: remaining
                 ) do
              {:ok, _response} -> :ok
              {:error, %GRPC.RPCError{status: status}} when status == 5 -> :already_not_owned
              {:error, reason} -> {:error, reason}
            end
          end
        )

      assert :already_not_owned = outcome

      assert {:ok, %{kvs: []}} =
               Etcdserverpb.KV.Stub.range(
                 state.channel,
                 %Etcdserverpb.RangeRequest{key: ownership.opaque_key},
                 timeout: 500
               )

      assert :ok = Dilock.Lease.close(lease)
    end

    defp etcd_options(namespace) do
      [
        endpoint: System.fetch_env!("DILOCK_ETCD_URL"),
        namespace: namespace,
        epoch: System.fetch_env!("DILOCK_COORDINATION_EPOCH")
      ]
    end
  end
end
