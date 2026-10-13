if System.get_env("DILOCK_ERLANG_BEAM") do
  defmodule Dilock.ErlangFacadeLiveTest do
    use ExUnit.Case, async: false
    @moduletag :integration

    setup_all do
      beam_path = System.fetch_env!("DILOCK_ERLANG_BEAM")
      # Keep the Mix-built application's ebin first so :code.priv_dir/1
      # continues to resolve Dilock's Redis scripts from the app directory.
      true = :code.add_pathz(String.to_charlist(beam_path))
      :ok
    end

    test "Erlang facade contends and hands off a live Redis lease" do
      endpoint = System.fetch_env!("DILOCK_REDIS_URL")
      namespace = "erlang-live-#{System.unique_integer([:positive])}"
      epoch = "erlang-epoch-#{System.unique_integer([:positive])}"
      options = [endpoint: endpoint, namespace: namespace, epoch: epoch]
      assert :ok = Dilock.Redis.provision_namespace(options)

      {:ok, first_provider} =
        erlang(:start_redis, [
          [
            {:endpoint, endpoint},
            {:namespace, namespace},
            {:epoch, epoch}
          ]
        ])

      {:ok, second_provider} =
        erlang(:start_redis, [
          [
            {:endpoint, endpoint},
            {:namespace, namespace},
            {:epoch, epoch}
          ]
        ])

      on_exit(fn ->
        if Process.alive?(first_provider), do: GenServer.stop(first_provider)
        if Process.alive?(second_provider), do: GenServer.stop(second_provider)
      end)

      {:ok, first_lock} = erlang(:lock, [first_provider, namespace, "handoff"])
      {:ok, second_lock} = erlang(:lock, [second_provider, namespace, "handoff"])
      options = %{lease_ms: 5_000}

      assert {:ok, first_lease} = erlang(:try_acquire, [first_lock, options])
      assert :busy = erlang(:try_acquire, [second_lock, options])
      assert :released = erlang(:release, [first_lease])
      assert :released = erlang(:release, [first_lease])
      assert {:ok, successor} = erlang(:try_acquire, [second_lock, options])
      assert :busy = erlang(:try_acquire, [first_lock, options])
      assert :released = erlang(:release, [successor])
      assert :released = erlang(:close_lease, [first_lease])
      assert :released = erlang(:close_lease, [successor])
    end

    test "Erlang facade queues etcd Lock and advances the verified fence" do
      endpoint = System.fetch_env!("DILOCK_ETCD_URL")
      namespace = System.fetch_env!("DILOCK_NAMESPACE")
      epoch = System.fetch_env!("DILOCK_COORDINATION_EPOCH")

      {:ok, first_provider} =
        erlang(:start_etcd, [
          [
            {:endpoint, endpoint},
            {:namespace, namespace},
            {:epoch, epoch}
          ]
        ])

      {:ok, second_provider} =
        erlang(:start_etcd, [
          [
            {:endpoint, endpoint},
            {:namespace, namespace},
            {:epoch, epoch}
          ]
        ])

      on_exit(fn ->
        if Process.alive?(first_provider), do: GenServer.stop(first_provider)
        if Process.alive?(second_provider), do: GenServer.stop(second_provider)
      end)

      resource = "erlang-etcd/#{System.unique_integer([:positive])}"
      {:ok, first_lock} = erlang(:lock, [first_provider, namespace, resource])
      {:ok, second_lock} = erlang(:lock, [second_provider, namespace, resource])
      options = %{wait_ms: 5_000, lease_ms: 5_000}
      {:ok, first_lease} = erlang(:acquire, [first_lock, options])
      ownership = erlang(:ownership, [first_lease])
      assert is_map(ownership)
      refute match?({:ok, _}, ownership)
      {:ok, first_fence} = erlang(:fence, [ownership])
      assert first_fence.epoch == epoch
      assert first_fence.revision > 0

      parent = self()

      queued =
        Task.async(fn ->
          {:ok, lease} = erlang(:acquire, [second_lock, options])
          send(parent, {:erlang_facade_successor, lease})
          receive do: (:keep_lease_alive -> :ok)
        end)

      assert Task.yield(queued, 250) == nil
      assert :released = erlang(:release, [first_lease])
      assert_receive {:erlang_facade_successor, successor}, 5_000

      try do
        {:ok, next_fence} = erlang(:fence, [erlang(:ownership, [successor])])
        assert next_fence.epoch == epoch
        assert next_fence.revision > first_fence.revision
        assert :released = erlang(:release, [successor])
      after
        send(queued.pid, :keep_lease_alive)
      end

      assert :ok = Task.await(queued, 5_000)
      assert :released = erlang(:close_lease, [first_lease])
    end

    defp erlang(function, arguments), do: apply(:dilock_erlang, function, arguments)
  end
end
