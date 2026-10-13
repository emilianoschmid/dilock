defmodule Dilock.ProviderTest do
  use ExUnit.Case, async: true

  defmodule UnknownReleaseBackend do
    @behaviour Dilock.Backend

    @impl true
    def validate(_options), do: :ok

    @impl true
    def init(options) do
      {:ok,
       %{
         test_pid: Keyword.fetch!(options, :test_pid),
         release_outcome: Keyword.get(options, :release_outcome, :unknown)
       }}
    end

    @impl true
    def close(_state), do: :ok

    @impl true
    def acquire(_state, _id, _options, _config) do
      now = System.monotonic_time(:millisecond)
      {:ok, %{effective_lease_ms: 30_000, deadline_ms: now + 30_000}}
    end

    @impl true
    def renew(_state, _ownership, _ttl_ms, _options), do: :ok

    @impl true
    def release(state, _ownership, _options) do
      send(state.test_pid, :backend_release_attempt)
      state.release_outcome
    end
  end

  defmodule LateAcquireRaisingReleaseBackend do
    @behaviour Dilock.Backend

    @impl true
    def validate(_options), do: :ok

    @impl true
    def init(options), do: {:ok, %{test_pid: Keyword.fetch!(options, :test_pid)}}

    @impl true
    def close(_state), do: :ok

    @impl true
    def acquire(state, _id, _options, _config) do
      send(state.test_pid, {:backend_acquire_waiting, self()})

      receive do
        :return_acquired_ownership ->
          now = System.monotonic_time(:millisecond)
          {:ok, %{effective_lease_ms: 30_000, deadline_ms: now + 30_000}}
      after
        5_000 -> {:error, :test_acquisition_timed_out}
      end
    end

    @impl true
    def renew(_state, _ownership, _ttl_ms, _options), do: :ok

    @impl true
    def release(state, _ownership, _options) do
      send(state.test_pid, :orphaned_backend_release_attempt)
      raise "simulated cleanup transport exception"
    end
  end

  defmodule BlockingUnknownReleaseBackend do
    @behaviour Dilock.Backend

    @impl true
    def validate(_options), do: :ok

    @impl true
    def init(options), do: {:ok, %{test_pid: Keyword.fetch!(options, :test_pid)}}

    @impl true
    def close(_state), do: :ok

    @impl true
    def acquire(_state, _id, _options, _config) do
      now = System.monotonic_time(:millisecond)
      {:ok, %{effective_lease_ms: 30_000, deadline_ms: now + 30_000}}
    end

    @impl true
    def renew(_state, _ownership, _ttl_ms, _options), do: :ok

    @impl true
    def release(state, _ownership, _options) do
      send(state.test_pid, {:backend_release_waiting, self()})

      receive do
        :finish_release -> :unknown
      after
        5_000 -> :unknown
      end
    end
  end

  defmodule RaisingReleaseBackend do
    @behaviour Dilock.Backend

    @impl true
    def validate(_options), do: :ok

    @impl true
    def init(options), do: {:ok, %{test_pid: Keyword.fetch!(options, :test_pid)}}

    @impl true
    def close(_state), do: :ok

    @impl true
    def acquire(_state, _id, _options, _config) do
      now = System.monotonic_time(:millisecond)
      {:ok, %{effective_lease_ms: 30_000, deadline_ms: now + 30_000}}
    end

    @impl true
    def renew(_state, _ownership, _ttl_ms, _options), do: :ok

    @impl true
    def release(state, _ownership, _options) do
      send(state.test_pid, :backend_release_attempt)
      raise "simulated transport exception"
    end
  end

  defmodule DelayedBackend do
    @behaviour Dilock.Backend

    @impl true
    def validate(_options), do: :ok

    @impl true
    def init(options) do
      {:ok,
       %{
         test_pid: Keyword.fetch!(options, :test_pid),
         delay_ms: Keyword.fetch!(options, :delay_ms)
       }}
    end

    @impl true
    def close(_state), do: :ok

    @impl true
    def acquire(_state, _id, _options, _config) do
      now = System.monotonic_time(:millisecond)
      {:ok, %{effective_lease_ms: 30_000, deadline_ms: now + 30_000}}
    end

    @impl true
    def renew(state, _ownership, _ttl_ms, _options) do
      Process.sleep(state.delay_ms)
      send(state.test_pid, :backend_renew_finished)
      :ok
    end

    @impl true
    def release(state, _ownership, _options) do
      Process.sleep(state.delay_ms)
      send(state.test_pid, :backend_release_finished)
      :released
    end
  end

  defmodule UncertainRenewBackend do
    @behaviour Dilock.Backend

    @impl true
    def validate(_options), do: :ok

    @impl true
    def init(options), do: {:ok, %{test_pid: Keyword.fetch!(options, :test_pid)}}

    @impl true
    def close(_state), do: :ok

    @impl true
    def acquire(_state, _id, _options, _config) do
      now = System.monotonic_time(:millisecond)
      {:ok, %{effective_lease_ms: 30_000, deadline_ms: now + 30_000}}
    end

    @impl true
    def renew(state, _ownership, _ttl_ms, _options) do
      send(state.test_pid, :backend_renew_attempt)
      {:error, :backend_unavailable}
    end

    @impl true
    def release(_state, _ownership, _options), do: :released
  end

  defmodule PreDispatchRenewBackend do
    @behaviour Dilock.Backend

    @impl true
    def validate(_options), do: :ok

    @impl true
    def init(options), do: {:ok, %{test_pid: Keyword.fetch!(options, :test_pid)}}

    @impl true
    def close(_state), do: :ok

    @impl true
    def acquire(_state, _id, _options, _config) do
      now = System.monotonic_time(:millisecond)
      {:ok, %{effective_lease_ms: 30_000, deadline_ms: now + 30_000}}
    end

    @impl true
    def renew(_state, _ownership, 86_400_001, _options), do: {:error, :invalid_ttl}
    def renew(_state, _ownership, 31_000, _options), do: {:error, :unsupported_capability}

    def renew(state, _ownership, 30_000, _options) do
      send(state.test_pid, :valid_renew_dispatched)
      :ok
    end

    @impl true
    def release(_state, _ownership, _options), do: :released
  end

  test "a backend task crash is reported as acquisition unknown" do
    ref = make_ref()
    tag = make_ref()
    from = {self(), tag}
    state = %{pending: %{ref => %{from: from, owner: self()}}}

    assert {:noreply, next_state} =
             Dilock.Provider.handle_info({:DOWN, ref, :process, self(), :killed}, state)

    assert next_state.pending == %{}
    assert_receive {^tag, {:error, {:acquisition_unknown, {:backend_task_down, :killed}}}}
  end

  test "late acquisition cleanup uncertainty is observed without crashing the provider" do
    test_pid = self()
    observer = fn event -> send(test_pid, {:cleanup_observed, event}) end

    {:ok, provider} =
      Dilock.Provider.start_link(
        backend: LateAcquireRaisingReleaseBackend,
        test_pid: self(),
        cleanup_observer: observer
      )

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)

    caller =
      spawn(fn ->
        Dilock.Provider.acquire(
          provider,
          "resource-id",
          %{wait_ms: 1_000, lease_ms: 30_000},
          true
        )
      end)

    assert_receive {:backend_acquire_waiting, backend_task}, 1_000
    Process.exit(caller, :kill)
    send(backend_task, :return_acquired_ownership)

    assert_receive :orphaned_backend_release_attempt, 1_000

    assert_receive {:cleanup_observed,
                    %{
                      backend: LateAcquireRaisingReleaseBackend,
                      outcome: :unknown,
                      reason: :caller_cancelled_after_dispatch
                    }},
                   1_000

    assert Process.alive?(provider)
  end

  test "release uncertainty is observed if its caller terminates before receiving the result" do
    test_pid = self()
    observer = fn event -> send(test_pid, {:cleanup_observed, event}) end

    {:ok, provider} =
      Dilock.Provider.start_link(
        backend: BlockingUnknownReleaseBackend,
        test_pid: self(),
        cleanup_observer: observer
      )

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)

    caller =
      spawn(fn ->
        {:ok, lock} = Dilock.lock(provider, "test", "release-resource")
        {:ok, lease} = Dilock.Lock.try_acquire(lock)
        send(test_pid, {:release_caller_has_lease, lease})
        Dilock.Lease.release(lease)
      end)

    assert_receive {:release_caller_has_lease, _lease}, 1_000
    assert_receive {:backend_release_waiting, provider}, 1_000
    Process.exit(caller, :kill)
    send(provider, :finish_release)

    assert_receive {:cleanup_observed,
                    %{
                      backend: BlockingUnknownReleaseBackend,
                      outcome: :unknown,
                      reason: :owner_terminated_after_release
                    }},
                   1_000

    assert Process.alive?(provider)
  end

  test "lease monitor cleanup does not retry an unknown release" do
    {:ok, provider} = Dilock.Provider.start_link(backend: UnknownReleaseBackend, test_pid: self())
    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)

    {:ok, lock} = Dilock.lock(provider, "release-unknown-test", "resource")
    {:ok, lease} = Dilock.Lock.try_acquire(lock)

    assert :unknown = Dilock.Lease.release(lease)
    assert_receive :backend_release_attempt
    assert :ok = Dilock.Lease.close(lease)
    refute_receive :backend_release_attempt, 100
  end

  test "with_lease preserves a body exception and observes unknown cleanup" do
    test_pid = self()
    observer = fn event -> send(test_pid, {:cleanup_observed, event}) end

    {:ok, provider} =
      Dilock.Provider.start_link(
        backend: UnknownReleaseBackend,
        test_pid: self(),
        cleanup_observer: observer
      )

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "scope-unknown-test", "resource")

    error =
      assert_raise RuntimeError, "work failed", fn ->
        Dilock.with_lease(lock, fn _lease -> raise "work failed" end)
      end

    assert error.message == "work failed"
    assert_receive :backend_release_attempt

    assert_receive {:cleanup_observed,
                    %{backend: UnknownReleaseBackend, outcome: :unknown, reason: :body_failed}}
  end

  test "with_lease returns and observes an unacknowledged scope release" do
    test_pid = self()
    observer = fn event -> send(test_pid, {:cleanup_observed, event}) end

    {:ok, provider} =
      Dilock.Provider.start_link(
        backend: UnknownReleaseBackend,
        test_pid: self(),
        cleanup_observer: observer
      )

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "scope-success-unknown-test", "resource")

    assert {:error, {:cleanup_not_acknowledged, :unknown, :work_result}} =
             Dilock.with_lease(lock, fn _lease -> :work_result end)

    assert_receive :backend_release_attempt

    assert_receive {:cleanup_observed,
                    %{backend: UnknownReleaseBackend, outcome: :unknown, reason: :scope_exit}}
  end

  test "with_lease never reports success when the lease is already not owned" do
    test_pid = self()
    observer = fn event -> send(test_pid, {:cleanup_observed, event}) end

    {:ok, provider} =
      Dilock.Provider.start_link(
        backend: UnknownReleaseBackend,
        test_pid: self(),
        release_outcome: :already_not_owned,
        cleanup_observer: observer
      )

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "scope-already-not-owned-test", "resource")

    assert {:error, {:lease_lost, :already_not_owned, :work_result}} =
             Dilock.with_lease(lock, fn _lease -> :work_result end)

    assert_receive :backend_release_attempt
    refute_receive {:cleanup_observed, _event}, 50
  end

  test "with_lease reports unknown cleanup when the lease process has already stopped" do
    {:ok, provider} = Dilock.Provider.start_link(backend: UnknownReleaseBackend, test_pid: self())
    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "scope-dead-lease-test", "resource")

    assert {:error, {:cleanup_not_acknowledged, :unknown, :work_result}} =
             Dilock.with_lease(lock, fn lease ->
               Process.exit(lease, :kill)
               Process.sleep(10)
               :work_result
             end)
  end

  test "with_lease preserves a work exception after its lease process has stopped" do
    {:ok, provider} = Dilock.Provider.start_link(backend: UnknownReleaseBackend, test_pid: self())
    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "scope-dead-lease-exception-test", "resource")

    assert_raise RuntimeError, "work failed", fn ->
      Dilock.with_lease(lock, fn lease ->
        Process.exit(lease, :kill)
        Process.sleep(10)
        raise "work failed"
      end)
    end
  end

  test "an exception during monitor cleanup does not crash the provider" do
    {:ok, provider} = Dilock.Provider.start_link(backend: RaisingReleaseBackend, test_pid: self())
    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    provider_monitor = Process.monitor(provider)

    {:ok, lock} = Dilock.lock(provider, "release-exception-test", "resource")
    {:ok, lease} = Dilock.Lock.try_acquire(lock)
    Process.exit(lease, :kill)

    assert_receive :backend_release_attempt
    assert is_map(:sys.get_state(provider))
    refute_receive {:DOWN, ^provider_monitor, :process, ^provider, _reason}, 100
  end

  test "release waits beyond the former fixed caller timeout for its bounded backend cleanup" do
    {:ok, provider} =
      Dilock.Provider.start_link(
        backend: DelayedBackend,
        test_pid: self(),
        delay_ms: 3_100,
        cleanup_timeout_ms: 5_000
      )

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "release-budget-test", "resource")
    {:ok, lease} = Dilock.Lock.try_acquire(lock)

    assert :released = Dilock.Lease.release(lease)
    assert_receive :backend_release_finished
    assert :ok = Dilock.Lease.close(lease)
  end

  test "renew waits beyond the former fixed caller timeout for its bounded backend RPC" do
    {:ok, provider} =
      Dilock.Provider.start_link(
        backend: DelayedBackend,
        test_pid: self(),
        delay_ms: 3_100,
        rpc_timeout_ms: 5_000,
        cleanup_timeout_ms: 5_000
      )

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "renew-budget-test", "resource")
    {:ok, lease} = Dilock.Lock.try_acquire(lock)

    assert :ok = Dilock.Lease.renew(lease)
    assert_receive :backend_renew_finished
    assert :released = Dilock.Lease.release(lease)
    assert :ok = Dilock.Lease.close(lease)
  end

  test "an unconfirmed renewal immediately marks the lease lost and prevents retries" do
    {:ok, provider} =
      Dilock.Provider.start_link(backend: UncertainRenewBackend, test_pid: self())

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "renew-unknown-test", "resource")
    {:ok, lease} = Dilock.Lock.try_acquire(lock)

    assert {:error, :backend_unavailable} = Dilock.Lease.renew(lease)
    assert_receive :backend_renew_attempt
    assert {:error, :lease_lost} = Dilock.Lease.renew(lease)
    refute_receive :backend_renew_attempt
    assert :released = Dilock.Lease.release(lease)
    assert {:error, :lease_lost} = Dilock.Lease.renew(lease)
    refute_receive :backend_renew_attempt
  end

  test "pre-dispatch renewal validation errors preserve held state" do
    {:ok, provider} =
      Dilock.Provider.start_link(backend: PreDispatchRenewBackend, test_pid: self())

    on_exit(fn -> if Process.alive?(provider), do: GenServer.stop(provider) end)
    {:ok, lock} = Dilock.lock(provider, "renew-validation-test", "resource")
    {:ok, lease} = Dilock.Lock.try_acquire(lock)

    assert {:error, :invalid_ttl} = Dilock.Lease.renew(lease, 86_400_001)
    assert {:error, :unsupported_capability} = Dilock.Lease.renew(lease, 31_000)
    assert :ok = Dilock.Lease.renew(lease, 30_000)
    assert_receive :valid_renew_dispatched
    assert :released = Dilock.Lease.release(lease)
    assert {:error, :lease_lost} = Dilock.Lease.renew(lease)
    refute_receive :valid_renew_dispatched
  end
end
