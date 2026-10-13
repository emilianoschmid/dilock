defmodule Dilock.Provider do
  @moduledoc "Supervised backend lifecycle and dispatch. Start it under an application supervisor."
  use GenServer

  def start_link(options),
    do: GenServer.start_link(__MODULE__, options, Keyword.take(options, [:name]))

  def child_spec(options),
    do: %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [options]},
      type: :worker
    }

  # Backend adapters enforce their own finite RPC and cleanup budgets. The call
  # itself stays open until that operation resolves, so a local timeout cannot
  # strand an acquired lease response in the caller's mailbox.
  def acquire(server, id, options, wait?),
    do: GenServer.call(server, {:acquire, id, options, wait?}, :infinity)

  def release(server, lease, timeout_ms),
    do: GenServer.call(server, {:release, lease, timeout_ms}, timeout_ms + 1_000)

  def renew(server, lease, ttl_ms, timeout_ms),
    do: GenServer.call(server, {:renew, lease, ttl_ms, timeout_ms}, timeout_ms + 1_000)

  @doc false
  def observe_cleanup(server, outcome, reason),
    do: GenServer.cast(server, {:observe_cleanup, outcome, reason})

  @impl true
  def init(options) do
    backend = Keyword.fetch!(options, :backend)
    config = Keyword.delete(options, :backend)

    with :ok <- backend.validate(config),
         {:ok, task_sup} <- Task.Supervisor.start_link(),
         {:ok, state} <- backend.init(config) do
      {:ok,
       %{
         backend: backend,
         state: state,
         config: config,
         task_sup: task_sup,
         leases: %{},
         pending: %{}
       }}
    else
      {:error, reason} -> {:stop, reason}
      other -> {:stop, {:invalid_backend, other}}
    end
  end

  @impl true
  def handle_call({:acquire, id, options, wait?}, from, state) do
    owner = elem(from, 0)

    task =
      Task.Supervisor.async_nolink(state.task_sup, fn ->
        if Process.alive?(owner) do
          state.backend.acquire(
            state.state,
            id,
            options,
            Keyword.merge(state.config, wait: wait?)
          )
        else
          {:error, :caller_cancelled_before_dispatch}
        end
      end)

    pending = Map.put(state.pending, task.ref, %{from: from, owner: owner})
    {:noreply, %{state | pending: pending}}
  end

  def handle_call({:release, ownership, timeout_ms}, {lease_pid, _tag}, state) do
    result =
      state.backend.release(
        state.state,
        ownership,
        Keyword.put(cleanup_options(state), :timeout_ms, timeout_ms)
      )

    {:reply, result, mark_release_attempted(state, lease_pid)}
  rescue
    _ -> {:reply, :unknown, mark_release_attempted(state, lease_pid)}
  end

  def handle_call({:renew, ownership, ttl_ms, timeout_ms}, _from, state) do
    result =
      state.backend.renew(
        state.state,
        ownership,
        ttl_ms,
        Keyword.put(state.config, :timeout_ms, timeout_ms)
      )

    {:reply, result, state}
  rescue
    error -> {:reply, {:error, {:backend_failure, error}}, state}
  end

  @impl true
  def handle_cast({:observe_cleanup, outcome, reason}, state) do
    case state.config[:cleanup_observer] do
      observer when is_function(observer, 1) ->
        event = %{backend: state.backend, outcome: outcome, reason: reason}

        _ =
          Task.Supervisor.start_child(state.task_sup, fn ->
            try do
              observer.(event)
            rescue
              _ -> :ok
            catch
              _, _ -> :ok
            end
          end)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    deadline = System.monotonic_time(:millisecond) + (state.config[:cleanup_timeout_ms] || 2_000)

    Enum.each(state.leases, fn
      {_monitor, %{pid: pid, ownership: ownership, release_attempted: false}} ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining > 0 do
          try do
            _ =
              state.backend.release(
                state.state,
                ownership,
                Keyword.put(state.config, :timeout_ms, remaining)
              )
          rescue
            _ -> :unknown
          catch
            _, _ -> :unknown
          end
        end

        try do
          if Process.alive?(pid), do: GenServer.stop(pid, :shutdown)
        catch
          _, _ -> :ok
        end

      {_monitor, %{pid: pid, release_attempted: true}} ->
        try do
          if Process.alive?(pid), do: GenServer.stop(pid, :shutdown)
        catch
          _, _ -> :ok
        end
    end)

    try do
      state.backend.close(state.state)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end

    :ok
  catch
    _, _ -> :ok
  end

  @impl true
  def handle_info({ref, result}, %{pending: pending} = state) when is_map_key(pending, ref) do
    Process.demonitor(ref, [:flush])
    %{from: from, owner: owner} = Map.fetch!(pending, ref)
    state = %{state | pending: Map.delete(pending, ref)}

    state =
      case finish_acquire(result, owner, state) do
        {:ok, lease, monitor, ownership} ->
          GenServer.reply(from, {:ok, lease})

          %{
            state
            | leases:
                Map.put(state.leases, monitor, %{
                  pid: lease,
                  ownership: ownership,
                  release_attempted: false
                })
          }

        response ->
          GenServer.reply(from, response)
          state
      end

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{pending: pending} = state)
      when is_map_key(pending, ref) do
    %{from: from} = Map.fetch!(pending, ref)
    # The backend task may die after a state-changing request was dispatched.
    # Without ownership evidence, the result is uncertain rather than a plain
    # backend failure; callers must not treat it as ordinary contention.
    GenServer.reply(from, {:error, {:acquisition_unknown, {:backend_task_down, reason}}})
    {:noreply, %{state | pending: Map.delete(pending, ref)}}
  end

  def handle_info({:DOWN, monitor, :process, lease, _reason}, state) do
    case Map.pop(state.leases, monitor) do
      {%{pid: ^lease, ownership: ownership, release_attempted: release_attempted}, leases} ->
        unless release_attempted do
          # Cleanup runs after the lease process is already gone. A backend
          # exception has no acknowledgement and must not take down this
          # provider (or its other leases); never retry this ambiguous release.
          cleanup_orphaned_ownership(state, ownership, :lease_process_down)
        end

        {:noreply, %{state | leases: leases}}

      _ ->
        {:noreply, state}
    end
  end

  defp finish_acquire({:ok, ownership}, owner, state) do
    unless Process.alive?(owner) do
      cleanup_orphaned_ownership(state, ownership, :caller_cancelled_after_dispatch)
      {:error, :caller_cancelled_after_dispatch}
    else
      lease_data =
        Map.merge(ownership, %{
          provider: self(),
          owner: owner,
          rpc_timeout_ms: state.config[:rpc_timeout_ms] || 2_000,
          cleanup_timeout_ms: state.config[:cleanup_timeout_ms] || 2_000
        })

      case Dilock.Lease.start_link(lease_data) do
        {:ok, lease} ->
          Process.unlink(lease)
          monitor = Process.monitor(lease)
          {:ok, lease, monitor, ownership}

        {:error, reason} ->
          outcome = cleanup_orphaned_ownership(state, ownership, :lease_process_start_failed)
          {:error, {:lease_process_start_failed, reason, outcome}}
      end
    end
  end

  defp finish_acquire(result, _owner, _state), do: result

  defp cleanup_orphaned_ownership(state, ownership, reason) do
    outcome =
      try do
        state.backend.release(state.state, ownership, cleanup_options(state))
      rescue
        _ -> :unknown
      catch
        _, _ -> :unknown
      end

    if outcome not in [:released, :already_not_owned],
      do: observe_cleanup(self(), outcome, reason)

    outcome
  end

  defp cleanup_options(state),
    do: Keyword.put(state.config, :timeout_ms, state.config[:cleanup_timeout_ms] || 2_000)

  defp mark_release_attempted(state, lease_pid) do
    leases =
      Map.new(state.leases, fn {monitor, lease} ->
        if lease.pid == lease_pid do
          {monitor, %{lease | release_attempted: true}}
        else
          {monitor, lease}
        end
      end)

    %{state | leases: leases}
  end
end
