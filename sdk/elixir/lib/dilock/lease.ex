defmodule Dilock.Lease do
  @moduledoc "One non-cloneable-by-convention acquisition process; release is explicit and idempotent."
  use GenServer

  def start_link(ownership), do: GenServer.start_link(__MODULE__, ownership)

  @doc "Releases the lease and waits for the backend's configured bounded cleanup operation."
  def release(lease), do: GenServer.call(lease, :release, :infinity)

  @doc "Renews the lease and waits for the backend's configured bounded RPC operation."
  def renew(lease, ttl_ms \\ nil), do: GenServer.call(lease, {:renew, ttl_ms}, :infinity)
  def ownership(lease), do: GenServer.call(lease, :ownership)
  def close(lease), do: GenServer.call(lease, :close)

  @impl true
  def init(ownership) do
    owner_monitor = Process.monitor(ownership.owner)
    provider_monitor = Process.monitor(ownership.provider)

    {:ok,
     %{
       ownership: ownership,
       state: :held,
       outcome: nil,
       owner_monitor: owner_monitor,
       provider_monitor: provider_monitor
     }}
  end

  @impl true
  def handle_call(:ownership, _from, state), do: {:reply, state.ownership, state}
  def handle_call(:close, _from, state), do: {:stop, :normal, :ok, state}

  def handle_call(:release, _from, %{outcome: outcome} = state) when not is_nil(outcome),
    do: {:reply, outcome, state}

  def handle_call(:release, _from, state) do
    result =
      Dilock.Provider.release(
        state.ownership.provider,
        state.ownership,
        state.ownership.cleanup_timeout_ms
      )

    {:reply, result, %{state | state: terminal_state(result), outcome: result}}
  catch
    :exit, _ -> {:reply, :unknown, %{state | state: :released_unknown, outcome: :unknown}}
  end

  def handle_call({:renew, nil}, from, %{state: :held} = state),
    do: handle_call({:renew, state.ownership.effective_lease_ms}, from, state)

  def handle_call({:renew, ttl_ms}, _from, %{state: :held} = state)
      when is_integer(ttl_ms) and ttl_ms > 0 do
    if System.monotonic_time(:millisecond) >= state.ownership.deadline_ms do
      {:reply, {:error, :lease_lost}, %{state | state: :lost}}
    else
      result =
        Dilock.Provider.renew(
          state.ownership.provider,
          state.ownership,
          ttl_ms,
          state.ownership.rpc_timeout_ms
        )

      case result do
        {:ok, ownership} -> {:reply, :ok, %{state | ownership: ownership}}
        {:error, :lease_lost} = error -> {:reply, error, %{state | state: :lost}}
        # A backend failure after dispatch leaves renewal outcome uncertain.
        # Fail closed locally so callers cannot continue relying on this lease
        # merely because its previous timer deadline has not elapsed yet.
        {:error, :invalid_ttl} = error -> {:reply, error, state}
        {:error, :unsupported_capability} = error -> {:reply, error, state}
        other -> {:reply, other, %{state | state: :lost}}
      end
    end
  end

  def handle_call({:renew, _}, _from, state), do: {:reply, {:error, :lease_lost}, state}

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{owner_monitor: monitor} = state) do
    if state.outcome do
      if state.outcome not in [:released, :already_not_owned],
        do:
          Dilock.Provider.observe_cleanup(
            state.ownership.provider,
            state.outcome,
            :owner_terminated_after_release
          )

      {:stop, :normal, state}
    else
      outcome =
        try do
          Dilock.Provider.release(
            state.ownership.provider,
            state.ownership,
            state.ownership.cleanup_timeout_ms
          )
        rescue
          _ -> :unknown
        catch
          _, _ -> :unknown
        end

      if outcome not in [:released, :already_not_owned],
        do: Dilock.Provider.observe_cleanup(state.ownership.provider, outcome, :owner_terminated)

      {:stop, :normal, %{state | state: terminal_state(outcome), outcome: outcome}}
    end
  catch
    :exit, _ -> {:stop, :normal, %{state | state: :released_unknown}}
  end

  def handle_info(
        {:DOWN, monitor, :process, _pid, _reason},
        %{provider_monitor: monitor} = state
      ),
      do: {:stop, :normal, %{state | state: :lost}}

  def handle_info(_message, state), do: {:noreply, state}

  defp terminal_state(:released), do: :released
  defp terminal_state(:already_not_owned), do: :released
  defp terminal_state(_), do: :released_unknown
end
