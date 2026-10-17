defmodule Dilock do
  @moduledoc """
  Distributed leases backed by Redis, etcd, or Consul. A lease is advisory: expiry does not
  stop a paused process, and Redis does not provide a strong fencing token.
  """

  @default_wait_ms 5_000
  @default_lease_ms 30_000

  @type options :: %{wait_ms: non_neg_integer(), lease_ms: pos_integer()}
  @type outcome :: :released | :already_not_owned | :unknown

  @doc "Returns the default acquisition options (5 second wait, 30 second lease)."
  @spec defaults() :: options()
  def defaults, do: %{wait_ms: @default_wait_ms, lease_ms: @default_lease_ms}

  @doc "Builds an opaque resource handle after validating its protocol identity."
  @spec lock(pid(), binary(), binary()) :: {:ok, Dilock.Lock.t()} | {:error, term()}
  def lock(provider, namespace, resource) when is_pid(provider) do
    with {:ok, id} <- Dilock.Identity.resource_id(namespace, resource) do
      {:ok, %Dilock.Lock{provider: provider, resource_id: id}}
    end
  end

  @doc "Runs a function with an acquired lease and always performs bounded release."
  @spec with_lease(Dilock.Lock.t(), options(), (pid() -> term())) ::
          {:ok, term()} | :busy | {:error, term()}
  def with_lease(lock, fun) when is_function(fun, 1), do: with_lease(lock, defaults(), fun)

  def with_lease(lock, options, fun) when is_function(fun, 1) do
    case Dilock.Lock.acquire(lock, options) do
      {:ok, lease} ->
        try do
          value = fun.(lease)
          outcome = release_scope(lock.provider, lease, :scope_exit)

          case outcome do
            :released -> {:ok, value}
            :already_not_owned -> {:error, {:lease_lost, :already_not_owned, value}}
            other -> {:error, {:cleanup_not_acknowledged, other, value}}
          end
        rescue
          error ->
            _ = release_scope(lock.provider, lease, :body_failed)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            _ = release_scope(lock.provider, lease, :body_failed)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      other ->
        other
    end
  end

  defp release_scope(provider, lease, reason) do
    outcome =
      try do
        Dilock.Lease.release(lease)
      rescue
        _ -> :unknown
      catch
        _, _ -> :unknown
      end

    if outcome not in [:released, :already_not_owned],
      do: Dilock.Provider.observe_cleanup(provider, outcome, reason)

    # Closing is local process cleanup only. The lease may already have died
    # after release failed (or the caller may have stopped it), so close must
    # never replace a work exception or turn an unacknowledged release into a
    # new exception.
    try do
      _ = Dilock.Lease.close(lease)
    catch
      :exit, _ -> :ok
    end

    outcome
  end
end
