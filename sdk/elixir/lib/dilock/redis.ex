defmodule Dilock.Redis do
  @moduledoc "Redis single-authority backend using Redix and owner-checked Lua scripts."
  @behaviour Dilock.Backend

  @profile "redis-single-authority-v1"
  @max_ms 86_400_000

  def start_link(options),
    do: Dilock.Provider.start_link(Keyword.put(options, :backend, __MODULE__))

  def child_spec(options),
    do: Dilock.Provider.child_spec(Keyword.put(options, :backend, __MODULE__))

  @impl true
  def validate(options) do
    with endpoint when is_binary(endpoint) and endpoint != "" <- options[:endpoint],
         namespace when is_binary(namespace) <- options[:namespace] || "default",
         {:ok, _} <- Dilock.Identity.resource_id(namespace, "validation"),
         epoch when is_binary(epoch) and epoch != "" <- options[:epoch],
         true <- valid_timeout?(options[:rpc_timeout_ms] || 2_000),
         true <- valid_timeout?(options[:cleanup_timeout_ms] || 2_000) do
      :ok
    else
      _ -> {:error, :invalid_configuration}
    end
  end

  @impl true
  def init(options) do
    endpoint = options[:endpoint]
    namespace = options[:namespace] || "default"
    epoch = options[:epoch]
    rpc_timeout = options[:rpc_timeout_ms] || 2_000
    cleanup_timeout = options[:cleanup_timeout_ms] || 2_000

    with {:ok, client} <- Redix.start_link(endpoint, sync_connect: true),
         {:ok, cleanup} <- Redix.start_link(endpoint, sync_connect: true),
         {:ok, raw} <-
           command(client, ["GET", "dilock:namespace:#{namespace}:meta"], rpc_timeout),
         :ok <- Dilock.Identity.validate_metadata(raw, @profile, epoch) do
      {:ok,
       %{
         client: client,
         cleanup: cleanup,
         namespace: namespace,
         rpc_timeout_ms: rpc_timeout,
         cleanup_timeout_ms: cleanup_timeout
       }}
    else
      {:ok, nil} -> {:error, :namespace_metadata_missing}
      {:error, _} = error -> error
      other -> {:error, {:redis_initialization_failed, other}}
    end
  end

  @impl true
  def close(state) do
    Enum.each([state.client, state.cleanup], fn client ->
      if Process.alive?(client), do: GenServer.stop(client, :normal, state.cleanup_timeout_ms)
    end)

    :ok
  end

  @doc "Creates namespace metadata only when absent. Mismatches are never overwritten."
  def provision_namespace(options) do
    with :ok <- validate(options),
         {:ok, client} <- Redix.start_link(options[:endpoint], sync_connect: true) do
      try do
        namespace = options[:namespace] || "default"
        epoch = options[:epoch]
        key = "dilock:namespace:#{namespace}:meta"
        value = Dilock.Identity.metadata(namespace, @profile, epoch)
        result = command(client, ["SET", key, value, "NX"], options[:rpc_timeout_ms] || 2_000)

        case result do
          {:ok, "OK"} ->
            :ok

          {:ok, nil} ->
            with {:ok, current} <-
                   command(client, ["GET", key], options[:rpc_timeout_ms] || 2_000),
                 :ok <- Dilock.Identity.validate_metadata(current, @profile, epoch),
                 do: :ok

          {:error, reason} ->
            {:error, {:backend_unavailable, reason}}
        end
      after
        GenServer.stop(client)
      end
    end
  end

  @impl true
  def acquire(state, resource_id, options, config) do
    case options do
      %{wait_ms: wait_ms, lease_ms: lease_ms} when is_integer(wait_ms) and is_integer(lease_ms) ->
        if valid_policy?(wait_ms, lease_ms, config[:wait] != false) do
          acquire_until(
            state,
            Dilock.Identity.redis_key(resource_id),
            wait_ms,
            lease_ms,
            config[:wait] != false,
            System.monotonic_time(:millisecond) + wait_ms
          )
        else
          {:error, :invalid_acquisition_options}
        end

      _ ->
        {:error, :invalid_acquisition_options}
    end
  end

  defp acquire_until(state, key, wait_ms, lease_ms, wait?, deadline) do
    now = System.monotonic_time(:millisecond)
    lease_rpc_budget = lease_ms - safety(lease_ms)
    rpc_budget = min(state.rpc_timeout_ms, lease_rpc_budget)
    rpc_budget = if wait?, do: min(rpc_budget, max(deadline - now, 0)), else: rpc_budget

    if rpc_budget <= 0 do
      {:error, :acquisition_timeout}
    else
      acquire_once(state, key, wait_ms, lease_ms, wait?, deadline, now + rpc_budget)
    end
  end

  defp acquire_once(state, key, wait_ms, lease_ms, wait?, deadline, rpc_deadline) do
    token = "dilock/1/" <> Base.encode16(:crypto.strong_rand_bytes(32), case: :lower)
    sent_at = System.monotonic_time(:millisecond)
    set_timeout = rpc_deadline - sent_at

    result =
      if wait? and set_timeout <= 0 do
        {:error, :wait_budget_expired_before_set}
      else
        command(
          state.client,
          ["SET", key, token, "NX", "PX", Integer.to_string(lease_ms)],
          max(set_timeout, 1)
        )
      end

    case result do
      {:ok, "OK"} ->
        remaining =
          lease_ms - safety(lease_ms) - max(System.monotonic_time(:millisecond) - sent_at, 0)

        if remaining > 0 do
          case late_wait_cleanup(
                 wait?,
                 deadline,
                 System.monotonic_time(:millisecond),
                 fn -> cleanup(state, key, token) end
               ) do
            :continue ->
              {:ok,
               %{
                 backend_state: state,
                 key: key,
                 owner_token: token,
                 effective_lease_ms: lease_ms,
                 deadline_ms: sent_at + lease_ms - safety(lease_ms)
               }}

            {:error, _} = error ->
              error
          end
        else
          late_acquisition_cleanup(fn -> cleanup(state, key, token) end)
        end

      {:ok, nil} ->
        type_timeout = rpc_deadline - System.monotonic_time(:millisecond)

        if type_timeout <= 0 do
          {:error, {:backend_unavailable, :acquisition_rpc_budget_exhausted}}
        else
          classify_contention(state, key, wait_ms, lease_ms, wait?, deadline, type_timeout)
        end

      {:error, reason} ->
        if reason == :wait_budget_expired_before_set do
          {:error, :acquisition_timeout}
        else
          cleanup(state, key, token)
          {:error, {:acquisition_unknown, reason}}
        end
    end
  end

  defp classify_contention(state, key, wait_ms, lease_ms, wait?, deadline, type_timeout) do
    case command(state.client, ["TYPE", key], type_timeout) do
      {:ok, type} when type in ["string", "none"] ->
        if not wait?,
          do: :busy,
          else: wait_or_timeout(state, key, wait_ms, lease_ms, deadline)

      {:ok, type} ->
        {:error, {:protocol_mismatch, {:unexpected_redis_type, type}}}

      {:error, reason} ->
        {:error, {:backend_unavailable, reason}}
    end
  end

  defp wait_or_timeout(state, key, wait, lease, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, :acquisition_timeout}
    else
      Process.sleep(min(25 + :rand.uniform(25), remaining))

      if System.monotonic_time(:millisecond) >= deadline do
        {:error, :acquisition_timeout}
      else
        acquire_until(state, key, wait, lease, true, deadline)
      end
    end
  end

  @impl true
  def renew(_provider_state, %{backend_state: state} = ownership, ttl_ms, options) do
    if not valid_ttl?(ttl_ms),
      do: {:error, :invalid_ttl},
      else: do_renew(state, ownership, ttl_ms, options)
  end

  defp do_renew(state, ownership, ttl_ms, options) do
    sent_at = System.monotonic_time(:millisecond)
    script = File.read!(Path.join(:code.priv_dir(:dilock), "redis/redis-renew.lua"))

    case command(
           state.client,
           ["EVAL", script, "1", ownership.key, ownership.owner_token, Integer.to_string(ttl_ms)],
           options[:timeout_ms] || state.rpc_timeout_ms
         ) do
      {:ok, 1} ->
        if ttl_ms - safety(ttl_ms) - (System.monotonic_time(:millisecond) - sent_at) > 0 do
          {:ok,
           Map.merge(ownership, %{
             effective_lease_ms: ttl_ms,
             deadline_ms: sent_at + ttl_ms - safety(ttl_ms)
           })}
        else
          {:error, :lease_lost}
        end

      {:ok, _} ->
        {:error, :lease_lost}

      {:error, reason} ->
        {:error, {:backend_unavailable, reason}}
    end
  end

  @impl true
  def release(_provider_state, %{backend_state: state} = ownership, options) do
    script = File.read!(Path.join(:code.priv_dir(:dilock), "redis/redis-release.lua"))

    case command(
           state.cleanup,
           ["EVAL", script, "1", ownership.key, ownership.owner_token],
           options[:timeout_ms] || state.cleanup_timeout_ms
         ) do
      {:ok, 1} -> :released
      {:ok, _} -> :already_not_owned
      {:error, _} -> :unknown
    end
  end

  defp cleanup(state, key, token),
    do:
      release(state, %{backend_state: state, key: key, owner_token: token},
        timeout_ms: state.cleanup_timeout_ms
      )

  @doc false
  def late_wait_cleanup(wait?, deadline, now, cleanup)
      when is_boolean(wait?) and is_integer(deadline) and is_integer(now) and
             is_function(cleanup, 0) do
    if wait? and now >= deadline do
      late_acquisition_cleanup(cleanup)
    else
      :continue
    end
  rescue
    _ -> {:error, {:acquisition_unknown, :late_acquisition_cleanup_unconfirmed}}
  catch
    _, _ -> {:error, {:acquisition_unknown, :late_acquisition_cleanup_unconfirmed}}
  end

  def late_wait_cleanup(_wait?, _deadline, _now, _cleanup),
    do: {:error, :invalid_acquisition_options}

  @doc false
  def late_acquisition_cleanup(cleanup) when is_function(cleanup, 0) do
    case cleanup.() do
      outcome when outcome in [:released, :already_not_owned] -> {:error, :acquisition_timeout}
      _ -> {:error, {:acquisition_unknown, :late_acquisition_cleanup_unconfirmed}}
    end
  rescue
    _ -> {:error, {:acquisition_unknown, :late_acquisition_cleanup_unconfirmed}}
  catch
    _, _ -> {:error, {:acquisition_unknown, :late_acquisition_cleanup_unconfirmed}}
  end

  def late_acquisition_cleanup(_cleanup),
    do: {:error, :invalid_acquisition_options}

  defp command(client, argv, timeout), do: Redix.command(client, argv, timeout: timeout)
  defp safety(ttl_ms), do: max(10, div(ttl_ms + 99, 100))
  defp valid_ttl?(ms), do: is_integer(ms) and ms in 1..@max_ms

  defp valid_policy?(wait, lease, true),
    do: is_integer(wait) and wait in 1..86_400_000 and valid_ttl?(lease)

  defp valid_policy?(wait, lease, false),
    do: is_integer(wait) and wait == 0 and valid_ttl?(lease)

  defp valid_timeout?(ms), do: is_integer(ms) and ms in 1..60_000
end
