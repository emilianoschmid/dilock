defmodule Dilock.Etcd.Streaming do
  @moduledoc false

  # etcd LeaseKeepAlive is bidirectional streaming. This adapter intentionally
  # opens one stream per keepalive, sends one request, consumes one response,
  # then cancels the stream so repeated renewals do not leak gRPC streams.
  def request(stream, request, timeout) do
    sent_stream =
      try do
        GRPC.Stub.send_request(stream, request)
      rescue
        error ->
          cancel(stream)
          reraise error, __STACKTRACE__
      catch
        kind, reason ->
          cancel(stream)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end

    try do
      case GRPC.Stub.recv(sent_stream, timeout: timeout) do
        {:ok, responses} when is_list(responses) -> first_response(responses)
        {:ok, responses} -> first_stream_response(responses)
        {:error, _} = error -> error
        response -> {:ok, response}
      end
    after
      cancel(sent_stream)
    end
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  def invoke(stub, method, channel, request, timeout) do
    # Generated client-streaming and bidi RPC functions accept only the
    # channel. Keep this invocation here so tests can guard the arity.
    stream = apply(stub, method, [channel])
    request(stream, request, timeout)
  end

  defp first_response([{:ok, response} | _]), do: {:ok, response}
  defp first_response([{:error, reason} | _]), do: {:error, reason}
  defp first_response([response | _]), do: {:ok, response}
  defp first_response([]), do: {:error, :empty_keepalive_stream}

  defp first_stream_response(stream) do
    case Enum.take(stream, 1) do
      [{:ok, response}] -> {:ok, response}
      [{:error, reason}] -> {:error, reason}
      [response] -> {:ok, response}
      [] -> {:error, :empty_keepalive_stream}
    end
  end

  defp cancel(stream) do
    _ = GRPC.Stub.cancel(stream)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end

defmodule Dilock.Etcd do
  @moduledoc "Native etcd v3 Lock/Lease adapter using the generated Elixir gRPC stubs."
  @behaviour Dilock.Backend

  @profile "etcd-native-lock-v1"
  @max_ms 86_400_000

  defmodule Fence do
    @moduledoc "An etcd acquisition revision paired with its provisioned coordination epoch."
    @enforce_keys [:epoch, :revision]
    defstruct [:epoch, :revision]
    @type t :: %__MODULE__{epoch: binary(), revision: pos_integer()}
  end

  def start_link(options),
    do: Dilock.Provider.start_link(Keyword.put(options, :backend, __MODULE__))

  def child_spec(options),
    do: Dilock.Provider.child_spec(Keyword.put(options, :backend, __MODULE__))

  @impl true
  def validate(options) do
    endpoint = options[:endpoint]
    namespace = options[:namespace] || "default"
    epoch = options[:epoch]

    with true <- is_binary(endpoint) and endpoint != "",
         {:ok, uri} <- parse_endpoint(endpoint),
         :ok <- validate_transport(uri),
         {:ok, _} <- Dilock.Identity.resource_id(namespace, "validation"),
         true <- is_binary(epoch) and epoch != "",
         true <- valid_timeout?(options[:rpc_timeout_ms] || 2_000),
         true <- valid_timeout?(options[:cleanup_timeout_ms] || 2_000) do
      :ok
    else
      false -> {:error, :invalid_configuration}
      {:error, _} = error -> error
      _ -> {:error, :invalid_configuration}
    end
  end

  @impl true
  def init(options) do
    {:ok, uri} = parse_endpoint(options[:endpoint])
    target = "#{uri.host}:#{uri.port || 2_379}"

    case GRPC.Stub.connect(target, adapter: GRPC.Client.Adapters.Mint) do
      {:ok, channel} ->
        state = %{
          channel: channel,
          target: target,
          namespace: options[:namespace] || "default",
          epoch: options[:epoch],
          rpc_timeout_ms: options[:rpc_timeout_ms] || 2_000,
          cleanup_timeout_ms: options[:cleanup_timeout_ms] || 2_000
        }

        case validate_metadata(state) do
          :ok ->
            {:ok, state}

          {:error, reason} ->
            close(state)
            {:error, {:etcd_initialization_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:etcd_initialization_failed, reason}}
    end
  rescue
    error -> {:error, {:etcd_initialization_failed, error}}
  catch
    kind, reason -> {:error, {:etcd_initialization_failed, {kind, reason}}}
  end

  @impl true
  def close(state) do
    if function_exported?(GRPC.Stub, :disconnect, 1),
      do: apply(GRPC.Stub, :disconnect, [state.channel])

    :ok
  end

  @doc "Returns an error: etcd's native blocking Lock RPC has no safe one-shot try operation."
  def try_acquire(_provider, _resource_id, _options), do: {:error, :unsupported_capability}

  @impl true
  def acquire(state, resource_id, options, config) do
    if config[:wait] == false do
      {:error, :unsupported_capability}
    else
      do_acquire(state, resource_id, options)
    end
  end

  defp do_acquire(state, resource_id, %{wait_ms: wait_ms, lease_ms: lease_ms})
       when is_integer(wait_ms) and wait_ms in 1..86_400_000 and is_integer(lease_ms) and
              lease_ms in 1..@max_ms do
    deadline = System.monotonic_time(:millisecond) + wait_ms

    with :ok <- check_rpc_policy(state, lease_ms),
         {:ok, lease_id, effective_ttl, grant_sent_at} <-
           grant_lease(state, lease_ms, max(deadline - System.monotonic_time(:millisecond), 1)) do
      remaining = deadline - System.monotonic_time(:millisecond)

      if remaining <= 0 do
        cleanup_acquisition(state, lease_id, :acquisition_timeout)
      else
        keepalive_timeout = min(remaining, state.rpc_timeout_ms)

        case start_wait_keepalive(
               state,
               lease_id,
               effective_ttl,
               grant_sent_at,
               keepalive_timeout
             ) do
          {:ok, keepalive} ->
            remaining = deadline - System.monotonic_time(:millisecond)

            if remaining <= 0 do
              _ = stop_wait_keepalive(keepalive)
              cleanup_acquisition(state, lease_id, :acquisition_timeout)
            else
              name = Dilock.Identity.etcd_lock_name(resource_id)

              locked =
                grpc(
                  state,
                  V3lockpb.Lock.Stub,
                  :lock,
                  struct(V3lockpb.LockRequest, name: name, lease: lease_id),
                  remaining
                )

              finish_lock(
                state,
                locked,
                keepalive,
                lease_id,
                effective_ttl,
                grant_sent_at,
                deadline
              )
            end

          {:error, reason} ->
            cleanup_acquisition(
              state,
              lease_id,
              {:backend_unavailable, {:initial_keepalive_unconfirmed, reason}}
            )
        end
      end
    else
      false -> {:error, :acquisition_timeout}
      {:error, _} = error -> error
    end
  end

  defp do_acquire(_state, _resource_id, _options), do: {:error, :invalid_acquisition_options}

  defp finish_lock(
         state,
         {:ok, response},
         keepalive,
         lease_id,
         effective_ttl,
         grant_sent_at,
         deadline
       ) do
    wait_state = stop_wait_keepalive(keepalive)

    with :ok <- wait_budget_available(deadline, System.monotonic_time(:millisecond)),
         :ok <- keepalive_result(wait_state),
         key when is_binary(key) and byte_size(key) > 0 <- field(response, :key),
         {:ok, entry} <- range_lock_key(state, key, lease_id, deadline),
         :ok <- wait_budget_available(deadline, System.monotonic_time(:millisecond)),
         {:ok, lease_deadline, current_ttl} <-
           confirmed_budget(wait_state, effective_ttl, grant_sent_at) do
      if System.monotonic_time(:millisecond) >= lease_deadline do
        cleanup_acquisition(state, lease_id, :acquisition_unknown)
      else
        {:ok,
         %{
           backend_state: state,
           opaque_key: key,
           lease_id: lease_id,
           effective_lease_ms: current_ttl,
           deadline_ms: lease_deadline,
           fence: %Fence{epoch: state.epoch, revision: field(entry, :create_revision)}
         }}
      end
    else
      nil -> cleanup_acquisition(state, lease_id, :protocol_mismatch)
      {:error, reason} -> cleanup_acquisition(state, lease_id, reason)
      _ -> cleanup_acquisition(state, lease_id, :protocol_mismatch)
    end
  end

  defp finish_lock(state, {:error, reason}, keepalive, lease_id, _ttl, _sent, _deadline) do
    _ = stop_wait_keepalive(keepalive)
    cleanup = revoke(state, lease_id)

    if deadline_exceeded?(reason) and cleanup == :ok,
      do: {:error, :acquisition_timeout},
      else: {:error, {:acquisition_unknown, reason, cleanup}}
  end

  defp finish_lock(state, other, keepalive, lease_id, _ttl, _sent, _deadline) do
    _ = stop_wait_keepalive(keepalive)
    cleanup = revoke(state, lease_id)
    {:error, {:acquisition_unknown, other, cleanup}}
  end

  @impl true
  def renew(
        state,
        %{lease_id: lease_id, effective_lease_ms: current} = ownership,
        ttl_ms,
        options
      ) do
    previous_deadline = ownership.deadline_ms

    cond do
      not valid_ttl?(ttl_ms) ->
        {:error, :invalid_ttl}

      ttl_ms != current ->
        {:error, :unsupported_capability}

      System.monotonic_time(:millisecond) >= ownership.deadline_ms ->
        {:error, :lease_lost}

      true ->
        sent_at = System.monotonic_time(:millisecond)

        with {:ok, response} <-
               keepalive_once(state, lease_id, options[:timeout_ms] || state.rpc_timeout_ms),
             ttl when is_integer(ttl) and ttl > 0 <- field(response, :TTL) do
          effective = ttl * 1_000
          deadline = sent_at + effective - safety(effective)

          if renewal_deadlines_valid?(
               previous_deadline,
               deadline,
               System.monotonic_time(:millisecond)
             ) do
            {:ok, Map.merge(ownership, %{effective_lease_ms: effective, deadline_ms: deadline})}
          else
            {:error, :lease_lost}
          end
        else
          {:ok, _} -> {:error, :lease_lost}
          {:error, reason} -> {:error, {:backend_unavailable, reason}}
          _ -> {:error, :lease_lost}
        end
    end
  end

  @doc false
  def renewal_deadlines_valid?(previous_deadline, renewed_deadline, now)
      when is_integer(previous_deadline) and is_integer(renewed_deadline) and is_integer(now),
      do: now < previous_deadline and now < renewed_deadline

  @doc false
  def wait_budget_available(deadline, now)
      when is_integer(deadline) and is_integer(now) do
    if now < deadline, do: :ok, else: {:error, :acquisition_timeout}
  end

  def wait_budget_available(_deadline, _now), do: {:error, :invalid_acquisition_options}

  @impl true
  def release(state, %{lease_id: lease_id, opaque_key: key}, options) do
    timeout = options[:timeout_ms] || state.cleanup_timeout_ms

    release_with_budget(
      timeout,
      fn -> System.monotonic_time(:millisecond) end,
      fn remaining -> lock_ownership(state, key, lease_id, remaining) end,
      fn remaining ->
        grpc(
          state,
          V3lockpb.Lock.Stub,
          :unlock,
          struct(V3lockpb.UnlockRequest, key: key),
          remaining
        )
      end,
      fn remaining -> revoke(state, lease_id, remaining) end
    )
  end

  @doc false
  def release_with_budget(timeout_ms, now, unlock, revoke)
      when is_integer(timeout_ms) and timeout_ms > 0 and is_function(now, 0) and
             is_function(unlock, 1) and is_function(revoke, 1) do
    release_with_budget(timeout_ms, now, fn _remaining -> :owned end, unlock, revoke)
  end

  def release_with_budget(_timeout_ms, _now, _unlock, _revoke), do: :unknown

  def release_with_budget(timeout_ms, now, ownership_check, unlock, revoke)
      when is_integer(timeout_ms) and timeout_ms > 0 and is_function(now, 0) and
             is_function(ownership_check, 1) and is_function(unlock, 1) and
             is_function(revoke, 1) do
    deadline = now.() + timeout_ms

    ownership_budget = max(deadline - now.(), 0)

    ownership =
      if ownership_budget > 0 do
        try do
          ownership_check.(ownership_budget)
        rescue
          _ -> :unknown
        catch
          _, _ -> :unknown
        end
      else
        :unknown
      end

    unlock_budget = max(deadline - now.(), 0)

    unlock_acknowledged =
      if ownership == :owned and unlock_budget > 0 do
        try do
          acknowledged?(unlock.(unlock_budget))
        rescue
          _ -> false
        catch
          _, _ -> false
        end
      else
        false
      end

    revoke_budget = max(deadline - now.(), 0)

    revoke_outcome =
      if revoke_budget > 0 do
        try do
          case revoke.(revoke_budget) do
            :ok -> :acknowledged
            :already_not_owned -> :lease_not_found
            _ -> :unknown
          end
        rescue
          _ -> :unknown
        catch
          _, _ -> :unknown
        end
      else
        :unknown
      end

    cond do
      ownership == :already_not_owned ->
        :already_not_owned

      revoke_outcome == :lease_not_found ->
        :already_not_owned

      ownership == :owned and unlock_acknowledged and revoke_outcome == :acknowledged ->
        :released

      revoke_outcome == :acknowledged ->
        :already_not_owned

      true ->
        :unknown
    end
  rescue
    _ -> :unknown
  catch
    _, _ -> :unknown
  end

  def release_with_budget(_timeout_ms, _now, _ownership_check, _unlock, _revoke),
    do: :unknown

  defp lock_ownership(state, key, lease_id, timeout) do
    case grpc(
           state,
           Etcdserverpb.KV.Stub,
           :range,
           struct(Etcdserverpb.RangeRequest, key: key),
           timeout
         ) do
      {:ok, response} -> classify_lock_ownership(response, key, lease_id)
      {:error, _reason} -> :unknown
      _ -> :unknown
    end
  end

  @doc false
  def classify_lock_ownership(response, key, lease_id) when is_binary(key) do
    case Enum.find(field(response, :kvs) || [], &(field(&1, :key) == key)) do
      nil -> :already_not_owned
      entry -> if(field(entry, :lease) == lease_id, do: :owned, else: :already_not_owned)
    end
  end

  def classify_lock_ownership(_response, _key, _lease_id), do: :unknown

  @doc false
  def classify_revoke_result({:ok, _response}), do: :ok
  def classify_revoke_result({:error, %GRPC.RPCError{status: 5}}), do: :already_not_owned
  def classify_revoke_result({:error, reason}), do: {:error, reason}
  def classify_revoke_result(_), do: {:error, :unexpected_response}

  defp acknowledged?(:ok), do: true
  defp acknowledged?({:ok, _response}), do: true
  defp acknowledged?(_), do: false

  def fence(%{fence: %Fence{} = fence}), do: {:ok, fence}
  def fence(_), do: {:error, :unsupported_capability}

  defp grant_lease(state, lease_ms, timeout) do
    seconds = max(div(lease_ms + 999, 1_000), 1)
    sent_at = System.monotonic_time(:millisecond)

    case grpc(
           state,
           Etcdserverpb.Lease.Stub,
           :lease_grant,
           struct(Etcdserverpb.LeaseGrantRequest, TTL: seconds, ID: 0),
           min(timeout, state.rpc_timeout_ms)
         ) do
      {:ok, response} ->
        id = field(response, :ID)
        ttl = field(response, :TTL)

        if is_integer(id) and id != 0 and is_integer(ttl) and ttl > 0,
          do: {:ok, id, ttl * 1_000, sent_at},
          else: {:error, {:backend_unavailable, {:lease_grant_rejected, field(response, :error)}}}

      {:error, reason} ->
        {:error, {:acquisition_unknown, {:lease_grant_outcome_unknown, reason}}}
    end
  end

  defp start_wait_keepalive(state, lease_id, _ttl_ms, _grant_sent_at, timeout) do
    sent_at = System.monotonic_time(:millisecond)

    with {:ok, response} <- keepalive_once(state, lease_id, timeout),
         ttl when is_integer(ttl) and ttl > 0 <- field(response, :TTL) do
      effective_ttl = ttl * 1_000

      {:ok, start_wait_keepalive_loop(state, lease_id, effective_ttl, {sent_at, effective_ttl})}
    else
      {:ok, _} -> {:error, :invalid_keepalive_ttl}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_keepalive_response}
    end
  end

  defp start_wait_keepalive_loop(state, lease_id, ttl_ms, initial) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        monitor = Process.monitor(parent)
        wait_keepalive_loop(parent, monitor, ref, state, lease_id, ttl_ms, elem(initial, 0))
      end)

    %{pid: pid, ref: ref, initial: initial}
  end

  defp wait_keepalive_loop(parent, parent_monitor, ref, state, lease_id, ttl_ms, _sent_at) do
    interval = min(max(div(ttl_ms, 3), 200), 10_000)

    receive do
      {:stop_keepalive, ^ref} ->
        :ok

      {:DOWN, ^parent_monitor, :process, ^parent, _reason} ->
        _ = revoke(state, lease_id)
        :ok
    after
      interval ->
        started = System.monotonic_time(:millisecond)

        case keepalive_once(state, lease_id, state.rpc_timeout_ms) do
          {:ok, response} ->
            ttl = field(response, :TTL)

            if is_integer(ttl) and ttl > 0 do
              effective = ttl * 1_000
              send(parent, {:dilock_keepalive, ref, :ok, started, effective})

              wait_keepalive_loop(
                parent,
                parent_monitor,
                ref,
                state,
                lease_id,
                effective,
                started
              )
            else
              send(parent, {:dilock_keepalive, ref, :error, :lease_lost})
            end

          {:error, reason} ->
            send(parent, {:dilock_keepalive, ref, :error, reason})
        end
    end
  end

  defp stop_wait_keepalive(%{pid: pid, ref: ref, initial: initial}) do
    Process.monitor(pid)
    send(pid, {:stop_keepalive, ref})

    receive do
      {:DOWN, _monitor, :process, ^pid, _} -> :ok
    after
      2_100 ->
        Process.exit(pid, :kill)
        :ok
    end

    collect_keepalive(ref, initial)
  end

  defp collect_keepalive(ref, initial) do
    receive do
      {:dilock_keepalive, ^ref, :ok, sent_at, ttl} ->
        collect_keepalive(ref, max_budget(initial, {sent_at, ttl}))

      {:dilock_keepalive, ^ref, :error, reason} ->
        {:error, reason}
    after
      0 -> {:ok, initial}
    end
  end

  defp max_budget({old_sent, _old_ttl}, {new_sent, new_ttl}) when new_sent >= old_sent,
    do: {new_sent, new_ttl}

  defp max_budget(old, _new), do: old

  defp cleanup_acquisition(state, lease_id, reason) do
    case revoke(state, lease_id) do
      :ok -> {:error, reason}
      {:error, cleanup_reason} -> {:error, {:acquisition_unknown, reason, cleanup_reason}}
    end
  end

  defp confirmed_budget({:error, reason}, _ttl, _sent),
    do: {:error, {:backend_unavailable, reason}}

  defp confirmed_budget({:ok, {sent_at, ttl}}, _initial_ttl, _grant_sent),
    do: {:ok, sent_at + ttl - safety(ttl), ttl}

  defp keepalive_result({:error, reason}), do: {:error, {:backend_unavailable, reason}}
  defp keepalive_result({:ok, _}), do: :ok

  defp range_lock_key(state, key, lease_id, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, :acquisition_timeout}
    else
      result = verify_lock_key(state, key, lease_id, min(state.rpc_timeout_ms, remaining))

      if System.monotonic_time(:millisecond) >= deadline and match?({:error, _}, result),
        do: {:error, :acquisition_timeout},
        else: result
    end
  end

  defp verify_lock_key(state, key, lease_id, timeout) do
    with {:ok, response} <-
           grpc(
             state,
             Etcdserverpb.KV.Stub,
             :range,
             struct(Etcdserverpb.RangeRequest, key: key),
             timeout
           ),
         entry when not is_nil(entry) <-
           Enum.find(field(response, :kvs) || [], &(field(&1, :key) == key)),
         true <- field(entry, :lease) == lease_id,
         revision when is_integer(revision) and revision > 0 <- field(entry, :create_revision) do
      {:ok, entry}
    else
      {:error, :acquisition_timeout} -> {:error, :acquisition_timeout}
      {:error, reason} -> {:error, {:backend_unavailable, reason}}
      _ -> {:error, :protocol_mismatch}
    end
  end

  defp validate_metadata(state) do
    key = "/dilock/namespaces/#{state.namespace}/meta"

    with {:ok, response} <-
           grpc(
             state,
             Etcdserverpb.KV.Stub,
             :range,
             struct(Etcdserverpb.RangeRequest, key: key),
             state.rpc_timeout_ms
           ),
         entry when not is_nil(entry) <-
           Enum.find(field(response, :kvs) || [], &(field(&1, :key) == key)),
         :ok <- Dilock.Identity.validate_metadata(field(entry, :value), @profile, state.epoch) do
      :ok
    else
      nil -> {:error, :namespace_metadata_missing}
      {:error, _} = error -> error
      _ -> {:error, :protocol_mismatch}
    end
  end

  defp keepalive_once(state, lease_id, timeout) do
    request = struct(Etcdserverpb.LeaseKeepAliveRequest, ID: lease_id)

    case stream_rpc(state, Etcdserverpb.Lease.Stub, :lease_keep_alive, request, timeout) do
      {:ok, response} -> {:ok, response}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_keepalive_response, other}}
    end
  end

  defp stream_rpc(state, stub, method, request, timeout) do
    Dilock.Etcd.Streaming.invoke(stub, method, state.channel, request, timeout)
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp grpc(state, stub, method, request, timeout) do
    case apply(stub, method, [state.channel, request, [timeout: timeout]]) do
      {:ok, response} -> {:ok, response}
      {:ok, response, _metadata} -> {:ok, response}
      {:error, reason} -> {:error, reason}
      response -> {:ok, response}
    end
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp revoke(state, lease_id), do: revoke(state, lease_id, state.cleanup_timeout_ms)

  defp revoke(state, lease_id, timeout) do
    result =
      grpc(
        state,
        Etcdserverpb.Lease.Stub,
        :lease_revoke,
        struct(Etcdserverpb.LeaseRevokeRequest, ID: lease_id),
        timeout
      )

    classify_revoke_result(result)
  end

  defp check_rpc_policy(state, lease_ms) do
    margin = safety(lease_ms)

    if state.rpc_timeout_ms + margin < lease_ms,
      do: :ok,
      else: {:error, :rpc_budget_exceeds_lease}
  end

  defp parse_endpoint(endpoint) do
    uri = URI.parse(endpoint)

    cond do
      uri.scheme not in ["http", "https"] -> {:error, :invalid_endpoint_scheme}
      is_nil(uri.host) or uri.host == "" -> {:error, :invalid_endpoint_host}
      uri.userinfo || uri.query || uri.fragment -> {:error, :invalid_endpoint}
      uri.path not in [nil, "", "/"] -> {:error, :invalid_endpoint_path}
      true -> {:ok, uri}
    end
  rescue
    _ -> {:error, :invalid_endpoint}
  end

  defp validate_transport(%URI{scheme: "http"}), do: :ok
  defp validate_transport(%URI{scheme: "https"}), do: {:error, :unsupported_tls}
  defp validate_transport(_), do: {:error, :invalid_endpoint_scheme}

  defp deadline_exceeded?(%{status: status}), do: status in [:deadline_exceeded, 4]

  defp deadline_exceeded?(reason) when is_binary(reason),
    do: String.contains?(String.downcase(reason), "deadline_exceeded")

  defp deadline_exceeded?(_), do: false
  defp field(nil, _name), do: nil
  defp field(message, name), do: Map.get(message, name)
  defp safety(ttl), do: max(10, div(ttl + 99, 100))
  defp valid_ttl?(ms), do: is_integer(ms) and ms in 1..@max_ms
  defp valid_timeout?(ms), do: is_integer(ms) and ms in 1..60_000
end
