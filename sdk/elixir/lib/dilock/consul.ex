defmodule Dilock.Consul do
  @moduledoc "Consul HTTP/KV sessions with per-attempt TTL sessions and LockIndex fencing."
  @behaviour Dilock.Backend

  @profile "consul-kv-session-v1"
  @max_ttl_ms 86_400_000
  @min_ttl_ms 10_000

  defmodule Fence do
    @moduledoc "A Consul KV LockIndex paired with the provisioned coordination epoch."
    @enforce_keys [:epoch, :lock_index]
    defstruct [:epoch, :lock_index]
    @type t :: %__MODULE__{epoch: binary(), lock_index: pos_integer()}
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
    token = options[:token]

    with true <- is_binary(endpoint) and endpoint != "",
         {:ok, uri} <- parse_endpoint(endpoint),
         {:ok, _} <- Dilock.Identity.resource_id(namespace, "validation"),
         true <- is_binary(epoch) and epoch != "" and byte_size(epoch) <= 1024,
         true <- is_nil(token) or valid_token?(token),
         true <- valid_timeout?(options[:rpc_timeout_ms] || 2_000),
         true <- valid_timeout?(options[:cleanup_timeout_ms] || 2_000),
         true <- valid_ttl?(options[:default_lease_ms] || 30_000) do
      validate_transport(uri)
    else
      false -> {:error, :invalid_configuration}
      {:error, _} = error -> error
      _ -> {:error, :invalid_configuration}
    end
  end

  @impl true
  def init(options) do
    {:ok, uri} = parse_endpoint(options[:endpoint])
    state = %{
      endpoint: uri,
      namespace: options[:namespace] || "default",
      epoch: options[:epoch],
      token: options[:token],
      rpc_timeout_ms: options[:rpc_timeout_ms] || 2_000,
      cleanup_timeout_ms: options[:cleanup_timeout_ms] || 2_000
    }

    case validate_metadata(state) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, {:consul_initialization_failed, reason}}
    end
  rescue
    error -> {:error, {:consul_initialization_failed, error}}
  end

  @impl true
  def close(_state), do: :ok

  @impl true
  def acquire(state, resource_id, %{wait_ms: wait_ms, lease_ms: lease_ms}, config) do
    wait? = config[:wait] != false

    if valid_policy?(wait_ms, lease_ms, wait?) do
      ttl = div(lease_ms + 999, 1_000)
      effective_ttl = ttl * 1_000
      key = "dilock/locks/#{resource_id}"
      deadline = System.monotonic_time(:millisecond) + wait_ms

      with {:ok, session, sent_at} <- create_session(state, ttl) do
        lease_deadline = sent_at + effective_ttl - safety(effective_ttl)
        acquire_until(state, key, session, effective_ttl, lease_deadline, wait?, deadline, sent_at)
      end
    else
      {:error, :invalid_acquisition_options}
    end
  end

  def acquire(_state, _resource_id, _options, _config), do: {:error, :invalid_acquisition_options}

  defp acquire_until(state, key, session, ttl, lease_deadline, wait?, wait_deadline, last_renewal) do
    now = System.monotonic_time(:millisecond)

    cond do
      now >= lease_deadline -> cleanup_attempt(state, key, session, :acquisition_unknown)
      wait? and now >= wait_deadline -> cleanup_attempt(state, key, session, :acquisition_timeout)
      true ->
        case acquire_key(state, key, session, state.rpc_timeout_ms) do
          {:ok, true} ->
            case owner(state, key, state.rpc_timeout_ms) do
              {:ok, %{session: ^session, lock_index: index}} when index > 0 ->
                acquired_at = System.monotonic_time(:millisecond)

                if acquired_at >= lease_deadline or (wait? and acquired_at >= wait_deadline) do
                  cleanup_attempt(state, key, session, if(acquired_at >= wait_deadline, do: :acquisition_timeout, else: :acquisition_unknown))
                else
                  {:ok, %{backend_state: state, key: key, session: session, lock_index: index,
                    epoch: state.epoch, effective_lease_ms: ttl,
                    deadline_ms: lease_deadline, last_renewal_ms: last_renewal}}
                end

              {:error, reason} ->
                cleanup_attempt(state, key, session, {:acquisition_unknown, reason})

              _ ->
                cleanup_attempt(state, key, session, {:acquisition_unknown, :owner_verification_failed})
            end

          {:ok, false} when not wait? ->
            _ = destroy_session(state, session, state.cleanup_timeout_ms)
            :busy

          {:ok, false} ->
            sleep_ms = min(100, max(wait_deadline - now, 1))
            Process.sleep(sleep_ms)
            after_wait = System.monotonic_time(:millisecond)
            renewal_at = ttl - div(ttl, 3)

            if after_wait - last_renewal >= renewal_at do
              case renew_session(state, session, state.rpc_timeout_ms) do
                {:ok, renewed_at} ->
                  renewed_deadline = renewed_at + ttl - safety(ttl)

                  if System.monotonic_time(:millisecond) >= renewed_deadline do
                    cleanup_attempt(state, key, session, {:acquisition_unknown, :session_deadline_elapsed})
                  else
                    acquire_until(state, key, session, ttl, renewed_deadline, true, wait_deadline, renewed_at)
                  end

                {:error, reason} -> cleanup_attempt(state, key, session, {:acquisition_unknown, {:session_renewal_failed, reason}})
              end
            else
              acquire_until(state, key, session, ttl, lease_deadline, true, wait_deadline, last_renewal)
            end

          {:error, reason} -> cleanup_attempt(state, key, session, {:acquisition_unknown, reason})
        end
    end
  rescue
    error -> cleanup_attempt(state, key, session, {:acquisition_unknown, error})
  catch
    kind, reason -> cleanup_attempt(state, key, session, {:acquisition_unknown, {kind, reason}})
  end

  @impl true
  def renew(state, ownership, ttl_ms, options) do
    if not valid_ttl?(ttl_ms) do
      {:error, :invalid_ttl}
    else
      ttl = div(ttl_ms + 999, 1_000) * 1_000
      sent_at = System.monotonic_time(:millisecond)

      with true <- sent_at < ownership.deadline_ms,
           {:ok, renewed_at} <- renew_session(state, ownership.session, options[:timeout_ms] || state.rpc_timeout_ms),
           {:ok, %{session: session, lock_index: index}} <- owner(state, ownership.key, options[:timeout_ms] || state.rpc_timeout_ms),
           true <- session == ownership.session and index == ownership.lock_index,
           deadline <- renewed_at + ttl - safety(ttl),
           true <- System.monotonic_time(:millisecond) < deadline do
        {:ok, %{ownership | effective_lease_ms: ttl, deadline_ms: deadline, last_renewal_ms: renewed_at}}
      else
        false -> {:error, :lease_lost}
        {:ok, _} -> {:error, :lease_lost}
        {:error, reason} -> {:error, {:backend_unavailable, reason}}
      end
    end
  rescue
    _ -> {:error, {:backend_unavailable, :consul_renewal_failed}}
  end

  @impl true
  def release(state, ownership, options) do
    timeout = options[:timeout_ms] || state.cleanup_timeout_ms

    result =
      with {:ok, released} <- release_key(state, ownership.key, ownership.session, timeout) do
        if released do
          :released
        else
          case owner(state, ownership.key, timeout) do
            {:ok, %{session: session}} when session == ownership.session -> :unknown
            {:ok, _} -> :already_not_owned
            {:error, _} -> :unknown
          end
        end
      else
        _ -> :unknown
      end

    destroy = destroy_session(state, ownership.session, timeout)
    if destroy == :ok, do: result, else: :unknown
  rescue
    _ -> :unknown
  catch
    _, _ -> :unknown
  end

  @doc "Returns an epoch-scoped fence derived from the acquired KV entry's LockIndex."
  def fence(%{epoch: epoch, lock_index: index}) when is_binary(epoch) and is_integer(index) and index > 0,
    do: {:ok, %Fence{epoch: epoch, lock_index: index}}
  def fence(_ownership), do: {:error, :invalid_fence}

  @doc "Provision namespace metadata with CAS=0; compatible existing entries are preserved."
  def provision_namespace(options) do
    with :ok <- validate(options),
         {:ok, uri} <- parse_endpoint(options[:endpoint]) do
      state = %{endpoint: uri, namespace: options[:namespace] || "default", epoch: options[:epoch],
        token: options[:token], rpc_timeout_ms: options[:rpc_timeout_ms] || 2_000,
        cleanup_timeout_ms: options[:cleanup_timeout_ms] || 2_000}
      key = "dilock/namespaces/#{state.namespace}/meta"
      body = Dilock.Identity.metadata(state.namespace, @profile, state.epoch)

      case request(state, :put, "/v1/kv/#{key}?cas=0", body, state.rpc_timeout_ms, true) do
        {:ok, true} -> :ok
        {:ok, false} ->
          case request(state, :get, "/v1/kv/#{key}?consistent", nil, state.rpc_timeout_ms, false) do
            {:ok, rows} -> validate_metadata_value(rows, key, state.epoch)
            {:error, reason} -> {:error, {:backend_unavailable, reason}}
          end
        {:error, reason} -> {:error, {:backend_unavailable, reason}}
      end
    end
  end

  defp validate_metadata(state) do
    key = "dilock/namespaces/#{state.namespace}/meta"

    with {:ok, rows} <- request(state, :get, "/v1/kv/#{key}?consistent", nil, state.rpc_timeout_ms, false),
         :ok <- validate_metadata_value(rows, key, state.epoch) do
      :ok
    else
      {:error, :not_found} -> {:error, :namespace_metadata_missing}
      {:error, _} = error -> error
      _ -> {:error, :protocol_mismatch}
    end
  end

  defp validate_metadata_value([%{"Key" => key, "Value" => encoded}], key, epoch) do
    with {:ok, raw} <- Base.decode64(encoded),
         true <- byte_size(raw) in 1..8_192,
         :ok <- Dilock.Identity.validate_metadata(raw, @profile, epoch) do
      :ok
    else
      _ -> {:error, :protocol_mismatch}
    end
  end

  defp validate_metadata_value(_, _, _), do: {:error, :protocol_mismatch}

  defp create_session(state, ttl) do
    sent_at = System.monotonic_time(:millisecond)
    body = Jason.encode!(%{"Name" => "dilock lease", "Behavior" => "release", "LockDelay" => "0s", "TTL" => "#{ttl}s", "Checks" => []})

    with {:ok, %{"ID" => session}} <- request(state, :put, "/v1/session/create", body, state.rpc_timeout_ms, true),
         true <- is_binary(session) and session != "" and byte_size(session) <= 128 do
      {:ok, session, sent_at}
    else
      {:error, reason} -> {:error, {:acquisition_unknown, reason}}
      _ -> {:error, {:acquisition_unknown, :invalid_session_create_response}}
    end
  end

  defp renew_session(state, session, timeout) do
    sent_at = System.monotonic_time(:millisecond)

    case request(state, :put, "/v1/session/renew/#{session}", nil, timeout, true) do
      {:ok, [%{"ID" => ^session}]} -> {:ok, sent_at}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_session_renewal_response}
    end
  end

  defp acquire_key(state, key, session, timeout),
    do: request(state, :put, "/v1/kv/#{key}?acquire=#{session}", "", timeout, true)

  defp release_key(state, key, session, timeout),
    do: request(state, :put, "/v1/kv/#{key}?release=#{session}", "", timeout, true)

  defp owner(state, key, timeout) do
    case request(state, :get, "/v1/kv/#{key}?consistent", nil, timeout, false) do
      {:ok, [%{"Key" => ^key, "Session" => session, "LockIndex" => index}]}
      when (is_binary(session) or is_nil(session)) and is_integer(index) and index >= 0 ->
        {:ok, %{session: session, lock_index: index}}
      {:error, :not_found} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :protocol_mismatch}
    end
  end

  defp destroy_session(state, session, timeout) do
    case request(state, :put, "/v1/session/destroy/#{session}", nil, timeout, true) do
      {:ok, _} -> :ok
      {:error, _} -> :unknown
    end
  end

  defp cleanup_attempt(state, key, session, outcome) do
    release = release_key(state, key, session, state.cleanup_timeout_ms)
    destroyed = destroy_session(state, session, state.cleanup_timeout_ms)
    if match?({:ok, _}, release) and destroyed == :ok do
      if outcome == :acquisition_timeout, do: {:error, :acquisition_timeout}, else: {:error, outcome}
    else
      {:error, {:acquisition_unknown, :cleanup_unconfirmed}}
    end
  end

  defp request(state, method, path, body, timeout, mutation?) do
    # Merge an absolute API path so a configured endpoint ending in `/` cannot
    # produce a double slash in the request URL.
    url = URI.to_string(URI.merge(state.endpoint, path))
    headers = [{~c"accept", ~c"application/json"}]
    headers = if state.token, do: [{~c"x-consul-token", String.to_charlist(state.token)} | headers], else: headers
    http_options = [timeout: timeout, connect_timeout: timeout, autoredirect: false]
    http_options = if state.endpoint.scheme == "https", do: Keyword.put(http_options, :ssl, ssl_options(state.endpoint.host)), else: http_options
    options = [body_format: :binary]

    request =
      case method do
        :get -> {String.to_charlist(url), headers}
        :put -> {String.to_charlist(url), headers, ~c"application/json", body || ""}
      end

    case :httpc.request(method, request, http_options, options) do
      {:ok, {{_version, code, _reason}, _response_headers, response_body}} when code in 200..299 ->
        decode_response(response_body, mutation?)
      {:ok, {{_version, 404, _reason}, _headers, _body}} -> {:error, :not_found}
      {:ok, {{_version, code, _reason}, _headers, _body}} when mutation? and code >= 500 ->
        {:error, :mutation_outcome_unknown}
      {:ok, {{_version, code, _reason}, _headers, _body}} -> {:error, {:http_rejected, code}}
      {:error, reason} when mutation? -> {:error, {:mutation_outcome_unknown, reason}}
      {:error, reason} -> {:error, {:http_client_error, reason}}
    end
  rescue
    error ->
      if mutation? do
        {:error, {:mutation_outcome_unknown, error}}
      else
        {:error, {:http_client_error, error}}
      end
  end

  defp decode_response(body, mutation?) do
    case Jason.decode(body) do
      {:ok, value} -> {:ok, value}
      {:error, reason} when mutation? -> {:error, {:mutation_response_invalid, reason}}
      {:error, reason} -> {:error, {:invalid_json, reason}}
    end
  end

  defp ssl_options(host) do
    [verify: :verify_peer, cacerts: :public_key.cacerts_get(),
     server_name_indication: String.to_charlist(host),
     customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]]
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
  defp validate_transport(%URI{scheme: "https"}), do: :ok
  defp validate_transport(_), do: {:error, :invalid_endpoint_scheme}
  defp valid_token?(token), do: is_binary(token) and token != "" and String.printable?(token) and String.valid?(token) and Enum.all?(:binary.bin_to_list(token), &(&1 in 33..126))
  defp valid_policy?(wait, lease, true), do: is_integer(wait) and wait in 1..@max_ttl_ms and valid_ttl?(lease)
  defp valid_policy?(wait, lease, false), do: wait == 0 and valid_ttl?(lease)
  defp valid_ttl?(ms), do: is_integer(ms) and ms in @min_ttl_ms..@max_ttl_ms
  defp valid_timeout?(ms), do: is_integer(ms) and ms in 1..60_000
  defp safety(ttl), do: max(100, div(ttl + 99, 100))
end
