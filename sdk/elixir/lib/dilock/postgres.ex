defmodule Dilock.Postgres do
  @moduledoc "Experimental PostgreSQL session advisory locks. Each acquisition owns one private Postgrex session."
  @behaviour Dilock.Backend

  @profile "postgres-advisory-session-v1"
  @scheme "sha256-framed-utf8-v1"

  def start_link(options), do: Dilock.Provider.start_link(Keyword.put(options, :backend, __MODULE__))
  def child_spec(options), do: Dilock.Provider.child_spec(Keyword.put(options, :backend, __MODULE__))

  @impl true
  def validate(options) do
    connection = options[:connection_string]
    namespace = options[:namespace] || "default"
    epoch = options[:epoch]

    with true <- is_binary(connection) and connection != "",
         true <- valid_connection_string?(connection),
         {:ok, _} <- Dilock.Identity.resource_id(namespace, "validation"),
         true <- is_binary(epoch) and epoch != "" and byte_size(epoch) <= 1024,
         true <- valid_timeout?(options[:rpc_timeout_ms] || 2_000),
         true <- valid_timeout?(options[:cleanup_timeout_ms] || 2_000),
         true <- valid_poll?(options[:poll_interval_ms] || 25),
         true <- valid_lease?(options[:default_lease_ms] || 30_000) do
      :ok
    else
      _ -> {:error, :invalid_configuration}
    end
  end

  @impl true
  def init(options) do
    state = state_from(options)

    case with_session(state, fn connection -> validate_metadata(connection, state) end) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, {:postgres_initialization_failed, reason}}
    end
  end

  @impl true
  def close(_state), do: :ok

  @impl true
  def acquire(state, resource_id, %{wait_ms: wait_ms, lease_ms: lease_ms}, config) do
    wait? = config[:wait] != false
    require_fencing? = config[:require_fencing] == true
    auto_renew? = config[:auto_renew] == true

    cond do
      require_fencing? or auto_renew? -> {:error, :unsupported_capability}
      not valid_policy?(wait_ms, lease_ms, wait?) -> {:error, :invalid_acquisition_options}
      true -> acquire_until(state, resource_id, wait?, wait_ms, lease_ms)
    end
  end

  def acquire(_state, _resource_id, _options, _config), do: {:error, :invalid_acquisition_options}

  defp acquire_until(state, resource_id, wait?, wait_ms, lease_ms) do
    deadline = System.monotonic_time(:millisecond) + wait_ms
    key = advisory_key(resource_id)

    case open_session(state) do
      {:ok, connection} ->
        case query(connection, state.rpc_timeout_ms, "SELECT pg_try_advisory_lock($1::bigint)", [key]) do
          {:ok, %{rows: [[true]]}} ->
            {:ok, %{backend_state: state, connection: connection, key: key,
              effective_lease_ms: lease_ms, deadline_ms: :infinity}}

          {:ok, %{rows: [[false]]}} ->
            stop_session(connection)
            if wait? and System.monotonic_time(:millisecond) < deadline do
              Process.sleep(min(state.poll_interval_ms, max(deadline - System.monotonic_time(:millisecond), 1)))
              acquire_until(state, resource_id, true, max(deadline - System.monotonic_time(:millisecond), 1), lease_ms)
            else
              :busy
            end

          {:error, reason} ->
            stop_session(connection)
            {:error, {:acquisition_unknown, reason}}

          _ ->
            stop_session(connection)
            {:error, {:acquisition_unknown, :malformed_advisory_lock_response}}
        end

      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def renew(state, %{connection: connection} = ownership, _ttl_ms, options) do
    case query(connection, options[:timeout_ms] || state.rpc_timeout_ms, "SELECT 1", []) do
      {:ok, %{rows: [[1]]}} -> {:ok, ownership}
      {:ok, _} -> {:error, :lease_lost}
      {:error, reason} -> {:error, {:backend_unavailable, reason}}
    end
  end

  @impl true
  def release(state, %{connection: connection, key: key}, options) do
    timeout = options[:timeout_ms] || state.cleanup_timeout_ms
    result =
      case query(connection, timeout, "SELECT pg_advisory_unlock($1::bigint)", [key]) do
        {:ok, %{rows: [[true]]}} -> :released
        {:ok, %{rows: [[false]]}} -> :already_not_owned
        _ -> :unknown
      end

    stop_session(connection)
    result
  rescue
    _ -> stop_session(connection); :unknown
  end

  @doc "Creates the metadata table and row only when absent; conflicts are validated, never overwritten."
  def provision_namespace(options) do
    with :ok <- validate(options),
         state = state_from(options),
         {:ok, connection} <- open_session(state) do
      result =
        case Postgrex.transaction(connection, fn transaction ->
               with {:ok, _} <- query(transaction, state.rpc_timeout_ms, "SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))", []),
                    {:ok, _} <- query(transaction, state.rpc_timeout_ms, "CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)", []),
                    {:ok, _} <- query(transaction, state.rpc_timeout_ms, "INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES ($1,1,$2,$3,$4,NULL) ON CONFLICT (namespace) DO NOTHING", [state.namespace, @scheme, @profile, state.epoch]) do
                 {:ok, :provisioned}
               else
                 {:error, reason} -> Postgrex.rollback(transaction, reason)
               end
             end, timeout: state.rpc_timeout_ms) do
          {:ok, _} -> validate_metadata(connection, state)
          {:error, reason} -> {:error, reason}
        end

      stop_session(connection)
      result
    end
  end

  defp state_from(options), do: %{connection_string: options[:connection_string], namespace: options[:namespace] || "default", epoch: options[:epoch], rpc_timeout_ms: options[:rpc_timeout_ms] || 2_000, cleanup_timeout_ms: options[:cleanup_timeout_ms] || 2_000, poll_interval_ms: options[:poll_interval_ms] || 25}

  defp open_session(state) do
    options = connection_options(state.connection_string) ++ [timeout: state.rpc_timeout_ms, pool_size: 1]

    case Postgrex.start_link(options) do
      {:ok, connection} ->
        Process.unlink(connection)
        {:ok, connection}
      {:error, reason} -> {:error, {:backend_unavailable, reason}}
    end
  rescue
    error -> {:error, {:invalid_connection_string, error}}
  end

  defp with_session(state, fun) do
    case open_session(state) do
      {:ok, connection} -> result = fun.(connection); stop_session(connection); result
      {:error, reason} -> {:error, reason}
    end
  end

  defp stop_session(connection), do: GenServer.stop(connection, :normal, 1_000)

  defp query(connection, timeout, sql, params) do
    case Postgrex.query(connection, sql, params, timeout: timeout) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_metadata(connection, state) do
    case query(connection, state.rpc_timeout_ms, "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace=$1", [state.namespace]) do
      {:ok, %{num_rows: 1, rows: [[1, @scheme, @profile, epoch, nil]]}} when epoch == state.epoch -> :ok
      {:ok, _} -> {:error, :protocol_mismatch}
      {:error, reason} -> {:error, {:backend_unavailable, reason}}
    end
  end

  @doc "Converts the first eight identity digest bytes to signed PostgreSQL bigint."
  def advisory_key(resource_id) do
    <<word::unsigned-big-integer-size(64), _::binary>> = Base.decode16!(resource_id, case: :lower)
    <<signed::signed-big-integer-size(64)>> = <<word::unsigned-big-integer-size(64)>>
    signed
  end

  defp valid_timeout?(value), do: is_integer(value) and value in 1..60_000
  defp valid_poll?(value), do: is_integer(value) and value in 1..60_000
  defp valid_lease?(value), do: is_integer(value) and value in 1..86_400_000
  defp valid_policy?(wait, lease, true), do: is_integer(wait) and wait in 1..86_400_000 and valid_lease?(lease)
  defp valid_policy?(_wait, lease, false), do: valid_lease?(lease)

  defp valid_connection_string?(value) do
    case URI.parse(value) do
      %URI{scheme: scheme, host: host, path: path}
      when scheme in ["postgres", "postgresql"] and is_binary(host) and is_binary(path) ->
        String.trim_leading(path, "/") != ""

      _ ->
        false
    end
  end

  defp connection_options(value) do
    %URI{scheme: scheme, host: host, port: port, path: path, userinfo: userinfo, query: query} = URI.parse(value)
    true = scheme in ["postgres", "postgresql"]
    database = URI.decode(String.trim_leading(path, "/"))
    options = [hostname: host, port: port || 5432, database: database]

    options =
      case userinfo do
        nil -> options
        encoded ->
          [username | password] = String.split(encoded, ":", parts: 2)
          options
          |> Keyword.put(:username, URI.decode(username))
          |> maybe_put_password(password)
      end

    case URI.decode_query(query || "") do
      %{} = parameters when map_size(parameters) == 0 -> options
      %{"sslmode" => mode} when mode in ["disable", "allow"] -> Keyword.put(options, :ssl, false)
      %{"sslmode" => mode} when mode in ["prefer", "require", "verify-ca", "verify-full"] -> Keyword.put(options, :ssl, true)
      parameters -> raise ArgumentError, "unsupported PostgreSQL connection parameters: #{inspect(Map.keys(parameters))}"
    end
  end

  defp maybe_put_password(options, [password]), do: Keyword.put(options, :password, URI.decode(password))
  defp maybe_put_password(options, _), do: options
end
