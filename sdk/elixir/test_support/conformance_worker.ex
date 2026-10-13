defmodule Dilock.ConformanceWorker do
  @moduledoc false

  @max_line_bytes 65_536
  @max_active 128
  @operations ~w(hello acquire try_acquire renew release inspect cancel_request shutdown)
  @fields ~w(id op resource lease_id target_id wait_ms lease_ms auto_renew)

  def max_line_bytes, do: @max_line_bytes

  def parse_request(line) when is_binary(line) do
    cond do
      byte_size(line) > @max_line_bytes ->
        {:error, :line_too_long}

      not String.valid?(line) ->
        {:error, :invalid_utf8}

      true ->
        case Jason.decode(line) do
          {:ok, request} when is_map(request) -> validate_request(request)
          {:ok, _} -> {:error, :request_not_object}
          {:error, _} -> {:error, :invalid_json}
        end
    end
  end

  def parse_request(_), do: {:error, :invalid_utf8}

  def run(config, input \\ :stdio, output \\ :stdio, diagnostics \\ :stderr) do
    with {:ok, provider, backend, namespace} <- start_provider(config) do
      parent = self()
      spawn(fn -> read_input(input, parent, <<>>, false) end)

      state = %{
        provider: provider,
        backend: backend,
        namespace: namespace,
        output: output,
        diagnostics: diagnostics,
        active: %{},
        leases: %{},
        released: %{},
        cancelled_refs: %{}
      }

      if System.get_env("DILOCK_M8_WORKER_READY") == "1",
        do: diagnostic(state, "ready")

      loop(state)
    end
  end

  defp validate_request(request) do
    keys = Map.keys(request)
    op = request["op"]
    id = request["id"]

    cond do
      Enum.any?(keys, &(not is_binary(&1))) ->
        {:error, :invalid_field_name}

      Enum.any?(keys -- @fields, fn _ -> true end) ->
        {:error, :unknown_field}

      not (is_binary(id) and byte_size(id) > 0 and String.length(id) <= 128) ->
        {:error, :invalid_id}

      op not in @operations ->
        {:error, :unknown_operation}

      missing?(op, keys) ->
        {:error, :required_field_missing}

      not valid_string_fields?(request) ->
        {:error, :invalid_string_field}

      not valid_duration_fields?(request) ->
        {:error, :invalid_duration}

      Map.has_key?(request, "auto_renew") and not is_boolean(request["auto_renew"]) ->
        {:error, :invalid_auto_renew}

      true ->
        {:ok, request}
    end
  end

  defp missing?("acquire", keys), do: "resource" not in keys or "wait_ms" not in keys
  defp missing?("try_acquire", keys), do: "resource" not in keys
  defp missing?(op, keys) when op in ["release", "renew", "inspect"], do: "lease_id" not in keys
  defp missing?("cancel_request", keys), do: "target_id" not in keys
  defp missing?(_, _), do: false

  defp valid_string_fields?(request) do
    Enum.all?(~w(resource lease_id target_id), fn field ->
      case Map.fetch(request, field) do
        :error -> true
        {:ok, value} -> is_binary(value) and byte_size(value) > 0
      end
    end)
  end

  defp valid_duration_fields?(request) do
    wait_minimum = if request["op"] == "acquire", do: 1, else: 0
    valid_duration?(request, "wait_ms", wait_minimum) and valid_duration?(request, "lease_ms", 1)
  end

  defp valid_duration?(request, field, minimum) do
    case Map.fetch(request, field) do
      :error -> true
      {:ok, value} -> is_integer(value) and value >= minimum and value <= 86_400_000
    end
  end

  defp start_provider(config) do
    with {:ok, value} <- read_config(config),
         backend when backend in ["redis", "etcd"] <- value["backend"],
         endpoint when is_binary(endpoint) and endpoint != "" <- value["endpoint"],
         namespace when is_binary(namespace) and namespace != "" <- value["namespace"],
         epoch when is_binary(epoch) and epoch != "" <- value["epoch"],
         module <- if(backend == "redis", do: Dilock.Redis, else: Dilock.Etcd),
         {:ok, provider} <-
           module.start_link(
             endpoint: endpoint,
             namespace: namespace,
             epoch: epoch,
             rpc_timeout_ms: value["rpc_timeout_ms"] || 2_000,
             cleanup_timeout_ms: value["cleanup_timeout_ms"] || 2_000
           ) do
      {:ok, provider, backend, namespace}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_config}
    end
  rescue
    _ -> {:error, :provider_start_failed}
  catch
    _, _ -> {:error, :provider_start_failed}
  end

  defp read_config(path) when is_binary(path) do
    with {:ok, bytes} <- File.read(path),
         true <- byte_size(bytes) <= @max_line_bytes,
         true <- String.valid?(bytes),
         {:ok, value} when is_map(value) <- Jason.decode(bytes) do
      {:ok, value}
    else
      _ -> {:error, :invalid_config}
    end
  end

  defp read_config(_), do: {:error, :invalid_config}

  # Read a byte at a time so a malicious unterminated line cannot grow memory
  # without bound. Overlong lines are discarded through their newline.
  defp read_input(input, parent, current, overlong?) do
    case IO.binread(input, 1) do
      :eof ->
        if byte_size(current) > 0 and not overlong?, do: send(parent, {:input, current})
        if overlong?, do: send(parent, {:input_error, :line_too_long})
        send(parent, :input_eof)

      {:error, _} ->
        send(parent, {:input_error, :read_failed})
        send(parent, :input_eof)

      "\n" ->
        if overlong?,
          do: send(parent, {:input_error, :line_too_long}),
          else: send(parent, {:input, trim_cr(current)})

        read_input(input, parent, <<>>, false)

      byte ->
        if overlong? do
          read_input(input, parent, <<>>, true)
        else
          next = current <> byte

          if byte_size(next) > @max_line_bytes do
            read_input(input, parent, <<>>, true)
          else
            read_input(input, parent, next, false)
          end
        end
    end
  end

  defp trim_cr(line) do
    if String.ends_with?(line, "\r"), do: binary_part(line, 0, byte_size(line) - 1), else: line
  end

  defp loop(state) do
    receive do
      {:input, line} ->
        case parse_request(line) do
          {:ok, %{"op" => "shutdown", "id" => id}} ->
            trace(state, "received op=shutdown")

            if Map.has_key?(state.active, id) do
              write(state, %{id: id, status: "invalid_argument"})
              loop(state)
            else
              shutdown(state, id)
            end

          {:ok, request} ->
            trace(state, "received op=#{request["op"]}")
            loop(handle_request(request, state))

          {:error, reason} ->
            diagnostic(state, "invalid request (#{reason})")
            write(state, %{id: "", status: "invalid_argument"})
            loop(state)
        end

      {:input_error, reason} ->
        diagnostic(state, "invalid input (#{reason})")
        write(state, %{id: "", status: "invalid_argument"})
        loop(state)

      {:operation_complete, id, response, lease_id, pid} ->
        loop(operation_complete(id, response, lease_id, pid, state))

      {:lease_complete, id, lease_id, response, terminal?} ->
        loop(lease_complete(id, lease_id, response, terminal?, state))

      {:DOWN, monitor, :process, pid, reason} ->
        loop(process_down(monitor, pid, reason, state))

      :input_eof ->
        shutdown(state, nil)
    end
  end

  defp handle_request(request, state) do
    id = request["id"]
    op = request["op"]

    cond do
      Map.has_key?(state.active, id) ->
        write(state, %{id: id, status: "invalid_argument"})
        state

      map_size(state.active) >= @max_active ->
        write(state, %{id: id, status: "internal_error"})
        state

      op == "cancel_request" ->
        cancel_request(state, id, request["target_id"])

      op == "hello" ->
        write(state, hello(state, id))
        state

      op in ["acquire", "try_acquire"] ->
        start_acquisition(state, request)

      op in ["release", "renew"] ->
        start_lease_operation(state, request)

      op == "inspect" ->
        inspect_lease(state, id, request["lease_id"])

      true ->
        write(state, %{id: id, status: "invalid_argument"})
        state
    end
  end

  defp hello(state, id) do
    client = if state.backend == "redis", do: :redix, else: :grpc

    %{
      id: id,
      status: "ok",
      sdk: "elixir",
      version: version(:dilock),
      protocol: 1,
      backend: state.backend,
      server_version: "unknown",
      client_version: version(client),
      capabilities: %{
        renewal: true,
        auto_renew: false,
        fencing: state.backend == "etcd",
        try_acquire: state.backend == "redis"
      }
    }
  end

  defp version(app) do
    case Application.spec(app, :vsn) do
      nil -> "unknown"
      value when is_list(value) -> List.to_string(value)
      value -> to_string(value)
    end
  end

  defp start_acquisition(state, request) do
    id = request["id"]
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        acquire_and_hold(parent, id, request, state.provider, state.namespace, state.backend)
      end)

    %{state | active: Map.put(state.active, id, %{pid: pid, monitor: monitor, op: request["op"]})}
  end

  defp acquire_and_hold(parent, id, request, provider, namespace, backend) do
    operation = request["op"]

    result =
      cond do
        request["auto_renew"] == true ->
          {:error, :unsupported_capability}

        operation == "try_acquire" and backend == "etcd" ->
          {:error, :unsupported_capability}

        true ->
          with {:ok, lock} <- Dilock.lock(provider, namespace, request["resource"]) do
            options = %{
              wait_ms: if(operation == "try_acquire", do: 0, else: request["wait_ms"] || 5_000),
              lease_ms: request["lease_ms"] || 30_000
            }

            if operation == "try_acquire",
              do: Dilock.Lock.try_acquire(lock, options),
              else: Dilock.Lock.acquire(lock, options)
          end
      end

    case result do
      {:ok, lease} ->
        ownership = Dilock.Lease.ownership(lease)
        lease_id = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
        response = acquired_response(id, lease_id, ownership)
        send(parent, {:operation_complete, id, response, lease_id, self()})
        lease_owner_loop(parent, id, lease_id, lease, ownership)

      :busy ->
        send(parent, {:operation_complete, id, %{id: id, status: "busy"}, nil, self()})

      {:error, reason} ->
        response = error_response(id, reason)
        send(parent, {:operation_complete, id, response, nil, self()})
    end
  rescue
    error ->
      send(
        parent,
        {:operation_complete, id, error_response(id, {:internal_error, error.__struct__}), nil,
         self()}
      )
  catch
    kind, _ ->
      send(
        parent,
        {:operation_complete, id, error_response(id, {:internal_error, kind}), nil, self()}
      )
  end

  defp acquired_response(id, lease_id, ownership) do
    response = %{
      id: id,
      status: "acquired",
      lease_id: lease_id,
      effective_lease_ms: ownership.effective_lease_ms
    }

    case Map.get(ownership, :fence) do
      %Dilock.Etcd.Fence{epoch: epoch, revision: revision} ->
        Map.put(response, :fence, %{epoch: epoch, value: Integer.to_string(revision)})

      _ ->
        response
    end
  end

  defp lease_owner_loop(parent, acquire_id, lease_id, lease, ownership) do
    receive do
      {:lease_command, request_id, :release} ->
        outcome = Dilock.Lease.release(lease)
        response = %{id: request_id, status: release_status(outcome)}
        send(parent, {:lease_complete, request_id, lease_id, response, true})
        _ = Dilock.Lease.close(lease)

      {:lease_command, request_id, {:renew, requested_ms}} ->
        result = Dilock.Lease.renew(lease, requested_ms || ownership.effective_lease_ms)
        response = renew_response(request_id, result)
        send(parent, {:lease_complete, request_id, lease_id, response, false})
        lease_owner_loop(parent, acquire_id, lease_id, lease, Dilock.Lease.ownership(lease))

      {:lease_command, request_id, :inspect} ->
        current = Dilock.Lease.ownership(lease)

        state =
          if System.monotonic_time(:millisecond) < current.deadline_ms, do: "held", else: "lost"

        send(
          parent,
          {:lease_complete, request_id, lease_id, %{id: request_id, status: "ok", state: state},
           false}
        )

        lease_owner_loop(parent, acquire_id, lease_id, lease, current)

      {:shutdown_release, token} ->
        outcome = Dilock.Lease.release(lease)
        send(parent, {:shutdown_complete, token, lease_id, outcome})
        _ = Dilock.Lease.close(lease)
    end
  end

  defp start_lease_operation(state, request) do
    id = request["id"]
    lease_id = request["lease_id"]

    cond do
      Map.has_key?(state.leases, lease_id) ->
        lease = Map.fetch!(state.leases, lease_id)
        command = if request["op"] == "release", do: :release, else: {:renew, request["lease_ms"]}
        send(lease.pid, {:lease_command, id, command})

        %{
          state
          | active:
              Map.put(state.active, id, %{
                pid: lease.pid,
                monitor: lease.monitor,
                op: request["op"],
                lease_id: lease_id
              })
        }

      Map.has_key?(state.released, lease_id) and request["op"] == "release" ->
        write(state, %{id: id, status: Map.fetch!(state.released, lease_id)})
        state

      true ->
        write(state, %{id: id, status: "invalid_argument"})
        state
    end
  end

  defp inspect_lease(state, id, lease_id) do
    case Map.fetch(state.leases, lease_id) do
      {:ok, lease} ->
        send(lease.pid, {:lease_command, id, :inspect})

        %{
          state
          | active:
              Map.put(state.active, id, %{
                pid: lease.pid,
                monitor: lease.monitor,
                op: "inspect",
                lease_id: lease_id
              })
        }

      :error ->
        write(state, %{id: id, status: "invalid_argument"})
        state
    end
  end

  defp operation_complete(id, response, lease_id, pid, state) do
    case Map.pop(state.active, id) do
      {nil, _active} ->
        state

      {%{pid: ^pid, monitor: monitor}, active} when not is_nil(lease_id) ->
        write(state, response)
        lease = %{pid: pid, monitor: monitor}
        %{state | active: active, leases: Map.put(state.leases, lease_id, lease)}

      {%{monitor: monitor}, active} ->
        Process.demonitor(monitor, [:flush])
        write(state, response)
        %{state | active: active}
    end
  end

  defp lease_complete(id, lease_id, response, terminal?, state) do
    write(state, response)
    active = Map.delete(state.active, id)

    if terminal? do
      status = response.status

      %{
        state
        | active: active,
          leases: Map.delete(state.leases, lease_id),
          released: Map.put(state.released, lease_id, status)
      }
    else
      %{state | active: active}
    end
  end

  defp process_down(monitor, pid, _reason, state) do
    case Map.pop(state.cancelled_refs, monitor) do
      {cancelled_id, cancelled_refs} when not is_nil(cancelled_id) ->
        leases =
          Map.reject(state.leases, fn {_lease_id, value} ->
            value.monitor == monitor and value.pid == pid
          end)

        %{state | cancelled_refs: cancelled_refs, leases: leases}

      {nil, _cancelled_refs} ->
        process_down_active_or_lease(monitor, pid, state)
    end
  end

  defp process_down_active_or_lease(monitor, pid, state) do
    case Enum.find(state.active, fn {_id, value} ->
           value.monitor == monitor and value.pid == pid
         end) do
      {id, request} ->
        response =
          if request.op in ["acquire", "try_acquire"],
            do: %{id: id, status: "acquisition_unknown", remote_outcome_unknown: true},
            else: %{
              id: id,
              status:
                if(request.op == "release", do: "release_unknown", else: "backend_unavailable"),
              remote_outcome_unknown: true
            }

        write(state, response)

        %{
          state
          | active: Map.delete(state.active, id),
            leases: Map.delete(state.leases, request[:lease_id])
        }

      nil ->
        case Enum.find(state.leases, fn {_id, value} ->
               value.monitor == monitor and value.pid == pid
             end) do
          {lease_id, _lease} ->
            write(state, %{event: "lease_lost", lease_id: lease_id, reason: "owner_process_down"})
            %{state | leases: Map.delete(state.leases, lease_id)}

          nil ->
            state
        end
    end
  end

  defp cancel_request(state, id, target_id) do
    case Map.fetch(state.active, target_id) do
      {:ok, request} ->
        Process.exit(request.pid, :kill)
        target_status = if request.op == "release", do: "release_unknown", else: "cancelled"
        write(state, %{id: target_id, status: target_status, remote_outcome_unknown: true})
        write(state, %{id: id, status: "ok", cancelled: true})
        refs = Map.put(state.cancelled_refs, request.monitor, target_id)
        %{state | active: Map.delete(state.active, target_id), cancelled_refs: refs}

      :error ->
        write(state, %{id: id, status: "ok", cancelled: false})
        state
    end
  end

  defp shutdown(state, id) do
    pending =
      Enum.filter(state.active, fn {_key, entry} -> entry.op in ["acquire", "try_acquire"] end)

    Enum.each(pending, fn {request_id, entry} ->
      Process.exit(entry.pid, :kill)

      if not is_nil(id),
        do: write(state, %{id: request_id, status: "cancelled", remote_outcome_unknown: true})
    end)

    token = make_ref()

    Enum.each(state.leases, fn {_lease_id, entry} ->
      send(entry.pid, {:shutdown_release, token})
    end)

    deadline = System.monotonic_time(:millisecond) + 2_500
    outcomes = collect_shutdown(token, map_size(state.leases), deadline, 0)
    outstanding = outcomes + length(pending)

    Enum.each(state.leases, fn {_lease_id, entry} ->
      if Process.alive?(entry.pid), do: Process.exit(entry.pid, :kill)
    end)

    try do
      GenServer.stop(state.provider, :normal, 3_000)
    catch
      _, _ -> :ok
    end

    if not is_nil(id), do: write(state, %{id: id, status: "ok", outstanding_cleanup: outstanding})
    :ok
  end

  defp collect_shutdown(_token, 0, _deadline, failures), do: failures

  defp collect_shutdown(token, remaining, deadline, failures) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:shutdown_complete, ^token, _lease_id, outcome} ->
        collect_shutdown(
          token,
          remaining - 1,
          deadline,
          failures + if(outcome == :released, do: 0, else: 1)
        )
    after
      timeout -> failures + remaining
    end
  end

  defp renew_response(id, :ok), do: %{id: id, status: "renewed"}
  defp renew_response(id, {:error, reason}), do: error_response(id, reason)
  defp renew_response(id, _), do: %{id: id, status: "lost"}

  defp release_status(:released), do: "released"
  defp release_status(:already_not_owned), do: "already_not_owned"
  defp release_status(_), do: "release_unknown"

  defp error_response(id, reason) do
    status = error_status(reason)
    response = %{id: id, status: status}

    if status in ["acquisition_unknown", "backend_unavailable", "timeout", "cancelled"],
      do: Map.put(response, :remote_outcome_unknown, status == "acquisition_unknown"),
      else: response
  end

  defp error_status(:acquisition_timeout), do: "timeout"
  defp error_status(:acquisition_unknown), do: "acquisition_unknown"
  defp error_status({:acquisition_unknown, _}), do: "acquisition_unknown"
  defp error_status({:acquisition_unknown, _, _}), do: "acquisition_unknown"
  defp error_status(:unsupported_capability), do: "unsupported_capability"
  defp error_status({:backend_unavailable, _}), do: "backend_unavailable"
  defp error_status(:backend_unavailable), do: "backend_unavailable"
  defp error_status({:protocol_mismatch, _}), do: "protocol_mismatch"
  defp error_status(:protocol_mismatch), do: "protocol_mismatch"
  defp error_status(:invalid_acquisition_options), do: "invalid_argument"
  defp error_status(:lease_lost), do: "already_not_owned"
  defp error_status(:caller_cancelled_before_dispatch), do: "cancelled"
  defp error_status(:caller_cancelled_after_dispatch), do: "cancelled"
  defp error_status({:caller_cancelled_after_dispatch, _}), do: "cancelled"
  defp error_status(_), do: "internal_error"

  defp write(state, response) do
    IO.binwrite(state.output, [Jason.encode!(response), "\n"])
    trace(state, "wrote id=#{Map.get(response, :id)} status=#{Map.get(response, :status)}")
    :ok
  rescue
    _ -> diagnostic(state, "response write failed")
  end

  defp trace(state, message) do
    if System.get_env("DILOCK_M8_WORKER_TRACE") == "1", do: diagnostic(state, message)
  end

  defp diagnostic(state, message), do: IO.puts(state.diagnostics, "worker: #{message}")
end
