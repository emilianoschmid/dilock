# Local-only OTP example. No remote backend, TTL, monitors, renewal or fencing.
defmodule DilockStyle.MemoryStore do
  @moduledoc "Local mutable-state fixture; GenServer serialization is not distributed consensus."
  use GenServer
  def start_link, do: GenServer.start_link(__MODULE__, %{})
  def claim(server, resource, owner), do: GenServer.call(server, {:claim, resource, owner})
  def release(server, resource, owner), do: GenServer.call(server, {:release, resource, owner})
  def expire_for_test(server, resource), do: GenServer.call(server, {:expire, resource})
  @impl true
  def init(state), do: {:ok, state}
  @impl true
  def handle_call({:claim, resource, owner}, _from, state) do
    if Map.has_key?(state, resource),
      do: {:reply, :busy, state},
      else: {:reply, :ok, Map.put(state, resource, owner)}
  end
  def handle_call({:release, resource, owner}, _from, state) do
    if Map.get(state, resource) == owner,
      do: {:reply, :released, Map.delete(state, resource)},
      else: {:reply, :already_not_owned, state}
  end
  def handle_call({:expire, resource}, _from, state),
    do: {:reply, :ok, Map.delete(state, resource)}
end

defmodule DilockStyle do
  @moduledoc "Tagged tuples and scoped execution; no transplanted class hierarchy."
  alias DilockStyle.MemoryStore
  @opaque lease :: {:lease, pid(), binary(), reference()}
  @spec try_acquire(pid(), binary()) :: {:ok, lease()} | :busy | {:error, :invalid_resource}
  def try_acquire(_server, ""), do: {:error, :invalid_resource}
  def try_acquire(server, resource) when is_binary(resource) do
    owner = make_ref() # Local identity, not the protocol's wire value.
    case MemoryStore.claim(server, resource, owner) do
      :ok -> {:ok, {:lease, server, resource, owner}}
      :busy -> :busy
    end
  end
  @spec release(lease()) :: :released | :already_not_owned
  def release({:lease, server, resource, owner}), do: MemoryStore.release(server, resource, owner)
  def with_lease(server, resource, fun) when is_function(fun, 1) do
    case try_acquire(server, resource) do
      {:ok, lease} ->
        try do
          {:ok, fun.(lease)}
        after
          release(lease)
        end
      other -> other
    end
  end
end

alias DilockStyle.MemoryStore
{:ok, server} = MemoryStore.start_link()
try do
  {:ok, first} = DilockStyle.try_acquire(server, "example/resource")
  :busy = DilockStyle.try_acquire(server, "example/resource")
  :ok = MemoryStore.expire_for_test(server, "example/resource")
  {:ok, second} = DilockStyle.try_acquire(server, "example/resource")
  :already_not_owned = DilockStyle.release(first)
  :busy = DilockStyle.try_acquire(server, "example/resource")
  :released = DilockStyle.release(second)
  :already_not_owned = DilockStyle.release(second)
  try do
    DilockStyle.with_lease(server, "example/resource", fn _ -> raise "consumer failure" end)
  rescue
    RuntimeError -> :ok
  end
  {:ok, 42} = DilockStyle.with_lease(server, "example/resource", fn _ -> 42 end)
  {:error, :invalid_resource} = DilockStyle.try_acquire(server, "")
  IO.puts("PASS elixir: busy, stale owner, idempotence, try/after, tagged errors")
after
  GenServer.stop(server)
end
