defmodule Dilock.Lock do
  @moduledoc "Reusable resource handle. Each acquisition creates a distinct lease generation."
  @enforce_keys [:provider, :resource_id]
  defstruct [:provider, :resource_id]
  @type t :: %__MODULE__{provider: pid(), resource_id: binary()}

  @doc "Attempts once; `:busy` means ordinary contention only."
  def try_acquire(%__MODULE__{} = lock, options \\ Dilock.defaults()) do
    case options do
      %{lease_ms: lease_ms} when is_integer(lease_ms) and lease_ms in 1..86_400_000 ->
        Dilock.Provider.acquire(
          lock.provider,
          lock.resource_id,
          %{wait_ms: 0, lease_ms: lease_ms},
          false
        )

      _ ->
        {:error, :invalid_acquisition_options}
    end
  end

  @doc "Waits up to `wait_ms`; timeout, cancellation and backend errors are distinct tagged errors."
  def acquire(%__MODULE__{} = lock, options \\ Dilock.defaults()) do
    case options do
      %{wait_ms: wait_ms, lease_ms: lease_ms}
      when is_integer(wait_ms) and wait_ms in 1..86_400_000 and
             is_integer(lease_ms) and lease_ms in 1..86_400_000 ->
        Dilock.Provider.acquire(lock.provider, lock.resource_id, options, true)

      _ ->
        {:error, :invalid_acquisition_options}
    end
  end
end
