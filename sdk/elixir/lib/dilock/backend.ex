defmodule Dilock.Backend do
  @moduledoc "Backend boundary used by provider processes."
  @callback validate(keyword()) :: :ok | {:error, term()}
  @callback init(keyword()) :: {:ok, map()} | {:error, term()}
  @callback close(map()) :: :ok
  @callback acquire(map(), binary(), map(), keyword()) :: {:ok, map()} | :busy | {:error, term()}
  @callback renew(map(), map(), pos_integer(), keyword()) :: :ok | {:error, term()}
  @callback release(map(), map(), keyword()) :: Dilock.outcome()
end
