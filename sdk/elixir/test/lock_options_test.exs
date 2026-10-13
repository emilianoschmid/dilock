defmodule Dilock.LockOptionsTest do
  use ExUnit.Case, async: true

  test "blocking acquire rejects a zero wait budget before backend dispatch" do
    lock = %Dilock.Lock{provider: self(), resource_id: "resource-id"}

    assert {:error, :invalid_acquisition_options} =
             Dilock.Lock.acquire(lock, %{wait_ms: 0, lease_ms: 1_000})
  end
end
