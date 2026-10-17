defmodule Dilock.ConsulContractTest do
  use ExUnit.Case, async: true

  @options [endpoint: "http://127.0.0.1:8500", namespace: "default", epoch: "test-epoch"]

  test "validates the endpoint and profile options without network access" do
    assert :ok = Dilock.Consul.validate(@options)
    assert :ok = Dilock.Consul.validate(Keyword.put(@options, :token, "acl-token"))
  end

  test "rejects endpoint paths, malformed tokens and invalid timeouts" do
    assert {:error, :invalid_endpoint_path} =
             Dilock.Consul.validate(Keyword.put(@options, :endpoint, "http://127.0.0.1:8500/consul"))

    assert {:error, :invalid_configuration} =
             Dilock.Consul.validate(Keyword.put(@options, :token, "token with spaces"))

    assert {:error, :invalid_configuration} =
             Dilock.Consul.validate(Keyword.put(@options, :rpc_timeout_ms, 0))
  end

  test "enforces Consul session TTL bounds and rounds only after validation" do
    assert {:error, :invalid_configuration} =
             Dilock.Consul.validate(Keyword.put(@options, :default_lease_ms, 9_999))

    assert {:error, :invalid_configuration} =
             Dilock.Consul.validate(Keyword.put(@options, :default_lease_ms, 86_400_001))
  end

  test "exposes the epoch and positive LockIndex as the fence" do
    assert {:ok, %Dilock.Consul.Fence{epoch: "epoch-a", lock_index: 7}} =
             Dilock.Consul.fence(%{epoch: "epoch-a", lock_index: 7})

    assert {:error, :invalid_fence} = Dilock.Consul.fence(%{epoch: "epoch-a", lock_index: 0})
  end
end
