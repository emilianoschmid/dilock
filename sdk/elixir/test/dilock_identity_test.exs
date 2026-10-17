defmodule Dilock.IdentityTest do
  use ExUnit.Case, async: true

  test "matches the shared UTF-8 framed identity vector" do
    assert {:ok, "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6"} =
             Dilock.Identity.resource_id("default", "camera/123/reindex")
  end

  test "does not normalize Unicode or trim resource bytes" do
    assert {:ok, composed} = Dilock.Identity.resource_id("default", "café")
    assert {:ok, decomposed} = Dilock.Identity.resource_id("default", "cafe\u0301")
    assert composed != decomposed
    assert {:ok, spaced} = Dilock.Identity.resource_id("default", "camera ")
    assert {:ok, plain} = Dilock.Identity.resource_id("default", "camera")
    assert spaced != plain
  end

  test "rejects invalid namespace, NUL, empty and overlong resources" do
    assert {:error, :invalid_namespace} = Dilock.Identity.resource_id("Prod", "camera")
    assert {:error, :invalid_resource} = Dilock.Identity.resource_id("default", "")
    assert {:error, :invalid_resource} = Dilock.Identity.resource_id("default", "a\0b")

    assert {:error, :invalid_resource} =
             Dilock.Identity.resource_id("default", String.duplicate("x", 1_025))
  end

  test "strictly validates four-byte UTF-8 scalar boundaries" do
    # U+10FFFF is the maximum Unicode scalar and must remain a valid resource.
    assert {:ok, id} = Dilock.Identity.resource_id("default", <<0xF4, 0x8F, 0xBF, 0xBF>>)
    assert byte_size(id) == 64

    for malformed <- [
          # Above U+10FFFF
          <<0xF4, 0x90, 0x80, 0x80>>,
          # Overlong encoding
          <<0xF0, 0x8F, 0xBF, 0xBF>>,
          # Truncated four-byte sequence
          <<0xF1, 0x80, 0x80>>
        ] do
      assert {:error, :invalid_resource} = Dilock.Identity.resource_id("default", malformed)
    end
  end

  test "validates metadata fields semantically and rejects profile/epoch drift" do
    raw = Dilock.Identity.metadata("default", "redis-single-authority-v1", "cluster-a")
    metadata = Jason.decode!(raw)
    assert :ok = Dilock.Identity.validate_metadata(raw, "redis-single-authority-v1", "cluster-a")

    assert {:error, :protocol_mismatch} =
             Dilock.Identity.validate_metadata(
               Jason.encode!(Map.put(metadata, "future_field", "unsupported")),
               "redis-single-authority-v1",
               "cluster-a"
             )

    assert {:error, :protocol_mismatch} =
             Dilock.Identity.validate_metadata(raw, "etcd-native-lock-v1", "cluster-a")

    assert {:error, :protocol_mismatch} =
             Dilock.Identity.validate_metadata(raw, "redis-single-authority-v1", "cluster-b")

    assert {:error, :protocol_mismatch} =
             Dilock.Identity.validate_metadata(
               "not-json",
               "redis-single-authority-v1",
               "cluster-a"
             )

    # Elixir's loose numeric equality considers 1.0 equal to 1. Protocol
    # metadata requires the exact integer scalar, as do the other SDKs.
    for invalid_protocol_major <- [1.0, "1", true] do
      wrong_type = Jason.encode!(Map.put(metadata, "protocol_major", invalid_protocol_major))

      assert {:error, :protocol_mismatch} =
               Dilock.Identity.validate_metadata(
                 wrong_type,
                 "redis-single-authority-v1",
                 "cluster-a"
               )
    end
  end

  test "etcd fence keeps the coordinator epoch paired with the key revision" do
    fence = %Dilock.Etcd.Fence{epoch: "cluster-a", revision: 17}
    assert Dilock.Etcd.fence(%{fence: fence}) == {:ok, fence}
    assert {:error, :unsupported_capability} = Dilock.Etcd.fence(%{})
    assert {:error, :unsupported_capability} = Dilock.Etcd.try_acquire(self(), "id", %{})
  end

  test "etcd TLS endpoints fail explicitly until verified TLS credentials are implemented" do
    assert {:error, :unsupported_tls} =
             Dilock.Etcd.validate(
               endpoint: "https://etcd.example:2379",
               namespace: "default",
               epoch: "cluster-a"
             )
  end
end
