defmodule Dilock.ConformanceWorkerTest do
  use ExUnit.Case, async: true

  Code.require_file("../test_support/conformance_worker.ex", __DIR__)

  test "accepts the protocol's valid request shapes" do
    assert {:ok, %{"id" => "a", "op" => "hello"}} = parse(%{id: "a", op: "hello"})

    assert {:ok, %{"op" => "acquire"}} =
             parse(%{id: "b", op: "acquire", resource: "r", wait_ms: 1})

    assert {:ok, %{"op" => "try_acquire"}} =
             parse(%{id: "b2", op: "try_acquire", resource: "r", wait_ms: 0})

    assert {:error, :required_field_missing} =
             parse(%{id: "b3", op: "acquire", resource: "r"})

    assert {:ok, %{"op" => "cancel_request"}} =
             parse(%{id: "c", op: "cancel_request", target_id: "b"})
  end

  test "rejects malformed JSON, arrays, and invalid UTF-8 without echoing input" do
    assert {:error, :invalid_json} = Dilock.ConformanceWorker.parse_request("{broken")
    assert {:error, :request_not_object} = Dilock.ConformanceWorker.parse_request("[]")
    assert {:error, :invalid_utf8} = Dilock.ConformanceWorker.parse_request(<<255, 10>>)
  end

  test "enforces the byte bound before decoding" do
    assert {:error, :line_too_long} =
             Dilock.ConformanceWorker.parse_request(String.duplicate("x", 65_537))

    assert Dilock.ConformanceWorker.max_line_bytes() == 65_536
  end

  test "rejects unknown fields and operations" do
    assert {:error, :unknown_field} = parse(%{id: "a", op: "hello", password: "do-not-echo"})
    assert {:error, :unknown_operation} = parse(%{id: "a", op: "delete"})
  end

  test "requires operation fields and nonempty string identifiers" do
    assert {:error, :required_field_missing} = parse(%{id: "a", op: "acquire"})
    assert {:error, :invalid_string_field} = parse(%{id: "a", op: "release", lease_id: ""})
    assert {:error, :required_field_missing} = parse(%{id: "a", op: "cancel_request"})
  end

  test "bounds ids and integer duration fields and rejects boolean lookalikes" do
    assert {:error, :invalid_id} = parse(%{id: "", op: "hello"})

    assert {:error, :invalid_duration} =
             parse(%{id: "a", op: "acquire", resource: "r", wait_ms: -1})

    assert {:error, :invalid_duration} =
             parse(%{id: "a", op: "acquire", resource: "r", wait_ms: 0})

    assert {:error, :invalid_duration} =
             parse(%{id: "a", op: "release", lease_id: "l", lease_ms: 0})

    assert {:error, :invalid_duration} = parse(%{id: "a", op: "hello", wait_ms: 86_400_001})

    assert {:error, :invalid_auto_renew} =
             parse(%{id: "a", op: "acquire", resource: "r", wait_ms: 1, auto_renew: 1})
  end

  defp parse(value), do: Dilock.ConformanceWorker.parse_request(Jason.encode!(value))
end
