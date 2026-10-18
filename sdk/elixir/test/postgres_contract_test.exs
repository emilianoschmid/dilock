defmodule Dilock.PostgresContractTest do
  use ExUnit.Case

  test "profile and signed advisory key vectors are defined" do
    assert Dilock.Postgres.validate(connection_string: "postgresql://localhost/db", epoch: "epoch") == :ok
    assert Dilock.Postgres.advisory_key("0000000000000000" <> String.duplicate("0", 48)) == 0
    assert Dilock.Postgres.advisory_key("ffffffffffffffff" <> String.duplicate("0", 48)) == -1
  end
end
