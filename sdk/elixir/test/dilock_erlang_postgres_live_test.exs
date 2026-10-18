if System.get_env("DILOCK_ERLANG_BEAM") do
  defmodule Dilock.ErlangPostgresLiveTest do
    use ExUnit.Case, async: false

    @moduletag :integration

    setup_all do
      beam_path = System.fetch_env!("DILOCK_ERLANG_BEAM")
      true = :code.add_pathz(String.to_charlist(beam_path))
      :ok
    end

    test "Erlang facade contends, renews and hands off PostgreSQL" do
      dsn = System.fetch_env!("DILOCK_POSTGRES_DSN")
      namespace = "erlang-postgres-live"
      epoch = "erlang-postgres-live-epoch"
      options = [
        {:connection_string, dsn},
        {:namespace, namespace},
        {:epoch, epoch}
      ]

      assert :ok = erlang(:provision_postgres_namespace, [options])
      {:ok, first_provider} = erlang(:start_postgres, [options])
      {:ok, second_provider} = erlang(:start_postgres, [options])

      on_exit(fn ->
        if Process.alive?(first_provider), do: GenServer.stop(first_provider)
        if Process.alive?(second_provider), do: GenServer.stop(second_provider)
      end)

      {:ok, first_lock} = erlang(:lock, [first_provider, namespace, "shared"])
      {:ok, second_lock} = erlang(:lock, [second_provider, namespace, "shared"])
      {:ok, other_lock} = erlang(:lock, [second_provider, namespace, "other"])
      acquire_options = %{wait_ms: 500, lease_ms: 30_000}

      assert {:ok, first_lease} = erlang(:acquire, [first_lock, acquire_options])
      assert :busy = erlang(:try_acquire, [second_lock, %{lease_ms: 30_000}])
      assert {:ok, other_lease} = erlang(:acquire, [other_lock, acquire_options])
      assert :ok = erlang(:renew, [first_lease, 30_000])
      assert :released = erlang(:release, [other_lease])
      assert :released = erlang(:release, [first_lease])
      assert {:ok, successor} = erlang(:acquire, [second_lock, acquire_options])
      assert :released = erlang(:release, [successor])
    end

    defp erlang(function, arguments), do: apply(:dilock_erlang, function, arguments)
  end
end
