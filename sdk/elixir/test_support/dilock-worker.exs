Code.require_file("conformance_worker.ex", __DIR__)

case System.argv() do
  [config] ->
    case Dilock.ConformanceWorker.run(config) do
      :ok ->
        :ok

      {:error, reason} ->
        IO.puts(:stderr, "worker: startup failed (#{inspect(reason)})")
        System.halt(2)
    end

  _ ->
    IO.puts(:stderr, "usage: dilock-worker <config.json>")
    System.halt(2)
end
