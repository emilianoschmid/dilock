open Lwt.Infix

let run () =
  let host = Option.value (Sys.getenv_opt "DILOCK_REDIS_HOST") ~default:"127.0.0.1" in
  let port = Option.value (Sys.getenv_opt "DILOCK_REDIS_PORT") ~default:"6379" |> int_of_string in
  let namespace = Option.value (Sys.getenv_opt "DILOCK_NAMESPACE") ~default:"default" in
  let epoch = Option.value (Sys.getenv_opt "DILOCK_COORDINATION_EPOCH") ~default:"local-redis-epoch" in
  let spec = Redis_lwt.Client.connection_spec ~port host in
  Redis_lwt.Client.connect spec >>= fun connection ->
  Lwt.finalize
    (fun () ->
      Dilock.provider connection namespace epoch >>= fun provider ->
      (* Different handles share the provider and its caller-owned connection. *)
      Dilock.lock provider "reports/daily" >>= fun handle ->
      Dilock.with_lease handle Dilock.defaults (fun _lease ->
        print_endline "Default: run one short, idempotent report operation here";
        Lwt.return_unit) >>= fun () ->
      Dilock.lock provider "reports/weekly" >>= fun configured ->
      Dilock.with_lease configured { Dilock.wait_ms = 3_000; lease_ms = 20_000 } (fun _lease ->
        print_endline "Configured: run one short, idempotent report operation here";
        Lwt.return_unit))
    (fun () -> Redis_lwt.Client.disconnect connection)

let () = Lwt_main.run (run ())
