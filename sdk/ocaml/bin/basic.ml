open Lwt.Infix

let run connection epoch =
  Dilock.provider connection "examples" epoch >>= fun provider ->
  Dilock.lock provider "daily-report" >>= fun handle ->
  Dilock.with_lease handle Dilock.defaults (fun _lease ->
      Lwt_io.printf "Lock acquired; running the protected work.\n")

let () =
  let host = Option.value (Sys.getenv_opt "DILOCK_REDIS_HOST") ~default:"127.0.0.1" in
  let port = Option.value (Sys.getenv_opt "DILOCK_REDIS_PORT") ~default:"6379" |> int_of_string in
  let epoch = Option.value (Sys.getenv_opt "DILOCK_COORDINATION_EPOCH") ~default:"local-example" in
  let spec = Redis_lwt.Client.connection_spec ~port host in
  Lwt_main.run
    (Redis_lwt.Client.connect spec >>= fun connection ->
     Lwt.finalize (fun () -> run connection epoch)
       (fun () -> Redis_lwt.Client.disconnect connection))
