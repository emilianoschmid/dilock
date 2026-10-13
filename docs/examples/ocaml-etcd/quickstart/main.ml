open Lwt.Infix

let run () =
  let endpoint = Option.value (Sys.getenv_opt "DILOCK_ETCD_ENDPOINT") ~default:"127.0.0.1:2379" in
  let namespace = Option.value (Sys.getenv_opt "DILOCK_NAMESPACE") ~default:"default" in
  let epoch = Option.value (Sys.getenv_opt "DILOCK_COORDINATION_EPOCH") ~default:"local-etcd-epoch" in
  let options = { Etcd.defaults with endpoint; namespace; coordination_epoch = epoch } in
  Etcd.provider options >>= fun provider ->
  (* Resource handles share the provider and its configured transport. *)
  Etcd.lock provider "reports/daily" >>= fun resource ->
  Etcd.with_lease resource (fun lease ->
    let fence = Etcd.fence lease in
    Printf.printf "Default fence %s/%Ld at the protected resource\n" fence.coordination_epoch fence.create_revision;
    Lwt.return_unit) >>= fun () ->
  Etcd.lock provider "reports/weekly" >>= fun configured ->
  let policy : Etcd.acquire_options = { wait_ms = 3_000; lease_ms = 20_000 } in
  Etcd.with_lease ~options:policy configured (fun lease ->
    let fence = Etcd.fence lease in
    Printf.printf "Configured fence %s/%Ld at the protected resource\n" fence.coordination_epoch fence.create_revision;
    Lwt.return_unit)

let () = Lwt_main.run (run ())
