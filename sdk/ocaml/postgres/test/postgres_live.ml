open Lwt.Infix

let expect condition message =
  if condition then Lwt.return_unit else Lwt.fail (Failure message)

let env name fallback =
  match Sys.getenv_opt name with Some value when value <> "" -> value | _ -> fallback

let run () =
  let dsn = env "DILOCK_POSTGRES_DSN" "" in
  if dsn = "" then begin
    print_endline "SKIP OCaml PostgreSQL live consumer: DILOCK_POSTGRES_DSN is unset";
    Lwt.return_unit
  end else begin
    let epoch = env "DILOCK_TEST_EPOCH" "ocaml-postgres-live-epoch" in
    let namespace = env "DILOCK_TEST_NAMESPACE" "ocaml-postgres-live" in
    let options = { (Postgres.defaults epoch) with connection_string = dsn; namespace } in
    Postgres.provision_namespace options >>= fun () ->
    Postgres.connect options >>= fun owner_provider ->
    Lwt.finalize
      (fun () -> Postgres.connect options >>= fun successor_provider ->
       Lwt.finalize
         (fun () -> Postgres.lock owner_provider "ocaml/postgres-live" >>= fun owner_lock ->
          Lwt.finalize
            (fun () -> Postgres.lock successor_provider "ocaml/postgres-live" >>= fun successor_lock ->
             Lwt.finalize
               (fun () -> Postgres.try_acquire owner_lock >>= function
                | None -> Lwt.fail (Failure "OCaml owner acquisition failed")
                | Some owner ->
                  Lwt.finalize
                    (fun () -> Postgres.try_acquire successor_lock >>= fun contended ->
                     expect (contended = None) "OCaml contender acquired a held PostgreSQL key" >>= fun () ->
                     Postgres.renew owner >>= fun renewed ->
                     expect renewed "OCaml PostgreSQL liveness check failed" >>= fun () ->
                     Postgres.release owner >>= fun outcome ->
                     expect (outcome = Postgres.Released) "OCaml owner release was not acknowledged" >>= fun () ->
                     Postgres.try_acquire successor_lock >>= function
                     | None -> Lwt.fail (Failure "OCaml successor did not acquire after release")
                     | Some successor ->
                       Lwt.finalize
                         (fun () -> Postgres.release successor >>= fun successor_outcome ->
                          expect (successor_outcome = Postgres.Released)
                            "OCaml successor release was not acknowledged")
                         (fun () -> Postgres.lease_destroy successor))
                    (fun () -> Postgres.lease_destroy owner))
               (fun () -> Postgres.lock_destroy successor_lock))
            (fun () -> Postgres.lock_destroy owner_lock))
         (fun () -> Postgres.provider_close successor_provider))
      (fun () -> Postgres.provider_close owner_provider)
    >>= fun () ->
    print_endline "PASS OCaml PostgreSQL: provision, contention, liveness, release, and handoff";
    Lwt.return_unit
  end

let () = Lwt_main.run (run ())
