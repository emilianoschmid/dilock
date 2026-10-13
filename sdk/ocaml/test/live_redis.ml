open Lwt.Infix

let expect condition message = if not condition then failwith message

let env name fallback = match Sys.getenv_opt name with Some value -> value | None -> fallback

let run () =
  let host = env "DILOCK_REDIS_HOST" "redis" in
  let port = int_of_string (env "DILOCK_REDIS_PORT" "6379") in
  let namespace = env "DILOCK_NAMESPACE" "ocaml-live" in
  let epoch = env "DILOCK_COORDINATION_EPOCH" "ocaml-live-epoch" in
  let resource = "redis-live-consumer" in
  let spec = Redis_lwt.Client.connection_spec ~port host in
  Redis_lwt.Client.connect spec >>= fun first_connection ->
  Redis_lwt.Client.connect spec >>= fun second_connection ->
  let finish () =
    Redis_lwt.Client.disconnect first_connection >>= fun () ->
    Redis_lwt.Client.disconnect second_connection
  in
  Lwt.finalize
    (fun () ->
      let metadata =
        Printf.sprintf
          "{\"protocol_major\":1,\"identity_scheme\":\"sha256-framed-utf8-v1\",\"profile\":\"redis-single-authority-v1\",\"coordination_epoch\":\"%s\",\"fixed_lease_ms\":null}"
          epoch
      in
      Redis_lwt.Client.set first_connection ("dilock:namespace:" ^ namespace ^ ":meta") metadata
      >>= fun metadata_set ->
      expect metadata_set "namespace metadata was not provisioned";
      let metadata_key = "dilock:namespace:" ^ namespace ^ ":meta" in
      Redis_lwt.Client.send_custom_request first_connection ["DEL"; metadata_key] >>= fun _ ->
      Redis_lwt.Client.send_custom_request first_connection ["RPUSH"; metadata_key; "metadata-marker"] >>= fun _ ->
      Lwt.catch
        (fun () ->
          Dilock.provider first_connection namespace epoch >>= fun _ ->
          Lwt.fail_with "non-string namespace metadata was accepted")
        (function
          | Dilock.Error (Dilock.Protocol_mismatch _) ->
              Redis_lwt.Client.type_of first_connection metadata_key >>= fun metadata_type ->
              expect (metadata_type = `List)
                "namespace type validation changed or removed the unexpected metadata key";
              Redis_lwt.Client.send_custom_request first_connection ["DEL"; metadata_key] >>= fun _ ->
              Redis_lwt.Client.set first_connection metadata_key metadata >>= fun restored ->
              expect restored "namespace metadata was not restored after wrong-type check";
              Lwt.return_unit
          | ex -> Lwt.fail ex) >>= fun () ->
      Dilock.provider first_connection namespace epoch >>= fun first_provider ->
      Dilock.provider second_connection namespace epoch >>= fun second_provider ->
      Dilock.lock first_provider resource >>= fun first_handle ->
      Dilock.lock second_provider resource >>= fun second_handle ->
      Dilock.try_acquire first_handle 300 >>= function
      | None -> Lwt.fail_with "first provider failed to acquire an empty lock"
      | Some expired_owner ->
          Dilock.try_acquire second_handle 300 >>= fun contended ->
          expect (contended = None) "second provider acquired while the first lease was live";
          Lwt_unix.sleep 0.36 >>= fun () ->
          Dilock.try_acquire second_handle 1000 >>= function
          | None -> Lwt.fail_with "successor could not acquire after lease expiry"
          | Some successor ->
              Dilock.release expired_owner >>= fun stale_release ->
              expect (stale_release = Dilock.Already_not_owned)
                "expired owner's release did not report loss of ownership";
              Dilock.try_acquire first_handle 1000 >>= fun still_busy ->
              expect (still_busy = None) "stale release removed the successor's lock";
              Dilock.release successor >>= fun release_result ->
              expect (release_result = Dilock.Released) "successor release failed";
              Dilock.release successor >>= fun repeated_release ->
              expect (repeated_release = Dilock.Already_not_owned)
                "repeated release changed its outcome";
              Dilock.try_acquire first_handle 1000 >>= function
              | None -> Lwt.fail_with "lock was not reacquirable after release"
              | Some final_owner ->
                  Dilock.release final_owner >>= fun _ ->
                  let wrong_type_handle = first_handle in
                  Redis_lwt.Client.send_custom_request first_connection
                    ["HSET"; wrong_type_handle.key; "field"; "value"]
                  >>= fun _ ->
                  Lwt.catch
                    (fun () ->
                      Dilock.try_acquire wrong_type_handle 1000 >>= fun _ ->
                      Lwt.fail_with "incompatible key type was reported as ordinary contention")
                    (function
                      | Dilock.Error (Dilock.Protocol_mismatch _) ->
                          Redis_lwt.Client.send_custom_request first_connection ["DEL"; wrong_type_handle.key]
                          >|= fun _ ->
                          print_endline
                            "live Redis checks passed: contention, expiry handoff, stale release, repeat release, reacquire, incompatible key type"
                      | ex -> Lwt.fail ex))
    finish

let () =
  try Lwt_main.run (run ())
  with ex ->
    prerr_endline (Printexc.to_string ex);
    exit 1
