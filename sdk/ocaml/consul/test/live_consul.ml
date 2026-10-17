open Lwt.Infix

let run () =
  let get name fallback = match Sys.getenv_opt name with Some value -> value | None -> fallback in
  let epoch = get "DILOCK_TEST_CONSUL_EPOCH" "ocaml-live-test" in
  let options = {
    Consul.(defaults epoch) with
    endpoint = get "DILOCK_TEST_CONSUL_ENDPOINT" "http://127.0.0.1:8500";
    namespace = get "DILOCK_TEST_CONSUL_NAMESPACE" "dilock-ocaml-live";
    acl_token = Sys.getenv_opt "DILOCK_TEST_CONSUL_TOKEN";
  } in
  Consul.provision_namespace options >>= fun () ->
  Consul.connect options >>= fun provider ->
  Consul.lock provider "live-lifecycle" >>= fun resource ->
  Consul.acquire_default resource >>= fun lease ->
  let fence = Consul.fence lease in
  if fence.lock_index = "0" then Lwt.fail_with "Consul returned a zero LockIndex" else
  Consul.renew lease >>= fun () ->
  Consul.release lease >>= fun outcome ->
  Consul.close provider >>= fun () ->
  (match outcome with Consul.Released -> Lwt.return_unit | _ -> Lwt.fail_with "Consul lease was not released")

let () = match Sys.getenv_opt "DILOCK_TEST_CONSUL_ENDPOINT" with
  | None -> print_endline "SKIP: DILOCK_TEST_CONSUL_ENDPOINT is not set"
  | Some _ -> Lwt_main.run (run ())
