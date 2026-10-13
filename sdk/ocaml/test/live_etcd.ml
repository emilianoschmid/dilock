open Lwt.Infix

let expect condition message = if not condition then failwith message

let env name fallback = match Sys.getenv_opt name with Some value -> value | None -> fallback

let string_of_error = function
  | Etcd.Invalid_argument message -> "Invalid_argument: " ^ message
  | Etcd.Backend_error message -> "Backend_error: " ^ message
  | Etcd.Protocol_mismatch message -> "Protocol_mismatch: " ^ message
  | Etcd.Acquisition_unknown message -> "Acquisition_unknown: " ^ message
  | Etcd.Wait_timeout -> "Wait_timeout"
  | Etcd.Unsupported_capability message -> "Unsupported_capability: " ^ message

let string_of_release = function
  | Dilock.Released -> "Released"
  | Dilock.Already_not_owned -> "Already_not_owned"
  | Dilock.Unknown -> "Unknown"

let rec await_exact_key_absence lease attempts_left =
  Etcd.release_range lease ~timeout_ms:250 >>= function
  | Etcd.Exact_absent -> Lwt.return Etcd.Exact_absent
  | ownership when attempts_left <= 0 -> Lwt.return ownership
  | _ -> Lwt_unix.sleep 0.1 >>= fun () -> await_exact_key_absence lease (attempts_left - 1)

let run () =
  let endpoint = env "DILOCK_ETCD_ENDPOINT" "127.0.0.1:2379" in
  let namespace = env "DILOCK_NAMESPACE" "ocaml-live" in
  let epoch = env "DILOCK_COORDINATION_EPOCH" "ocaml-live-epoch" in
  let options : Etcd.options = {
    endpoint;
    namespace;
    coordination_epoch = epoch;
    rpc_timeout_ms = 3_000;
    cleanup_timeout_ms = 3_000;
    safety_margin_ms = 100;
    default_acquisition = { Etcd.wait_ms = 10_000; lease_ms = 3_500 };
  } in
  Etcd.provider options >>= fun provider ->
  let resource_name = Printf.sprintf "ocaml-live-%d-%06x" (Unix.getpid ()) (Random.bits () land 0xffffff) in
  Etcd.lock provider resource_name >>= fun resource ->
  Etcd.acquire resource >>= fun owner ->
  expect (Etcd.valid owner) "new etcd lease was not valid";
  let owner_fence = Etcd.fence owner in
  expect (owner_fence.coordination_epoch = epoch) "fence epoch did not match configured epoch";
  expect (owner_fence.create_revision > 0L) "owner has no etcd create_revision";
  let initial_deadline = !(owner.Etcd.keeper.deadline_ns) in
  let waiter_options : Etcd.acquire_options = { wait_ms = 20_000; lease_ms = 5_000 } in
  let waiter = Etcd.acquire ~options:waiter_options resource in
  Lwt_unix.sleep 0.5 >>= fun () ->
  expect (Lwt.is_sleeping waiter) "second Lock did not remain queued behind the owner";
  let timeout_options : Etcd.acquire_options = { wait_ms = 300; lease_ms = 5_000 } in
  Lwt.catch
    (fun () ->
      Etcd.acquire ~options:timeout_options resource >>= fun _ ->
      Lwt.fail_with "acquisition past its local wait deadline returned a lease")
    (function
      | Etcd.Error Etcd.Wait_timeout -> Lwt.return_unit
      | Etcd.Error (Etcd.Acquisition_unknown _ as error) -> Lwt.fail (Etcd.Error error)
      | ex -> Lwt.fail ex) >>= fun () ->
  Etcd.lock provider (resource_name ^ "-parallel") >>= fun parallel_resource ->
  Etcd.acquire ~options:({ wait_ms = 2_000; lease_ms = 5_000 } : Etcd.acquire_options) parallel_resource >>= fun parallel_lease ->
  expect (parallel_lease.Etcd.lease_id <> owner.Etcd.lease_id)
    "independent resources were assigned the same etcd lease id";
  Etcd.release parallel_lease >>= fun parallel_release ->
  expect (parallel_release = Dilock.Released) "parallel lease cleanup was not confirmed";
  Lwt_unix.sleep 4.5 >>= fun () ->
  let keepalive_monitor = match Lwt.state owner.Etcd.keeper.failure with Lwt.Sleep -> "active" | _ -> "failed" in
  let acknowledgements = !(owner.Etcd.keeper.acknowledgements) in
  let deadline_advanced = Int64.compare !(owner.Etcd.keeper.deadline_ns) initial_deadline > 0 in
  Printf.printf "KeepAlive evidence: lease_id=%Ld effective_ttl_ms=%d requests=%d acknowledgements=%d deadline_advanced=%b monitor=%s\n%!"
    owner.Etcd.lease_id owner.Etcd.effective_ttl_ms !(owner.Etcd.keeper.requests_sent)
    acknowledgements deadline_advanced keepalive_monitor;
  expect (acknowledgements >= 2) "KeepAlive did not receive repeated acknowledgements beyond the original TTL";
  expect deadline_advanced "KeepAlive acknowledgements did not advance the conservative deadline";
  expect (Etcd.valid owner) "owner lost validity after one effective TTL while KeepAlive should renew it";
  expect (Lwt.is_sleeping waiter) "waiter left the queue while the renewed owner remained held";
  Lwt.cancel waiter;
  Lwt.catch
    (fun () -> waiter >>= fun _ -> Lwt.fail_with "cancelled queued acquisition returned a lease")
    (function Lwt.Canceled -> Lwt.return_unit | ex -> Lwt.fail ex) >>= fun () ->
  expect (Etcd.valid owner) "canceling the queued acquisition invalidated the owner";
  Etcd.release owner >>= fun owner_release ->
  expect (owner_release = Dilock.Released) "owner lease cleanup was not confirmed";
  let expiry_options = { options with
    rpc_timeout_ms = 100;
    cleanup_timeout_ms = 1_000;
    default_acquisition = { Etcd.wait_ms = 2_000; lease_ms = 500 };
  } in
  Etcd.provider expiry_options >>= fun expiry_provider ->
  Etcd.lock expiry_provider (resource_name ^ "-natural-expiry") >>= fun expiry_resource ->
  Etcd.acquire expiry_resource >>= fun expiring_owner ->
  Etcd.stop_keeper expiring_owner.Etcd.keeper >>= fun () ->
  await_exact_key_absence expiring_owner 100 >>= fun expiry_ownership ->
  Etcd.release expiring_owner >>= fun expiry_release ->
  expect (expiry_release = Dilock.Already_not_owned)
    (Printf.sprintf "natural etcd lease expiry was not reported as Already_not_owned (ownership=%s release=%s lease_id=%Ld)"
      (match expiry_ownership with Etcd.Exact_owner -> "Exact_owner" | Etcd.Exact_absent -> "Exact_absent" | Etcd.Exact_mismatch -> "Exact_mismatch" | Etcd.Ownership_unknown -> "Ownership_unknown")
      (string_of_release expiry_release) expiring_owner.Etcd.lease_id);
  Etcd.acquire resource >>= fun successor ->
  let successor_fence = Etcd.fence successor in
  expect (successor_fence.coordination_epoch = epoch) "successor epoch did not match configured epoch";
  expect (successor_fence.create_revision > owner_fence.create_revision)
    "successor did not receive a strictly higher etcd create_revision";
  Etcd.release successor >>= fun successor_release ->
  expect (successor_release = Dilock.Released) "successor lease cleanup was not confirmed";
  print_endline
    "live etcd checks passed: native queue, waiter cancellation, Wait_timeout cleanup, independent lease revoke, exact key, epoch, increasing create_revision";
  Lwt.return_unit

let () =
  Random.self_init ();
  try Lwt_main.run (run ())
  with ex ->
    prerr_endline (match ex with Etcd.Error error -> string_of_error error | _ -> Printexc.to_string ex);
    exit 1
