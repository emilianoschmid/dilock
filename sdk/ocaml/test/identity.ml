open Lwt.Infix

let () =
  let got = Lwt_main.run (Dilock.resource_id "default" "camera/123/reindex") in
  if got <> "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6" then failwith "draft 1 identity vector mismatch";
  (match Lwt_main.run (Dilock.resource_id "Bad" "x") with
   | _ -> failwith "invalid namespace accepted"
   | exception Dilock.Error (Dilock.Invalid_argument _) -> ());
  List.iter (fun resource ->
    match Lwt_main.run (Dilock.resource_id "default" resource) with
    | _ -> failwith "invalid UTF-8 resource accepted"
    | exception Dilock.Error (Dilock.Invalid_argument _) -> ())
    [ ""; "bad\255utf8"; "\192\128"; "\224\128\128"; "\237\160\128";
      "\244\144\128\128"; "\240\159\146"; "nul\000byte" ];
  let unicode_id = Lwt_main.run (Dilock.resource_id "default" "café/☕") in
  if String.length unicode_id <> 64 then failwith "valid Unicode resource was rejected";
  let boundary_id = Lwt_main.run (Dilock.resource_id "default" (String.make 1024 'a')) in
  if String.length boundary_id <> 64 then failwith "1024-byte resource boundary was rejected";
  (match Lwt_main.run (Dilock.resource_id "default" (String.make 1025 'a')) with
   | _ -> failwith "resource above the UTF-8 byte limit was accepted"
   | exception Dilock.Error (Dilock.Invalid_argument _) -> ());
  Dilock.validate_contention_type `String;
  Dilock.validate_contention_type `None;
  (match Dilock.validate_contention_type `Hash with
   | () -> failwith "unexpected Redis key type accepted as contention"
   | exception Dilock.Error (Dilock.Protocol_mismatch _) -> ());
  Dilock.validate_metadata_type `String;
  (match Dilock.validate_metadata_type `None with
   | () -> failwith "missing metadata key accepted"
   | exception Dilock.Error (Dilock.Protocol_mismatch _) -> ());
  (match Dilock.validate_metadata_type `List with
   | () -> failwith "non-string metadata key accepted"
   | exception Dilock.Error (Dilock.Protocol_mismatch _) -> ());
  let backend_failure_classified =
    Lwt.catch
      (fun () -> Dilock.backend_call "metadata read" (fun () -> Lwt.fail (Failure "offline")) >|= fun _ -> false)
      (function
       | Dilock.Error (Dilock.Backend_error message) -> Lwt.return (String.length message > 0)
       | _ -> Lwt.return_false)
  in
  let cancellation_preserved =
    Lwt.catch
      (fun () -> Dilock.backend_call "metadata read" (fun () -> Lwt.fail Lwt.Canceled) >|= fun _ -> false)
      (function Lwt.Canceled -> Lwt.return_true | _ -> Lwt.return_false)
  in
  let backend_failure_classified, cancellation_preserved =
    Lwt_main.run (Lwt.both backend_failure_classified cancellation_preserved)
  in
  if not backend_failure_classified then failwith "Redis backend failure was not classified as Backend_error";
  if not cancellation_preserved then failwith "Redis metadata cancellation was converted to another error";
  let release_state = ref "held" in
  let release_gate = Lwt_mutex.create () in
  let release_calls = ref 0 in
  let lose_release_ack () =
    incr release_calls;
    Lwt.pause () >|= fun () -> Dilock.Unknown
  in
  let first_release =
    Dilock.release_once release_gate (fun () -> !release_state) (fun state -> release_state := state) lose_release_ack
  in
  let second_release =
    Dilock.release_once release_gate (fun () -> !release_state) (fun state -> release_state := state) lose_release_ack
  in
  let first_outcome, second_outcome = Lwt_main.run (Lwt.both first_release second_release) in
  if first_outcome <> Dilock.Unknown || second_outcome <> Dilock.Unknown || !release_calls <> 1 then
    failwith "ambiguous concurrent release was retried or outcome changed";
  let cancellation_state = ref "held" in
  let cancellation_gate = Lwt_mutex.create () in
  let cancellation_calls = ref 0 in
  let started, started_wakener = Lwt.wait () in
  let pending_attempt, _ = Lwt.task () in
  let cancel_during_release () =
    incr cancellation_calls;
    Lwt.wakeup_later started_wakener ();
    pending_attempt
  in
  let cancelled_release =
    Dilock.release_once cancellation_gate (fun () -> !cancellation_state)
      (fun state -> cancellation_state := state) cancel_during_release
  in
  let cancellation_propagated =
    Lwt.catch
      (fun () -> started >>= fun () -> Lwt.cancel cancelled_release; cancelled_release >|= fun _ -> false)
      (function Lwt.Canceled -> Lwt.return true | exn -> Lwt.fail exn)
  in
  let retry_after_cancellation =
    Dilock.release_once cancellation_gate (fun () -> !cancellation_state)
      (fun state -> cancellation_state := state)
      (fun () -> incr cancellation_calls; Lwt.return Dilock.Released)
  in
  let cancellation_propagated, retry_outcome =
    Lwt_main.run (Lwt.both cancellation_propagated retry_after_cancellation)
  in
  if not cancellation_propagated || retry_outcome <> Dilock.Unknown || !cancellation_calls <> 1 then
    failwith "release cancellation was hidden or retried after an ambiguous result";
  let lifecycle_gate = Lwt_mutex.create () in
  let lifecycle_state = ref "held" in
  let renewal_started, renewal_started_wakener = Lwt.wait () in
  let pending_renewal, pending_renewal_wakener = Lwt.task () in
  let renewal_calls = ref 0 in
  let release_calls = ref 0 in
  let renewal =
    Dilock.with_lease_mutation lifecycle_gate (fun () ->
      if !lifecycle_state <> "held" then Lwt.return_false
      else begin
        incr renewal_calls;
        Lwt.wakeup_later renewal_started_wakener ();
        pending_renewal >|= fun () -> true
      end)
  in
  let release =
    Dilock.release_once lifecycle_gate (fun () -> !lifecycle_state)
      (fun state -> lifecycle_state := state)
      (fun () -> incr release_calls; Lwt.return Dilock.Released)
  in
  let renewal_precedes_release =
    renewal_started >>= fun () ->
    Lwt.pause () >>= fun () ->
    if !release_calls <> 0 then Lwt.fail_with "release overlapped an in-flight renewal"
    else begin
      Lwt.wakeup_later pending_renewal_wakener ();
      Lwt.both renewal release >|= fun (renewed, released) ->
      renewed && released = Dilock.Released && !renewal_calls = 1 &&
      !release_calls = 1 && !lifecycle_state = "released"
    end
  in
  if not (Lwt_main.run renewal_precedes_release) then
    failwith "renewal and release did not serialize on one lease gate";
  let no_renewal_after_release =
    Dilock.with_lease_mutation lifecycle_gate (fun () ->
      if !lifecycle_state <> "held" then Lwt.return_false
      else Lwt.return_true)
  in
  if Lwt_main.run no_renewal_after_release then
    failwith "a renewal was allowed after the serialized release completed";
  print_endline "identity checks passed"
