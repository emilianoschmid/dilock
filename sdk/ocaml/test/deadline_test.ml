open Lwt.Infix

let expect condition message = if not condition then failwith message

let () =
  let deadline = Dilock.Deadline.after_milliseconds ~now:10_000_000L 250 in
  expect (deadline = Some 260_000_000L) "millisecond deadline conversion";
  expect
    (not (Dilock.Deadline.expired ~now:259_999_999L ~deadline:260_000_000L))
    "deadline should still be live before its boundary";
  expect
    (Dilock.Deadline.expired ~now:260_000_000L ~deadline:260_000_000L)
    "deadline should expire at its boundary";
  expect
    (Dilock.Deadline.renewal_deadline ~now:259_999_999L
       ~previous_deadline:260_000_000L ~sent:200_000_000L 250
     = Some 450_000_000L)
    "renewal acknowledged before the old deadline should advance conservatively from send time";
  expect
    (Dilock.Deadline.renewal_deadline ~now:260_000_000L
       ~previous_deadline:260_000_000L ~sent:200_000_000L 250
     = None)
    "renewal acknowledged at the old deadline must be rejected for owner-checked cleanup";
  expect (Dilock.Deadline.after_milliseconds ~now:0L 0 = None)
    "zero timeout must be rejected";
  expect
    (Dilock.Deadline.after_milliseconds ~now:Int64.max_int 1 = None)
    "deadline addition must reject overflow";
  let renewal_state = ref "held" in
  let renewal_error = Failure "renewal acknowledgement lost" in
  let renewal_sends = ref 0 in
  let send_renewal () = incr renewal_sends; Lwt.fail renewal_error in
  let renewal_preserved =
    try
      ignore (Lwt_main.run
        (Dilock.dispatch_renewal ~is_held:(!renewal_state = "held")
           ~mark_lost:(fun () -> renewal_state := "lost")
           send_renewal));
      false
    with Failure message -> message = "renewal acknowledgement lost"
  in
  expect renewal_preserved "uncertain renewal must preserve its transport exception";
  expect (!renewal_state = "lost") "uncertain renewal must terminate local lease validity";
  let later_renewal = Lwt_main.run
      (Dilock.dispatch_renewal ~is_held:(!renewal_state = "held")
         ~mark_lost:(fun () -> renewal_state := "lost") send_renewal) in
  expect (later_renewal = None && !renewal_sends = 1)
    "a terminally lost lease must not dispatch a later renewal";
  let on_time = Lwt_main.run
      (Dilock.accept_acquired ~now:9L ~deadline:10L
         ~release:(fun _ -> failwith "on-time lease was unexpectedly cleaned up") "lease") in
  expect (on_time = "lease") "on-time acquisition should be returned";
  let late_acknowledged =
    try
      ignore (Lwt_main.run
        (Dilock.accept_acquired ~now:10L ~deadline:10L
           ~release:(fun _ -> Lwt.return Dilock.Released) "lease"));
      false
    with Dilock.Error Dilock.Wait_timeout -> true
  in
  expect late_acknowledged "late acquisition with confirmed cleanup should time out";
  let late_unknown =
    try
      ignore (Lwt_main.run
        (Dilock.accept_acquired ~now:11L ~deadline:10L
           ~release:(fun _ -> Lwt.return Dilock.Unknown) "lease"));
      false
    with Dilock.Error (Dilock.Acquisition_unknown _) -> true
  in
  expect late_unknown "late acquisition with unknown cleanup must not become ordinary timeout";
  let observed = ref None in
  let scoped_value = Lwt_main.run
      (Dilock.with_release_observer
         ~release:(fun _ -> Lwt.return Dilock.Unknown)
         ~on_release:(fun outcome -> observed := Some outcome)
         (fun () -> Lwt.return 42) "lease") in
  expect (scoped_value = 42 && !observed = Some Dilock.Unknown)
    "scope must report an unknown cleanup outcome without changing the body value";
  let observer_failure_suppressed =
    Lwt_main.run
      (Dilock.with_release_observer
         ~release:(fun _ -> Lwt.return Dilock.Unknown)
         ~on_release:(fun _ -> failwith "observer failed")
         (fun () -> Lwt.return 43) "lease") = 43
  in
  expect observer_failure_suppressed "an observer exception must not replace successful work";
  let body_error_preserved =
    try
      ignore (Lwt_main.run
        (Dilock.with_release_observer
           ~release:(fun _ -> Lwt.fail (Failure "cleanup transport failed"))
           ~on_release:(fun outcome -> observed := Some outcome)
           (fun () -> Lwt.fail (Failure "body failed")) "lease"));
      false
    with Failure message when message = "body failed" -> !observed = Some Dilock.Unknown | _ -> false
  in
  expect body_error_preserved
    "cleanup failure must be observed as unknown without masking a body exception";
  let cleanup_started, cleanup_started_wakener = Lwt.wait () in
  let cleanup_pending, cleanup_wakener = Lwt.task () in
  let cleanup_reported, cleanup_reported_wakener = Lwt.wait () in
  let cleanup_observation = ref None in
  let scoped =
    Dilock.with_release_observer
      ~release:(fun _ ->
        Lwt.wakeup_later cleanup_started_wakener ();
        cleanup_pending)
      ~on_release:(fun outcome ->
        cleanup_observation := Some outcome;
        Lwt.wakeup_later cleanup_reported_wakener ())
      (fun () -> Lwt.return_unit) "lease"
  in
  let _scope_result_handler =
    Lwt.catch (fun () -> scoped >|= fun _ -> ()) (fun _ -> Lwt.return_unit)
  in
  let scope_cleanup_survived_cancellation =
    Lwt_main.run
      (cleanup_started >>= fun () ->
       Lwt.cancel scoped;
       Lwt.pause () >>= fun () ->
       let survived = Lwt.is_sleeping cleanup_pending in
       if survived then Lwt.wakeup_later cleanup_wakener Dilock.Unknown;
       cleanup_reported >|= fun () ->
       survived && !cleanup_observation = Some Dilock.Unknown)
  in
  expect scope_cleanup_survived_cancellation
    "cancelling scoped work must not cancel in-flight cleanup or lose its release observation";
  let first = Mtime_clock.elapsed_ns () in
  let second = Mtime_clock.elapsed_ns () in
  expect (Int64.compare second first >= 0) "monotonic clock moved backwards";
  print_endline "monotonic deadline checks passed"
