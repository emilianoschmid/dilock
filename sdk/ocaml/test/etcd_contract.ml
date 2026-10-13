let expect condition message = if not condition then failwith message

let options = {
  Etcd.defaults with
  coordination_epoch = "contract-epoch";
}

let provider = {
  Etcd.options = options;
  host = "127.0.0.1";
  port = "2379";
  authority = "127.0.0.1:2379";
}

let resource = { Etcd.provider = provider; name = "/dilock/locks/test" }

let () =
  let key = Bytes.of_string "/dilock/locks/exact-key" in
  let owner = Kv.Mvccpb.KeyValue.make ~key ~lease:77L ~create_revision:12L () in
  let successor = Kv.Mvccpb.KeyValue.make ~key ~lease:88L ~create_revision:13L () in
  let unexpected = Kv.Mvccpb.KeyValue.make ~key:(Bytes.of_string "/unexpected") ~lease:77L () in
  let ownership kvs = Etcd.classify_release_range ~key ~lease_id:77L kvs in
  expect (ownership [owner] = Etcd.Exact_owner) "exact key and lease did not confirm release ownership";
  expect (ownership [] = Etcd.Exact_absent) "empty exact-key Range did not confirm absence";
  expect (ownership [successor] = Etcd.Exact_mismatch) "different lease was not classified as ownership mismatch";
  expect (ownership [unexpected] = Etcd.Ownership_unknown) "unexpected key was accepted as ownership evidence";
  expect (ownership [owner; successor] = Etcd.Ownership_unknown) "multiple Range rows were accepted as exact ownership";
  expect (Etcd.release_ownership_confirmed Etcd.Exact_owner) "confirmed owner would not be unlocked";
  List.iter (fun state ->
    expect (not (Etcd.release_ownership_confirmed state)) "unconfirmed key would be unlocked")
    [Etcd.Exact_absent; Etcd.Exact_mismatch; Etcd.Ownership_unknown];
  let classify ownership unlock_acknowledged revoke_outcome =
    Etcd.classify_release_outcome ~ownership ~unlock_acknowledged ~revoke_outcome
  in
  expect (classify Etcd.Exact_owner true Etcd.Revoke_acknowledged = Dilock.Released)
    "exact owner with both positive acknowledgements was not Released";
  expect (classify Etcd.Exact_owner true Etcd.Revoke_unknown = Dilock.Unknown)
    "Unlock-only acknowledgement was incorrectly reported as Released";
  expect (classify Etcd.Exact_owner false Etcd.Revoke_acknowledged = Dilock.Already_not_owned)
    "Revoke-only acknowledgement was not Already_not_owned";
  expect (classify Etcd.Exact_owner false Etcd.Revoke_unknown = Dilock.Unknown)
    "unacknowledged owner cleanup did not fail closed";
  expect (classify Etcd.Exact_absent false Etcd.Revoke_unknown = Dilock.Already_not_owned)
    "confirmed exact-key absence was not Already_not_owned";
  expect (classify Etcd.Exact_mismatch false Etcd.Revoke_unknown = Dilock.Already_not_owned)
    "confirmed lease mismatch was not Already_not_owned";
  expect (classify Etcd.Ownership_unknown false Etcd.Revoke_acknowledged = Dilock.Already_not_owned)
    "positive unique-lease Revoke did not confirm Already_not_owned";
  expect (classify Etcd.Ownership_unknown false Etcd.Revoke_unknown = Dilock.Unknown)
    "uncertain observation and cleanup did not stay Unknown";
  expect (classify Etcd.Exact_owner true Etcd.Revoke_not_found = Dilock.Already_not_owned)
    "expiry between Range and Unlock/Revoke was incorrectly reported Released";
  Etcd.ensure_acquisition_deadline ~now:9_999_999L ~deadline:10_000_000L;
  (match Etcd.ensure_acquisition_deadline ~now:10_000_000L ~deadline:10_000_000L with
   | () -> failwith "acquisition remained valid at the total wait deadline"
   | exception Etcd.Error Etcd.Wait_timeout -> ());
  expect (Etcd.classify_lock_rpc_error
      (Etcd.Error (Etcd.Backend_error "v3lockpb.Lock/Lock local timeout")) = Etcd.Error Etcd.Wait_timeout)
    "Lock RPC reaching the acquisition deadline was reported as a backend failure";
  let server_error = Etcd.Error (Etcd.Backend_error "HTTP/2 status unavailable") in
  expect (Etcd.classify_lock_rpc_error server_error = server_error)
    "etcd server failure at the acquisition deadline was hidden as wait timeout";
  let host, port = Etcd.validate_options options in
  expect (host = "127.0.0.1" && port = "2379") "default endpoint parsing changed";
  expect (options.default_acquisition.wait_ms = 5_000) "default wait changed";
  expect (options.default_acquisition.lease_ms = 30_000) "default lease changed";
  expect (Etcd.remaining_budget_ms ~now:1_000_000_000L (Some 2_000_000_000L) = 1_000)
    "cleanup budget did not retain its full initial RPC allowance";
  expect (Etcd.remaining_budget_ms ~now:1_750_000_000L (Some 2_000_000_000L) = 250)
    "second cleanup RPC did not receive only the remaining shared budget";
  expect (Etcd.remaining_budget_ms ~now:2_000_000_000L (Some 2_000_000_000L) = 0)
    "cleanup budget remained available at its deadline";
  expect (Etcd.remaining_budget_ms ~now:2_000_000_001L (Some 2_000_000_000L) = 0)
    "cleanup budget remained available after its deadline";
  expect (Etcd.remaining_budget_ms ~now:1_000_000_000L None = 0)
    "overflowed cleanup deadline did not fail closed";
  expect (Etcd.bounded_timeout_ms ~now:1_000_000_000L ~deadline:3_000_000_000L ~maximum:2_000 = 2_000)
    "acquisition RPC timeout exceeded its configured maximum";
  expect (Etcd.bounded_timeout_ms ~now:2_750_000_000L ~deadline:3_000_000_000L ~maximum:2_000 = 250)
    "acquisition RPC did not receive only the remaining wait budget";
  expect (Etcd.bounded_timeout_ms ~now:3_000_000_000L ~deadline:3_000_000_000L ~maximum:2_000 = 0)
    "acquisition RPC remained available at its wait deadline";
  expect (Etcd.acquisition_lock_timeout_ms ~now:1_000_000_000L 10_000_000_000L = 9_000)
    "native Lock wait was incorrectly capped by the shorter unary setup RPC timeout";
  (match Etcd.acquisition_lock_timeout_ms ~now:10_000_000_000L 10_000_000_000L with
   | _ -> failwith "native Lock wait accepted an exhausted acquisition deadline"
   | exception Etcd.Error Etcd.Wait_timeout -> ());
  expect (Etcd.remaining_timeout_ms ~started:1_000_000_000L ~timeout_ms:2_000 1_250_000_000L = 1_750)
    "RPC timeout did not include elapsed connection setup time";
  expect (Etcd.remaining_timeout_ms ~started:1_000_000_000L ~timeout_ms:2_000 3_000_000_000L = 0)
    "RPC timeout remained available at its absolute deadline";
  expect (Etcd.remaining_timeout_ms ~started:Int64.max_int ~timeout_ms:2_000 Int64.max_int = 0)
    "RPC timeout overflow did not fail closed";
  (match Lwt_main.run (Etcd.try_acquire resource) with
   | _ -> failwith "etcd try_acquire unexpectedly claimed native Lock support"
   | exception Etcd.Error (Etcd.Unsupported_capability _) -> ());
  (match Lwt_main.run (Etcd.lock provider "") with
   | _ -> failwith "empty resource was accepted"
   | exception Etcd.Error (Etcd.Invalid_argument _) -> ());
  (match Lwt_main.run (Etcd.lock provider "bad\255utf8") with
   | _ -> failwith "invalid UTF-8 resource was accepted"
   | exception Etcd.Error (Etcd.Invalid_argument _) -> ());
  (match Etcd.validate_options { options with namespace = "Bad" } with
   | _ -> failwith "invalid namespace was accepted"
   | exception Etcd.Error (Etcd.Invalid_argument _) -> ());
  List.iter (fun endpoint ->
    match Etcd.validate_options { options with endpoint } with
    | _ -> failwith ("invalid endpoint port was accepted: " ^ endpoint)
    | exception Etcd.Error (Etcd.Invalid_argument _) -> ())
    [ "127.0.0.1:0"; "127.0.0.1:65536"; "127.0.0.1:port"; "127.0.0.1:+2379"; "[::1]:999999999999999999999999" ];
  (match Etcd.validate_options { options with coordination_epoch = "" } with
   | _ -> failwith "empty epoch was accepted"
   | exception Etcd.Error (Etcd.Invalid_argument _) -> ());
  print_endline "etcd contract checks passed"
