open Lwt.Infix

type error =
  | Invalid_argument of string
  | Backend_error of string
  | Protocol_mismatch of string
  | Acquisition_unknown of string
  | Wait_timeout
  | Lease_lost
  | Unsupported_capability of string

exception Error of error

type acquire_options = { wait_ms : int; lease_ms : int }
type options = {
  endpoint : string;
  namespace : string;
  coordination_epoch : string;
  acl_token : string option;
  rpc_timeout_ms : int;
  cleanup_timeout_ms : int;
  poll_interval_ms : int;
  default_acquisition : acquire_options;
}
type fence = { coordination_epoch : string; lock_index : string }
type provider = { options : options; mutable closed : bool; leases : lease list ref }
and resource = { provider : provider; resource_name : string; key : string }
and lease = {
  resource : resource;
  session_id : string;
  mutable effective_ttl_ms : int;
  mutable deadline_ns : int64;
  mutable lock_index : string;
  mutable state : string;
  mutation_gate : Lwt_mutex.t;
  stop_renewal : unit Lwt.t;
  stop_renewal_wakener : unit Lwt.u;
  mutable renewal_task : unit Lwt.t option;
}
type release_outcome = Released | Already_not_owned | Unknown
type response = { status : int; body : string }
type kv_row = { key : string; session : string option; lock_index : string; value : string option }
type session_row = { session_id : string; ttl_ms : int option }

let defaults epoch = {
  endpoint = "http://127.0.0.1:8500";
  namespace = "default";
  coordination_epoch = epoch;
  acl_token = None;
  rpc_timeout_ms = 2_000;
  cleanup_timeout_ms = 2_000;
  poll_interval_ms = 50;
  default_acquisition = { wait_ms = 5_000; lease_ms = 30_000 };
}

let max_duration_ms = 86_400_000
let min_lease_ms = 10_000
let margin_ms ttl = max 100 (ttl / 100)
let monotonic_ns = Mtime_clock.elapsed_ns
let int64_add_ms base ms =
  let amount = Int64.mul (Int64.of_int ms) 1_000_000L in
  if Int64.compare base (Int64.sub Int64.max_int amount) > 0 then
    raise (Error (Invalid_argument "duration overflows the monotonic clock"));
  Int64.add base amount

let valid_namespace value =
  let length = String.length value in
  length >= 1 && length <= 64 &&
  let first = value.[0] in
  ((first >= 'a' && first <= 'z') || (first >= '0' && first <= '9')) &&
  String.for_all (fun c ->
    (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c = '_' || c = '.' || c = '-') value

let strict_utf8 value limit =
  let length = String.length value in
  let rec scan index =
    if index = length then true else
    let byte i = Char.code value.[i] in
    let first = byte index in
    if first = 0 then false else
    let count, initial =
      if first <= 0x7f then 1, first
      else if first >= 0xc2 && first <= 0xdf then 2, first land 0x1f
      else if first >= 0xe0 && first <= 0xef then 3, first land 0x0f
      else if first >= 0xf0 && first <= 0xf4 then 4, first land 0x07
      else 0, 0
    in
    if count = 0 || index + count > length then false else
    let rec tail i scalar =
      if i = count then
        scalar >= (if count = 1 then 0 else if count = 2 then 0x80 else if count = 3 then 0x800 else 0x10000) &&
        scalar <= 0x10ffff && not (scalar >= 0xd800 && scalar <= 0xdfff)
      else
        let next = byte (index + i) in
        next land 0xc0 = 0x80 && tail (i + 1) ((scalar lsl 6) lor (next land 0x3f))
    in
    tail 1 initial && scan (index + count)
  in
  length > 0 && length <= limit && scan 0

let validate_endpoint endpoint =
  let uri = Uri.of_string endpoint in
  let scheme = Uri.scheme uri in
  match scheme, Uri.host uri with
  | (Some ("http" | "https")), Some host
    when host <> "" && Uri.userinfo uri = None &&
         (Uri.path uri = "" || Uri.path uri = "/") && Uri.query uri = [] && Uri.fragment uri = None -> ()
  | _ -> raise (Error (Invalid_argument "Consul endpoint must be an HTTP(S) authority without credentials, path, query, or fragment"))

let validate_options options =
  if not (valid_namespace options.namespace) then raise (Error (Invalid_argument "invalid namespace"));
  if not (strict_utf8 options.coordination_epoch 1024) then raise (Error (Invalid_argument "epoch must be 1..1024 UTF-8 bytes and contain no NUL"));
  validate_endpoint options.endpoint;
  (match options.acl_token with
   | None -> ()
   | Some token when token <> "" && String.for_all (fun c -> Char.code c >= 33 && Char.code c <= 126) token -> ()
   | Some _ -> raise (Error (Invalid_argument "ACL token must be printable non-space ASCII")));
  let bounded value = value > 0 && value <= max_duration_ms in
  if not (bounded options.rpc_timeout_ms && bounded options.cleanup_timeout_ms &&
          bounded options.poll_interval_ms && bounded options.default_acquisition.wait_ms &&
          options.default_acquisition.lease_ms >= min_lease_ms &&
          options.default_acquisition.lease_ms <= max_duration_ms) then
    raise (Error (Invalid_argument "Consul budgets or default TTL are outside supported bounds"))

let validate_acquire options =
  if options.wait_ms <= 0 || options.wait_ms > max_duration_ms ||
     options.lease_ms < min_lease_ms || options.lease_ms > max_duration_ms then
    raise (Error (Invalid_argument "wait must be 1..86400000 ms and lease must be 10000..86400000 ms"))

let request options meth path body timeout_ms =
  let endpoint = if String.ends_with ~suffix:"/" options.endpoint
    then String.sub options.endpoint 0 (String.length options.endpoint - 1) else options.endpoint in
  let uri = Uri.of_string (endpoint ^ path) in
  let headers =
    let fields = ["accept", "application/json"] in
    let fields = match body with None -> fields | Some _ -> ("content-type", "application/json") :: fields in
    let fields = match options.acl_token with None -> fields | Some token -> ("x-consul-token", token) :: fields in
    Cohttp.Header.of_list fields
  in
  let call = match body with
    | None -> Cohttp_lwt_unix.Client.call ~headers (Cohttp.Code.method_of_string meth) uri
    | Some value -> Cohttp_lwt_unix.Client.call ~headers ~body:(Cohttp_lwt.Body.of_string value)
        (Cohttp.Code.method_of_string meth) uri
  in
  let call = call >>= fun (response, body) ->
    Cohttp_lwt.Body.to_string body >|= fun body ->
    { status = Cohttp.Code.code_of_status (Cohttp.Response.status response); body }
  in
  let timed_call = Lwt.pick [
    call;
    (Lwt_unix.sleep (float_of_int timeout_ms /. 1000.) >>= fun () ->
     Lwt.fail (Failure "Consul HTTP request exceeded its local timeout"));
  ] in
  Lwt.catch
    (fun () -> timed_call >>= fun response ->
      if (response.status >= 200 && response.status < 300) || response.status = 404 then Lwt.return response
      else if (meth = "PUT" || meth = "POST") && response.status >= 500 then
        Lwt.fail (Error (Acquisition_unknown ("Consul mutation returned HTTP " ^ string_of_int response.status)))
      else Lwt.fail (Error (Backend_error ("Consul HTTP " ^ string_of_int response.status))))
    (function
      | Error _ as exn -> Lwt.fail exn
      | exn ->
        let message = Printexc.to_string exn in
        if meth = "PUT" || meth = "POST" then Lwt.fail (Error (Acquisition_unknown message))
        else Lwt.fail (Error (Backend_error message)))

let metadata_json epoch = `Assoc [
  "protocol_major", `Int 1;
  "identity_scheme", `String "sha256-framed-utf8-v1";
  "profile", `String "consul-kv-session-v1";
  "coordination_epoch", `String epoch;
  "fixed_lease_ms", `Null;
]

let exact_metadata epoch raw =
  try match Yojson.Safe.from_string raw with
  | `Assoc fields ->
    List.sort compare (List.map fst fields) = List.sort compare
      ["protocol_major"; "identity_scheme"; "profile"; "coordination_epoch"; "fixed_lease_ms"] &&
    List.mem ("protocol_major", `Int 1) fields &&
    List.mem ("identity_scheme", `String "sha256-framed-utf8-v1") fields &&
    List.mem ("profile", `String "consul-kv-session-v1") fields &&
    List.mem ("coordination_epoch", `String epoch) fields &&
    List.mem ("fixed_lease_ms", `Null) fields
  | _ -> false
  with _ -> false

let string_field name json = try Yojson.Safe.Util.(json |> member name |> to_string) with _ -> ""
let option_string_field name json = try
  match Yojson.Safe.Util.member name json with `String value -> Some value | _ -> None
with _ -> None

let parse_lock_index json =
  let field = try Yojson.Safe.Util.member "LockIndex" json with _ -> `Null in
  let raw = match field with
    | `Int value when value >= 0 -> string_of_int value
    | `Intlit value -> value
    | _ -> ""
  in
  if raw = "" || not (String.for_all (fun c -> c >= '0' && c <= '9') raw) then None
  else Some raw

let positive_decimal raw = String.exists (fun c -> c <> '0') raw

let parse_row raw =
  try match Yojson.Safe.from_string raw with
  | `List [row] ->
    let key = string_field "Key" row in
    (match parse_lock_index row with
     | Some lock_index when key <> "" ->
       Some { key; session = option_string_field "Session" row;
              lock_index; value = option_string_field "Value" row }
     | _ -> None)
  | _ -> None
  with _ -> None

let decode_metadata_value encoded = try Some (Base64.decode_exn encoded) with _ -> None
let resource_id namespace resource = Dilock.resource_id namespace resource
let metadata_key namespace = "dilock/namespaces/" ^ namespace ^ "/meta"
let json_bool raw = try Yojson.Safe.from_string raw = `Bool true with _ -> false

let read_key options key timeout_ms =
  request options "GET" ("/v1/kv/" ^ key ^ "?consistent") None timeout_ms >>= fun response ->
  if response.status = 404 then Lwt.return_none
  else match parse_row response.body with Some row -> Lwt.return_some row | None -> Lwt.return_none

let provision_namespace options =
  Lwt.catch (fun () ->
    validate_options options;
    let key = metadata_key options.namespace in
    let body = Yojson.Safe.to_string (metadata_json options.coordination_epoch) in
    request options "PUT" ("/v1/kv/" ^ key ^ "?cas=0") (Some body) options.rpc_timeout_ms >>= fun response ->
    if json_bool response.body then Lwt.return_unit else
    read_key options key options.rpc_timeout_ms >>= function
    | Some { value = Some encoded; _ } ->
      (match decode_metadata_value encoded with
       | Some raw when exact_metadata options.coordination_epoch raw -> Lwt.return_unit
       | _ -> Lwt.fail (Error (Protocol_mismatch "existing Consul metadata is incompatible")))
    | _ -> Lwt.fail (Error (Acquisition_unknown "metadata CAS was not acknowledged and the resulting record is unknown")))
    (function Error _ as exn -> Lwt.fail exn | exn -> Lwt.fail (Error (Backend_error (Printexc.to_string exn))))

let connect options =
  Lwt.catch (fun () ->
    validate_options options;
    let key = metadata_key options.namespace in
    read_key options key options.rpc_timeout_ms >>= function
    | Some { key = actual; value = Some encoded; _ } when actual = key ->
      (match decode_metadata_value encoded with
       | Some raw when exact_metadata options.coordination_epoch raw ->
         Lwt.return { options; closed = false; leases = ref [] }
       | _ -> Lwt.fail (Error (Protocol_mismatch "Consul namespace metadata is incompatible")))
    | _ -> Lwt.fail (Error (Protocol_mismatch "Consul namespace metadata is absent or incompatible")))
    (function Error _ as exn -> Lwt.fail exn | exn -> Lwt.fail (Error (Backend_error (Printexc.to_string exn))))

let lock provider resource_name =
  if provider.closed then Lwt.fail (Error (Invalid_argument "provider is closed")) else
  resource_id provider.options.namespace resource_name >|= fun id ->
  { provider; resource_name; key = "dilock/locks/" ^ id }

let duration_seconds ttl_ms = (ttl_ms + 999) / 1000
let effective_ttl ttl_ms = duration_seconds ttl_ms * 1000
let remaining_ms deadline =
  let remaining = Int64.sub deadline (monotonic_ns ()) in
  if Int64.compare remaining 0L <= 0 then 0
  else Int64.to_int (Int64.div remaining 1_000_000L)
let json_string name raw = try string_field name (Yojson.Safe.from_string raw) with _ -> ""
let uri_component = Uri.pct_encode

let create_session options ttl_ms =
  let ttl = string_of_int (duration_seconds ttl_ms) ^ "s" in
  let body = Yojson.Safe.to_string (`Assoc [
    "Name", `String "dilock-attempt"; "TTL", `String ttl;
    "Behavior", `String "release"; "LockDelay", `String "0s"; "Checks", `List []
  ]) in
  let sent = monotonic_ns () in
  request options "PUT" "/v1/session/create" (Some body) options.rpc_timeout_ms >>= fun response ->
  let session_id = json_string "ID" response.body in
  if session_id = "" then Lwt.fail (Error (Acquisition_unknown "Consul session creation was acknowledged without an ID"))
  else Lwt.return (session_id, int64_add_ms sent (effective_ttl ttl_ms - margin_ms (effective_ttl ttl_ms)))

let renew_session options session_id ttl_ms =
  let sent = monotonic_ns () in
  request options "PUT" ("/v1/session/renew/" ^ uri_component session_id) None options.rpc_timeout_ms >>= fun response ->
  let rows = try Yojson.Safe.from_string response.body with _ -> `Null in
  match rows with
  | `List (`Assoc fields :: _) when List.mem ("ID", `String session_id) fields ->
    Lwt.return (int64_add_ms sent (effective_ttl ttl_ms - margin_ms (effective_ttl ttl_ms)))
  | _ -> Lwt.fail (Error Lease_lost)

let destroy_session options session_id =
  Lwt.catch (fun () ->
    request options "PUT" ("/v1/session/destroy/" ^ uri_component session_id) None options.cleanup_timeout_ms >|= fun response ->
    json_bool response.body)
    (fun _ -> Lwt.return_false)

let finish_release outcome destroyed = if destroyed then outcome else Unknown

let renew_waiter options session_id ttl_ms deadline_ref failure_ref =
  let interval = max 100 (effective_ttl ttl_ms / 3) in
  let rec loop () =
    Lwt_unix.sleep (float_of_int interval /. 1000.) >>= fun () ->
    renew_session options session_id ttl_ms >>= fun deadline ->
    deadline_ref := deadline;
    loop ()
  in
  let task = loop () in
  Lwt.on_failure task (fun exn -> failure_ref := Some exn);
  task

let acquire_with resource acquire_options =
  Lwt.catch (fun () ->
    validate_acquire acquire_options;
    if resource.provider.closed then Lwt.fail (Error (Invalid_argument "provider is closed")) else
    let options = resource.provider.options in
    let ttl_ms = effective_ttl acquire_options.lease_ms in
    let deadline = int64_add_ms (monotonic_ns ()) acquire_options.wait_ms in
    create_session options ttl_ms >>= fun (session_id, lease_deadline) ->
    let waiter_deadline = ref lease_deadline in
    let waiter_failure = ref None in
    let waiter = renew_waiter options session_id ttl_ms waiter_deadline waiter_failure in
    let finish_waiter () = Lwt.cancel waiter in
    let rec attempt () =
      (match !waiter_failure with Some exn -> Lwt.fail exn | None -> Lwt.return_unit) >>= fun () ->
      if Int64.compare (monotonic_ns ()) deadline >= 0 then Lwt.fail (Error Wait_timeout) else
      let remaining = max 1 (remaining_ms deadline) in
      request options "PUT"
        ("/v1/kv/" ^ resource.key ^ "?acquire=" ^ uri_component session_id)
        (Some "") (min options.rpc_timeout_ms remaining) >>= fun response ->
      if json_bool response.body then
        read_key options resource.key (min options.rpc_timeout_ms (max 1 (remaining_ms deadline))) >>= (function
          | Some row when row.session = Some session_id && row.key = resource.key && positive_decimal row.lock_index ->
            Lwt.return row
          | _ -> Lwt.fail (Error (Acquisition_unknown "Consul acknowledged acquire but ownership could not be confirmed")))
      else if remaining_ms deadline <= 0 then Lwt.fail (Error Wait_timeout)
      else Lwt_unix.sleep (float_of_int (min options.poll_interval_ms (remaining_ms deadline)) /. 1000.) >>= attempt
    in
    Lwt.catch
      (fun () -> attempt () >>= fun row ->
        finish_waiter ();
        let stopped, wakener = Lwt.wait () in
        let lease = {
          resource; session_id; effective_ttl_ms = ttl_ms; deadline_ns = !waiter_deadline;
          lock_index = row.lock_index; state = "held"; mutation_gate = Lwt_mutex.create ();
          stop_renewal = stopped; stop_renewal_wakener = wakener; renewal_task = None;
        } in
        resource.provider.leases := lease :: !(resource.provider.leases);
        let rec renew_loop () =
          Lwt.pick [stopped; (Lwt_unix.sleep (float_of_int (max 100 (ttl_ms / 3)) /. 1000.) >|= fun () -> ())] >>= fun () ->
          if Lwt.is_sleeping stopped then Lwt_mutex.with_lock lease.mutation_gate (fun () ->
            if lease.state <> "held" then Lwt.return_unit else
            if remaining_ms lease.deadline_ns <= 0 then (lease.state <- "lost"; Lwt.fail (Error Lease_lost)) else
            renew_session options session_id ttl_ms >>= fun new_deadline ->
            read_key options resource.key options.rpc_timeout_ms >>= (function
              | Some current when current.session = Some session_id && current.lock_index = lease.lock_index ->
                lease.deadline_ns <- new_deadline; Lwt.return_unit
              | _ -> lease.state <- "lost"; Lwt.fail (Error Lease_lost)))
            >>= fun () -> renew_loop ()
          else Lwt.return_unit
        in
        lease.renewal_task <- Some (Lwt.catch renew_loop (fun _ -> lease.state <- "lost"; Lwt.return_unit));
        Lwt.return lease)
       (fun exn -> Lwt.cancel waiter; destroy_session options session_id >>= fun destroyed ->
         if destroyed then Lwt.fail exn
         else Lwt.fail (Error (Acquisition_unknown "Consul attempt session cleanup was not confirmed"))))
    (function Error _ as exn -> Lwt.fail exn | exn -> Lwt.fail (Error (Backend_error (Printexc.to_string exn))))

let acquire ?options resource =
  let options = Option.value options ~default:resource.provider.options.default_acquisition in
  acquire_with resource options

let acquire_default resource = acquire resource

let try_acquire ?options resource =
  let options = Option.value options ~default:resource.provider.options.default_acquisition in
  let wait_options = { options with wait_ms = 1 } in
  Lwt.catch (fun () -> acquire_with resource wait_options >|= fun lease -> Some lease)
    (function Error Wait_timeout -> Lwt.return_none | exn -> Lwt.fail exn)

let valid lease = lease.state = "held" && remaining_ms lease.deadline_ns > 0
let effective_ttl_ms lease = lease.effective_ttl_ms
let fence lease = { coordination_epoch = lease.resource.provider.options.coordination_epoch; lock_index = lease.lock_index }

let renew lease =
  Lwt_mutex.with_lock lease.mutation_gate (fun () ->
    if lease.state <> "held" || remaining_ms lease.deadline_ns <= 0 then
      (lease.state <- "lost"; Lwt.fail (Error Lease_lost))
    else renew_session lease.resource.provider.options lease.session_id lease.effective_ttl_ms >>= fun new_deadline ->
      read_key lease.resource.provider.options lease.resource.key lease.resource.provider.options.rpc_timeout_ms >>= (function
        | Some row when row.session = Some lease.session_id && row.lock_index = lease.lock_index ->
          lease.deadline_ns <- new_deadline; Lwt.return_unit
        | _ -> lease.state <- "lost"; Lwt.fail (Error Lease_lost)))

let release lease =
  (try Lwt.wakeup_later lease.stop_renewal_wakener () with Invalid_argument _ -> ());
  let options = lease.resource.provider.options in
  Lwt_mutex.with_lock lease.mutation_gate (fun () ->
    if lease.state = "released" then Lwt.return Already_not_owned else
    if lease.state = "unknown" then Lwt.return Unknown else
    let release_call = request options "PUT"
      ("/v1/kv/" ^ lease.resource.key ^ "?release=" ^ uri_component lease.session_id)
      (Some "") options.rpc_timeout_ms in
    Lwt.catch (fun () -> release_call >>= fun response ->
      if json_bool response.body then (lease.state <- "released"; Lwt.return Released)
      else read_key options lease.resource.key options.rpc_timeout_ms >>= function
        | Some row when row.session = Some lease.session_id -> lease.state <- "unknown"; Lwt.return Unknown
        | _ -> lease.state <- "released"; Lwt.return Already_not_owned)
      (fun _ -> lease.state <- "unknown"; Lwt.return Unknown) >>= fun outcome ->
    destroy_session options lease.session_id >|= fun destroyed ->
    let final_outcome = finish_release outcome destroyed in
    if final_outcome = Unknown then lease.state <- "unknown";
    final_outcome)

let with_lease resource acquire_options callback =
  acquire ~options:acquire_options resource >>= fun lease ->
  Lwt.finalize (fun () -> callback lease) (fun () -> release lease >|= fun _ -> ())

let close provider =
  provider.closed <- true;
  Lwt_list.iter_p (fun lease -> release lease >|= fun _ -> ()) !(provider.leases)
