open Lwt.Infix

module Deadline = struct
  let nanoseconds_per_millisecond = 1_000_000L

  let after_milliseconds ~now milliseconds =
    if milliseconds <= 0 then None
    else
      let milliseconds = Int64.of_int milliseconds in
      let maximum_milliseconds =
        Int64.div Int64.max_int nanoseconds_per_millisecond
      in
      if Int64.compare milliseconds maximum_milliseconds > 0 then None
      else
        let duration = Int64.mul milliseconds nanoseconds_per_millisecond in
        if Int64.compare now (Int64.sub Int64.max_int duration) > 0 then None
        else Some (Int64.add now duration)

  let expired ~now ~deadline = Int64.compare now deadline >= 0

  let renewal_deadline ~now ~previous_deadline ~sent milliseconds =
    if expired ~now ~deadline:previous_deadline then None
    else after_milliseconds ~now:sent milliseconds
end

type error = Invalid_argument of string | Backend_error of string | Acquisition_unknown of string
           | Protocol_mismatch of string | Wait_timeout
exception Error of error
type provider = { connection : Redis_lwt.Client.connection; namespace : string; epoch : string }
type handle = { provider : provider; key : string }
type lease = { handle : handle; owner : string; ttl_ms : int; mutable deadline : int64; mutable state : string; release_gate : Lwt_mutex.t }
type acquire_options = { wait_ms : int; lease_ms : int }
type release_outcome = Released | Already_not_owned | Unknown

let validate_contention_type = function
  | `String | `None -> ()
  | _ -> raise (Error (Protocol_mismatch "lock key has an incompatible Redis type"))

let validate_metadata_type = function
  | `String -> ()
  | `None -> raise (Error (Protocol_mismatch "Redis namespace metadata is missing"))
  | _ -> raise (Error (Protocol_mismatch "Redis namespace metadata must be a string"))

let backend_call context operation =
  Lwt.catch operation (function
    | Lwt.Canceled as exn -> Lwt.fail exn
    | Error _ as exn -> Lwt.fail exn
    | exn -> Lwt.fail (Error (Backend_error (context ^ ": " ^ Printexc.to_string exn))))

let valid_namespace ns = String.length ns > 0 && String.length ns <= 64 &&
  let valid c = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c = '_' || c = '.' || c = '-' in
  let first = ns.[0] in ((first >= 'a' && first <= 'z') || (first >= '0' && first <= '9')) &&
  String.for_all valid ns

let valid_utf8_nonempty value max_bytes =
  let length = String.length value in
  let rec scan i =
    if i = length then true
    else
      let byte i = Char.code value.[i] in
      let first = byte i in
      if first = 0 then false
      else
        let count, initial =
          if first <= 0x7f then 1, first
          else if first >= 0xc2 && first <= 0xdf then 2, first land 0x1f
          else if first >= 0xe0 && first <= 0xef then 3, first land 0x0f
          else if first >= 0xf0 && first <= 0xf4 then 4, first land 0x07
          else 0, 0
        in
        if count = 0 || i + count > length then false
        else
          let rec continuation j scalar =
            if j = count then
              scalar >= (if count = 1 then 0 else if count = 2 then 0x80 else if count = 3 then 0x800 else 0x10000) &&
              scalar <= 0x10ffff && not (scalar >= 0xd800 && scalar <= 0xdfff)
            else
              let next = byte (i + j) in
              if next land 0xc0 <> 0x80 then false
              else continuation (j + 1) ((scalar lsl 6) lor (next land 0x3f))
          in
          continuation 1 initial && scan (i + count)
  in
  length > 0 && length <= max_bytes && scan 0

let resource_id namespace resource =
  if not (valid_namespace namespace) then Lwt.fail (Error (Invalid_argument "invalid namespace")) else
  if not (valid_utf8_nonempty resource 1024) then
    Lwt.fail (Error (Invalid_argument "resource must be strict UTF-8, nonempty, at most 1024 bytes, and contain no NUL"))
  else
    let u32 n = String.init 4 (fun i -> Char.chr ((n lsr (24 - (8 * i))) land 255)) in
    let framed = "dilock.lock.identity\000" ^ u32 (String.length namespace) ^ namespace ^
      u32 (String.length resource) ^ resource in
    Lwt.return (Digestif.SHA256.(to_hex (digest_string framed)))

let read_script name =
  let filename = name ^ ".lua" in
  let package_directory =
    try Findlib.package_directory "dilock" with _ -> ""
  in
  let source_directory = Filename.concat (Filename.dirname __FILE__) "../data" in
  let candidates =
    [Filename.concat package_directory filename;
     Filename.concat source_directory filename]
  in
  let rec find_script = function
    | [] -> failwith ("Dilock Redis script is missing from the installed package: " ^ filename)
    | path :: rest -> if Sys.file_exists path then path else find_script rest
  in
  let path = find_script candidates in
  let ch = open_in_bin path in Fun.protect ~finally:(fun () -> close_in ch) (fun () -> really_input_string ch (in_channel_length ch))

let metadata_ok epoch raw = try
  let json = Yojson.Basic.from_string raw in
  Yojson.Basic.Util.member "protocol_major" json = `Int 1 &&
  Yojson.Basic.Util.member "identity_scheme" json = `String "sha256-framed-utf8-v1" &&
  Yojson.Basic.Util.member "profile" json = `String "redis-single-authority-v1" &&
  Yojson.Basic.Util.member "coordination_epoch" json = `String epoch &&
  Yojson.Basic.Util.member "fixed_lease_ms" json = `Null
with _ -> false

let provider connection namespace epoch =
  if epoch = "" || String.length epoch > 1024 then Lwt.fail (Error (Invalid_argument "invalid epoch")) else
  resource_id namespace "validation" >>= fun _ ->
  let key = "dilock:namespace:" ^ namespace ^ ":meta" in
  backend_call "Redis namespace metadata TYPE" (fun () -> Redis_lwt.Client.type_of connection key) >>= fun key_type ->
  (try validate_metadata_type key_type; Lwt.return_unit with Error error -> Lwt.fail (Error error)) >>= fun () ->
  backend_call "Redis namespace metadata GET" (fun () -> Redis_lwt.Client.get connection key) >>= function
  | Some raw when String.length raw <= 8192 && metadata_ok epoch raw -> Lwt.return { connection; namespace; epoch }
  | _ -> Lwt.fail (Error (Protocol_mismatch "namespace metadata missing or incompatible"))

let lock provider resource = resource_id provider.namespace resource >|= fun id ->
  { provider; key = "dilock:{" ^ id ^ "}:lease" }

let random_owner () =
  let ch = open_in_bin "/dev/urandom" in
  let raw = Fun.protect ~finally:(fun () -> close_in ch) (fun () -> really_input_string ch 32) in
  "dilock/1/" ^ String.concat "" (List.init 32 (fun i -> Printf.sprintf "%02x" (Char.code raw.[i])))

let eval connection script key args = Redis_lwt.Client.eval connection script [key] args
let monotonic_ns = Mtime_clock.elapsed_ns

let deadline_after now milliseconds =
  match Deadline.after_milliseconds ~now milliseconds with
  | Some deadline -> deadline
  | None -> raise (Error (Invalid_argument "duration overflows the monotonic clock"))

let with_lease_mutation gate operation = Lwt_mutex.with_lock gate operation

let release_once gate get_state set_state attempt =
  with_lease_mutation gate (fun () ->
    match get_state () with
    | "released" -> Lwt.return Already_not_owned
    | "released_unknown" -> Lwt.return Unknown
    | _ ->
      set_state "releasing";
      Lwt.catch
        (fun () -> attempt () >|= fun outcome ->
          set_state (if outcome = Unknown then "released_unknown" else "released");
          outcome)
        (function
          | Lwt.Canceled as exn -> set_state "released_unknown"; Lwt.fail exn
          | _ -> set_state "released_unknown"; Lwt.return Unknown))

let release lease =
  release_once lease.release_gate (fun () -> lease.state) (fun state -> lease.state <- state)
    (fun () ->
      eval lease.handle.provider.connection (read_script "redis-release") lease.handle.key [lease.owner]
      >|= function
      | `Int 1 -> Released
      | `Int 0 -> Already_not_owned
      | _ -> Unknown)

let accept_acquired ~now ~deadline ~release lease =
  if not (Deadline.expired ~now ~deadline) then Lwt.return lease
  else
    Lwt.catch
      (fun () ->
        release lease >>= function
        | Released | Already_not_owned -> Lwt.fail (Error Wait_timeout)
        | Unknown -> Lwt.fail (Error (Acquisition_unknown
            "acquisition completed after the wait deadline and owner cleanup was not acknowledged")))
      (function
        | Error Wait_timeout -> Lwt.fail (Error Wait_timeout)
        | Lwt.Canceled -> Lwt.fail (Error (Acquisition_unknown
            "acquisition completed after the wait deadline but owner cleanup was cancelled"))
        | _ -> Lwt.fail (Error (Acquisition_unknown
            "acquisition completed after the wait deadline and owner cleanup failed")))

let try_acquire handle ttl_ms =
  if ttl_ms < 100 || ttl_ms > 86400000 then Lwt.fail (Error (Invalid_argument "lease must be 100..86400000 ms")) else
  let owner = random_owner () in
  let sent = monotonic_ns () in
  Lwt.catch (fun () -> Redis_lwt.Client.set handle.provider.connection ~px:ttl_ms ~nx:true handle.key owner >>= fun acquired ->
    if not acquired then
      Lwt.catch
        (fun () -> Redis_lwt.Client.type_of handle.provider.connection handle.key >>= fun key_type ->
          validate_contention_type key_type;
          Lwt.return_none)
        (function
          | Error (Protocol_mismatch _ as error) -> Lwt.fail (Error error)
          | _ -> Lwt.fail (Error (Backend_error "Redis TYPE check failed after contention")))
    else
    let margin = max 10 (ttl_ms / 100) in
    let deadline = deadline_after sent (ttl_ms - margin) in
    if Deadline.expired ~now:(monotonic_ns ()) ~deadline then
      let provisional = { handle; owner; ttl_ms; deadline; state = "held"; release_gate = Lwt_mutex.create () } in
      release provisional >>= fun _ -> Lwt.fail (Error (Acquisition_unknown "late SET acknowledgement"))
    else Lwt.return_some { handle; owner; ttl_ms; deadline; state = "held"; release_gate = Lwt_mutex.create () })
    (function
      | Error (Backend_error _ | Protocol_mismatch _) as ex -> Lwt.fail ex
      | ex ->
        let provisional = { handle; owner; ttl_ms; deadline = deadline_after sent ttl_ms; state = "held"; release_gate = Lwt_mutex.create () } in
        Lwt.catch (fun () -> release provisional >>= fun _ -> Lwt.return_unit) (fun _ -> Lwt.return_unit) >>= fun () ->
        Lwt.fail (Error (Acquisition_unknown (Printexc.to_string ex))))

let acquire handle opts =
  if opts.wait_ms <= 0 then Lwt.fail (Error (Invalid_argument "wait_ms must be positive")) else
  try
    let until = deadline_after (monotonic_ns ()) opts.wait_ms in
    let rec loop () = if Deadline.expired ~now:(monotonic_ns ()) ~deadline:until then Lwt.fail (Error Wait_timeout) else
      try_acquire handle opts.lease_ms >>= function
      | Some lease -> accept_acquired ~now:(monotonic_ns ()) ~deadline:until ~release lease
      | None -> Lwt_unix.sleep 0.025 >>= loop
    in loop ()
  with Error error -> Lwt.fail (Error error)

let protect_renewal_acknowledgement ~mark_lost send =
  Lwt.catch send (fun error ->
    (* A failed acknowledgement may follow an applied PEXPIRE. Stop local work
       under this lease before propagating transport failure or cancellation. *)
    mark_lost ();
    Lwt.fail error)

let dispatch_renewal ~is_held ~mark_lost send =
  if not is_held then Lwt.return_none
  else protect_renewal_acknowledgement ~mark_lost send >|= fun response -> Some response

let renew lease =
  with_lease_mutation lease.release_gate (fun () ->
    if lease.state <> "held" then Lwt.return (`Done false)
    else if Deadline.expired ~now:(monotonic_ns ()) ~deadline:lease.deadline then begin
      (* The local deadline is deliberately conservative; Redis can still retain
         the key for the safety margin. Release it without risking a successor. *)
      lease.state <- "lost";
      Lwt.return `Cleanup
    end else
    let sent = monotonic_ns () in
    dispatch_renewal ~is_held:(lease.state = "held")
      ~mark_lost:(fun () -> lease.state <- "lost")
      (fun () -> eval lease.handle.provider.connection (read_script "redis-renew") lease.handle.key
        [lease.owner; string_of_int lease.ttl_ms]) >>= function
    | None -> Lwt.return (`Done false)
    | Some (`Int 1) ->
        (match Deadline.renewal_deadline ~now:(monotonic_ns ()) ~previous_deadline:lease.deadline
            ~sent (lease.ttl_ms - max 10 (lease.ttl_ms / 100)) with
         | Some deadline -> lease.deadline <- deadline; Lwt.return (`Done true)
         | None ->
             (* Redis may have extended the key, but the local lease validity elapsed
                before its acknowledgement. Do not report ownership; release only
                if the same owner token is still present. *)
             lease.state <- "lost";
             Lwt.return `Cleanup)
    | Some _ -> lease.state <- "lost"; Lwt.return (`Done false)) >>= function
  | `Done renewed -> Lwt.return renewed
  | `Cleanup -> release lease >|= fun _ -> false

let with_release_observer ~release ~on_release work lease =
  let report outcome = try on_release outcome with _ -> () in
  let cleanup () =
    (* Scope cleanup is part of the caller's resource lifetime, not its work
       cancellation. In particular, cancellation of [with_lease] must not
       cancel an in-flight Redis owner-checked release. etcd's release method
       also protects its internal cleanup promise; shielding here gives both
       adapters the same scope guarantee. *)
    Lwt.protected
      (Lwt.catch
         (fun () -> release lease >>= fun outcome -> report outcome; Lwt.return_unit)
         (fun exn -> report Unknown; Lwt.fail exn))
  in
  Lwt.try_bind work
    (fun value -> cleanup () >|= fun () -> value)
    (fun body_error ->
      Lwt.catch cleanup (fun _ -> Lwt.return_unit) >>= fun () ->
      Lwt.fail body_error)

let with_lease ?(on_release = fun _ -> ()) handle opts f =
  acquire handle opts >>= fun lease ->
  with_release_observer ~release ~on_release (fun () -> f lease) lease

let defaults = { wait_ms = 5000; lease_ms = 30000 }
