open Lwt.Infix

type error = Invalid_argument of string | Backend_error of string | Busy
           | Acquisition_unknown of string | Protocol_mismatch of string
           | Wait_timeout | Lease_lost | Unsupported_capability of string
exception Error of error

type acquire_options = { wait_ms : int; lease_ms : int }
type options = {
  connection_string : string; namespace : string; coordination_epoch : string;
  rpc_timeout_ms : int; cleanup_timeout_ms : int; poll_interval_ms : int;
  default_acquisition : acquire_options;
}
type release_outcome = Released | Already_not_owned | Unknown
type provider = { options : options; pointer : unit Ctypes.ptr; mutable closed : bool; leases : lease list ref }
and resource = { provider : provider; resource_name : string; resource_pointer : unit Ctypes.ptr }
and lease = { resource : resource; lease_pointer : unit Ctypes.ptr; mutable state : string; gate : Lwt_mutex.t }

let defaults epoch = {
  connection_string = "postgresql://127.0.0.1:5432/dilock";
  namespace = "default"; coordination_epoch = epoch;
  rpc_timeout_ms = 2_000; cleanup_timeout_ms = 2_000; poll_interval_ms = 25;
  default_acquisition = { wait_ms = 5_000; lease_ms = 30_000 };
}

module C = struct
  open Ctypes
  open Foreign
  external keep_stubs : unit -> unit = "dilock_ocaml_postgres_keep_stubs"
  let provision = foreign "dilock_ocaml_postgres_provision"
    (string @-> string @-> string @-> int64_t @-> int64_t @-> int64_t @-> int64_t @-> int64_t @-> ptr char @-> returning int)
  let provider_create = foreign "dilock_ocaml_postgres_provider_create"
    (string @-> string @-> string @-> int64_t @-> int64_t @-> int64_t @-> int64_t @-> int64_t @-> ptr (ptr void) @-> ptr char @-> returning int)
  let provider_destroy = foreign "dilock_ocaml_postgres_provider_destroy" (ptr void @-> returning void)
  let lock_create = foreign "dilock_ocaml_postgres_lock_create"
    (ptr void @-> string @-> ptr (ptr void) @-> ptr char @-> returning int)
  let lock_destroy = foreign "dilock_ocaml_postgres_lock_destroy" (ptr void @-> returning void)
  let acquire = foreign "dilock_ocaml_postgres_acquire"
    (ptr void @-> int64_t @-> int64_t @-> ptr (ptr void) @-> ptr char @-> returning int)
  let try_acquire = foreign "dilock_ocaml_postgres_try_acquire"
    (ptr void @-> ptr (ptr void) @-> ptr char @-> returning int)
  let lease_valid = foreign "dilock_ocaml_postgres_lease_valid" (ptr void @-> returning int)
  let lease_renew = foreign "dilock_ocaml_postgres_lease_renew"
    (ptr void @-> ptr int @-> ptr char @-> returning int)
  let lease_release = foreign "dilock_ocaml_postgres_lease_release"
    (ptr void @-> ptr int @-> ptr char @-> returning int)
  let lease_destroy = foreign "dilock_ocaml_postgres_lease_destroy" (ptr void @-> returning void)
end

let () = C.keep_stubs ()

let max_duration_ms = 86_400_000
let valid_duration value = value > 0 && value <= max_duration_ms
let validate_text field value =
  if value = "" || String.contains value '\000' then
    raise (Error (Invalid_argument (field ^ " must be nonempty and contain no NUL")))
let validate_options options =
  validate_text "connection string" options.connection_string;
  validate_text "namespace" options.namespace;
  validate_text "coordination epoch" options.coordination_epoch;
  if not (valid_duration options.rpc_timeout_ms && valid_duration options.cleanup_timeout_ms &&
          valid_duration options.poll_interval_ms && valid_duration options.default_acquisition.wait_ms &&
          valid_duration options.default_acquisition.lease_ms) then
    raise (Error (Invalid_argument "PostgreSQL timeout and acquisition values are invalid"))
let validate_acquire options =
  if not (valid_duration options.wait_ms && valid_duration options.lease_ms) then
    raise (Error (Invalid_argument "PostgreSQL wait and lease values must be 1..86400000 ms"))

let message_buffer () = Ctypes.CArray.make Ctypes.char 384
let message_value buffer =
  let value = Bytes.create (Ctypes.CArray.length buffer) in
  for index = 0 to Ctypes.CArray.length buffer - 1 do
    Bytes.set value index (Ctypes.CArray.get buffer index)
  done;
  let value = Bytes.unsafe_to_string value in
  try String.sub value 0 (String.index value '\000') with Not_found -> value
let status_error status message = match status with
  | 1 -> Busy
  | 2 -> Invalid_argument message
  | 4 -> Wait_timeout
  | 5 -> Backend_error message
  | 6 -> Acquisition_unknown message
  | 7 -> Protocol_mismatch message
  | 8 -> Lease_lost
  | 9 -> Unsupported_capability message
  | _ -> Backend_error message
let check_status status message = if status <> 0 then raise (Error (status_error status message))
let null_pointer = Ctypes.from_voidp Ctypes.void Ctypes.null
let pointer_value out = Ctypes.(!@) out

let with_provider_call _options operation =
  Lwt_preemptive.detach (fun () ->
    let message = message_buffer () in
    let out = Ctypes.allocate (Ctypes.ptr Ctypes.void) null_pointer in
    let status = operation out (Ctypes.CArray.start message) in
    let pointer = pointer_value out in
    check_status status (message_value message);
    if Ctypes.is_null pointer then raise (Error (Backend_error "C ABI returned a null PostgreSQL provider"));
    pointer) ()

let connect options =
  validate_options options;
  with_provider_call options (fun out message ->
    C.provider_create options.connection_string options.namespace options.coordination_epoch
      (Int64.of_int options.rpc_timeout_ms) (Int64.of_int options.cleanup_timeout_ms)
      (Int64.of_int options.poll_interval_ms) (Int64.of_int options.default_acquisition.wait_ms)
      (Int64.of_int options.default_acquisition.lease_ms) out message) >|= fun pointer ->
  { options; pointer; closed = false; leases = ref [] }

let provision_namespace options =
  validate_options options;
  Lwt_preemptive.detach (fun () ->
    let message = message_buffer () in
    let status = C.provision options.connection_string options.namespace options.coordination_epoch
      (Int64.of_int options.rpc_timeout_ms) (Int64.of_int options.cleanup_timeout_ms)
      (Int64.of_int options.poll_interval_ms) (Int64.of_int options.default_acquisition.wait_ms)
      (Int64.of_int options.default_acquisition.lease_ms) (Ctypes.CArray.start message) in
    check_status status (message_value message)) ()

let lock provider resource_name =
  if provider.closed then Lwt.fail (Error (Invalid_argument "provider is closed")) else
  Dilock.resource_id provider.options.namespace resource_name >>= fun _ ->
  Lwt_preemptive.detach (fun () ->
    let message = message_buffer () in
    let out = Ctypes.allocate (Ctypes.ptr Ctypes.void) null_pointer in
    let status = C.lock_create provider.pointer resource_name out (Ctypes.CArray.start message) in
    let pointer = pointer_value out in
    check_status status (message_value message);
    if Ctypes.is_null pointer then raise (Error (Backend_error "C ABI returned a null PostgreSQL lock"));
    { provider; resource_name; resource_pointer = pointer }) ()

let make_lease resource pointer =
  let value = { resource; lease_pointer = pointer; state = "held"; gate = Lwt_mutex.create () } in
  resource.provider.leases := value :: !(resource.provider.leases); value

let try_acquire resource =
  if resource.provider.closed then Lwt.fail (Error (Invalid_argument "provider is closed")) else
  Lwt_preemptive.detach (fun () ->
    let message = message_buffer () in
    let out = Ctypes.allocate (Ctypes.ptr Ctypes.void) null_pointer in
    let status = C.try_acquire resource.resource_pointer out (Ctypes.CArray.start message) in
    let pointer = pointer_value out in
    if status = 1 then None else begin
      check_status status (message_value message);
      if Ctypes.is_null pointer then raise (Error (Backend_error "C ABI returned a null PostgreSQL lease"));
      Some (make_lease resource pointer)
    end) ()

let acquire resource options =
  validate_acquire options;
  let deadline = Int64.add (Mtime_clock.elapsed_ns ()) (Int64.mul (Int64.of_int options.wait_ms) 1_000_000L) in
  Lwt_preemptive.detach (fun () ->
    let message = message_buffer () in
    let out = Ctypes.allocate (Ctypes.ptr Ctypes.void) null_pointer in
    let status = C.acquire resource.resource_pointer (Int64.of_int options.wait_ms) (Int64.of_int options.lease_ms)
      out (Ctypes.CArray.start message) in
    let pointer = pointer_value out in
    check_status status (message_value message);
    if Ctypes.is_null pointer then raise (Error (Backend_error "C ABI returned a null PostgreSQL lease"));
    Some pointer) () >>= function
  | Some pointer -> Lwt.return (make_lease resource pointer)
  | None -> if Int64.compare (Mtime_clock.elapsed_ns ()) deadline >= 0 then Lwt.fail (Error Wait_timeout)
            else Lwt.fail (Error Wait_timeout)

let renew lease = Lwt_mutex.with_lock lease.gate (fun () ->
  if lease.state <> "held" then Lwt.return_false else
  Lwt_preemptive.detach (fun () ->
    let message = message_buffer () in
    let renewed = Ctypes.allocate Ctypes.int 0 in
    let status = C.lease_renew lease.lease_pointer renewed (Ctypes.CArray.start message) in
    check_status status (message_value message); Ctypes.(!@) renewed <> 0) () >>= fun renewed ->
  if renewed then Lwt.return_true else (lease.state <- "lost"; Lwt.return_false))

let release lease = Lwt_mutex.with_lock lease.gate (fun () ->
  if lease.state = "released" then Lwt.return Already_not_owned else
  if lease.state = "released_unknown" then Lwt.return Unknown else begin
    lease.state <- "releasing";
    Lwt.catch (fun () -> Lwt_preemptive.detach (fun () ->
      let message = message_buffer () in
      let outcome = Ctypes.allocate Ctypes.int 2 in
      let status = C.lease_release lease.lease_pointer outcome (Ctypes.CArray.start message) in
      if status <> 0 then raise (Error (status_error status (message_value message)));
      match Ctypes.(!@) outcome with 0 -> Released | 1 -> Already_not_owned | _ -> Unknown) () >>= fun outcome ->
      lease.state <- if outcome = Unknown then "released_unknown" else "released";
      Lwt.return outcome)
      (fun _ -> lease.state <- "released_unknown"; Lwt.return Unknown)
  end)

let with_lease resource options f = acquire resource options >>= fun lease ->
  Lwt.try_bind (fun () -> f lease)
    (fun value -> release lease >|= fun outcome -> value, outcome)
    (fun error -> release lease >>= fun _ -> Lwt.fail error)

let provider_close provider =
  if provider.closed then Lwt.return_unit else begin
    provider.closed <- true;
    Lwt_list.iter_s (fun lease -> release lease >>= fun _ -> Lwt.return_unit) !(provider.leases) >>= fun () ->
    Lwt_preemptive.detach (fun () -> C.provider_destroy provider.pointer) ()
  end

let lock_destroy resource = Lwt_preemptive.detach (fun () -> C.lock_destroy resource.resource_pointer) ()
let lease_destroy lease = Lwt_preemptive.detach (fun () -> C.lease_destroy lease.lease_pointer) ()
