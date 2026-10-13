open Lwt.Infix
open Lwt.Syntax

(** Native etcd v3 Lock/Lease adapter. The current source preview supports
    plaintext HTTP/2 endpoints. Each unary RPC owns its socket; cancellation of
    Lock closes that socket before an independent LeaseRevoke is attempted. *)
open Dilock
exception Etcd_not_found
  type error =
    | Invalid_argument of string
    | Backend_error of string
    | Protocol_mismatch of string
    | Acquisition_unknown of string
    | Wait_timeout
    | Unsupported_capability of string

  exception Error of error

  type acquire_options = { wait_ms : int; lease_ms : int }
  type options = {
    endpoint : string;
    namespace : string;
    coordination_epoch : string;
    rpc_timeout_ms : int;
    cleanup_timeout_ms : int;
    safety_margin_ms : int;
    default_acquisition : acquire_options;
  }
  type fence = { coordination_epoch : string; create_revision : int64 }
  type provider = { options : options; host : string; port : string; authority : string }
  type resource = { provider : provider; name : string }
  type keeper = {
    socket : Lwt_unix.file_descr;
    task : unit Lwt.t;
    failure : exn option Lwt.t;
    deadline_ns : int64 ref;
    requests_sent : int ref;
    acknowledgements : int ref;
    mutable stopped : bool;
  }
  type lease = {
    resource : resource;
    lease_id : int64;
    effective_ttl_ms : int;
    key : bytes;
    fence : fence;
    keeper : keeper;
    mutable state : string;
    release_gate : Lwt_mutex.t;
  }

  let ns_per_ms = 1_000_000L
  let max_duration_ms = 86_400_000
  let monotonic_ns = Mtime_clock.elapsed_ns
  let fail error = Lwt.fail (Error error)
  let backend_error where ex = match ex with
    | Error _ | Lwt.Canceled -> Lwt.fail ex
    | _ -> fail (Backend_error (where ^ ": " ^ Printexc.to_string ex))

  let valid_namespace value =
    let length = String.length value in
    length >= 1 && length <= 64 &&
    let first = value.[0] in
    ((first >= 'a' && first <= 'z') || (first >= '0' && first <= '9')) &&
    String.for_all (fun c ->
      (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c = '_' || c = '.' || c = '-') value

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
                scalar <= 0x10ffff && not (scalar >= 0xd800 && scalar <= 0xdfff) &&
                (count <> 4 || scalar <= 0x10ffff)
              else
                let next = byte (i + j) in
                if next land 0xc0 <> 0x80 then false
                else continuation (j + 1) ((scalar lsl 6) lor (next land 0x3f))
            in
            continuation 1 initial && scan (i + count)
    in
    length > 0 && length <= max_bytes && scan 0

  let split_endpoint endpoint =
    if String.length endpoint = 0 then invalid_arg "endpoint is empty";
    if endpoint.[0] = '[' then
      match String.index_opt endpoint ']' with
      | None -> invalid_arg "IPv6 endpoint must use [address]:port"
      | Some close when close + 1 < String.length endpoint && endpoint.[close + 1] = ':' ->
          String.sub endpoint 1 (close - 1), String.sub endpoint (close + 2) (String.length endpoint - close - 2)
      | _ -> invalid_arg "endpoint must include a port"
    else
      match String.rindex_opt endpoint ':' with
      | None -> invalid_arg "endpoint must be host:port"
      | Some colon -> String.sub endpoint 0 colon, String.sub endpoint (colon + 1) (String.length endpoint - colon - 1)

  let valid_port value =
    String.length value > 0 &&
    String.for_all (fun c -> c >= '0' && c <= '9') value &&
    try
      let port = int_of_string value in
      port >= 1 && port <= 65_535
    with Failure _ -> false

  let validate_options options =
    if not (valid_namespace options.namespace) then raise (Error (Invalid_argument "invalid namespace"));
    if not (valid_utf8_nonempty options.coordination_epoch 1024) then raise (Error (Invalid_argument "invalid coordination epoch"));
    let valid_duration n = n > 0 && n <= max_duration_ms in
    if options.endpoint = "" || not (valid_duration options.rpc_timeout_ms) ||
       not (valid_duration options.cleanup_timeout_ms) || options.safety_margin_ms < 0 ||
       options.safety_margin_ms > max_duration_ms || not (valid_duration options.default_acquisition.wait_ms) ||
       not (valid_duration options.default_acquisition.lease_ms) then
      raise (Error (Invalid_argument "invalid endpoint or timeout option"));
    if options.rpc_timeout_ms > 99_999_999 || options.cleanup_timeout_ms > 99_999_999 then
      raise (Error (Invalid_argument "gRPC timeout exceeds the supported header range"));
    let host, port = split_endpoint options.endpoint in
    if host = "" || not (valid_port port) then raise (Error (Invalid_argument "endpoint must have a numeric TCP port in 1..65535"));
    host, port

  let defaults = {
    endpoint = "127.0.0.1:2379";
    namespace = "default";
    coordination_epoch = "";
    rpc_timeout_ms = 2_000;
    cleanup_timeout_ms = 2_000;
    safety_margin_ms = 25;
    default_acquisition = { wait_ms = 5_000; lease_ms = 30_000 };
  }

  let deadline_add_ms sent milliseconds =
    if milliseconds <= 0 then None else
    let duration = Int64.mul (Int64.of_int milliseconds) ns_per_ms in
    if Int64.compare sent (Int64.sub Int64.max_int duration) > 0 then None
    else Some (Int64.add sent duration)

  let remaining_budget_ms ~now deadline =
    match deadline with
    | None -> 0
    | Some deadline when Int64.compare now deadline >= 0 -> 0
    | Some deadline ->
        let remaining_ns = Int64.sub deadline now in
        Int64.to_int (Int64.div remaining_ns ns_per_ms)

  let bounded_timeout_ms ~now ~deadline ~maximum =
    min maximum (remaining_budget_ms ~now (Some deadline))

  let acquisition_rpc_timeout provider wait_deadline =
    let remaining = bounded_timeout_ms ~now:(monotonic_ns ()) ~deadline:wait_deadline
      ~maximum:provider.options.rpc_timeout_ms in
    if remaining <= 0 then raise (Error Wait_timeout);
    remaining

  let acquisition_lock_timeout_ms ~now wait_deadline =
    let remaining = remaining_budget_ms ~now (Some wait_deadline) in
    if remaining <= 0 then raise (Error Wait_timeout);
    remaining

  let ensure_acquisition_deadline ~now ~deadline =
    if Deadline.expired ~now ~deadline then raise (Error Wait_timeout)

  let classify_lock_rpc_error = function
    (* Lock's RPC timeout is the remaining caller wait budget, unlike the
       shorter per-call timeout used for grant/range/keepalive setup. *)
    | Error (Backend_error "v3lockpb.Lock/Lock local timeout")
      -> Error Wait_timeout
    | ex -> ex

  let headers provider () =
    (* grpc-lwt's H2 request hook does not synthesize :authority. etcd rejects
       requests without it, so preserve the configured endpoint authority.
       Deadlines are enforced locally by canceling and closing the dedicated
       socket; grpc-timeout hangs with the verified grpc-lwt/etcd transport. *)
    H2.Headers.of_list [ (":authority", provider.authority); ("te", "trailers"); ("content-type", "application/grpc+proto") ]

  let remaining_timeout_ms ~started ~timeout_ms now =
    remaining_budget_ms ~now (deadline_add_ms started timeout_ms)

  let connect provider ~timeout_ms =
    if timeout_ms <= 0 then fail (Backend_error "etcd connection timed out before connect") else
    let owned_socket = ref None in
    let transferred = ref false in
    let work =
      Lwt.catch
        (fun () ->
          Lwt_unix.getaddrinfo provider.host provider.port [Unix.(AI_SOCKTYPE SOCK_STREAM)] >>= function
          | [] -> fail (Backend_error "etcd DNS returned no addresses")
          | address :: _ ->
              let socket = Lwt_unix.socket address.Unix.ai_family address.Unix.ai_socktype address.Unix.ai_protocol in
              owned_socket := Some socket;
              Lwt.finalize
                (fun () ->
                  Lwt_unix.connect socket address.Unix.ai_addr >>= fun () ->
                  H2_lwt_unix.Client.create_connection ~error_handler:ignore socket >>= fun connection ->
                  transferred := true;
                  Lwt.return (socket, connection))
                (fun () ->
                  if !transferred then Lwt.return_unit
                  else Lwt.catch (fun () -> Lwt_unix.close socket) (fun _ -> Lwt.return_unit)))
        (backend_error "etcd name resolution or connect failed")
    in
    let timer = Lwt_unix.sleep (float timeout_ms /. 1000.) in
    Lwt.finalize
      (fun () ->
        Lwt.choose [ (work >|= fun result -> `Connected result); (timer >|= fun () -> `Timed_out) ] >>= function
        | `Connected result -> Lwt.return result
        | `Timed_out ->
            (match Lwt.state work with
             | Lwt.Return (socket, _) -> Lwt_unix.close socket
             | Lwt.Sleep -> Lwt.cancel work; Lwt.catch (fun () -> work >|= fun _ -> ()) (fun _ -> Lwt.return_unit)
             | Lwt.Fail _ -> Lwt.return_unit) >>= fun () ->
            (match !owned_socket with
             | None -> Lwt.return_unit
             | Some socket -> Lwt.catch (fun () -> Lwt_unix.close socket) (fun _ -> Lwt.return_unit)) >>= fun () ->
            fail (Backend_error "etcd connection local timeout"))
      (fun () ->
        Lwt.cancel timer;
        if !transferred then Lwt.return_unit
        else begin
          Lwt.cancel work;
          Lwt.catch (fun () -> work >|= fun _ -> ()) (fun _ -> Lwt.return_unit) >>= fun () ->
          (match !owned_socket with
           | None -> Lwt.return_unit
           | Some socket -> Lwt.catch (fun () -> Lwt_unix.close socket) (fun _ -> Lwt.return_unit))
        end)

  let with_connection provider f =
    connect provider ~timeout_ms:provider.options.rpc_timeout_ms >>= fun (socket, connection) ->
    Lwt.finalize
      (fun () -> f connection)
      (fun () -> Lwt.catch (fun () -> Lwt_unix.close socket) (fun _ -> Lwt.return_unit))

  let rpc ?(not_found_is_distinct = false) provider ~timeout_ms ~service ~method_name descriptor request =
    if timeout_ms <= 0 || timeout_ms > 99_999_999 then fail (Invalid_argument "invalid RPC timeout") else
    let started = monotonic_ns () in
    let encode, decode = Ocaml_protoc_plugin.Service.make_client_functions descriptor in
    let payload = encode request |> Ocaml_protoc_plugin.Writer.contents in
    let connect_timeout_ms = remaining_timeout_ms ~started ~timeout_ms (monotonic_ns ()) in
    connect provider ~timeout_ms:connect_timeout_ms >>= fun (socket, connection) ->
    let close_socket () =
      Lwt.catch (fun () -> Lwt_unix.close socket) (fun _ -> Lwt.return_unit)
    in
    let cancel_call call =
      (* grpc-lwt 0.2.0 builds its response/status waits from [Lwt.task] and
         exposes no stream-cancel handle. Canceling the outer promise alone
         does not establish that its transport wait has terminated. Closing
         this RPC's dedicated socket first makes the response wait settle; only
         then is it safe to await the canceled call and start independent cleanup. *)
      Lwt.cancel call;
      close_socket () >>= fun () ->
      Lwt.catch (fun () -> call >|= fun _ -> ()) (fun _ -> Lwt.return_unit)
    in
    Lwt.finalize (fun () ->
      let call = Grpc_lwt.Client.call ~scheme:"http" ~headers:(headers provider ())
        ~service ~rpc:method_name
        ~do_request:(H2_lwt_unix.Client.request connection ~error_handler:ignore)
        ~handler:(Grpc_lwt.Client.Rpc.unary payload ~f:(fun raw ->
          let+ raw = raw in
          match raw with
          | None -> failwith "missing unary protobuf response"
          | Some raw -> decode (Ocaml_protoc_plugin.Reader.create raw))) () in
      let call_timeout_ms = remaining_timeout_ms ~started ~timeout_ms (monotonic_ns ()) in
      if call_timeout_ms <= 0 then cancel_call call >>= fun () -> fail (Backend_error (service ^ "/" ^ method_name ^ " local timeout")) else
      let timeout = Lwt_unix.sleep (float call_timeout_ms /. 1000.) >|= fun () -> `Timeout in
      Lwt.finalize
        (fun () ->
          Lwt.choose [ (call >|= fun result -> `Result result); timeout ] >>= function
          | `Timeout ->
              cancel_call call >>= fun () ->
              fail (Backend_error (service ^ "/" ^ method_name ^ " local timeout"))
          | `Result (Error status) -> fail (Backend_error ("HTTP/2 status " ^ H2.Status.to_string status))
          | `Result (Ok (Error decode_error, _)) -> fail (Protocol_mismatch (Ocaml_protoc_plugin.Result.show_error decode_error))
          | `Result (Ok (Ok response, status)) ->
              (match Grpc.Status.code status with
               | Grpc.Status.OK -> Lwt.return response
               | Grpc.Status.Deadline_exceeded -> fail (Backend_error (service ^ "/" ^ method_name ^ " deadline exceeded"))
               | Grpc.Status.Not_found when not_found_is_distinct -> Lwt.fail Etcd_not_found
               | _ -> fail (Backend_error (Grpc.Status.show status))))
        (fun () ->
          Lwt.cancel timeout;
          cancel_call call))
      (fun () -> close_socket ())

  let metadata_ok (options : options) raw =
    if String.length raw = 0 || String.length raw > 8192 then false else
    try
      let json = Yojson.Basic.from_string raw in
      Yojson.Basic.Util.member "protocol_major" json = `Int 1 &&
      Yojson.Basic.Util.member "identity_scheme" json = `String "sha256-framed-utf8-v1" &&
      Yojson.Basic.Util.member "profile" json = `String "etcd-native-lock-v1" &&
      Yojson.Basic.Util.member "coordination_epoch" json = `String options.coordination_epoch &&
      Yojson.Basic.Util.member "fixed_lease_ms" json = `Null
    with _ -> false

  let provider options =
    try
      let host, port = validate_options options in
      let provider = { options; host; port; authority = options.endpoint } in
      let key = Bytes.of_string ("/dilock/namespaces/" ^ options.namespace ^ "/meta") in
      let request = Rpc.Etcdserverpb.RangeRequest.make ~key () in
      rpc provider ~timeout_ms:options.rpc_timeout_ms ~service:"etcdserverpb.KV" ~method_name:"Range" Rpc.Etcdserverpb.KV.range request
      >>= fun response ->
      (match response.Rpc.Etcdserverpb.RangeResponse.kvs with
       | [item] when item.Kv.Mvccpb.KeyValue.key = key && metadata_ok options (Bytes.to_string item.Kv.Mvccpb.KeyValue.value) -> Lwt.return provider
       | _ -> fail (Protocol_mismatch "etcd namespace metadata missing or incompatible"))
    with Error error -> fail error | Invalid_argument message -> fail (Invalid_argument message)

  let lock provider resource =
    if not (valid_utf8_nonempty resource 1024) then fail (Invalid_argument "resource must be strict UTF-8, nonempty, at most 1024 bytes, and contain no NUL")
    else
      Lwt.catch
        (fun () ->
          Dilock.resource_id provider.options.namespace resource >|= fun id ->
          { provider; name = "/dilock/locks/" ^ id })
        (function Error (Invalid_argument message) -> fail (Invalid_argument message) | ex -> Lwt.fail ex)

  let start_keeper provider lease_id ttl_ms initial_deadline ~timeout_ms =
    connect provider ~timeout_ms >>= fun (socket, connection) ->
    let deadline_ns = ref initial_deadline in
    let requests_sent = ref 0 in
    let acknowledgements = ref 0 in
    let stopped = ref false in
    let encode, decode = Ocaml_protoc_plugin.Service.make_client_functions Rpc.Etcdserverpb.Lease.leaseKeepAlive in
    let handler = Grpc_lwt.Client.Rpc.bidirectional_streaming ~f:(fun push responses ->
      let request = Rpc.Etcdserverpb.LeaseKeepAliveRequest.make ~iD:lease_id () in
      let request_bytes = encode request |> Ocaml_protoc_plugin.Writer.contents in
      let sent_times = Queue.create () in
      let rec send_loop () =
        if !stopped then Lwt.return_unit else
        let remaining = Int64.sub !deadline_ns (monotonic_ns ()) in
        if remaining <= 0L then fail (Backend_error "etcd lease validity elapsed without a KeepAlive acknowledgement")
        else begin
          Queue.add (monotonic_ns ()) sent_times;
          incr requests_sent;
          push (Some request_bytes);
          Lwt_unix.sleep (min (float ttl_ms /. 3000.) (Int64.to_float remaining /. 3_000_000_000.)) >>= send_loop
        end
      in
      let receive_loop () =
        Lwt_stream.iter_s (fun raw ->
         (match decode (Ocaml_protoc_plugin.Reader.create raw) with
         | Error error -> fail (Protocol_mismatch (Ocaml_protoc_plugin.Result.show_error error))
         | Ok response ->
              if response.Rpc.Etcdserverpb.LeaseKeepAliveResponse.iD <> lease_id || response.Rpc.Etcdserverpb.LeaseKeepAliveResponse.tTL <= 0L then
                fail (Backend_error "etcd KeepAlive returned an invalid lease identity or TTL")
              else
                let seconds = response.Rpc.Etcdserverpb.LeaseKeepAliveResponse.tTL in
                if Int64.compare seconds 86_400L > 0 then fail (Backend_error "etcd KeepAlive TTL exceeds supported range")
                else if Queue.is_empty sent_times then fail (Backend_error "etcd KeepAlive returned an unsolicited response")
                else
                  let ttl = Int64.to_int seconds * 1000 in
                  let sent = Queue.take sent_times in
                  match deadline_add_ms sent (ttl - provider.options.safety_margin_ms) with
                  | None -> fail (Backend_error "etcd KeepAlive deadline overflow")
                  | Some deadline when Int64.compare (monotonic_ns ()) deadline >= 0 -> fail (Backend_error "etcd KeepAlive acknowledgement arrived after conservative deadline")
                  | Some deadline -> deadline_ns := deadline; incr acknowledgements; Lwt.return_unit)) responses >>= fun () ->
        fail (Backend_error "etcd KeepAlive response stream ended")
      in
      Lwt.join [send_loop (); receive_loop ()]) in
    let task = Grpc_lwt.Client.call ~scheme:"http" ~headers:(headers provider ())
      ~service:"etcdserverpb.Lease" ~rpc:"LeaseKeepAlive"
      ~do_request:(H2_lwt_unix.Client.request connection ~error_handler:ignore) ~handler ()
      >>= fun result ->
      (match result with
       | Ok (_, status) when Grpc.Status.code status = Grpc.Status.OK -> fail (Backend_error "etcd KeepAlive stream ended unexpectedly")
       | Ok (_, status) -> fail (Backend_error (Grpc.Status.show status))
       | Error status -> fail (Backend_error ("HTTP/2 status " ^ H2.Status.to_string status))) in
    let failure = Lwt.catch (fun () -> task >|= fun () -> Some (Error (Backend_error "etcd KeepAlive stream ended")))
      (fun ex -> Lwt.return (Some ex)) in
    Lwt.on_cancel task (fun () -> stopped := true; Lwt.async (fun () -> Lwt.catch (fun () -> Lwt_unix.close socket) (fun _ -> Lwt.return_unit)));
    Lwt.async (fun () -> Lwt.catch (fun () -> task) (fun _ -> Lwt.return_unit));
    Lwt.return { socket; task; failure; deadline_ns; requests_sent; acknowledgements; stopped = false }

  let stop_keeper keeper =
    if keeper.stopped then Lwt.return_unit else begin
      keeper.stopped <- true;
      Lwt.cancel keeper.task;
      Lwt.catch (fun () -> Lwt_unix.close keeper.socket) (fun _ -> Lwt.return_unit) >>= fun () ->
      Lwt.catch (fun () -> keeper.task) (fun _ -> Lwt.return_unit)
    end

  let grant provider lease_ms ~timeout_ms =
    if lease_ms <= provider.options.rpc_timeout_ms + provider.options.safety_margin_ms || lease_ms > max_duration_ms then
      fail (Invalid_argument "etcd lease must exceed RPC timeout plus safety margin and be at most 24 hours")
    else
      let ttl_seconds = (lease_ms + 999) / 1000 in
      let sent = monotonic_ns () in
      let request = Rpc.Etcdserverpb.LeaseGrantRequest.make ~tTL:(Int64.of_int ttl_seconds) () in
      rpc provider ~timeout_ms ~service:"etcdserverpb.Lease" ~method_name:"LeaseGrant" Rpc.Etcdserverpb.Lease.leaseGrant request
      >|= fun response ->
      if response.Rpc.Etcdserverpb.LeaseGrantResponse.iD = 0L || response.Rpc.Etcdserverpb.LeaseGrantResponse.tTL <= 0L then
        raise (Error (Backend_error "etcd LeaseGrant returned an invalid lease"));
      if Int64.compare response.Rpc.Etcdserverpb.LeaseGrantResponse.tTL 86_400L > 0 then
        raise (Error (Backend_error "etcd LeaseGrant TTL exceeds supported range"));
      let effective_ms = Int64.to_int response.Rpc.Etcdserverpb.LeaseGrantResponse.tTL * 1000 in
      match deadline_add_ms sent (effective_ms - provider.options.safety_margin_ms) with
      | None -> raise (Error (Acquisition_unknown "etcd lease deadline overflow"))
      | Some deadline when Int64.compare (monotonic_ns ()) deadline >= 0 -> raise (Error (Acquisition_unknown "LeaseGrant response arrived after conservative validity elapsed"))
      | Some deadline -> response.Rpc.Etcdserverpb.LeaseGrantResponse.iD, effective_ms, deadline

  let revoke ?timeout_ms provider lease_id =
    let request = Rpc.Etcdserverpb.LeaseRevokeRequest.make ~iD:lease_id () in
    rpc provider ~timeout_ms:(Option.value timeout_ms ~default:provider.options.cleanup_timeout_ms) ~service:"etcdserverpb.Lease" ~method_name:"LeaseRevoke" Rpc.Etcdserverpb.Lease.leaseRevoke request
    >|= fun _ -> true

  let cleanup_grant provider lease_id =
    Lwt.protected (Lwt.catch (fun () -> revoke provider lease_id) (fun _ -> Lwt.return_false))

  let verify_key resource lease_id key ~timeout_ms =
    let request = Rpc.Etcdserverpb.RangeRequest.make ~key () in
    rpc resource.provider ~timeout_ms ~service:"etcdserverpb.KV" ~method_name:"Range" Rpc.Etcdserverpb.KV.range request
    >|= fun response ->
    match response.Rpc.Etcdserverpb.RangeResponse.kvs with
    | [item] when item.Kv.Mvccpb.KeyValue.key = key && item.Kv.Mvccpb.KeyValue.lease = lease_id && item.Kv.Mvccpb.KeyValue.create_revision > 0L ->
        { coordination_epoch = resource.provider.options.coordination_epoch; create_revision = item.Kv.Mvccpb.KeyValue.create_revision }
    | _ -> raise (Error (Protocol_mismatch "etcd Range did not confirm the exact lock key and expected lease"))

  let acquire_with resource acquire_options =
    let provider = resource.provider in
    let wait_ms = acquire_options.wait_ms in
    let requested_lease_ms = acquire_options.lease_ms in
    if wait_ms <= 0 || wait_ms > max_duration_ms then fail (Invalid_argument "wait budget must be 1..86400000 ms") else
    let started = monotonic_ns () in
    match deadline_add_ms started wait_ms with
    | None -> fail (Invalid_argument "wait deadline overflow")
    | Some wait_deadline ->
      let grant_timeout_ms = bounded_timeout_ms ~now:(monotonic_ns ()) ~deadline:wait_deadline
        ~maximum:provider.options.rpc_timeout_ms in
      if grant_timeout_ms <= 0 then fail Wait_timeout else
      grant provider requested_lease_ms ~timeout_ms:grant_timeout_ms >>= fun (lease_id, effective_ttl_ms, initial_deadline) ->
      let acquire_after_grant () =
        start_keeper provider lease_id effective_ttl_ms initial_deadline
          ~timeout_ms:(acquisition_rpc_timeout provider wait_deadline) >>= fun keeper ->
        let lock_timeout =
          try Some (acquisition_lock_timeout_ms ~now:(monotonic_ns ()) wait_deadline)
          with Error Wait_timeout -> None
        in
        match lock_timeout with
        | None -> stop_keeper keeper >>= fun () -> fail Wait_timeout
        | Some lock_timeout ->
        let lock_request = V3lock.V3lockpb.LockRequest.make ~name:(Bytes.of_string resource.name) ~lease:lease_id () in
        let lock_call =
          Lwt.catch
            (fun () -> rpc provider ~timeout_ms:lock_timeout ~service:"v3lockpb.Lock" ~method_name:"Lock" V3lock.V3lockpb.Lock.lock lock_request)
            (fun ex -> Lwt.fail (classify_lock_rpc_error ex))
        in
        let remaining_ns = Int64.sub wait_deadline (monotonic_ns ()) in
        let wait_timer =
          if remaining_ns <= 0L then Lwt.return_unit
          else Lwt_unix.sleep (Int64.to_float remaining_ns /. 1_000_000_000.)
        in
        let rec wait_for_lock () =
          match Lwt.state lock_call, Lwt.state keeper.failure with
          | Lwt.Return response, _ -> Lwt.return response
          | Lwt.Fail ex, _ -> Lwt.fail (classify_lock_rpc_error ex)
          | Lwt.Sleep, Lwt.Return (Some ex) -> Lwt.fail ex
          | Lwt.Sleep, Lwt.Fail ex -> Lwt.fail ex
          | Lwt.Sleep, (Lwt.Return None | Lwt.Sleep) ->
              Lwt.choose [ (lock_call >|= fun response -> `Locked response);
                           (wait_timer >|= fun () -> `Wait_elapsed);
                           (keeper.failure >|= fun failure -> `Keeper_failed failure) ] >>= function
              | `Locked response -> Lwt.return response
              | `Wait_elapsed -> fail Wait_timeout
              | `Keeper_failed (Some ex) -> Lwt.fail ex
              | `Keeper_failed None -> wait_for_lock ()
        in
        let returned_lease = ref false in
        Lwt.finalize
          (fun () -> wait_for_lock () >>= fun lock_response ->
            if Int64.compare (monotonic_ns ()) !(keeper.deadline_ns) >= 0 then fail (Acquisition_unknown "etcd Lock acknowledged after conservative lease validity elapsed")
            else
              verify_key resource lease_id lock_response.V3lock.V3lockpb.LockResponse.key
                ~timeout_ms:(acquisition_rpc_timeout provider wait_deadline) >>= fun fence ->
              ensure_acquisition_deadline ~now:(monotonic_ns ()) ~deadline:wait_deadline;
              if Int64.compare (monotonic_ns ()) !(keeper.deadline_ns) >= 0 then fail (Acquisition_unknown "etcd Range verification exceeded conservative lease validity")
              else
                let lease = { resource; lease_id; effective_ttl_ms; key = lock_response.V3lock.V3lockpb.LockResponse.key; fence; keeper; state = "held"; release_gate = Lwt_mutex.create () } in
                returned_lease := true;
                Lwt.return lease)
          (fun () ->
            Lwt.cancel wait_timer;
            Lwt.cancel lock_call;
            Lwt.catch (fun () -> lock_call >|= fun _ -> ()) (fun _ -> Lwt.return_unit) >>= fun () ->
            if !returned_lease then Lwt.return_unit else stop_keeper keeper)
      in
      Lwt.catch acquire_after_grant (fun ex ->
        cleanup_grant provider lease_id >>= fun cleaned ->
        if not cleaned then fail (Acquisition_unknown ("etcd acquisition cleanup was not confirmed: " ^ Printexc.to_string ex))
        else match ex with
          | Lwt.Canceled -> Lwt.fail Lwt.Canceled
          | Error Wait_timeout -> fail Wait_timeout
          | Error (Invalid_argument _ | Protocol_mismatch _ | Backend_error _ | Unsupported_capability _) -> Lwt.fail ex
          | Error (Acquisition_unknown _ as error) -> fail error
          | _ -> fail (Acquisition_unknown ("etcd acquisition outcome is unknown: " ^ Printexc.to_string ex)))

  let acquire ?options resource = acquire_with resource (Option.value options ~default:resource.provider.options.default_acquisition)
  let try_acquire _resource = fail (Unsupported_capability "etcd native Lock has no safe nonblocking try-acquire")
  let fence lease = lease.fence
  let effective_ttl_ms lease = lease.effective_ttl_ms
  let valid lease = lease.state = "held" && not lease.keeper.stopped && Int64.compare (monotonic_ns ()) !(lease.keeper.deadline_ns) < 0 && Lwt.is_sleeping lease.keeper.failure

type release_ownership = Exact_owner | Exact_absent | Exact_mismatch | Ownership_unknown
type revoke_outcome = Revoke_acknowledged | Revoke_not_found | Revoke_unknown

let classify_release_range ~key ~lease_id kvs =
  match kvs with
  | [] -> Exact_absent
  | [item] when item.Kv.Mvccpb.KeyValue.key <> key -> Ownership_unknown
  | [item] when item.Kv.Mvccpb.KeyValue.lease = lease_id -> Exact_owner
  | [_] -> Exact_mismatch
  | _ -> Ownership_unknown

let release_ownership_confirmed = function Exact_owner -> true | Exact_absent | Exact_mismatch | Ownership_unknown -> false

let classify_release_outcome ~ownership ~unlock_acknowledged ~revoke_outcome =
  match ownership, unlock_acknowledged, revoke_outcome with
  | Exact_owner, true, Revoke_acknowledged -> Released
  | (Exact_absent | Exact_mismatch), _, _ -> Already_not_owned
  | _, _, (Revoke_acknowledged | Revoke_not_found) -> Already_not_owned
  | _ -> Unknown

let release_range lease ~timeout_ms =
  let request = Rpc.Etcdserverpb.RangeRequest.make ~key:lease.key () in
  rpc lease.resource.provider ~timeout_ms ~service:"etcdserverpb.KV" ~method_name:"Range"
    Rpc.Etcdserverpb.KV.range request
  >|= fun response -> classify_release_range ~key:lease.key ~lease_id:lease.lease_id response.Rpc.Etcdserverpb.RangeResponse.kvs

let release_revoke lease ~timeout_ms =
  let request = Rpc.Etcdserverpb.LeaseRevokeRequest.make ~iD:lease.lease_id () in
  rpc ~not_found_is_distinct:true lease.resource.provider ~timeout_ms ~service:"etcdserverpb.Lease"
    ~method_name:"LeaseRevoke" Rpc.Etcdserverpb.Lease.leaseRevoke request
  >|= fun _ -> Revoke_acknowledged

let release lease =
    let cleanup_deadline = deadline_add_ms (monotonic_ns ()) lease.resource.provider.options.cleanup_timeout_ms in
    Lwt.protected (Lwt_mutex.with_lock lease.release_gate (fun () ->
      match lease.state with
      | "released" -> Lwt.return Already_not_owned
      | "released_unknown" -> Lwt.return Unknown
      | _ ->
           lease.state <- "releasing";
           Lwt.catch
             (fun () ->
               stop_keeper lease.keeper >>= fun () ->
              let range_timeout_ms = remaining_budget_ms ~now:(monotonic_ns ()) cleanup_deadline in
              (if range_timeout_ms = 0 then Lwt.return Ownership_unknown else
                Lwt.catch (fun () -> release_range lease ~timeout_ms:range_timeout_ms)
                  (fun _ -> Lwt.return Ownership_unknown)) >>= fun ownership ->
              let unlock_timeout_ms = remaining_budget_ms ~now:(monotonic_ns ()) cleanup_deadline in
              (if not (release_ownership_confirmed ownership) || unlock_timeout_ms = 0 then Lwt.return_false else
                let unlock_request = V3lock.V3lockpb.UnlockRequest.make ~key:lease.key () in
                Lwt.catch
                  (fun () -> rpc lease.resource.provider ~timeout_ms:unlock_timeout_ms ~service:"v3lockpb.Lock" ~method_name:"Unlock" V3lock.V3lockpb.Lock.unlock unlock_request >|= fun _ -> true)
                  (fun _ -> Lwt.return_false)) >>= fun unlock_ack ->
              let revoke_timeout_ms = remaining_budget_ms ~now:(monotonic_ns ()) cleanup_deadline in
              (if revoke_timeout_ms = 0 then Lwt.return Revoke_unknown else
                Lwt.protected (Lwt.catch
                  (fun () -> release_revoke lease ~timeout_ms:revoke_timeout_ms)
                  (function Etcd_not_found -> Lwt.return Revoke_not_found | _ -> Lwt.return Revoke_unknown))) >>= fun revoke_outcome ->
              let outcome = classify_release_outcome ~ownership ~unlock_acknowledged:unlock_ack ~revoke_outcome in
              lease.state <- (match outcome with Unknown -> "released_unknown" | Released | Already_not_owned -> "released");
              Lwt.return outcome)
            (fun _ -> lease.state <- "released_unknown"; Lwt.return Unknown)))

let with_lease ?options ?(on_release = fun _ -> ()) resource body =
  acquire ?options resource >>= fun lease ->
  Dilock.with_release_observer ~release ~on_release (fun () -> body lease) lease
