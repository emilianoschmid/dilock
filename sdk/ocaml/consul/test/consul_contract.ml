let expect_invalid thunk =
  try thunk (); Alcotest.fail "expected Invalid_argument"
  with Consul.Error (Consul.Invalid_argument _) -> ()

let test_defaults () =
  let options = Consul.defaults "epoch-a" in
  Alcotest.(check string) "default endpoint" "http://127.0.0.1:8500" options.endpoint;
  Alcotest.(check int) "default ttl" 30_000 options.default_acquisition.lease_ms;
  Consul.validate_options options

let test_validation () =
  let options = Consul.defaults "epoch-a" in
  expect_invalid (fun () -> Consul.validate_options { options with namespace = "Upper" });
  expect_invalid (fun () -> Consul.validate_options { options with endpoint = "http://user:pass@localhost:8500" });
  expect_invalid (fun () -> Consul.validate_options { options with coordination_epoch = "" });
  expect_invalid (fun () -> Consul.validate_acquire { wait_ms = 10; lease_ms = 9_999 });
  expect_invalid (fun () -> Consul.validate_acquire { wait_ms = 0; lease_ms = 10_000 })

let test_ttl_rounding () =
  Alcotest.(check int) "round upward to session seconds" 11_000 (Consul.effective_ttl 10_001);
  Alcotest.(check int) "exact seconds remain exact" 20_000 (Consul.effective_ttl 20_000)

let test_release_requires_destroy_ack () =
  Alcotest.(check bool) "destroy ack preserves release" true
    (Consul.finish_release Consul.Released true = Consul.Released);
  Alcotest.(check bool) "missing destroy ack yields unknown" true
    (Consul.finish_release Consul.Released false = Consul.Unknown)

let test_namespace_metadata () =
  let compatible = "{\"protocol_major\":1,\"identity_scheme\":\"sha256-framed-utf8-v1\",\"profile\":\"consul-kv-session-v1\",\"coordination_epoch\":\"epoch-a\",\"fixed_lease_ms\":null}" in
  Alcotest.(check bool) "exact profile metadata accepted" true (Consul.exact_metadata "epoch-a" compatible);
  Alcotest.(check bool) "wrong epoch rejected" false (Consul.exact_metadata "epoch-b" compatible);
  Alcotest.(check bool) "extra metadata rejected" false
    (Consul.exact_metadata "epoch-a" (String.sub compatible 0 (String.length compatible - 1) ^ ",\"unexpected\":true}"))

let () = Alcotest.run "dilock-consul-contract" [
  "options", [
    Alcotest.test_case "defaults" `Quick test_defaults;
    Alcotest.test_case "reject invalid options before I/O" `Quick test_validation;
    Alcotest.test_case "Consul TTL rounding" `Quick test_ttl_rounding;
    Alcotest.test_case "release requires session destroy acknowledgement" `Quick test_release_requires_destroy_ack;
    Alcotest.test_case "exact namespace metadata" `Quick test_namespace_metadata;
  ]
]
