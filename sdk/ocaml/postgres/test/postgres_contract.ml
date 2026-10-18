let () =
  let options = Postgres.defaults "epoch" in
  assert (options.default_acquisition.wait_ms = 5_000);
  assert (options.default_acquisition.lease_ms = 30_000);
  print_endline "PASS: OCaml PostgreSQL contract defaults"
