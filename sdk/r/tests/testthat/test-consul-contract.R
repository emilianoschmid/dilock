test_that("Consul metadata profile and epoch are checked exactly", {
  good <- paste0('{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",',
                 '"profile":"consul-kv-session-v1","coordination_epoch":"epoch-a",',
                 '"fixed_lease_ms":null}')
  expect_invisible(.dilock_consul_validate_meta(good, "epoch-a"))
  expect_error(.dilock_consul_validate_meta(good, "epoch-b"), class = "dilock_protocol_mismatch")
  expect_error(.dilock_consul_validate_meta(sub('"protocol_major":1', '"protocol_major":true', good, fixed = TRUE), "epoch-a"),
               class = "dilock_protocol_mismatch")
  expect_error(.dilock_consul_validate_meta(sub('"fixed_lease_ms":null}', '"fixed_lease_ms":null,"future_field":true}', good, fixed = TRUE), "epoch-a"),
               class = "dilock_protocol_mismatch")
})

test_that("Consul fencing indices preserve exact unsigned decimal text", {
  expect_identical(.dilock_consul_lock_index('[{"LockIndex":18446744073709551615}]'),
                   "18446744073709551615")
  expect_error(.dilock_consul_lock_index('[{"LockIndex":1,"LockIndex":2}]'),
               class = "dilock_protocol_mismatch")
  expect_error(.dilock_consul_lock_index('[{"LockIndex":-1}]'),
               class = "dilock_protocol_mismatch")
})

test_that("Consul provider input rejects invalid TTL and token", {
  expect_error(dilock_consul_provider(epoch = "e", default_lease_ms = 9999L),
               class = "dilock_invalid_argument")
  expect_error(dilock_consul_provider(epoch = "e", token = "line\nbreak"),
               class = "dilock_invalid_argument")
})

test_that("unconfirmed Consul session destruction keeps release unknown", {
  expect_identical(.dilock_consul_finish_release("released", FALSE), "unknown")
  expect_identical(.dilock_consul_finish_release("already_not_owned", TRUE), "already_not_owned")
})
