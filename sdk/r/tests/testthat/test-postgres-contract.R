testthat::test_that("PostgreSQL profile is catalogued", {
  testthat::expect_equal(.dilock_pg_profile, "postgres-advisory-session-v1")
  testthat::expect_equal(.dilock_pg_scheme, "sha256-framed-utf8-v1")
  testthat::expect_match(.dilock_pg_key_sql, "bit\\(64\\)")
  testthat::expect_equal(.dilock_pg_connection_args("host=127.0.0.1 port=5432 dbname=dilock user=postgres password=postgres"),
                         list(host = "127.0.0.1", port = "5432", dbname = "dilock", user = "postgres", password = "postgres"))
  testthat::expect_equal(.dilock_pg_connection_args("postgresql://postgres:postgres@127.0.0.1:5432/dilock"),
                         list(user = "postgres", password = "postgres", host = "127.0.0.1", port = "5432", dbname = "dilock"))
})
