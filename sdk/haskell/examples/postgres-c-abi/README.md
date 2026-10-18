# Haskell PostgreSQL C ABI quickstart

Build with `-f postgres-c-abi` against a Dilock C ABI compiled with the
PostgreSQL backend. Provisioning is explicit and should target a disposable
database. The profile uses one private PostgreSQL session per acquisition and
provides no TTL or fencing token; it remains experimental.
