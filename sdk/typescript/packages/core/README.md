# `@dilock/core`

Backend-independent identity helpers and error classes for the Dilock TypeScript SDK. This package has no Redis or gRPC runtime dependency. Install a backend package separately to use distributed locks.

The identity helpers implement the shared draft 1 wire contract. The errors let applications classify acquisition timeout, unknown outcome, backend failure, protocol mismatch and unsupported capabilities without loading any backend client.

Package name and scope are local candidates. The artifact remains private pending ownership verification.
