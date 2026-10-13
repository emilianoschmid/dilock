# Backend capabilities are explicit

Status: accepted as blueprint decision; no runtime evidence implied.

## Decision

Redis/etcd/NATS are not interchangeable failure models; no automatic backend fallback.

## Detailed contract

See `docs/architecture/BACKENDS.md` from repository root. Any correction must record compatibility impact, alternatives and new tests.
