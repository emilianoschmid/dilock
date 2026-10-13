# Conformance worker protocol — draft 1

This protocol is a test interface only, not an Dilock server or production wire protocol.

Start each worker with a config path containing backend endpoint references, namespace, profile and timeouts. Secrets are passed via environment/file mechanisms appropriate to CI, not emitted in replies. The orchestrator uses an argv array, never a shell-interpolated command string.

One JSON object per UTF-8 line. Request envelope: `{ "id": "r1", "op": "try_acquire", ... }`. Response envelope: `{ "id": "r1", "status": "acquired", "lease_id": "local-opaque-id", ... }`. Unsolicited loss events use `{ "event": "lease_lost", "lease_id": "...", "reason": "..." }` with no request ID. Duration fields are integer milliseconds; 64-bit fence values/revisions are decimal strings.

Operations: `hello`, `acquire`, `try_acquire`, `renew`, `release`, `inspect`, `cancel_request`, `shutdown`. `hello` reports SDK version, protocol, server/client version and capabilities. `acquire` specifies resource/wait_ms/lease_ms/auto_renew. `renew` takes a local `lease_id` and a requested `lease_ms`; adapters without explicit manual renewal report `unsupported_capability`. `release` takes the local opaque lease_id. `inspect` returns a **local state observation**, not authoritative remote ownership. `cancel_request` targets a pending request without blocking the reader. At most one response per request.

Capability maps may report `auto_renew_mandatory: true` when the selected backend/API always renews active ownership and cannot disable keepalive. Pair tests must then verify ownership survives the nominal TTL and test cleanup/handoff, rather than expecting TTL expiry while the owner is alive. Omitted `auto_renew_mandatory` means renewal may be disabled per acquisition.

Acquisition statuses: `acquired`, `busy` (try only), `timeout`, `cancelled`, `acquisition_unknown`, `backend_unavailable`, `invalid_argument`, `unsupported_capability`, `protocol_mismatch`, `internal_error`. Cancellation response includes `remote_outcome_unknown: true/false`; it must not lose this distinction. Acquired replies include effective_lease_ms, optional fence object `{epoch, value}` and diagnostic lease_id, never the ownership secret.

Release statuses: `released`, `already_not_owned`, `release_unknown`. Repeat release is idempotent and must not become an unknown new acquisition. Unknown lease_id is an argument error. Shutdown is bounded and reports outstanding cleanup attempts.

After an etcd lease expires, a stale `release` may report `release_unknown` if
the client cannot confirm the Unlock/Revoke outcome. Conformance must preserve
that uncertainty and separately verify that the successor remains held; it
must not reinterpret the result as `already_not_owned`.

The reader must continue accepting commands while acquisitions wait. Protect the handle map; never serialize the whole worker behind one awaited acquire. Logs belong on stderr. Line size, malformed JSON, duplicate request IDs, unknown operations and EOF all have bounded/documented behavior. Enforce an input size limit, reject duplicate active IDs and avoid echoing secrets from malformed input.

Pause/kill/network faults are injected outside the production SDK by the harness. Avoid public “disable safety” hooks. If a test-only clock/backend seam is needed, keep it internal and prove the packaged runtime cannot enable an unsafe mode through ordinary config.
