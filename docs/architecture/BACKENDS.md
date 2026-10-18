# Backend profiles and support policy

This table is a **target design**, not a report of implemented support.

| Backend profile | Initial status | Lease model | Fencing | Key operational boundary |
|---|---|---|---|---|
| Redis single authority | planned for v0.1 | owner-checked TTL + renewal | not provided as a strong guarantee | Restart/failover/eviction can invalidate assumptions. |
| etcd native Lock | planned for v0.1 | distinct server lease per acquisition + native Lock RPC | creation revision, within a managed cluster epoch | Quorum required; pause and external-resource fencing still matter. |
| NATS JetStream KV | experimental follow-up | CAS and a validated fixed bucket-TTL profile | not advertised initially | TTL refresh, stale reads, retries, markers and recovery must be tested. |
| PostgreSQL advisory session | experimental implementation | one private database session per acquisition + `pg_try_advisory_lock(bigint)` | none; `require_fencing` is rejected | Session loss releases the advisory lock; pooling/session lifetime is not a TTL, and the lock key is a truncated identity digest. |
| Consul HTTP/KV session | draft-1 profile in `spec/backends/CONSUL.md`; implementation and local lifecycle evidence across all 33 active routes are tracked in `docs/plans/CONSUL-ALL-SDKS.md` | TTL session + KV acquire/release | per-key `LockIndex`, conditional on retaining the lock entry | Single-node local validation does not establish cross-language interoperability, quorum behavior or failover; see the profile. |
| Kafka | out of scope | no selected general-purpose locking protocol | not applicable | A consumer group is not a named-resource mutex API. |

All participants must use the same coordinator, database/bucket, namespace, protocol and compatible configuration. Matching resource names on different backends do not interact. Changing backend or namespace is a coordinated migration, never a local fallback.

PostgreSQL support uses a session advisory lock, not a transaction advisory
lock: a transaction pool may move statements between sessions and therefore
cannot own a Dilock acquisition. Each logical acquisition opens and owns one
private session, polls `pg_try_advisory_lock`, releases with
`pg_advisory_unlock`, and closes the session. Connection loss is the backend's
cleanup boundary; it is not a fencing mechanism and does not make stale work
safe. The first 64 identity bits are the portable lock key, so the profile
documents the residual digest-collision assumption explicitly.

Capabilities include lease duration granularity, auto-renewal, loss detection, cancellation of waiting, fencing, fairness claims and supported failure profile. Unsupported requirements fail at provider validation or acquisition, before entering user work.

Do not use “safe” as an undifferentiated backend ranking. Document what is linearizable under which assumptions, what requires resource-side enforcement, and what stops making progress when quorum is unavailable. Backend authentication protects access; the lock library is not a security boundary between malicious tenants sharing credentials.

Support requires evidence per SDK/server/client version. A backend README must state minimum and tested versions, topology, durability/eviction settings, time assumptions, ACLs, restoration procedure and known unsupported modes. A green unit suite does not upgrade `experimental` to `supported`.
