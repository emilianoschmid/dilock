# Backend profiles and support policy

This table is a **target design**, not a report of implemented support.

| Backend profile | Initial status | Lease model | Fencing | Key operational boundary |
|---|---|---|---|---|
| Redis single authority | planned for v0.1 | owner-checked TTL + renewal | not provided as a strong guarantee | Restart/failover/eviction can invalidate assumptions. |
| etcd native Lock | planned for v0.1 | distinct server lease per acquisition + native Lock RPC | creation revision, within a managed cluster epoch | Quorum required; pause and external-resource fencing still matter. |
| NATS JetStream KV | experimental follow-up | CAS and a validated fixed bucket-TTL profile | not advertised initially | TTL refresh, stale reads, retries, markers and recovery must be tested. |
| PostgreSQL | roadmap | session/transaction advisory locks | no generic fence | Pooling/session lifetime is not a Redis TTL. |
| Consul | roadmap | session + KV acquisition | requires its own design | Session invalidation and lock delay need explicit semantics. |
| Kafka | out of scope | no selected general-purpose locking protocol | not applicable | A consumer group is not a named-resource mutex API. |

All participants must use the same coordinator, database/bucket, namespace, protocol and compatible configuration. Matching resource names on different backends do not interact. Changing backend or namespace is a coordinated migration, never a local fallback.

Capabilities include lease duration granularity, auto-renewal, loss detection, cancellation of waiting, fencing, fairness claims and supported failure profile. Unsupported requirements fail at provider validation or acquisition, before entering user work.

Do not use “safe” as an undifferentiated backend ranking. Document what is linearizable under which assumptions, what requires resource-side enforcement, and what stops making progress when quorum is unavailable. Backend authentication protects access; the lock library is not a security boundary between malicious tenants sharing credentials.

Support requires evidence per SDK/server/client version. A backend README must state minimum and tested versions, topology, durability/eviction settings, time assumptions, ACLs, restoration procedure and known unsupported modes. A green unit suite does not upgrade `experimental` to `supported`.
