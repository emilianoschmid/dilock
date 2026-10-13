# Integration and fault environments to implement

Create version-pinned disposable Compose profiles for Redis, one-node etcd, three-node etcd and later three-node NATS JetStream. Pin actual image digests verified in M0; this snapshot intentionally does not invent future version tags/digests. Add Toxiproxy or a comparable isolated fault mechanism after validating its maintenance and license.

Bind user-facing ports to loopback and give each run independent names/volumes. Provide health/readiness checks, bounded startup/teardown and logs on failure. One-node etcd/NATS validates functionality only, not quorum/failover guarantees. The CI failure jobs use the real clustered profiles.

Native SDK development must still allow an explicitly configured local test server without requiring all Compose services. Reject destructive mode unless endpoints belong to the disposable environment. Do not flush shared databases, change global firewall rules or remove unrelated volumes.
