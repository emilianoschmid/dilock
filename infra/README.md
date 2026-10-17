# Integration and fault environments to implement

Create version-pinned disposable Compose profiles for Redis, one-node etcd, three-node etcd and later three-node NATS JetStream. Pin actual image digests verified in M0; this snapshot intentionally does not invent future version tags/digests. Add Toxiproxy or a comparable isolated fault mechanism after validating its maintenance and license.

Bind user-facing ports to loopback and give each run independent names/volumes. Provide health/readiness checks, bounded startup/teardown and logs on failure. One-node etcd/NATS validates functionality only, not quorum/failover guarantees. The CI failure jobs use the real clustered profiles.

Native SDK development must still allow an explicitly configured local test server without requiring all Compose services. Reject destructive mode unless endpoints belong to the disposable environment. Do not flush shared databases, change global firewall rules or remove unrelated volumes.

## Consul draft profile

`consul.compose.yml` runs one in-memory Consul development agent on
`127.0.0.1:28500`. The official HashiCorp image is pinned by its observed
`1.22.5` manifest digest. This is a functional test service only: it has no
persistent volume, ACL bootstrap, quorum, failover, or restore coverage.
Start/stop it with `docker compose -f infra/consul.compose.yml up -d` and
`docker compose -f infra/consul.compose.yml down`. The service uses no host
network mode and publishes only its HTTP API on loopback.
