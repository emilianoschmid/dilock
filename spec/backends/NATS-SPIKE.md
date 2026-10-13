# NATS JetStream KV investigation and promotion gate

Status: **experimental design candidate, not a verified locking protocol**. Core NATS pub/sub or a queue group alone is not the selected coordinator; this uses JetStream KV.

## Candidate to evaluate

Use one explicitly provisioned bucket per coordination domain, a fixed bucket-wide TTL/stream MaxAge, a single retained value per key if supported by the selected profile, and conditional create/update/delete using server revisions. All clients must use the same bucket and effective duration. A client requesting a different duration fails; do not create a second TTL-specific bucket for the same resource, which would split exclusion.

Investigate whether the selected server/client revisions provide the required conditional mutations and refresh the fixed bucket TTL on update. Use only acknowledged CAS operations as mutation authority. Reads and watchers are observations/wake-up hints; stale reads must never justify unconditional acquisition or delete.

A renewal writes the same owner with expected current revision and records the returned revision only after acknowledgement. Release must be conditional on that acquisition's current revision/ownership evidence. Serialize mutations. Unknown response/retry/deduplication handling must be designed and tested; until resolved, fail closed locally and rely on expiry rather than declaring successful renewal.

## TTL distinction

The official KV TTL documentation distinguishes per-key create TTL from bucket limits and warns that ordinary updates do not preserve a create-only per-key TTL. Therefore do not copy a Redis renewal assumption. Delete-then-create is explicitly forbidden as a renewal strategy because it opens a competing-acquisition interval.

Fixed bucket TTL is the candidate, **not an already proven solution**. Verify behavior in the specific server/client versions, including history retention, expiry markers, tombstones, deletion with expected revision, follower reads, stream limits, rejected writes and restart recovery. Record sources and a reproducer for each assumption.

## Hard questions and tests

1. Does every mutation use a server-checked expected revision? How is an absent/deleted key re-created safely?
2. Can stale `get` results or expiring history cause a renewed old acquisition to appear to own a new generation?
3. What happens when an acknowledgement is lost, a publish is retried, or a deduplication window returns a prior acknowledgement?
4. Can stream limits evict the last lock value? Reject any profile that silently makes a held key disappear to make room.
5. Does a renewal reset the intended expiry without losing ownership between operations?
6. Do replica failover, lost quorum and snapshots preserve the advertised behavior? Test a three-replica bucket, not just one local process.
7. Do .NET, Python and C++ clients expose compatible semantics? Pin actual client/server versions.
8. Is any claimed fencing sequence valid across deletions/recreation and the supported restore policy? Default: no public strong fence.

Deliver a report with traces and pass/fail outcomes. A successful basic CAS demonstration is insufficient. Promote only after a reviewed ADR, vectors, interoperability tests and failure tests. Otherwise leave the adapter unshipped/experimental with a concrete defect list. This must not block Redis/etcd work.
