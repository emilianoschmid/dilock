# Use native etcd Lock and one lease per acquisition

Status: accepted as blueprint decision; no runtime evidence implied.

## Decision

Avoid conflicting client-side recipes and accidental native reentrancy from shared lease IDs.

## Detailed contract

See `spec/backends/ETCD.md` from repository root. Any correction must record compatibility impact, alternatives and new tests.
