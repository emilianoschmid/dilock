# Perl + Redis quickstart

This consumer uses Dilock's native Perl Redis adapter synchronously with Redis::Fast. It acquires two handles sequentially through the same provider, first with defaults and then with an explicit 3-second wait / 20-second lease. It scopes cleanup with `with_lease`; the Redis profile has no strong fencing guarantee. The SDK remains experimental.

Prerequisites: Perl 5.20+, Dilock's Perl source under `sdk/perl/lib`, `Redis::Fast` 0.39+, `Crypt::URandom` 0.55+, a reachable Redis server, and metadata pre-provisioned for the configured namespace and epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The SDK validates metadata but does not create it.

Install the optional Redis dependencies, then run from the repository root (POSIX shell):

```sh
cpanm Redis::Fast Crypt::URandom
PERL5LIB=sdk/perl/lib perl docs/examples/perl-redis/quickstart/main.pl
```

The source defaults to namespace `default` and epoch `local-redis-epoch`; make them match the pre-provisioned record before running. `with_lease` awaits owner-checked release.
