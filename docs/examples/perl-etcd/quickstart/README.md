# Perl + etcd quickstart

This consumer uses Dilock's native Perl etcd adapter, `EV::Etcd`, and the etcd v3 asynchronous Lock/Lease APIs. It sequentially locks two resources through one provider/event loop, using 5-second wait / 30-second lease settings and then 3-second wait / 20-second lease settings. Each prints the epoch/create-revision fence and releases from the event loop. The adapter is experimental and does not automatically renew held leases; the protected resource must enforce fencing.

Prerequisites: Perl 5.20+, `EV::Etcd` 0.10+ (including EV), Dilock's source under `sdk/perl/lib`, a reachable etcd v3 endpoint, and metadata provisioned at `/dilock/namespaces/default/meta` for the configured epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). Metadata is read-only to the SDK.

Install the optional etcd dependencies, then run from the repository root (POSIX shell):

```sh
cpanm EV::Etcd
PERL5LIB=sdk/perl/lib perl docs/examples/perl-etcd/quickstart/main.pl
```

Set endpoint, namespace, and epoch in the environment/source to match the provisioned record. Keep the event loop running until the callback releases the lease.
