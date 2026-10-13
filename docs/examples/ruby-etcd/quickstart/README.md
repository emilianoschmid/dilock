# Ruby + etcd quickstart

This consumer uses Dilock's native Ruby etcd adapter and native v3 Lock/Lease RPCs. It runs two sequential scopes on one provider/channel: defaults first, then an explicit 3-second wait / 20-second lease. Each prints the epoch-scoped creation-revision fence. This SDK remains an experimental candidate; the protected resource must enforce the fence atomically.

Prerequisites: Ruby 3.2+, Bundler dependencies, a reachable etcd v3 endpoint, and metadata provisioned at `/dilock/namespaces/<namespace>/meta` for the same epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The application does not create metadata.

From the repository root:

```sh
cd sdk/ruby
bundle install
bundle exec ruby ../../docs/examples/ruby-etcd/quickstart/main.rb
```

Set `DILOCK_ETCD_ENDPOINT`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` to match the cluster. The sample prints a fence; production data writes must compare it at the resource.
