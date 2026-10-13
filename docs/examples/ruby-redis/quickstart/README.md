# Ruby + Redis quickstart

This consumer uses the native Dilock Ruby Redis adapter. It acquires two resources sequentially through the same provider, first with the 5-second wait / 30-second lease defaults and then with an explicit 3-second wait / 20-second lease. The handles share its owned Redis clients. The Redis client sends bundled owner-checked Lua scripts through Redis `EVAL`; there is no server-side script installation step. The SDK is an experimental candidate, and Redis failover does not provide strong fencing.

Prerequisites: Ruby 3.2+, Bundler dependencies from `sdk/ruby/Gemfile`, a reachable Redis server, and Redis metadata pre-provisioned for the selected namespace and epoch. See [`spec/NAMESPACE-METADATA.md`](../../../../spec/NAMESPACE-METADATA.md). The copied quickstart does not call the development provisioning helper.

From the repository root:

```sh
cd sdk/ruby
bundle install
bundle exec ruby ../../docs/examples/ruby-redis/quickstart/main.rb
```

Set `DILOCK_REDIS_URL`, `DILOCK_NAMESPACE`, and `DILOCK_EPOCH` to match the existing metadata. Do not use development provisioning against a shared or production namespace.
