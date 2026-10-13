# frozen_string_literal: true

require_relative 'lib/dilock/version'

Gem::Specification.new do |spec|
  spec.name = 'dilock'
  spec.version = Dilock::VERSION
  spec.summary = 'Distributed leases for Redis and etcd'
  spec.description = 'A small Ruby library implementing the Dilock draft 1 Redis and etcd lease profiles.'
  spec.authors = ['Dilock contributors']
  spec.license = 'Apache-2.0'
  spec.required_ruby_version = '>= 3.2'
  spec.files = %w[
    README.md
    LICENSE
    bin/dilock-worker
    lib/dilock.rb
    lib/dilock/version.rb
    lib/dilock/core.rb
    lib/dilock/core/namespace_metadata.rb
    lib/dilock/redis.rb
    lib/dilock/redis/acquisition.rb
    lib/dilock/redis/connection_setup.rb
    lib/dilock/conformance_worker.rb
    lib/dilock/redis/redis-release.lua
    lib/dilock/redis/redis-renew.lua
    lib/dilock/etcd.rb
    lib/dilock/etcd/acquisition.rb
    lib/dilock/etcd/connection_setup.rb
    lib/dilock/etcd/keepalive.rb
    lib/dilock/etcd/lease_lifecycle.rb
    lib/dilock/etcd/proto/kv_pb.rb
    lib/dilock/etcd/proto/rpc_pb.rb
    lib/dilock/etcd/proto/v3lock_pb.rb
    lib/dilock/etcd/proto/services.rb
    lib/dilock/etcd/proto/kv.proto
    lib/dilock/etcd/proto/rpc.proto
    lib/dilock/etcd/proto/v3lock.proto
    lib/dilock/etcd/proto/UPSTREAM.md
    lib/dilock/etcd/proto/LICENSE-etcd-api
    lib/dilock/etcd/proto/LICENSE-etcd-server
  ]
  spec.require_paths = ['lib']
  spec.bindir = 'bin'
  spec.executables = ['dilock-worker']

  spec.add_dependency 'google-protobuf', '~> 4.30'
  spec.add_dependency 'grpc', '~> 1.75'
  spec.add_dependency 'redis', '~> 6.0'
  spec.metadata['rubygems_mfa_required'] = 'true'
end
