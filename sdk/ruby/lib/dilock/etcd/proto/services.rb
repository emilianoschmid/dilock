# frozen_string_literal: true

# Service declarations for the generated etcd v3.7.2 wire messages. The message
# schema and upstream notices are bundled beside this file.
require "grpc"
require_relative "kv_pb"
require_relative "rpc_pb"
require_relative "v3lock_pb"

module Etcdserverpb
  class KV
    include GRPC::GenericService
    self.marshal_class_method = :encode
    self.unmarshal_class_method = :decode
    self.service_name = "etcdserverpb.KV"
    rpc :Range, RangeRequest, RangeResponse
  end

  class Lease
    include GRPC::GenericService
    self.marshal_class_method = :encode
    self.unmarshal_class_method = :decode
    self.service_name = "etcdserverpb.Lease"
    rpc :LeaseGrant, LeaseGrantRequest, LeaseGrantResponse
    rpc :LeaseRevoke, LeaseRevokeRequest, LeaseRevokeResponse
    rpc :LeaseKeepAlive, stream(LeaseKeepAliveRequest), stream(LeaseKeepAliveResponse)
  end
end

module V3lockpb
  class Lock
    include GRPC::GenericService
    self.marshal_class_method = :encode
    self.unmarshal_class_method = :decode
    self.service_name = "v3lockpb.Lock"
    rpc :Lock, LockRequest, LockResponse
    rpc :Unlock, UnlockRequest, UnlockResponse
  end
end
