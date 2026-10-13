defmodule Etcdserverpb.RangeRequest.SortOrder do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "etcdserverpb.RangeRequest.SortOrder",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:NONE, 0)
  field(:ASCEND, 1)
  field(:DESCEND, 2)
end

defmodule Etcdserverpb.RangeRequest.SortTarget do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "etcdserverpb.RangeRequest.SortTarget",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:KEY, 0)
  field(:VERSION, 1)
  field(:CREATE, 2)
  field(:MOD, 3)
  field(:VALUE, 4)
end

defmodule Etcdserverpb.ResponseHeader do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.ResponseHeader",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:cluster_id, 1, type: :uint64, json_name: "clusterId")
  field(:member_id, 2, type: :uint64, json_name: "memberId")
  field(:revision, 3, type: :int64)
  field(:raft_term, 4, type: :uint64, json_name: "raftTerm")
end

defmodule Etcdserverpb.RangeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.RangeRequest",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:key, 1, type: :bytes)
  field(:range_end, 2, type: :bytes, json_name: "rangeEnd")
  field(:limit, 3, type: :int64)
  field(:revision, 4, type: :int64)

  field(:sort_order, 5,
    type: Etcdserverpb.RangeRequest.SortOrder,
    json_name: "sortOrder",
    enum: true
  )

  field(:sort_target, 6,
    type: Etcdserverpb.RangeRequest.SortTarget,
    json_name: "sortTarget",
    enum: true
  )

  field(:serializable, 7, type: :bool)
  field(:keys_only, 8, type: :bool, json_name: "keysOnly")
  field(:count_only, 9, type: :bool, json_name: "countOnly")
  field(:min_mod_revision, 10, type: :int64, json_name: "minModRevision")
  field(:max_mod_revision, 11, type: :int64, json_name: "maxModRevision")
  field(:min_create_revision, 12, type: :int64, json_name: "minCreateRevision")
  field(:max_create_revision, 13, type: :int64, json_name: "maxCreateRevision")
end

defmodule Etcdserverpb.RangeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.RangeResponse",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:header, 1, type: Etcdserverpb.ResponseHeader)
  field(:kvs, 2, repeated: true, type: Mvccpb.KeyValue)
  field(:more, 3, type: :bool)
  field(:count, 4, type: :int64)
end

defmodule Etcdserverpb.LeaseGrantRequest do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.LeaseGrantRequest",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:TTL, 1, type: :int64)
  field(:ID, 2, type: :int64)
end

defmodule Etcdserverpb.LeaseGrantResponse do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.LeaseGrantResponse",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:header, 1, type: Etcdserverpb.ResponseHeader)
  field(:ID, 2, type: :int64)
  field(:TTL, 3, type: :int64)
  field(:error, 4, type: :string)
end

defmodule Etcdserverpb.LeaseRevokeRequest do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.LeaseRevokeRequest",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:ID, 1, type: :int64)
end

defmodule Etcdserverpb.LeaseRevokeResponse do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.LeaseRevokeResponse",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:header, 1, type: Etcdserverpb.ResponseHeader)
end

defmodule Etcdserverpb.LeaseKeepAliveRequest do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.LeaseKeepAliveRequest",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:ID, 1, type: :int64)
end

defmodule Etcdserverpb.LeaseKeepAliveResponse do
  @moduledoc false

  use Protobuf,
    full_name: "etcdserverpb.LeaseKeepAliveResponse",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:header, 1, type: Etcdserverpb.ResponseHeader)
  field(:ID, 2, type: :int64)
  field(:TTL, 3, type: :int64)
end

defmodule Etcdserverpb.KV.Service do
  @moduledoc false

  use GRPC.Service, name: "etcdserverpb.KV", protoc_gen_elixir_version: "0.17.0"

  rpc(:Range, Etcdserverpb.RangeRequest, Etcdserverpb.RangeResponse)
end

defmodule Etcdserverpb.KV.Stub do
  @moduledoc false

  use GRPC.Stub, service: Etcdserverpb.KV.Service
end

defmodule Etcdserverpb.Lease.Service do
  @moduledoc false

  use GRPC.Service, name: "etcdserverpb.Lease", protoc_gen_elixir_version: "0.17.0"

  rpc(:LeaseGrant, Etcdserverpb.LeaseGrantRequest, Etcdserverpb.LeaseGrantResponse)

  rpc(:LeaseRevoke, Etcdserverpb.LeaseRevokeRequest, Etcdserverpb.LeaseRevokeResponse)

  rpc(
    :LeaseKeepAlive,
    stream(Etcdserverpb.LeaseKeepAliveRequest),
    stream(Etcdserverpb.LeaseKeepAliveResponse)
  )
end

defmodule Etcdserverpb.Lease.Stub do
  @moduledoc false

  use GRPC.Stub, service: Etcdserverpb.Lease.Service
end
