defmodule V3lockpb.LockRequest do
  @moduledoc false

  use Protobuf,
    full_name: "v3lockpb.LockRequest",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:name, 1, type: :bytes)
  field(:lease, 2, type: :int64)
end

defmodule V3lockpb.LockResponse do
  @moduledoc false

  use Protobuf,
    full_name: "v3lockpb.LockResponse",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:header, 1, type: Etcdserverpb.ResponseHeader)
  field(:key, 2, type: :bytes)
end

defmodule V3lockpb.UnlockRequest do
  @moduledoc false

  use Protobuf,
    full_name: "v3lockpb.UnlockRequest",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:key, 1, type: :bytes)
end

defmodule V3lockpb.UnlockResponse do
  @moduledoc false

  use Protobuf,
    full_name: "v3lockpb.UnlockResponse",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field(:header, 1, type: Etcdserverpb.ResponseHeader)
end

defmodule V3lockpb.Lock.Service do
  @moduledoc false

  use GRPC.Service, name: "v3lockpb.Lock", protoc_gen_elixir_version: "0.17.0"

  rpc(:Lock, V3lockpb.LockRequest, V3lockpb.LockResponse)

  rpc(:Unlock, V3lockpb.UnlockRequest, V3lockpb.UnlockResponse)
end

defmodule V3lockpb.Lock.Stub do
  @moduledoc false

  use GRPC.Stub, service: V3lockpb.Lock.Service
end
