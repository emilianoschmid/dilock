defmodule Mvccpb.KeyValue do
  @moduledoc false

  use Protobuf, full_name: "mvccpb.KeyValue", protoc_gen_elixir_version: "0.17.0", syntax: :proto3

  field(:key, 1, type: :bytes)
  field(:create_revision, 2, type: :int64, json_name: "createRevision")
  field(:mod_revision, 3, type: :int64, json_name: "modRevision")
  field(:version, 4, type: :int64)
  field(:value, 5, type: :bytes)
  field(:lease, 6, type: :int64)
end
