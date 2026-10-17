defmodule Dilock.Identity do
  @moduledoc false
  @domain "dilock.lock.identity\0"
  @namespace ~r/\A[a-z0-9][a-z0-9_.-]{0,63}\z/

  @spec resource_id(binary(), binary()) ::
          {:ok, binary()} | {:error, :invalid_namespace | :invalid_resource}
  def resource_id(namespace, resource) when is_binary(namespace) and is_binary(resource) do
    cond do
      not Regex.match?(@namespace, namespace) ->
        {:error, :invalid_namespace}

      not String.valid?(resource) ->
        {:error, :invalid_resource}

      byte_size(resource) not in 1..1024 ->
        {:error, :invalid_resource}

      :binary.match(resource, <<0>>) != :nomatch ->
        {:error, :invalid_resource}

      true ->
        ns = byte_size(namespace)
        size = byte_size(resource)

        frame =
          <<@domain::binary, ns::unsigned-big-32, namespace::binary, size::unsigned-big-32,
            resource::binary>>

        {:ok, Base.encode16(:crypto.hash(:sha256, frame), case: :lower)}
    end
  end

  def resource_id(_, _), do: {:error, :invalid_resource}

  def redis_key(id), do: "dilock:{#{id}}:lease"
  def etcd_lock_name(id), do: "/dilock/locks/#{id}"

  def metadata(_namespace, profile, epoch) do
    Jason.encode!(%{
      protocol_major: 1,
      identity_scheme: "sha256-framed-utf8-v1",
      profile: profile,
      coordination_epoch: epoch,
      fixed_lease_ms: nil
    })
  end

  def validate_metadata(raw, profile, epoch) when is_binary(raw) do
    with {:ok, value} <- Jason.decode(raw),
         true <- is_map(value),
         true <- map_size(value) == 5,
         true <- value["protocol_major"] === 1,
         true <- value["identity_scheme"] === "sha256-framed-utf8-v1",
         true <- value["profile"] === profile,
         true <- is_binary(epoch) and epoch != "" and value["coordination_epoch"] === epoch,
         true <- Map.has_key?(value, "fixed_lease_ms") and is_nil(value["fixed_lease_ms"]) do
      :ok
    else
      _ -> {:error, :protocol_mismatch}
    end
  end

  def validate_metadata(_, _, _), do: {:error, :protocol_mismatch}
end
