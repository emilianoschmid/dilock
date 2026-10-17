# frozen_string_literal: true

require 'minitest/autorun'
require_relative '../lib/dilock/version'
require_relative '../lib/dilock/core'
require_relative '../lib/dilock/consul'

class ConsulContractTest < Minitest::Test
  def test_consul_rejects_short_native_session_ttl_before_network_io
    short = Dilock::AcquireOptions.new(wait_ms: 1_000, lease_ms: 5_000)
    error = assert_raises(ArgumentError) do
      Dilock::ConsulProvider.connect('http://127.0.0.1:8500', epoch: 'epoch-a',
                                     default_acquisition: short)
    end
    assert_match(/Consul session TTL/, error.message)
  end

  def test_consul_rejects_endpoint_path_and_credentials
    ['http://user:secret@127.0.0.1:8500', 'http://127.0.0.1:8500/prefix'].each do |endpoint|
      assert_raises(ArgumentError) do
        Dilock::ConsulProvider.connect(endpoint, epoch: 'epoch-a')
      end
    end
  end

  def test_consul_fence_exposes_epoch_and_lock_index
    fence = Dilock::ConsulFence.new(epoch: 'epoch-a', lock_index: 19)
    assert_equal 'epoch-a', fence.epoch
    assert_equal 19, fence.lock_index
  end

  def test_consul_metadata_rejects_unknown_profile_fields
    raw = JSON.generate(
      protocol_major: 1,
      identity_scheme: 'sha256-framed-utf8-v1',
      profile: 'consul-kv-session-v1',
      coordination_epoch: 'epoch-a',
      fixed_lease_ms: nil,
      future_field: 'unsupported'
    )
    assert_raises(Dilock::ProtocolMismatch) do
      Dilock::NamespaceMetadata.validate!(raw, 'consul-kv-session-v1', 'epoch-a')
    end
  end
end
