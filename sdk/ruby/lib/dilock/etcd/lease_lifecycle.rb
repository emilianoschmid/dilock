# frozen_string_literal: true

# etcd lease renewal and unlock behavior for the Dilock SDK.
module Dilock
  # etcd v3 lease lifecycle operations mixed into EtcdProvider.
  class EtcdProvider
    # Performs ownership-confirmed release with a single cleanup deadline.
    module ReleaseLifecycle
      private

      def release_with_budget(timeout_ms, lease:, now:, operations:)
        deadline = now.call + timeout_ms
        ownership = observe_release_ownership(deadline, now, operations.fetch(:range), lease)
        unlock_result = if ownership == :owned
                          invoke_cleanup_with_remaining(deadline, now, operations.fetch(:unlock), 'unlock')
                        else
                          :unknown
                        end
        revoke_result = invoke_cleanup_with_remaining(deadline, now, operations.fetch(:revoke), 'lease-revoke')

        classify_release_outcome(ownership, unlock_result, revoke_result)
      end

      def observe_release_ownership(deadline, now, range, lease)
        remaining_ms = [deadline - now.call, 0].max
        return :unknown unless remaining_ms.positive?

        range_response_ownership(range.call(remaining_ms), lease)
      rescue StandardError => e
        observe_cleanup('ownership-range', e.class.name)
        :unknown
      end

      def range_response_ownership(response, lease)
        entries = response.kvs
        return :unknown unless entries.respond_to?(:length)
        return :absent if entries.empty?
        return :unknown unless entries.length == 1 && entries.first.key == lease.__key

        entries.first.lease.to_s == lease.__owner.to_s ? :owned : :mismatch
      end

      def classify_release_outcome(ownership, unlock_result, revoke_result)
        return :already_not_owned if %i[absent mismatch].include?(ownership) || revoke_result == :not_found
        return :released if ownership == :owned && unlock_result == :acknowledged && revoke_result == :acknowledged
        return :already_not_owned if revoke_result == :acknowledged

        observe_cleanup('release-unknown', 'unlock-and-lease-revoke-unconfirmed')
        :unknown
      end

      def release_range(lease, remaining_ms)
        @kv.range(Etcdserverpb::RangeRequest.new(key: lease.__key), deadline: rpc_deadline(remaining_ms))
      end

      def release_unlock(lease, remaining_ms)
        @lock.unlock(V3lockpb::UnlockRequest.new(key: lease.__key), deadline: rpc_deadline(remaining_ms))
      end

      def release_revoke(lease, remaining_ms)
        @lease.lease_revoke(Etcdserverpb::LeaseRevokeRequest.new(ID: lease.__owner),
                            deadline: rpc_deadline(remaining_ms))
      end

      def invoke_cleanup_with_remaining(deadline, now, operation, label)
        remaining_ms = [deadline - now.call, 0].max
        return :unknown unless remaining_ms.positive?

        result = operation.call(remaining_ms)
        result ? :acknowledged : :unknown
      rescue GRPC::BadStatus => e
        observe_cleanup(label, e.class.name)
        cleanup_status_outcome(label, e)
      rescue StandardError => e
        observe_cleanup(label, e.class.name)
        :unknown
      end

      def cleanup_status_outcome(label, error)
        return :not_found if label == 'lease-revoke' && error.code == GRPC::Core::StatusCodes::NOT_FOUND

        :unknown
      end
    end

    # Applies renewals and confirmed releases to an etcd-backed lease.
    # @private Backend lifecycle adapter called by {Lease}.
    module LeaseLifecycle
      include ReleaseLifecycle

      def __renew(lease, lease_ms)
        validate_renewal!(lease, lease_ms)
        previous_deadline = lease.__deadline
        sent_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        response = keepalive_once(lease.__owner)
        effective_ms = confirmed_keepalive_ttl_ms(response)
        return false if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= previous_deadline

        apply_lease_deadline(lease, effective_ms, sent_at)
        true
      rescue GRPC::BadStatus => e
        raise BackendUnavailable.new("etcd lease keepalive failed (#{e.class})", category: :backend_unavailable)
      end

      def __release(lease)
        release_with_budget(
          @cleanup_timeout_ms,
          lease:,
          now: -> { (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1_000).floor },
          operations: {
            range: ->(remaining_ms) { release_range(lease, remaining_ms) },
            unlock: ->(remaining_ms) { release_unlock(lease, remaining_ms) },
            revoke: ->(remaining_ms) { release_revoke(lease, remaining_ms) }
          }
        )
      end

      private

      def validate_renewal!(lease, lease_ms)
        unless lease_ms.is_a?(Integer) && (1..86_400_000).cover?(lease_ms)
          raise ArgumentError, 'lease_ms must be 1 through 86400000'
        end
        return if lease_ms == lease.effective_lease_ms

        raise UnsupportedCapability.new('etcd keepalive preserves the server-granted TTL; reacquire to change it',
                                        category: :unsupported_capability)
      end

      def apply_lease_deadline(lease, effective_ms, sent_at)
        elapsed_ms = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - sent_at) * 1_000).ceil
        remaining = effective_ms - Internal.safety_margin_ms(effective_ms) - elapsed_ms
        unless remaining.positive?
          raise LeaseLost.new('etcd keepalive response arrived outside its validity budget',
                              category: :lease_lost)
        end

        lease.__set_effective_lease_ms(effective_ms)
        deadline = sent_at + ((effective_ms - Internal.safety_margin_ms(effective_ms)) / 1_000.0)
        lease.__set_deadline(deadline)
      end
    end

    include LeaseLifecycle

    private_constant :LeaseLifecycle, :ReleaseLifecycle
  end
end
