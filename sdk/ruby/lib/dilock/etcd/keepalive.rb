# frozen_string_literal: true

# etcd lease keepalive support for the Dilock SDK.
module Dilock
  # Bounded keepalive worker and lease cleanup RPCs used by EtcdProvider.
  class EtcdProvider
    # Keeps a newly granted lease alive while native Lock waits for a queue slot.
    module KeepaliveOperations
      private

      def start_keepalive(lease_id, initial_ttl, grant_sent_at)
        ttl_ms = initial_ttl * 1_000
        state = { mutex: Mutex.new, deadline: lease_deadline(grant_sent_at, ttl_ms),
                  ttl_ms: ttl_ms, stopped: false, error: nil }
        state[:thread] = Thread.new { keepalive_loop(state, lease_id, initial_ttl) }
        state
      end

      def keepalive_loop(state, lease_id, initial_ttl)
        interval = (initial_ttl / 3.0).clamp(0.2, 10.0)
        loop do
          break if keepalive_stopped?(state)

          sleep interval
          break if keepalive_stopped?(state)

          refresh_keepalive_state(state, lease_id)
        end
      end

      def keepalive_stopped?(state)
        state[:mutex].synchronize { state[:stopped] }
      end

      def refresh_keepalive_state(state, lease_id)
        previous_deadline = state[:mutex].synchronize { state[:deadline] }
        ensure_keepalive_can_dispatch!(previous_deadline)
        sent_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        ttl_ms = confirmed_keepalive_ttl_ms(keepalive_once(lease_id))
        new_deadline = lease_deadline(sent_at, ttl_ms)
        response_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        publish_keepalive_confirmation(state, previous_deadline, new_deadline, ttl_ms, response_at)
      rescue StandardError => e
        state[:mutex].synchronize { state[:error] = e }
      end

      def ensure_keepalive_can_dispatch!(previous_deadline)
        return if Process.clock_gettime(Process::CLOCK_MONOTONIC) < previous_deadline

        raise LeaseLost.new('etcd lease validity expired before keepalive was confirmed',
                            category: :lease_lost)
      end

      def publish_keepalive_confirmation(state, previous_deadline, new_deadline, ttl_ms, response_at)
        state[:mutex].synchronize do
          return if state[:stopped]

          error = keepalive_deadline_error(response_at, previous_deadline, new_deadline)
          if error
            state[:error] = error
          else
            state[:deadline] = new_deadline
            state[:ttl_ms] = ttl_ms
          end
        end
      end

      def keepalive_deadline_error(response_at, previous_deadline, new_deadline)
        if response_at >= previous_deadline
          return LeaseLost.new('etcd keepalive acknowledgement arrived after its prior lease deadline',
                               category: :lease_lost)
        end
        return unless response_at >= new_deadline

        LeaseLost.new('etcd keepalive acknowledgement arrived outside its new validity budget',
                      category: :lease_lost)
      end

      def confirmed_keepalive_ttl_ms(response)
        return response.TTL * 1_000 if response.TTL.positive? && response.TTL <= 86_400

        raise LeaseLost.new('etcd lease keepalive returned an invalid effective TTL', category: :lease_lost)
      end

      def lease_deadline(started_at, ttl_ms)
        started_at + ((ttl_ms - Internal.safety_margin_ms(ttl_ms)) / 1_000.0)
      end

      def stop_keepalive(state)
        return unless state

        state[:mutex].synchronize { state[:stopped] = true }
        state[:thread]&.join(@cleanup_timeout_ms / 1_000.0)
        state[:thread]&.kill if state[:thread]&.alive?
      end

      def keepalive_once(lease_id)
        stream = @lease.lease_keep_alive(
          [Etcdserverpb::LeaseKeepAliveRequest.new(ID: lease_id)],
          deadline: rpc_deadline(@rpc_timeout_ms)
        )
        response = stream.first
        return response if response

        raise BackendUnavailable.new('etcd returned an empty keepalive stream',
                                     category: :backend_unavailable)
      end

      def revoke(lease_id, timeout_ms = @cleanup_timeout_ms)
        return false unless lease_id

        @lease.lease_revoke(Etcdserverpb::LeaseRevokeRequest.new(ID: lease_id),
                            deadline: rpc_deadline(timeout_ms))
        true
      rescue GRPC::BadStatus => e
        observe_cleanup('lease-revoke', e.class.name)
        false
      end

      def rpc_deadline(timeout_ms)
        Time.now + (timeout_ms / 1_000.0)
      end

      def observe_cleanup(reason, outcome)
        @cleanup_observer&.call(reason: reason, outcome: outcome)
      rescue StandardError
        nil
      end
    end

    include KeepaliveOperations

    private_constant :KeepaliveOperations
  end
end
