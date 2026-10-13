# frozen_string_literal: true

# etcd acquisition flow for the Dilock SDK.
module Dilock
  # etcd native Lock and lease acquisition behavior.
  class EtcdProvider
    # Coordinates etcd Lock, lease keepalive, verification and cleanup steps.
    # @private Acquisition coordination internals for {EtcdProvider}.
    module AcquisitionFlow
      def __acquire(resource_id, options, wait:)
        state = prepare_acquisition_state(options, wait)
        perform_acquisition(resource_id, options, state)
      rescue GRPC::BadStatus => e
        raise unless state

        translate_acquisition_error(state, e)
      rescue Interrupt => e
        classify_interrupted_acquisition(state, e)
        raise
      ensure
        cleanup_incomplete_acquisition(state) if state
      end

      def perform_acquisition(resource_id, options, state)
        lock_name = Identity.etcd_lock_name(resource_id)
        state[:lease_id], sent_at, ttl = grant_acquisition_lease(options, state)
        ensure_wait_budget_for_acquisition!(state)
        state[:keepalive] = start_keepalive(state[:lease_id], ttl, sent_at)
        response = await_lock(lock_name, state[:lease_id], state)
        stop_keepalive(state[:keepalive])
        ensure_wait_lease_healthy!(state)
        lease = build_acquired_lease(response, state)
        state[:completed] = true
        lease
      end

      private

      def prepare_acquisition_state(options, wait)
        ensure_open!
        validate_acquisition!(options, wait)
        new_acquisition_state(options)
      end

      def validate_acquisition!(options, wait)
        raise ArgumentError, 'options must be AcquireOptions' unless options.is_a?(AcquireOptions)
        return if wait

        raise UnsupportedCapability.new('etcd native Lock has no safe try-acquire operation',
                                        category: :unsupported_capability)
      end

      def translate_acquisition_error(state, backend_error)
        revoked = cleanup_acquisition_lease(state) if state[:lease_id]
        if state[:lease_id] && !revoked
          raise AcquisitionUnknown.new(
            "etcd ownership verification failed and lease cleanup was not acknowledged (#{backend_error.class})",
            category: :acquisition_unknown
          )
        end

        translate_verification_deadline!(state, backend_error, revoked)

        raise BackendUnavailable.new("etcd acquisition verification failed (#{backend_error.class})",
                                     category: :backend_unavailable)
      end

      def translate_verification_deadline!(state, backend_error, revoked)
        return unless backend_error.code == GRPC::Core::StatusCodes::DEADLINE_EXCEEDED &&
                      state[:verify_wait_limited] && revoked

        raise AcquisitionTimeout.new('etcd ownership verification exceeded the acquisition wait budget',
                                     category: :acquisition_timeout)
      end

      def cleanup_incomplete_acquisition(state)
        return if state[:completed]

        stop_keepalive(state[:keepalive])
        cleanup_acquisition_lease(state) if state[:lease_id]
      end

      def await_lock(lock_name, lease_id, state)
        ensure_wait_budget_for_acquisition!(state)
        remaining = Internal.remaining_ms(state[:wait_deadline])
        # Native Lock is intentionally blocking while another lease owns the
        # key, so its RPC must use the caller's remaining acquisition budget.
        # The shorter unary-RPC timeout applies to non-queueing requests only.
        timeout_ms = remaining
        state[:lock_wait_limited] = true
        deadline = rpc_deadline(timeout_ms)
        @lock.lock(V3lockpb::LockRequest.new(name: lock_name.b, lease: lease_id), deadline: deadline)
      rescue GRPC::BadStatus => e
        translate_lock_error(state, e)
      end

      def translate_lock_error(state, backend_error)
        stop_keepalive(state[:keepalive])
        revoked = cleanup_acquisition_lease(state)
        if backend_error.code == GRPC::Core::StatusCodes::DEADLINE_EXCEEDED &&
           state[:lock_wait_limited] && revoked
          raise AcquisitionTimeout.new('etcd native Lock wait budget expired', category: :acquisition_timeout)
        end

        outcome = revoked ? 'acknowledged' : 'unacknowledged'
        raise AcquisitionUnknown.new("etcd Lock outcome is unknown (#{backend_error.class}); cleanup #{outcome}",
                                     category: :acquisition_unknown)
      end

      def ensure_wait_lease_healthy!(state)
        keepalive_error = state[:keepalive][:error]
        return unless keepalive_error

        cleanup_acknowledged = cleanup_acquisition_lease(state)
        raise AcquisitionUnknown.new(
          "etcd keepalive failed while waiting (#{keepalive_error.class}); " \
          "cleanup #{cleanup_acknowledged ? 'acknowledged' : 'unacknowledged'}",
          category: :acquisition_unknown
        )
      end

      def cleanup_acquisition_lease(state)
        return false if state[:cleanup_attempted]

        state[:cleanup_attempted] = true
        revoke(state[:lease_id])
      rescue Interrupt => e
        # An interrupted revoke has an unknown outcome. Record it and never
        # replay the RPC; callers classify the acquisition as unknown.
        state[:cleanup_interruption] = e
        observe_cleanup('lease-revoke', "interrupted:#{e.class}")
        false
      end
    end

    # Keeps the full acquisition wait budget across grant, lock wait and verification.
    module AcquisitionBudget
      private

      def new_acquisition_state(options)
        { lease_id: nil, keepalive: nil, completed: false, cleanup_attempted: false,
          wait_deadline: Internal.deadline_ms(options.wait_ms) }
      end

      def ensure_wait_budget_for_acquisition!(state)
        return if Internal.remaining_ms(state[:wait_deadline]).positive?

        revoked = cleanup_acquisition_lease(state) if state[:lease_id]
        if state[:lease_id] && !revoked
          raise AcquisitionUnknown.new('etcd wait budget expired and attempt-lease cleanup is unknown',
                                       category: :acquisition_unknown)
        end
        raise AcquisitionTimeout.new('etcd acquisition wait budget expired', category: :acquisition_timeout)
      end

      def classify_interrupted_acquisition(state, error)
        return unless state && state[:lease_id] && !state[:completed]

        stop_keepalive(state[:keepalive])
        return if cleanup_acquisition_lease(state)

        raise AcquisitionUnknown.new(
          'etcd acquisition was interrupted and attempt-lease cleanup was not acknowledged',
          category: :acquisition_unknown
        ), cause: state[:cleanup_interruption] || error
      end

      def grant_acquisition_lease(options, state)
        ensure_wait_budget_for_acquisition!(state)
        timeout_ms = Internal.bounded_rpc_timeout_ms(state[:wait_deadline], @rpc_timeout_ms)
        sent_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        grant = dispatch_lease_grant(options, timeout_ms)
        state[:lease_id] = grant.ID if grant.ID.positive?
        validate_grant_and_cleanup!(grant, state)
        [grant.ID, sent_at, grant.TTL]
      rescue GRPC::BadStatus => e
        raise AcquisitionUnknown.new("etcd lease grant outcome is unknown (#{e.class})",
                                     category: :acquisition_unknown)
      end

      def validate_grant_and_cleanup!(grant, state)
        validate_lease_grant!(grant)
      rescue ProtocolMismatch => e
        if state[:lease_id] && !cleanup_acquisition_lease(state)
          raise AcquisitionUnknown.new('malformed etcd lease grant cleanup was not acknowledged',
                                       category: :acquisition_unknown), cause: e
        end
        raise
      end

      def dispatch_lease_grant(options, timeout_ms)
        @lease.lease_grant(
          Etcdserverpb::LeaseGrantRequest.new(TTL: [(options.lease_ms + 999) / 1_000, 1].max),
          deadline: rpc_deadline(timeout_ms)
        )
      end

      def validate_lease_grant!(grant)
        if grant.ID.positive?
          return if grant.TTL.positive? && grant.TTL <= 86_400

          raise ProtocolMismatch.new('etcd returned an invalid effective lease TTL',
                                     category: :protocol_mismatch)
        end

        raise BackendUnavailable.new("etcd rejected lease grant: #{grant.error}",
                                     category: :backend_unavailable)
      end
    end

    # Confirms the native lock key and issues its epoch-scoped revision fence.
    module OwnershipVerification
      private

      def build_acquired_lease(response, state)
        deadline, ttl_ms = keepalive_confirmation(state)
        key = response.key
        validate_lock_key!(key, state)
        entry = verified_lock_entry(key, state)
        validate_remaining_lease!(deadline, state)
        ownership = Lease::Ownership.new(key: key, owner: state[:lease_id])
        Lease.new(self, ownership, ttl_ms, deadline,
                  fence: EtcdFence.new(epoch: @epoch, revision: entry.create_revision))
      end

      def keepalive_confirmation(state)
        state[:keepalive][:mutex].synchronize do
          [state[:keepalive][:deadline], state[:keepalive][:ttl_ms]]
        end
      end

      def validate_lock_key!(key, state)
        return if key && !key.empty?

        unless cleanup_acquisition_lease(state)
          raise AcquisitionUnknown.new(
            'etcd Lock returned an empty key and attempt-lease cleanup was not acknowledged',
            category: :acquisition_unknown
          )
        end
        raise ProtocolMismatch.new('etcd Lock returned an empty opaque key', category: :protocol_mismatch)
      end

      def verified_lock_entry(key, state)
        ensure_wait_budget_for_acquisition!(state)
        range = @kv.range(Etcdserverpb::RangeRequest.new(key: key), deadline: verification_deadline(state))
        entry = range.kvs.find { |candidate| candidate.key == key }
        ensure_wait_budget_for_acquisition!(state)
        return entry if valid_lock_entry?(entry, state)

        reject_invalid_lock_entry!(state)
      end

      def reject_invalid_lock_entry!(state)
        unless cleanup_acquisition_lease(state)
          raise AcquisitionUnknown.new(
            'etcd returned an invalid lock key and attempt-lease cleanup was not acknowledged',
            category: :acquisition_unknown
          )
        end
        raise ProtocolMismatch.new('etcd returned lock key is absent or attached to another lease',
                                   category: :protocol_mismatch)
      end

      def verification_deadline(state)
        remaining = Internal.remaining_ms(state[:wait_deadline])
        state[:verify_wait_limited] = remaining <= @rpc_timeout_ms
        rpc_deadline([remaining, @rpc_timeout_ms].min)
      end

      def valid_lock_entry?(entry, state)
        entry && entry.lease == state[:lease_id] && entry.create_revision.positive?
      end

      def validate_remaining_lease!(deadline, state)
        remaining = ((deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)) * 1_000).floor
        return if remaining.positive?

        cleanup_acquisition_lease(state)
        raise AcquisitionUnknown.new('etcd lock verification exceeded the confirmed lease budget',
                                     category: :acquisition_unknown)
      end
    end

    include AcquisitionFlow
    include AcquisitionBudget
    include OwnershipVerification

    private_constant :AcquisitionFlow
    private_constant :AcquisitionBudget
    private_constant :OwnershipVerification
  end
end
