<?php

declare(strict_types=1);

namespace Dilock\Etcd\Protocol;

use Etcdserverpb\LeaseGrantRequest;
use Etcdserverpb\LeaseGrantResponse;
use Etcdserverpb\LeaseKeepAliveRequest;
use Etcdserverpb\LeaseKeepAliveResponse;
use Etcdserverpb\LeaseRevokeRequest;
use Etcdserverpb\LeaseRevokeResponse;
use Etcdserverpb\RangeRequest;
use Etcdserverpb\RangeResponse;
use Grpc\BaseStub;
use Grpc\BidiStreamingCall;
use Grpc\UnaryCall;
use V3lockpb\LockRequest;
use V3lockpb\LockResponse;
use V3lockpb\UnlockRequest;
use V3lockpb\UnlockResponse;

/** Minimal generated-style clients for the pinned etcd v3.7.2 RPC surface. */
final class KvClient extends BaseStub
{
    /**
     * @param array<string, list<string>> $metadata
     * @param array<string, mixed> $options
     * @return UnaryCall<RangeResponse>
     */
    public function range(RangeRequest $request, array $metadata = [], array $options = []): UnaryCall
    {
        /** @var UnaryCall<RangeResponse> $call */
        $call = $this->_simpleRequest('/etcdserverpb.KV/Range', $request, [RangeResponse::class, 'decode'], $metadata, $options);
        return $call;
    }
}

final class LeaseClient extends BaseStub
{
    /**
     * @param array<string, list<string>> $metadata
     * @param array<string, mixed> $options
     * @return UnaryCall<LeaseGrantResponse>
     */
    public function grant(LeaseGrantRequest $request, array $metadata = [], array $options = []): UnaryCall
    {
        /** @var UnaryCall<LeaseGrantResponse> $call */
        $call = $this->_simpleRequest('/etcdserverpb.Lease/LeaseGrant', $request, [LeaseGrantResponse::class, 'decode'], $metadata, $options);
        return $call;
    }

    /**
     * @param array<string, list<string>> $metadata
     * @param array<string, mixed> $options
     * @return UnaryCall<LeaseRevokeResponse>
     */
    public function revoke(LeaseRevokeRequest $request, array $metadata = [], array $options = []): UnaryCall
    {
        /** @var UnaryCall<LeaseRevokeResponse> $call */
        $call = $this->_simpleRequest('/etcdserverpb.Lease/LeaseRevoke', $request, [LeaseRevokeResponse::class, 'decode'], $metadata, $options);
        return $call;
    }

    /**
     * @param array<string, list<string>> $metadata
     * @param array<string, mixed> $options
     * @return BidiStreamingCall<LeaseKeepAliveResponse>
     */
    public function keepAlive(array $metadata = [], array $options = []): BidiStreamingCall
    {
        /** @var BidiStreamingCall<LeaseKeepAliveResponse> $call */
        $call = $this->_bidiRequest('/etcdserverpb.Lease/LeaseKeepAlive', [LeaseKeepAliveResponse::class, 'decode'], $metadata, $options);
        return $call;
    }
}

final class LockClient extends BaseStub
{
    /**
     * @param array<string, list<string>> $metadata
     * @param array<string, mixed> $options
     * @return UnaryCall<LockResponse>
     */
    public function lock(LockRequest $request, array $metadata = [], array $options = []): UnaryCall
    {
        /** @var UnaryCall<LockResponse> $call */
        $call = $this->_simpleRequest('/v3lockpb.Lock/Lock', $request, [LockResponse::class, 'decode'], $metadata, $options);
        return $call;
    }

    /**
     * @param array<string, list<string>> $metadata
     * @param array<string, mixed> $options
     * @return UnaryCall<UnlockResponse>
     */
    public function unlock(UnlockRequest $request, array $metadata = [], array $options = []): UnaryCall
    {
        /** @var UnaryCall<UnlockResponse> $call */
        $call = $this->_simpleRequest('/v3lockpb.Lock/Unlock', $request, [UnlockResponse::class, 'decode'], $metadata, $options);
        return $call;
    }
}
