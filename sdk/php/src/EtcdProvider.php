<?php

declare(strict_types=1);

namespace Dilock;

use Dilock\Etcd\Protocol\KvClient;
use Dilock\Etcd\Protocol\LeaseClient;
use Dilock\Etcd\Protocol\LockClient;
use Etcdserverpb\LeaseGrantRequest;
use Etcdserverpb\LeaseKeepAliveRequest;
use Etcdserverpb\LeaseRevokeRequest;
use Etcdserverpb\RangeRequest;
use Grpc\Channel;
use Grpc\ChannelCredentials;
use Grpc\UnaryCall;
use V3lockpb\LockRequest;
use V3lockpb\UnlockRequest;

/** One etcd authority, one shared gRPC channel and native lock/lease stubs. */
final class EtcdProvider
{
    private bool $closed = false;
    /** @var array<int, EtcdLease> */
    private array $leases = [];
    private readonly KvClient $kv;
    private readonly LeaseClient $lease;
    private readonly LockClient $lock;

    private function __construct(
        private readonly Channel $channel,
        private readonly string $namespace,
        private readonly string $epoch,
        public readonly AcquireOptions $defaults,
        private readonly ?\Closure $cleanupObserver,
    ) {
        $target = $channel;
        $this->kv = new KvClient('', [], $target);
        $this->lease = new LeaseClient('', [], $target);
        $this->lock = new LockClient('', [], $target);
    }

    public static function connect(
        string $endpoint,
        string $epoch,
        string $namespace = 'default',
        ?AcquireOptions $defaults = null,
        ?callable $cleanupObserver = null,
    ): self {
        Identity::resourceId($namespace, 'validation');
        if ($epoch === '' || strlen($epoch) > 1024 || preg_match('//u', $epoch) !== 1) {
            throw new \InvalidArgumentException('epoch must be nonempty UTF-8 and at most 1024 bytes');
        }
        $parts = parse_url(str_contains($endpoint, '://') ? $endpoint : 'http://' . $endpoint);
        if (!is_array($parts) || !isset($parts['host']) || isset($parts['user']) || isset($parts['pass'])
            || (isset($parts['path']) && !in_array($parts['path'], ['', '/'], true))
            || isset($parts['query']) || isset($parts['fragment'])
            || !in_array($parts['scheme'] ?? '', ['http', 'https'], true)) {
            throw new \InvalidArgumentException('endpoint must be an http(s) gRPC authority without credentials or path');
        }
        if (!extension_loaded('grpc') || !class_exists(\Google\Protobuf\Internal\Message::class)) {
            throw new \RuntimeException('Dilock etcd requires ext-grpc, grpc/grpc and google/protobuf');
        }
        $defaults ??= new AcquireOptions();
        $target = $parts['host'] . ':' . ($parts['port'] ?? 2379);
        $credentials = ($parts['scheme'] === 'https') ? ChannelCredentials::createSsl() : ChannelCredentials::createInsecure();
        $channel = new Channel($target, ['credentials' => $credentials]);
        $provider = new self(
            $channel,
            $namespace,
            $epoch,
            $defaults,
            $cleanupObserver === null ? null : \Closure::fromCallable($cleanupObserver)
        );
        try {
            $provider->validateMetadata($defaults->lease->rpcSeconds);
            return $provider;
        } catch (\Throwable $error) {
            $provider->close();
            if ($error instanceof DilockException || $error instanceof \InvalidArgumentException) {
                throw $error;
            }
            throw new BackendUnavailable('failed connecting to etcd or validating namespace metadata', 0, $error);
        }
    }

    public function lock(string $resource): EtcdHandle
    {
        $this->ensureOpen();
        return new EtcdHandle($this, '/dilock/locks/' . Identity::resourceId($this->namespace, $resource));
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        foreach (array_values($this->leases) as $lease) {
            $lease->release();
        }
        $this->channel->close();
    }

    public function ensureOpen(): void
    {
        if ($this->closed) {
            throw new \LogicException('provider is closed');
        }
    }

    public function addLease(EtcdLease $lease): void
    {
        $this->leases[spl_object_id($lease)] = $lease;
    }
    public function removeLease(EtcdLease $lease): void
    {
        unset($this->leases[spl_object_id($lease)]);
    }
    public function kv(): KvClient
    {
        return $this->kv;
    }
    public function lease(): LeaseClient
    {
        return $this->lease;
    }
    public function lockClient(): LockClient
    {
        return $this->lock;
    }
    public function epoch(): string
    {
        return $this->epoch;
    }

    public function observeCleanup(ReleaseOutcome $outcome, string $reason): void
    {
        try {
            if ($this->cleanupObserver !== null) {
                ($this->cleanupObserver)(new CleanupObservation('etcd', $outcome, $reason));
            } else {
                error_log(sprintf('dilock etcd scope cleanup outcome=%s reason=%s', $outcome->value, $reason));
            }
        } catch (\Throwable) {
        }
    }

    public function validateMetadata(float $timeout): void
    {
        $key = '/dilock/namespaces/' . $this->namespace . '/meta';
        try {
            [$response] = self::await($this->kv->range(new RangeRequest(['key' => $key]), [], self::deadline($timeout)), $timeout, 'etcd namespace metadata read');
        } catch (WaitTimeout $error) {
            throw new BackendUnavailable('etcd namespace metadata read timed out', 0, $error);
        }
        $kvs = $response->getKvs();
        if (count($kvs) !== 1 || $kvs[0]->getKey() !== $key) {
            throw new ProtocolMismatch('etcd namespace metadata is missing');
        }
        $raw = $kvs[0]->getValue();
        if ($raw === '' || strlen($raw) > 8192) {
            throw new ProtocolMismatch('etcd namespace metadata is empty or too large');
        }
        try {
            $metadata = json_decode($raw, true, 16, JSON_THROW_ON_ERROR);
        } catch (\JsonException $error) {
            throw new ProtocolMismatch('etcd namespace metadata is not valid JSON', 0, $error);
        }
        if (!is_array($metadata)
            || ($metadata['protocol_major'] ?? null) !== 1
            || ($metadata['identity_scheme'] ?? null) !== Identity::SCHEME
            || ($metadata['profile'] ?? null) !== 'etcd-native-lock-v1'
            || ($metadata['coordination_epoch'] ?? null) !== $this->epoch
            || !array_key_exists('fixed_lease_ms', $metadata)
            || $metadata['fixed_lease_ms'] !== null) {
            throw new ProtocolMismatch('etcd namespace metadata is incompatible with draft 1');
        }
    }

    /**
     * @template T of \Google\Protobuf\Internal\Message
     * @param UnaryCall<T> $call
     * @return array{0:T,1:\stdClass}
     */
    public static function await(UnaryCall $call, float $timeout, string $operation): array
    {
        try {
            [$response, $status] = $call->wait();
        } catch (\Throwable $error) {
            throw new BackendUnavailable($operation . ' failed', 0, $error);
        }
        $code = $status->code ?? null;
        if (!is_int($code)) {
            throw new BackendUnavailable($operation . ' returned a malformed gRPC status');
        }
        if ($code !== \Grpc\STATUS_OK || $response === null) {
            if ($code === \Grpc\STATUS_DEADLINE_EXCEEDED) {
                throw new WaitTimeout($operation . ' exceeded its RPC budget');
            }
            throw new BackendUnavailable($operation . ' failed with gRPC status ' . $code);
        }
        return [$response, $status];
    }

    /** @return array{timeout:int} */
    public static function deadline(float $seconds): array
    {
        return ['timeout' => self::timeoutMicros($seconds)];
    }
    public static function timeoutMicros(float $seconds): int
    {
        return max(1, (int) ceil($seconds * 1_000_000));
    }
    public static function now(): float
    {
        return hrtime(true) / 1_000_000_000;
    }
    /** @internal Remaining part of a single monotonic cleanup deadline. */
    public static function remainingBudget(float $deadline, ?float $now = null): float
    {
        return max(0.0, $deadline - ($now ?? self::now()));
    }

    /** @internal A blocking Lock RPC is never dispatched without wait budget remaining. */
    public static function lockWaitBudget(float $deadline, float $maximum, ?float $now = null): float
    {
        $remaining = self::remainingBudget($deadline, $now);
        if ($remaining <= 0) {
            throw new WaitTimeout('etcd acquisition wait budget expired after lease grant');
        }
        return min($maximum, $remaining);
    }

    /** @internal Normalize bounded int64 response fields used in lease timing. */
    public static function positiveInt(int|string $value): ?int
    {
        $parsed = filter_var((string) $value, FILTER_VALIDATE_INT);
        return $parsed !== false && $parsed > 0 ? $parsed : null;
    }

    /** @internal Validate exact-key Range evidence before exposing an etcd lease or fence. */
    public static function verifiedOwnershipRevision(
        string $requestedKey,
        string $returnedKey,
        int|string $returnedLeaseId,
        int|string $expectedLeaseId,
        int|string $createRevision,
    ): string {
        $leaseId = self::positiveInt64Decimal((string) $returnedLeaseId);
        $expectedId = self::positiveInt64Decimal((string) $expectedLeaseId);
        $revision = self::positiveInt64Decimal((string) $createRevision);
        if ($requestedKey === '' || $returnedKey !== $requestedKey || $leaseId === null
            || $expectedId === null || $leaseId !== $expectedId || $revision === null) {
            throw new ProtocolMismatch('etcd ownership Range returned a different key, lease, or invalid create revision');
        }
        return $revision;
    }

    private static function positiveInt64Decimal(string $value): ?string
    {
        if (preg_match('/^[1-9][0-9]{0,18}$/D', $value) !== 1
            || (strlen($value) === 19 && strcmp($value, '9223372036854775807') > 0)) {
            return null;
        }
        return $value;
    }

    /** @internal A protocol mismatch is definitive only after attempt cleanup is acknowledged. */
    public static function ownershipVerificationFailure(\Throwable $error, bool $cleanupAcknowledged): \Throwable
    {
        if (!$cleanupAcknowledged) {
            return new AcquisitionUnknown('etcd ownership verification failed and attempt-lease cleanup is unknown', 0, $error);
        }
        if ($error instanceof ProtocolMismatch || $error instanceof AcquisitionUnknown) {
            return $error;
        }
        return new AcquisitionUnknown('etcd lock ownership verification is unknown', 0, $error);
    }

    /** @internal Only an expired total wait budget plus acknowledged cleanup is a wait timeout. */
    public static function ownershipVerificationTimeoutFailure(
        \Throwable $error,
        bool $cleanupAcknowledged,
        bool $waitBudgetExpired,
    ): \Throwable {
        if ($error instanceof WaitTimeout && $cleanupAcknowledged && $waitBudgetExpired) {
            return new WaitTimeout('etcd ownership verification exceeded the acquisition wait budget', 0, $error);
        }
        return self::ownershipVerificationFailure($error, $cleanupAcknowledged);
    }

    /** @internal A malformed grant is definitive only when its lease is cleaned up. */
    public static function malformedGrantFailure(bool $hasUsableLeaseId, bool $cleanupAcknowledged): \Throwable
    {
        if (!$hasUsableLeaseId || !$cleanupAcknowledged) {
            return new AcquisitionUnknown('malformed etcd lease grant cleanup is not confirmed');
        }
        return new ProtocolMismatch('etcd returned an invalid lease grant');
    }

    /** @internal A missing lock key alone does not prove its acquisition lease was revoked. */
    public static function releaseVerificationOutcome(bool $lockKeyAbsent, bool $leaseRevokeAcknowledged): ReleaseOutcome
    {
        return $lockKeyAbsent && $leaseRevokeAcknowledged
            ? ReleaseOutcome::AlreadyNotOwned
            : ReleaseOutcome::Unknown;
    }

    /** @internal Classify release only after exact-key ownership was observed. */
    public static function releaseAcknowledgementOutcome(
        bool $ownershipConfirmed,
        bool $unlockAcknowledged,
        bool $revokeAcknowledged,
        bool $revokeNotFound,
    ): ReleaseOutcome {
        // Unlock has no delete-count field. Requiring a positive revoke response
        // distinguishes our acknowledged deletion from a key that expired in the
        // race between the ownership Range and native Unlock.
        if ($ownershipConfirmed && $unlockAcknowledged && $revokeAcknowledged) {
            return ReleaseOutcome::Released;
        }
        return ($revokeAcknowledged || $revokeNotFound)
            ? ReleaseOutcome::AlreadyNotOwned
            : ReleaseOutcome::Unknown;
    }

    /** @internal Classify the two terminal gRPC statuses for the attempt-lease revoke. */
    public static function releaseRevocationStatus(int $grpcStatusCode): string
    {
        return match ($grpcStatusCode) {
            \Grpc\STATUS_OK => 'acknowledged',
            \Grpc\STATUS_NOT_FOUND => 'not_found',
            default => 'unknown',
        };
    }
}

/** Native etcd Lock+Lease adapter. Queued wait is included in the granted TTL. */
final class EtcdHandle
{
    public function __construct(private readonly EtcdProvider $provider, private readonly string $name)
    {
    }

    public function tryAcquire(?Options $options = null): ?EtcdLease
    {
        $options ??= $this->provider->defaults->lease;
        $result = $this->acquireInternal(new AcquireOptions($options->rpcSeconds, $options), true);
        return $result;
    }

    /**
     * Wait for confirmed ownership within one finite caller budget.
     *
     * @throws WaitTimeout when contention exhausts the caller's wait budget after attempt-lease cleanup
     * @throws AcquisitionUnknown when ownership or cleanup cannot be confirmed
     * @throws BackendUnavailable when etcd cannot complete a required operation
     */
    public function acquire(?AcquireOptions $acquisition = null): EtcdLease
    {
        return $this->acquireInternal($acquisition ?? $this->provider->defaults, false)
            ?? throw new WaitTimeout('etcd contention wait budget exhausted');
    }

    /** @return EtcdLease|null */
    private function acquireInternal(AcquireOptions $acquisition, bool $trying): ?EtcdLease
    {
        $this->provider->ensureOpen();
        $options = $acquisition->lease;
        $started = EtcdProvider::now();
        $waitDeadline = $started + $acquisition->waitSeconds;
        $this->provider->validateMetadata(min($options->rpcSeconds, $acquisition->waitSeconds));
        if ($options->requireFencing && $this->provider->epoch() === '') {
            throw new UnsupportedCapability('etcd fencing requires an operator-approved epoch');
        }
        $ttlSeconds = (int) ceil($acquisition->waitSeconds + $options->leaseSeconds + $options->marginSeconds());
        if ($ttlSeconds < 1 || $ttlSeconds > 86400) {
            throw new \InvalidArgumentException('etcd queued acquisition TTL must be within 1..86400 seconds');
        }
        $grantBudget = min($options->rpcSeconds, $waitDeadline - EtcdProvider::now());
        if ($grantBudget <= 0) {
            throw new WaitTimeout('etcd acquisition budget expired before lease grant');
        }
        try {
            [$grant] = EtcdProvider::await($this->provider->lease()->grant(new LeaseGrantRequest(['TTL' => $ttlSeconds]), [], EtcdProvider::deadline($grantBudget)), $grantBudget, 'etcd lease grant');
        } catch (\Throwable $error) {
            if ($error instanceof WaitTimeout || $error instanceof BackendUnavailable) {
                throw new AcquisitionUnknown('etcd lease grant acknowledgement is unknown', 0, $error);
            }
            throw $error;
        }
        $leaseId = $grant->getID();
        $serverTtl = EtcdProvider::positiveInt($grant->getTTL());
        if (EtcdProvider::positiveInt($leaseId) === null) {
            throw EtcdProvider::malformedGrantFailure(false, false);
        }
        if ($serverTtl === null || $serverTtl > 86400) {
            $revoked = $this->revoke($leaseId, $options->cleanupSeconds);
            throw EtcdProvider::malformedGrantFailure(true, $revoked);
        }
        $localDeadline = $started + $serverTtl - max(0.01, $serverTtl * 0.01);
        try {
            $lockBudget = EtcdProvider::lockWaitBudget($waitDeadline, $acquisition->waitSeconds);
        } catch (WaitTimeout $error) {
            if (!$this->revoke($leaseId, $options->cleanupSeconds)) {
                throw new AcquisitionUnknown('etcd lease grant arrived after the wait deadline and cleanup is unknown');
            }
            throw $error;
        }
        try {
            [$locked] = EtcdProvider::await($this->provider->lockClient()->lock(new LockRequest(['name' => $this->name, 'lease' => $leaseId]), [], EtcdProvider::deadline($lockBudget)), $lockBudget, 'etcd native lock');
        } catch (WaitTimeout $error) {
            $revoked = $this->revoke($leaseId, $options->cleanupSeconds);
            if (!$revoked) {
                throw new AcquisitionUnknown('timed out etcd acquisition could not confirm attempt-lease cleanup', 0, $error);
            }
            if ($trying) {
                return $this->confirmBusy($options) ? null : throw new AcquisitionUnknown('etcd try result is ambiguous without a confirmed waiter');
            }
            throw new WaitTimeout('etcd contention wait budget exhausted', 0, $error);
        } catch (\Throwable $error) {
            $revoked = $this->revoke($leaseId, $options->cleanupSeconds);
            if (!$revoked) {
                throw new AcquisitionUnknown('etcd Lock outcome and attempt cleanup are unknown', 0, $error);
            }
            throw $error;
        }
        $key = $locked->getKey();
        if ($key === '' || EtcdProvider::now() >= $localDeadline) {
            $this->revoke($leaseId, $options->cleanupSeconds);
            throw new AcquisitionUnknown('etcd lock reply is empty or arrived after conservative lease deadline');
        }
        try {
            $verify = new RangeRequest(['key' => $key]);
            $verificationBudget = EtcdProvider::lockWaitBudget($waitDeadline, $options->rpcSeconds);
            [$range] = EtcdProvider::await(
                $this->provider->kv()->range($verify, [], EtcdProvider::deadline($verificationBudget)),
                $verificationBudget,
                'etcd lock ownership verification'
            );
            $kvs = $range->getKvs();
            if (count($kvs) !== 1) {
                throw new ProtocolMismatch('etcd lock key is missing or attached to another lease');
            }
            $fenceRevision = EtcdProvider::verifiedOwnershipRevision(
                $key,
                $kvs[0]->getKey(),
                $kvs[0]->getLease(),
                $leaseId,
                $kvs[0]->getCreateRevision(),
            );
            if (EtcdProvider::now() >= $localDeadline || EtcdProvider::now() >= $waitDeadline) {
                throw new AcquisitionUnknown('etcd ownership confirmation arrived after its wait or lease deadline');
            }
            $lease = new EtcdLease(
                $this->provider,
                $leaseId,
                $key,
                $localDeadline,
                $options,
                $this->provider->epoch(),
                $fenceRevision
            );
            $this->provider->addLease($lease);
            return $lease;
        } catch (WaitTimeout $error) {
            $revoked = $this->revoke($leaseId, $options->cleanupSeconds);
            $failure = EtcdProvider::ownershipVerificationTimeoutFailure(
                $error,
                $revoked,
                EtcdProvider::remainingBudget($waitDeadline) <= 0
            );
            throw $failure;
        } catch (\Throwable $error) {
            $revoked = $this->revoke($leaseId, $options->cleanupSeconds);
            throw EtcdProvider::ownershipVerificationFailure($error, $revoked);
        }
    }

    private function confirmBusy(Options $options): bool
    {
        try {
            [$range] = EtcdProvider::await($this->provider->kv()->range(new RangeRequest(['key' => $this->name . '/', 'range_end' => $this->name . '0']), [], EtcdProvider::deadline($options->cleanupSeconds)), $options->cleanupSeconds, 'etcd contention probe');
            return $range->getCount() > 0;
        } catch (\Throwable $error) {
            throw new AcquisitionUnknown('etcd lock timeout could not be classified', 0, $error);
        }
    }

    private function revoke(int|string $leaseId, float $timeout): bool
    {
        try {
            EtcdProvider::await($this->provider->lease()->revoke(new LeaseRevokeRequest(['ID' => $leaseId]), [], EtcdProvider::deadline($timeout)), $timeout, 'etcd attempt-lease cleanup');
            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    public function withLease(callable $work, ?AcquireOptions $options = null): mixed
    {
        $lease = $this->acquire($options);
        $workError = null;
        try {
            return $work($lease);
        } catch (\Throwable $error) {
            $workError = $error;
            throw $error;
        } finally {
            // An acknowledged Unlock cleans an expired key; it does not mean the work stayed within its lease.
            $leaseWasLost = $lease->state() === LeaseState::Lost;
            $outcome = $this->scopeRelease($lease, $workError === null ? 'scope_exit' : 'body_failed');
            if ($workError === null) {
                if ($outcome === ReleaseOutcome::Unknown) {
                    throw new CleanupError('etcd lease cleanup was not acknowledged');
                }
                if ($leaseWasLost) {
                    throw new LeaseLost('etcd lease validity expired while scoped work was running');
                }
                if ($outcome === ReleaseOutcome::AlreadyNotOwned) {
                    throw new LeaseLost('etcd lease was no longer owned when scoped work finished');
                }
            }
        }
    }

    private function scopeRelease(EtcdLease $lease, string $reason): ReleaseOutcome
    {
        try {
            $outcome = $lease->release();
            if ($outcome === ReleaseOutcome::Unknown) {
                $this->provider->observeCleanup($outcome, $reason);
            }
            return $outcome;
        } catch (\Throwable) {
            $this->provider->observeCleanup(ReleaseOutcome::Unknown, $reason);
            return ReleaseOutcome::Unknown;
        }
    }
}

/** One native etcd lease. Keepalive is explicit to avoid hidden background threads in PHP. */
final class EtcdLease
{
    private LeaseState $currentState = LeaseState::Held;
    private ?ReleaseOutcome $outcome = null;

    public function __construct(
        private readonly EtcdProvider $provider,
        private readonly int|string $leaseId,
        private readonly string $key,
        private float $deadline,
        public readonly Options $options,
        public readonly string $fenceEpoch,
        public readonly string $fenceRevision,
    ) {
    }

    public function state(): LeaseState
    {
        if ($this->currentState === LeaseState::Held && EtcdProvider::now() >= $this->deadline) {
            $this->currentState = LeaseState::Lost;
        }
        return $this->currentState;
    }

    /** @return array{epoch:string,value:string} */
    public function fence(): array
    {
        return ['epoch' => $this->fenceEpoch, 'value' => $this->fenceRevision];
    }

    public function effectiveLeaseMilliseconds(): int
    {
        return max(0, (int) (($this->deadline - EtcdProvider::now()) * 1000));
    }

    public function renew(): bool
    {
        if ($this->state() !== LeaseState::Held) {
            return false;
        }
        $previousDeadline = $this->deadline;
        $sent = EtcdProvider::now();
        try {
            $call = $this->provider->lease()->keepAlive([], EtcdProvider::deadline($this->options->rpcSeconds));
            // grpc's BidiStreamingCall::write() returns void; transport rejection throws.
            $call->write(new LeaseKeepAliveRequest(['ID' => $this->leaseId]));
            $response = $call->read();
            $call->writesDone();
            $serverTtl = $response === null ? null : EtcdProvider::positiveInt($response->getTTL());
            if ($response === null || (string) $response->getID() !== (string) $this->leaseId || $serverTtl === null || $serverTtl > 86400) {
                $this->currentState = LeaseState::Lost;
                return false;
            }
            if (EtcdProvider::now() >= $previousDeadline) {
                $this->currentState = LeaseState::Lost;
                return false;
            }
            $newDeadline = $sent + $serverTtl - max(0.01, $serverTtl * 0.01);
            if (EtcdProvider::now() >= $newDeadline) {
                $this->currentState = LeaseState::Lost;
                return false;
            }
            $this->deadline = $newDeadline;
            return true;
        } catch (\Throwable $error) {
            $this->currentState = LeaseState::Lost;
            throw new BackendUnavailable('etcd lease renewal could not be confirmed', 0, $error);
        }
    }

    public function release(): ReleaseOutcome
    {
        if ($this->outcome !== null) {
            return $this->outcome;
        }
        $this->currentState = LeaseState::Releasing;
        $cleanupDeadline = EtcdProvider::now() + $this->options->cleanupSeconds;
        $ownershipConfirmed = false;
        $unlockAcknowledged = false;
        $revokeAcknowledged = false;
        $revokeNotFound = false;
        try {
            $budget = $this->remainingCleanupBudget($cleanupDeadline);
            if ($budget > 0) {
                // Unlock's acknowledgement is meaningful only if this attempt still owns
                // the exact opaque key returned by Lock. A naturally expired lease must
                // never be reported as a successful explicit release.
                $request = new RangeRequest(['key' => $this->key]);
                [$range] = EtcdProvider::await($this->provider->kv()->range($request, [], EtcdProvider::deadline($budget)), $budget, 'etcd release ownership verification');
                foreach ($range->getKvs() as $kv) {
                    if ($kv->getKey() === $this->key && (string) $kv->getLease() === (string) $this->leaseId) {
                        $ownershipConfirmed = true;
                        break;
                    }
                }
                if ($ownershipConfirmed) {
                    $budget = $this->remainingCleanupBudget($cleanupDeadline);
                    if ($budget > 0) {
                        $request = new UnlockRequest(['key' => $this->key]);
                        EtcdProvider::await($this->provider->lockClient()->unlock($request, [], EtcdProvider::deadline($budget)), $budget, 'etcd unlock');
                        $unlockAcknowledged = true;
                    }
                }
            }
        } catch (\Throwable) {
        }
        try {
            $budget = $this->remainingCleanupBudget($cleanupDeadline);
            if ($budget > 0) {
                $request = new LeaseRevokeRequest(['ID' => $this->leaseId]);
                [$response, $status] = $this->provider->lease()->revoke($request, [], EtcdProvider::deadline($budget))->wait();
                $statusCode = $status->code ?? null;
                $revokeStatus = is_int($statusCode) ? EtcdProvider::releaseRevocationStatus($statusCode) : 'unknown';
                if ($revokeStatus === 'acknowledged') {
                    $revokeAcknowledged = $response !== null;
                } elseif ($revokeStatus === 'not_found') {
                    $revokeNotFound = true;
                } elseif (!is_int($statusCode) || $response === null) {
                    throw new BackendUnavailable('etcd lease revoke returned a malformed acknowledgement');
                } elseif ($statusCode === \Grpc\STATUS_DEADLINE_EXCEEDED) {
                    throw new WaitTimeout('etcd lease revoke exceeded its cleanup deadline');
                } else {
                    throw new BackendUnavailable('etcd lease revoke failed with gRPC status ' . $statusCode);
                }
            }
        } catch (\Throwable) {
        }
        $outcome = EtcdProvider::releaseAcknowledgementOutcome($ownershipConfirmed, $unlockAcknowledged, $revokeAcknowledged, $revokeNotFound);
        if ($outcome === ReleaseOutcome::Unknown) {
            try {
                $budget = $this->remainingCleanupBudget($cleanupDeadline);
                if ($budget <= 0) {
                    throw new WaitTimeout('etcd release cleanup budget exhausted before verification');
                }
                $request = new RangeRequest(['key' => $this->key]);
                [$range] = EtcdProvider::await($this->provider->kv()->range($request, [], EtcdProvider::deadline($budget)), $budget, 'etcd release verification');
                $outcome = EtcdProvider::releaseVerificationOutcome(count($range->getKvs()) === 0, false);
            } catch (\Throwable) {
                $outcome = ReleaseOutcome::Unknown;
            }
        }
        $this->outcome = $outcome;
        $this->currentState = $outcome === ReleaseOutcome::Unknown ? LeaseState::ReleasedUnknown : LeaseState::Released;
        $this->provider->removeLease($this);
        return $outcome;
    }

    private function remainingCleanupBudget(float $deadline): float
    {
        return EtcdProvider::remainingBudget($deadline);
    }
}
