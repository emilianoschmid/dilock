<?php

declare(strict_types=1);

namespace Dilock;

/** Reusable handle; it owns no connection and performs no work until called. */
final class RedisHandle
{
    public function __construct(private readonly RedisProvider $provider, private readonly string $key)
    {
    }

    public function tryAcquire(?Options $options = null): ?RedisLease
    {
        $this->provider->ensureOpen();
        $options ??= $this->provider->defaults->lease;
        if ($options->requireFencing) {
            throw new UnsupportedCapability('Redis profile does not provide a strong fencing token');
        }
        $rpcDeadline = self::now() + $options->rpcSeconds;
        $owner = 'dilock/1/' . bin2hex(random_bytes(32));
        $sent = self::now();
        $setBudget = self::remaining($rpcDeadline);
        if ($setBudget <= 0) {
            throw new BackendUnavailable('Redis acquisition RPC budget expired before SET');
        }
        try {
            $created = $this->provider->commands()->setNxPx(
                $this->key,
                $owner,
                $options->ttlMilliseconds(),
                $setBudget,
            );
        } catch (\Throwable $error) {
            $this->cleanupOwner($owner, $options->cleanupSeconds);
            throw new AcquisitionUnknown('dispatched Redis SET has no ownership acknowledgement', 0, $error);
        }
        if (!$created) {
            try {
                $typeBudget = self::remaining($rpcDeadline);
                if ($typeBudget <= 0) {
                    throw new BackendUnavailable('Redis acquisition RPC budget expired before contention classification');
                }
                $type = $this->provider->commands()->type($this->key, $typeBudget);
            } catch (\Throwable $error) {
                throw new BackendUnavailable('Redis contention key type could not be read', 0, $error);
            }
            if (!in_array($type, ['string', 'none'], true)) {
                throw new ProtocolMismatch('Redis lock key has an unexpected type');
            }
            return null;
        }
        $deadline = $sent + $options->leaseSeconds - $options->marginSeconds();
        if (self::now() >= $deadline || $this->provider->isClosed()) {
            $this->cleanupOwner($owner, $options->cleanupSeconds);
            throw new AcquisitionUnknown('Redis ownership acknowledgement arrived too late');
        }
        $lease = new RedisLease($this->provider, $this->key, $owner, $options, $deadline);
        $this->provider->addLease($lease);
        return $lease;
    }

    /**
     * Wait for confirmed ownership within one finite caller budget.
     *
     * @throws WaitTimeout when contention exhausts the caller's wait budget
     * @throws AcquisitionUnknown when ownership or cleanup cannot be confirmed
     * @throws BackendUnavailable when Redis cannot complete a required operation
     */
    public function acquire(?AcquireOptions $acquisition = null, ?Options $options = null): RedisLease
    {
        if ($acquisition !== null && $options !== null) {
            throw new \InvalidArgumentException('pass either complete acquisition options or lease options');
        }
        $acquisition ??= $this->provider->defaults;
        $options ??= $acquisition->lease;
        $deadline = self::now() + $acquisition->waitSeconds;
        do {
            $remaining = $deadline - self::now();
            if ($remaining <= 0) {
                break;
            }
            $bounded = new Options(
                $options->leaseSeconds,
                min($options->rpcSeconds, $remaining, $options->leaseSeconds - $options->marginSeconds() - 0.001),
                $options->cleanupSeconds,
                $options->requireFencing
            );
            $lease = $this->tryAcquire($bounded);
            if ($lease !== null) {
                if (self::now() >= $deadline) {
                    if ($lease->release() === ReleaseOutcome::Unknown) {
                        throw new AcquisitionUnknown('late Redis acquisition cleanup was not confirmed');
                    }
                    throw new WaitTimeout('acquisition completed after its wait deadline');
                }
                return $lease;
            }
            $remainingMicros = (int) floor(max(0.0, $deadline - self::now()) * 1_000_000);
            $delay = Options::boundedRetryDelayMicroseconds($remainingMicros, random_int(20_000, 50_000));
            if ($delay > 0) {
                usleep($delay);
            }
        } while (self::now() < $deadline);
        throw new WaitTimeout('contention wait budget exhausted');
    }

    /** Run work under an acquired lease. Scope cleanup is bounded and never masks work errors. */
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
            $outcome = $this->scopeRelease($lease, $workError === null ? 'scope_exit' : 'body_failed');
            if ($workError === null) {
                if ($outcome === ReleaseOutcome::AlreadyNotOwned) {
                    throw new LeaseLost('Redis lease was no longer owned when scoped work finished');
                }
                if ($outcome === ReleaseOutcome::Unknown) {
                    throw new CleanupError('Redis lease cleanup was not acknowledged');
                }
            }
        }
    }

    private function scopeRelease(RedisLease $lease, string $reason): ReleaseOutcome
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

    private function cleanupOwner(string $owner, float $budget): ReleaseOutcome
    {
        try {
            $result = $this->provider->cleanupCommands()->releaseOwner($this->key, $owner, $budget);
            return $result === 1 ? ReleaseOutcome::Released
                : ($result === 0 ? ReleaseOutcome::AlreadyNotOwned : ReleaseOutcome::Unknown);
        } catch (\Throwable) {
            return ReleaseOutcome::Unknown;
        }
    }

    private static function now(): float
    {
        return hrtime(true) / 1_000_000_000;
    }
    private static function remaining(float $deadline): float
    {
        return max(0.0, $deadline - self::now());
    }
}
