<?php

declare(strict_types=1);

namespace Dilock;

/** Reusable Consul resource; each acquisition gets a distinct TTL session. */
final class ConsulHandle
{
    public function __construct(private readonly ConsulProvider $provider, private readonly string $key)
    {
    }

    /** Returns null only for an acknowledged contention response and confirmed cleanup. */
    public function tryAcquire(?Options $options = null): ?ConsulLease
    {
        $this->provider->ensureOpen();
        $options ??= $this->provider->defaults->lease;
        $ttlSeconds = (int) ceil($options->leaseSeconds);
        if ($options->leaseSeconds < 10 || $ttlSeconds > 86400) {
            throw new \InvalidArgumentException('Consul session TTL must be between 10 seconds and 24 hours');
        }
        [$session, $sent] = $this->provider->createSession($ttlSeconds, $options->rpcSeconds);
        $deadline = $sent + $ttlSeconds - $options->marginSeconds();
        try {
            $granted = $this->provider->request('PUT', '/v1/kv/' . $this->key . '?acquire=' . rawurlencode($session), '', $options->rpcSeconds, true);
            if (!is_bool($granted)) {
                throw new ProtocolMismatch('Consul KV acquire response was not boolean');
            }
            if (!$granted) {
                if (!$this->provider->abandon($this->key, $session, $options->cleanupSeconds)) {
                    throw new AcquisitionUnknown('contended Consul session cleanup was not confirmed');
                }
                return null;
            }
            [$owner, $index] = $this->provider->owner($this->key, $options->rpcSeconds);
            if ($owner !== $session || $index < 1 || self::now() >= $deadline) {
                throw new AcquisitionUnknown('Consul ownership read did not confirm this session and LockIndex');
            }
            $lease = new ConsulLease($this->provider, $this->key, $session, $index, $ttlSeconds, $options, $deadline);
            $this->provider->addLease($lease);
            return $lease;
        } catch (\Throwable $error) {
            if (!$this->provider->abandon($this->key, $session, $options->cleanupSeconds)) {
                throw new AcquisitionUnknown('Consul acquisition cleanup was not confirmed', 0, $error);
            }
            if ($error instanceof AcquisitionUnknown || $error instanceof BackendUnavailable || $error instanceof ProtocolMismatch) {
                throw $error;
            }
            throw new AcquisitionUnknown('Consul acquisition outcome is unknown', 0, $error);
        }
    }

    /** Waits within one finite monotonic budget. */
    public function acquire(?AcquireOptions $acquisition = null, ?Options $options = null): ConsulLease
    {
        if ($acquisition !== null && $options !== null) {
            throw new \InvalidArgumentException('pass either complete acquisition options or lease options');
        }
        $acquisition ??= $this->provider->defaults;
        $options ??= $acquisition->lease;
        if ($acquisition->waitSeconds <= 0 || $acquisition->waitSeconds > 86400) {
            throw new \InvalidArgumentException('Consul wait must be finite and positive');
        }
        $waitDeadline = self::now() + $acquisition->waitSeconds;
        $ttl = (int) ceil($options->leaseSeconds);
        if ($options->leaseSeconds < 10 || $ttl > 86400) {
            throw new \InvalidArgumentException('Consul session TTL must be between 10 seconds and 24 hours');
        }
        $createBudget = min($options->rpcSeconds, $waitDeadline - self::now());
        if ($createBudget <= 0) {
            throw new WaitTimeout('Consul wait budget expired before session creation');
        }
        [$session, $sent] = $this->provider->createSession($ttl, $createBudget);
        $leaseDeadline = $sent + $ttl - $options->marginSeconds();
        $lastRenew = $sent;
        try {
            while (true) {
                if (self::now() >= $leaseDeadline) {
                    throw new AcquisitionUnknown('Consul acquisition session deadline elapsed');
                }
                $left = $waitDeadline - self::now();
                if ($left <= 0) {
                    throw new WaitTimeout('Consul contention wait budget exhausted');
                }
                $rpc = min($options->rpcSeconds, $left);
                $granted = $this->provider->request('PUT', '/v1/kv/' . $this->key . '?acquire=' . rawurlencode($session), '', $rpc, true);
                if (!is_bool($granted)) {
                    throw new ProtocolMismatch('Consul KV acquire response was not boolean');
                }
                if ($granted) {
                    $verifyBudget = min($options->rpcSeconds, $waitDeadline - self::now());
                    if ($verifyBudget <= 0) {
                        throw new AcquisitionUnknown('Consul ownership verification exceeded the wait budget');
                    }
                    [$owner, $index] = $this->provider->owner($this->key, $verifyBudget);
                    if ($owner !== $session || $index < 1 || self::now() >= $leaseDeadline || self::now() >= $waitDeadline) {
                        throw new AcquisitionUnknown('Consul ownership verification completed too late or named another session');
                    }
                    $lease = new ConsulLease($this->provider, $this->key, $session, $index, $ttl, $options, $leaseDeadline);
                    $this->provider->addLease($lease);
                    return $lease;
                }
                $remainingMicros = (int) floor(max(0.0, $waitDeadline - self::now()) * 1_000_000);
                $delay = Options::boundedRetryDelayMicroseconds($remainingMicros, random_int(20_000, 50_000));
                if ($delay > 0) { usleep($delay); }
                if (self::now() - $lastRenew >= ($ttl * 2 / 3)) {
                    $lastRenew = $this->provider->renewSession($session, $options->rpcSeconds);
                    $leaseDeadline = $lastRenew + $ttl - $options->marginSeconds();
                }
            }
        } catch (\Throwable $error) {
            if (!$this->provider->abandon($this->key, $session, $options->cleanupSeconds)) {
                throw new AcquisitionUnknown('Consul acquisition failed and session cleanup was not confirmed', 0, $error);
            }
            throw $error;
        }
    }

    /** Runs one callback and reports cleanup uncertainty without masking work errors. */
    public function withLease(callable $work, ?AcquireOptions $options = null): mixed
    {
        $lease = $this->acquire($options);
        $workError = null;
        try { return $work($lease); }
        catch (\Throwable $error) { $workError = $error; throw $error; }
        finally {
            $outcome = $lease->release();
            if ($workError !== null && $outcome !== ReleaseOutcome::Released) {
                $this->provider->observeCleanup($outcome, 'body_failed');
            } elseif ($workError === null && $outcome === ReleaseOutcome::AlreadyNotOwned) {
                throw new LeaseLost('Consul lock was no longer owned when scoped work finished');
            } elseif ($workError === null && $outcome === ReleaseOutcome::Unknown) {
                throw new CleanupError('Consul lease cleanup was not acknowledged');
            }
        }
    }

    private static function now(): float { return hrtime(true) / 1_000_000_000; }
}
