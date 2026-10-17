<?php

declare(strict_types=1);

namespace Dilock;

/** One per-attempt Consul session; no destructor performs network I/O. */
final class ConsulLease
{
    private LeaseState $currentState = LeaseState::Held;
    private ?ReleaseOutcome $outcome = null;
    private float $deadline;

    public function __construct(
        private readonly ConsulProvider $provider,
        private readonly string $key,
        private readonly string $session,
        public readonly int $lockIndex,
        private readonly int $ttlSeconds,
        public readonly Options $options,
        float $deadline,
    ) { $this->deadline = $deadline; }

    public function state(): LeaseState
    {
        if ($this->currentState === LeaseState::Held && self::now() >= $this->deadline) {
            $this->currentState = LeaseState::Lost;
        }
        return $this->currentState;
    }

    public function ensureHeld(): void
    {
        if ($this->state() !== LeaseState::Held) { throw new LeaseLost('Consul lease has no remaining conservative validity'); }
    }

    public function fence(): ConsulFence { return new ConsulFence($this->provider->epoch(), $this->lockIndex); }

    public function effectiveLeaseMilliseconds(): int
    {
        return max(0, (int) floor(($this->deadline - self::now()) * 1000));
    }

    /** Renews the same session and verifies its session and retained LockIndex. */
    public function renew(): bool
    {
        $this->ensureHeld();
        $oldDeadline = $this->deadline;
        try {
            $sent = $this->provider->renewSession($this->session, $this->options->rpcSeconds);
            [$owner, $index] = $this->provider->owner($this->key, $this->options->rpcSeconds);
            $next = $sent + $this->ttlSeconds - $this->options->marginSeconds();
            if ($owner !== $this->session || $index !== $this->lockIndex || self::now() >= $oldDeadline || self::now() >= $next) {
                $this->currentState = LeaseState::Lost;
                return false;
            }
            $this->deadline = $next;
            return true;
        } catch (\Throwable $error) {
            $this->currentState = LeaseState::Lost;
            throw new BackendUnavailable('Consul session renewal could not be confirmed', 0, $error);
        }
    }

    public function release(): ReleaseOutcome
    {
        if ($this->outcome !== null) { return $this->outcome; }
        $this->currentState = LeaseState::Releasing;
        $outcome = ReleaseOutcome::Unknown;
        try {
            $released = $this->provider->request('PUT', '/v1/kv/' . $this->key . '?release=' . rawurlencode($this->session), '', $this->options->cleanupSeconds, true);
            if ($released === true) {
                $outcome = ReleaseOutcome::Released;
            } elseif ($released === false) {
                [$owner] = $this->provider->owner($this->key, $this->options->cleanupSeconds);
                $outcome = $owner === $this->session ? ReleaseOutcome::Unknown : ReleaseOutcome::AlreadyNotOwned;
            }
        } catch (\Throwable) {
        }
        try {
            $this->provider->request('PUT', '/v1/session/destroy/' . rawurlencode($this->session), null, $this->options->cleanupSeconds, true);
        } catch (\Throwable) {
            $outcome = ReleaseOutcome::Unknown;
        }
        $this->outcome = $outcome;
        $this->currentState = $outcome === ReleaseOutcome::Unknown ? LeaseState::ReleasedUnknown : LeaseState::Released;
        $this->provider->removeLease($this);
        if ($outcome !== ReleaseOutcome::Released) { $this->provider->observeCleanup($outcome, 'release_unconfirmed'); }
        return $outcome;
    }

    private static function now(): float { return hrtime(true) / 1_000_000_000; }
}
