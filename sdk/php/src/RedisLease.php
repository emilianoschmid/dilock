<?php

declare(strict_types=1);

namespace Dilock;

/** One owner generation. There is deliberately no destructor network I/O. */
final class RedisLease
{
    private LeaseState $currentState = LeaseState::Held;
    private ?ReleaseOutcome $outcome = null;
    private float $deadline;

    public function __construct(
        private readonly RedisProvider $provider,
        private readonly string $key,
        private readonly string $owner,
        public readonly Options $options,
        float $deadline,
    ) {
        $this->deadline = $deadline;
    }

    public function state(): LeaseState
    {
        if ($this->currentState === LeaseState::Held && self::now() >= $this->deadline) {
            $this->currentState = LeaseState::Lost;
        }
        return $this->currentState;
    }

    public function ensureHeld(): void
    {
        if ($this->state() !== LeaseState::Held) {
            throw new LeaseLost('lease no longer has a confirmed local validity budget');
        }
    }

    /** Remaining conservative local validity budget, not a remote ownership probe. */
    public function effectiveLeaseMilliseconds(): int
    {
        return max(0, (int) floor(($this->deadline - self::now()) * 1000));
    }

    public function renew(): bool
    {
        $this->ensureHeld();
        $sent = self::now();
        try {
            $renewed = $this->provider->commands()->renewOwner($this->key, $this->owner, $this->options->ttlMilliseconds(), $this->options->rpcSeconds);
        } catch (\Throwable $error) {
            $this->currentState = LeaseState::Lost;
            throw new BackendUnavailable('Redis renewal outcome is unknown', 0, $error);
        }
        $newDeadline = $sent + $this->options->leaseSeconds - $this->options->marginSeconds();
        if ($renewed !== 1 || self::now() >= $this->deadline || self::now() >= $newDeadline) {
            $this->currentState = LeaseState::Lost;
            return false;
        }
        $this->deadline = $newDeadline;
        return true;
    }

    public function release(): ReleaseOutcome
    {
        if ($this->outcome !== null) {
            return $this->outcome;
        }
        $this->currentState = LeaseState::Releasing;
        try {
            $deleted = $this->provider->cleanupCommands()->releaseOwner($this->key, $this->owner, $this->options->cleanupSeconds);
            $this->outcome = $deleted === 1 ? ReleaseOutcome::Released
                : ($deleted === 0 ? ReleaseOutcome::AlreadyNotOwned : ReleaseOutcome::Unknown);
        } catch (\Throwable) {
            $this->outcome = ReleaseOutcome::Unknown;
        }
        $this->currentState = $this->outcome === ReleaseOutcome::Unknown
            ? LeaseState::ReleasedUnknown : LeaseState::Released;
        $this->provider->removeLease($this);
        return $this->outcome ?? ReleaseOutcome::Unknown;
    }

    private static function now(): float
    {
        return hrtime(true) / 1_000_000_000;
    }
}
