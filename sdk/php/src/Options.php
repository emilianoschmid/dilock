<?php

declare(strict_types=1);

namespace Dilock;

/** Complete synchronous acquisition policy. Durations are finite seconds. */
final class Options
{
    public function __construct(
        public readonly float $leaseSeconds = 30.0,
        public readonly float $rpcSeconds = 2.0,
        public readonly float $cleanupSeconds = 2.0,
        public readonly bool $requireFencing = false,
    ) {
        foreach ([$leaseSeconds, $rpcSeconds, $cleanupSeconds] as $budget) {
            if (!is_finite($budget) || $budget <= 0) {
                throw new \InvalidArgumentException('budgets must be finite and positive');
            }
        }
        if ($leaseSeconds < 0.1 || $leaseSeconds > 86400 || $rpcSeconds >= $leaseSeconds - $this->marginSeconds()) {
            throw new \InvalidArgumentException('RPC must fit within a supported conservative lease budget');
        }
    }

    public function ttlMilliseconds(): int
    {
        return (int) floor($this->leaseSeconds * 1000);
    }

    public function marginSeconds(): float
    {
        return max(0.01, $this->leaseSeconds * 0.01);
    }

    /** @internal Cap a sampled retry delay to the remaining wait budget. */
    public static function boundedRetryDelayMicroseconds(int $remainingMicroseconds, int $sampleMicroseconds): int
    {
        if ($remainingMicroseconds <= 0) {
            return 0;
        }
        if ($sampleMicroseconds < 20_000 || $sampleMicroseconds > 50_000) {
            throw new \InvalidArgumentException('retry sample must be within 20000..50000 microseconds');
        }
        return min($remainingMicroseconds, $sampleMicroseconds);
    }
}

/** Immutable provider defaults: 5 second wait and a 30 second lease. */
final class AcquireOptions
{
    public function __construct(public readonly float $waitSeconds = 5.0, public readonly Options $lease = new Options())
    {
        if (!is_finite($waitSeconds) || $waitSeconds <= 0) {
            throw new \InvalidArgumentException('wait must be finite and positive');
        }
    }
}

enum ReleaseOutcome: string
{
    case Released = 'released';
    case AlreadyNotOwned = 'already_not_owned';
    case Unknown = 'release_unknown';
}

enum LeaseState: string
{
    case Held = 'held';
    case Releasing = 'releasing';
    case Released = 'released';
    case ReleasedUnknown = 'released_unknown';
    case Lost = 'lost';
}

final class CleanupObservation
{
    public function __construct(public readonly string $backend, public readonly ReleaseOutcome $outcome, public readonly string $reason)
    {
    }
}
