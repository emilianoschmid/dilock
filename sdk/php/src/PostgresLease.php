<?php

declare(strict_types=1);

namespace Dilock;

/** Owns the exact PDO session on which the PostgreSQL advisory lock was acquired. */
final class PostgresLease
{
    private LeaseState $currentState = LeaseState::Held;
    private ?ReleaseOutcome $outcome = null;

    public function __construct(
        private readonly PostgresProvider $provider,
        private ?\PDO $connection,
        private readonly string $resource,
        public readonly Options $options,
    ) {
    }

    public function state(): LeaseState { return $this->currentState; }
    public function ensureHeld(): void
    {
        if ($this->currentState !== LeaseState::Held || $this->connection === null) {
            throw new LeaseLost('PostgreSQL advisory-lock session is no longer available');
        }
    }
    public function renew(): bool
    {
        $this->ensureHeld();
        try {
            $this->connection->query('SELECT 1');
            return true;
        } catch (\Throwable $error) {
            $this->currentState = LeaseState::Lost;
            throw new BackendUnavailable('PostgreSQL advisory-lock liveness check failed', 0, $error);
        }
    }
    public function release(): ReleaseOutcome
    {
        if ($this->outcome !== null) {
            return $this->outcome;
        }
        $this->currentState = LeaseState::Releasing;
        try {
            if ($this->connection === null) {
                return $this->finish(ReleaseOutcome::Unknown);
            }
            $statement = $this->connection->prepare('SELECT pg_advisory_unlock(CAST(:key AS bigint)) AS released');
            $statement->execute([':key' => (string) $this->provider->namespaceKey($this->resource)]);
            $row = $statement->fetch(\PDO::FETCH_ASSOC);
            if (!is_array($row) || !array_key_exists('released', $row)) {
                return $this->finish(ReleaseOutcome::Unknown);
            }
            return $this->finish((bool) $row['released'] ? ReleaseOutcome::Released : ReleaseOutcome::AlreadyNotOwned);
        } catch (\Throwable) {
            return $this->finish(ReleaseOutcome::Unknown);
        }
    }

    private function finish(ReleaseOutcome $outcome): ReleaseOutcome
    {
        $this->outcome = $outcome;
        $this->currentState = $outcome === ReleaseOutcome::Unknown ? LeaseState::ReleasedUnknown : LeaseState::Released;
        $this->connection = null;
        $this->provider->removeLease($this);
        if ($outcome === ReleaseOutcome::Unknown) {
            $this->provider->observeCleanup($outcome, 'release_unconfirmed');
        }
        return $outcome;
    }
}
