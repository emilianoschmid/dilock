<?php

declare(strict_types=1);

namespace Dilock;

/** Resource handle; every successful PostgreSQL acquisition receives a private PDO session. */
final class PostgresHandle
{
    public function __construct(private readonly PostgresProvider $provider, private readonly string $resource)
    {
    }

    public function tryAcquire(?Options $options = null): ?PostgresLease
    {
        $this->provider->ensureOpen();
        $options ??= $this->provider->defaults->lease;
        if ($options->requireFencing) {
            throw new UnsupportedCapability('PostgreSQL advisory-session does not provide fencing');
        }
        $connection = $this->provider->open($options->rpcSeconds);
        try {
            $statement = $connection->prepare('SELECT pg_try_advisory_lock(CAST(:key AS bigint)) AS acquired');
            $statement->execute([':key' => (string) $this->provider->namespaceKey($this->resource)]);
            $row = $statement->fetch(\PDO::FETCH_ASSOC);
            if (!is_array($row)) {
                throw new ProtocolMismatch('PostgreSQL advisory-lock response is malformed');
            }
            if (!(bool) ($row['acquired'] ?? false)) {
                $connection = null;
                return null;
            }
            $lease = new PostgresLease($this->provider, $connection, $this->resource, $options);
            $this->provider->addLease($lease);
            $connection = null;
            return $lease;
        } catch (UnsupportedCapability|ProtocolMismatch $error) {
            $connection = null;
            throw $error;
        } catch (\Throwable $error) {
            $connection = null;
            throw new BackendUnavailable('PostgreSQL advisory-lock acquisition failed', 0, $error);
        }
    }

    public function acquire(?AcquireOptions $acquisition = null, ?Options $options = null): PostgresLease
    {
        if ($acquisition !== null && $options !== null) {
            throw new \InvalidArgumentException('pass either complete acquisition options or lease options');
        }
        $acquisition ??= $this->provider->defaults;
        $options ??= $acquisition->lease;
        $deadline = self::now() + $acquisition->waitSeconds;
        do {
            $lease = $this->tryAcquire($options);
            if ($lease !== null) {
                return $lease;
            }
            $remaining = $deadline - self::now();
            if ($remaining > 0) {
                usleep(min((int) floor($remaining * 1_000_000), random_int(20_000, 50_000)));
            }
        } while (self::now() < $deadline);
        throw new WaitTimeout('PostgreSQL advisory-lock contention wait exhausted');
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
            $outcome = $lease->release();
            if ($workError === null && $outcome === ReleaseOutcome::Unknown) {
                throw new CleanupError('PostgreSQL advisory-lock cleanup was not acknowledged');
            }
            if ($workError === null && $outcome === ReleaseOutcome::AlreadyNotOwned) {
                throw new LeaseLost('PostgreSQL advisory-lock was no longer owned at scope exit');
            }
        }
    }

    private static function now(): float { return hrtime(true) / 1_000_000_000; }
}
