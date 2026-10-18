<?php

declare(strict_types=1);

namespace Dilock;

/** PostgreSQL session-advisory-lock provider. A held lease owns its PDO session. */
final class PostgresProvider
{
    private bool $closed = false;
    /** @var array<int, PostgresLease> */
    private array $leases = [];

    private function __construct(
        private readonly string $dsn,
        private readonly ?string $username,
        private readonly ?string $password,
        public readonly string $namespace,
        public readonly string $epoch,
        public readonly AcquireOptions $defaults,
        private readonly ?\Closure $cleanupObserver,
    ) {
    }

    public static function connect(
        string $dsn,
        string $epoch,
        string $namespace = 'default',
        ?string $username = null,
        ?string $password = null,
        ?AcquireOptions $defaults = null,
        ?callable $cleanupObserver = null,
    ): self {
        self::validateConfiguration($dsn, $epoch, $namespace);
        if (!in_array('pgsql', \PDO::getAvailableDrivers(), true)) {
            throw new \RuntimeException('Dilock PostgreSQL requires the pdo_pgsql extension');
        }
        $provider = new self($dsn, $username, $password, $namespace, $epoch, $defaults ?? new AcquireOptions(),
            $cleanupObserver === null ? null : \Closure::fromCallable($cleanupObserver));
        $connection = $provider->open($provider->defaults->lease->rpcSeconds);
        try {
            $provider->validateMetadata($connection);
        } finally {
            $connection = null;
        }
        return $provider;
    }

    /** Provisioning is operator/setup work; runtime clients only validate metadata. */
    public static function provisionNamespace(
        string $dsn,
        string $epoch,
        string $namespace = 'default',
        ?string $username = null,
        ?string $password = null,
        float $timeoutSeconds = 2.0,
    ): string {
        self::validateConfiguration($dsn, $epoch, $namespace);
        if (!is_finite($timeoutSeconds) || $timeoutSeconds <= 0) {
            throw new \InvalidArgumentException('PostgreSQL timeout must be finite and positive');
        }
        $provider = new self($dsn, $username, $password, $namespace, $epoch, new AcquireOptions(), null);
        $connection = $provider->open($timeoutSeconds);
        try {
            $connection->beginTransaction();
            $connection->exec("SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))");
            $connection->exec('CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)');
            $insert = $connection->prepare('INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES (:namespace, 1, :identity_scheme, :profile, :epoch, NULL) ON CONFLICT (namespace) DO NOTHING');
            $insert->execute([':namespace' => $namespace, ':identity_scheme' => Identity::SCHEME, ':profile' => 'postgres-advisory-session-v1', ':epoch' => $epoch]);
            $created = $insert->rowCount() === 1;
            $connection->commit();
            $provider->validateMetadata($connection);
            return $created ? 'created' : 'compatible-existing';
        } catch (\PDOException $error) {
            if ($connection->inTransaction()) {
                $connection->rollBack();
            }
            throw new BackendUnavailable('failed provisioning PostgreSQL namespace metadata', 0, $error);
        } catch (\Throwable $error) {
            if ($connection->inTransaction()) {
                $connection->rollBack();
            }
            throw $error;
        } finally {
            $connection = null;
        }
    }

    public function lock(string $resource): PostgresHandle
    {
        $this->ensureOpen();
        return new PostgresHandle($this, $resource);
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        foreach (array_values($this->leases) as $lease) {
            $lease->release();
        }
        $this->closed = true;
    }

    public function ensureOpen(): void
    {
        if ($this->closed) {
            throw new \LogicException('provider is closed');
        }
    }

    public function addLease(PostgresLease $lease): void { $this->leases[spl_object_id($lease)] = $lease; }
    public function removeLease(PostgresLease $lease): void { unset($this->leases[spl_object_id($lease)]); }
    public function observeCleanup(ReleaseOutcome $outcome, string $reason): void
    {
        try {
            if ($this->cleanupObserver !== null) {
                ($this->cleanupObserver)(new CleanupObservation('postgresql', $outcome, $reason));
            }
        } catch (\Throwable) {
        }
    }

    public function open(float $timeoutSeconds): \PDO
    {
        try {
            $pdo = new \PDO($this->dsn, $this->username, $this->password, [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_TIMEOUT => max(1, (int) ceil($timeoutSeconds)),
                \PDO::ATTR_EMULATE_PREPARES => false,
            ]);
            return $pdo;
        } catch (\PDOException $error) {
            throw new BackendUnavailable('failed connecting to PostgreSQL authority', 0, $error);
        }
    }

    public function validateMetadata(\PDO $connection): void
    {
        try {
            $query = $connection->prepare('SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = :namespace');
            $query->execute([':namespace' => $this->namespace]);
            $row = $query->fetch(\PDO::FETCH_ASSOC);
        } catch (\PDOException $error) {
            throw new BackendUnavailable('failed reading PostgreSQL namespace metadata', 0, $error);
        }
        if (!is_array($row) || $query->fetch(\PDO::FETCH_ASSOC) !== false
            || (int) ($row['protocol_major'] ?? 0) !== 1
            || ($row['identity_scheme'] ?? null) !== Identity::SCHEME
            || ($row['profile'] ?? null) !== 'postgres-advisory-session-v1'
            || ($row['coordination_epoch'] ?? null) !== $this->epoch
            || ($row['fixed_lease_ms'] ?? null) !== null) {
            throw new ProtocolMismatch('PostgreSQL namespace metadata is incompatible with draft 1');
        }
    }

    public function namespaceKey(string $resource): int
    {
        $identity = Identity::resourceId($this->namespace, $resource);
        $parts = unpack('Nhigh/Nlow', hex2bin(substr($identity, 0, 16)));
        if (!is_array($parts)) {
            throw new \LogicException('could not decode PostgreSQL advisory key');
        }
        $high = (int) $parts['high'];
        $low = (int) $parts['low'];
        return ($high << 32) | $low;
    }

    private static function validateConfiguration(string $dsn, string $epoch, string $namespace): void
    {
        if ($dsn === '') {
            throw new \InvalidArgumentException('PostgreSQL DSN must be non-empty');
        }
        Identity::resourceId($namespace, 'validation');
        if ($epoch === '' || strlen($epoch) > 1024 || preg_match('//u', $epoch) !== 1 || str_contains($epoch, "\0")) {
            throw new \InvalidArgumentException('epoch must be nonempty UTF-8, at most 1024 bytes and contain no NUL');
        }
    }
}
