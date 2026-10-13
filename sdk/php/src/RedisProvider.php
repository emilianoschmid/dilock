<?php

declare(strict_types=1);

namespace Dilock;

/** Owns one Redis authority and independent command clients for normal work and cleanup. */
final class RedisProvider
{
    private bool $closed = false;
    /** @var array<int, RedisLease> */
    private array $leases = [];

    private function __construct(
        private readonly RedisCommands $commands,
        private readonly RedisCommands $cleanupCommands,
        public readonly string $namespace,
        public readonly string $epoch,
        public readonly AcquireOptions $defaults,
        private readonly ?\Closure $cleanupObserver,
    ) {
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
        $defaults ??= new AcquireOptions();
        if (!class_exists(\Redis::class)) {
            throw new \RuntimeException('Dilock Redis requires the phpredis extension (ext-redis)');
        }
        try {
            $normal = PhpRedisCommands::connect($endpoint, $defaults->lease->rpcSeconds);
        } catch (\InvalidArgumentException $error) {
            throw $error;
        } catch (\Throwable $error) {
            throw new BackendUnavailable('failed connecting to Redis authority', 0, $error);
        }
        try {
            $cleanup = PhpRedisCommands::connect($endpoint, $defaults->lease->cleanupSeconds);
            $provider = new self(
                $normal,
                $cleanup,
                $namespace,
                $epoch,
                $defaults,
                $cleanupObserver === null ? null : \Closure::fromCallable($cleanupObserver)
            );
            $provider->validateMetadata();
            return $provider;
        } catch (\Throwable $error) {
            $normal->close();
            if (isset($cleanup)) {
                $cleanup->close();
            }
            throw $error;
        }
    }

    public function lock(string $resource): RedisHandle
    {
        $this->ensureOpen();
        $id = Identity::resourceId($this->namespace, $resource);
        return new RedisHandle($this, 'dilock:{' . $id . '}:lease');
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
        $this->cleanupCommands->close();
        $this->commands->close();
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    public function observeCleanup(ReleaseOutcome $outcome, string $reason): void
    {
        $event = new CleanupObservation('redis', $outcome, $reason);
        try {
            if ($this->cleanupObserver !== null) {
                ($this->cleanupObserver)($event);
            } else {
                error_log(sprintf('dilock redis scope cleanup outcome=%s reason=%s', $outcome->value, $reason));
            }
        } catch (\Throwable) {
            // Diagnostics must not replace application exceptions or cleanup outcomes.
        }
    }

    public function removeLease(RedisLease $lease): void
    {
        unset($this->leases[spl_object_id($lease)]);
    }

    public function addLease(RedisLease $lease): void
    {
        $this->leases[spl_object_id($lease)] = $lease;
    }

    public function commands(): RedisCommands
    {
        $this->ensureOpen();
        return $this->commands;
    }

    public function cleanupCommands(): RedisCommands
    {
        return $this->cleanupCommands;
    }

    public function ensureOpen(): void
    {
        if ($this->closed) {
            throw new \LogicException('provider is closed');
        }
    }

    private function validateMetadata(): void
    {
        try {
            $raw = $this->commands->get('dilock:namespace:' . $this->namespace . ':meta');
        } catch (\Throwable $error) {
            throw new BackendUnavailable('failed reading Redis namespace metadata', 0, $error);
        }
        if (!is_string($raw) || strlen($raw) > 8192) {
            throw new ProtocolMismatch('Redis namespace metadata is missing or too large');
        }
        try {
            $metadata = json_decode($raw, true, 16, JSON_THROW_ON_ERROR);
        } catch (\JsonException $error) {
            throw new ProtocolMismatch('Redis namespace metadata is not valid JSON', 0, $error);
        }
        if (!is_array($metadata)
            || ($metadata['protocol_major'] ?? null) !== 1
            || ($metadata['identity_scheme'] ?? null) !== Identity::SCHEME
            || ($metadata['profile'] ?? null) !== 'redis-single-authority-v1'
            || ($metadata['coordination_epoch'] ?? null) !== $this->epoch
            || !array_key_exists('fixed_lease_ms', $metadata)
            || $metadata['fixed_lease_ms'] !== null) {
            throw new ProtocolMismatch('Redis namespace metadata is incompatible with draft 1');
        }
    }
}
