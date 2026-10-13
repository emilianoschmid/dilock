<?php

declare(strict_types=1);

namespace Dilock;

/** Small synchronous command surface used by the phpredis adapter and deterministic tests. */
interface RedisCommands
{
    public function get(string $key): string|false;
    /** Returns string, none, or other without exposing phpredis constants. */
    public function type(string $key, float $timeoutSeconds): string;
    public function setNxPx(string $key, string $owner, int $ttlMilliseconds, float $timeoutSeconds): bool;
    public function releaseOwner(string $key, string $owner, float $timeoutSeconds): mixed;
    public function renewOwner(string $key, string $owner, int $ttlMilliseconds, float $timeoutSeconds): mixed;
    public function close(): void;
}

/** Production adapter around the established phpredis client extension. */
final class PhpRedisCommands implements RedisCommands
{
    public function __construct(private readonly \Redis $redis, private readonly float $baseTimeout)
    {
    }

    public static function connect(string $endpoint, float $timeoutSeconds): self
    {
        $parsed = RedisEndpoint::parse($endpoint);
        if (!class_exists(\Redis::class)) {
            throw new \RuntimeException('Dilock Redis requires the phpredis extension (ext-redis)');
        }
        $host = ($parsed->tls ? 'tls://' : '') . $parsed->host;
        $redis = new \Redis();
        try {
            if (!$redis->connect($host, $parsed->port, $timeoutSeconds, null, 0, $timeoutSeconds)) {
                throw new \RedisException('Redis connection failed');
            }
            $redis->setOption(\Redis::OPT_READ_TIMEOUT, $timeoutSeconds);
            if ($parsed->username !== null || $parsed->password !== null) {
                $auth = $parsed->username !== null
                    ? [$parsed->username, $parsed->password ?? '']
                    : ($parsed->password ?? '');
                if (!$redis->auth($auth)) {
                    throw new \RedisException('Redis authentication failed');
                }
            }
            if ($parsed->database !== null && !$redis->select($parsed->database)) {
                throw new \RedisException('Redis database selection failed');
            }
            return new self($redis, $timeoutSeconds);
        } catch (\Throwable $error) {
            $redis->close();
            throw $error;
        }
    }

    public function get(string $key): string|false
    {
        $this->redis->clearLastError();
        $value = $this->redis->get($key);
        if ($value === false && $this->redis->getLastError() !== null) {
            $error = $this->redis->getLastError();
            $this->redis->clearLastError();
            throw new \RedisException('Redis GET failed: ' . (string) $error);
        }
        if (!is_string($value) && $value !== false) {
            throw new \RedisException('Redis GET returned an invalid response type');
        }
        return $value;
    }

    public function type(string $key, float $timeoutSeconds): string
    {
        return $this->withTimeout($timeoutSeconds, fn (): string => $this->readType($key));
    }

    private function readType(string $key): string
    {
        $this->redis->clearLastError();
        $type = $this->redis->type($key);
        if ($type === false || $this->redis->getLastError() !== null) {
            $error = $this->redis->getLastError();
            $this->redis->clearLastError();
            throw new \RedisException('Redis TYPE failed' . ($error === null ? '' : ': ' . (string) $error));
        }
        return match (RedisResponse::typeCode($type)) {
            \Redis::REDIS_STRING => 'string',
            \Redis::REDIS_NOT_FOUND => 'none',
            default => 'other',
        };
    }

    public function setNxPx(string $key, string $owner, int $ttlMilliseconds, float $timeoutSeconds): bool
    {
        return $this->withTimeout($timeoutSeconds, fn (): bool => $this->setNxPxChecked($key, $owner, $ttlMilliseconds));
    }

    private function setNxPxChecked(string $key, string $owner, int $ttlMilliseconds): bool
    {
        $this->redis->clearLastError();
        $created = $this->redis->set($key, $owner, ['nx', 'px' => $ttlMilliseconds]);
        if ($created === false && $this->redis->getLastError() !== null) {
            $error = $this->redis->getLastError();
            $this->redis->clearLastError();
            throw new \RedisException('Redis SET failed: ' . (string) $error);
        }
        return RedisResponse::setNxPxResult($created);
    }

    public function releaseOwner(string $key, string $owner, float $timeoutSeconds): mixed
    {
        $script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0";
        return $this->withTimeout($timeoutSeconds, fn (): mixed => $this->evalChecked($script, [$key, $owner]));
    }

    public function renewOwner(string $key, string $owner, int $ttlMilliseconds, float $timeoutSeconds): mixed
    {
        $script = "local ttl=tonumber(ARGV[2]); if not ttl or ttl~=math.floor(ttl) or ttl<=0 or ttl>86400000 then return redis.error_reply('DILOCK_INVALID_TTL') end; if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ttl) end return 0";
        return $this->withTimeout($timeoutSeconds, fn (): mixed => $this->evalChecked($script, [$key, $owner, (string) $ttlMilliseconds]));
    }

    public function close(): void
    {
        $this->redis->close();
    }

    /** @param array<int, string|int|float> $arguments */
    private function evalChecked(string $script, array $arguments): mixed
    {
        $this->redis->clearLastError();
        $result = $this->redis->eval($script, $arguments, 1);
        if ($result === false && $this->redis->getLastError() !== null) {
            $error = $this->redis->getLastError();
            $this->redis->clearLastError();
            throw new \RedisException('Redis script failed: ' . (string) $error);
        }
        return $result;
    }

    /**
     * @template T
     * @param callable(): T $operation
     * @return T
     */
    private function withTimeout(float $timeoutSeconds, callable $operation): mixed
    {
        $this->redis->setOption(\Redis::OPT_READ_TIMEOUT, $timeoutSeconds);
        try {
            return $operation();
        } finally {
            $this->redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->baseTimeout);
        }
    }
}
