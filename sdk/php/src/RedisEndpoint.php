<?php

declare(strict_types=1);

namespace Dilock;

/** @internal Parsed connection settings for the phpredis adapter. */
final class RedisEndpoint
{
    private function __construct(
        public readonly string $host,
        public readonly int $port,
        public readonly bool $tls,
        public readonly ?string $username,
        public readonly ?string $password,
        public readonly ?int $database,
    ) {
    }

    public static function parse(string $endpoint): self
    {
        $parts = parse_url($endpoint);
        if (!is_array($parts) || !isset($parts['scheme'], $parts['host'])
            || !in_array($parts['scheme'], ['redis', 'rediss'], true)
            || isset($parts['query']) || isset($parts['fragment'])) {
            throw new \InvalidArgumentException('endpoint must be a redis:// or rediss:// URI without query or fragment');
        }

        $database = null;
        $path = $parts['path'] ?? '';
        if ($path !== '' && $path !== '/') {
            if (preg_match('/\\A\\/([0-9]+)\\z/D', $path, $matches) !== 1) {
                throw new \InvalidArgumentException('Redis database path must be a numeric database index');
            }
            $digits = ltrim($matches[1], '0');
            $parsedDatabase = filter_var($digits === '' ? '0' : $digits, FILTER_VALIDATE_INT);
            if ($parsedDatabase === false || $parsedDatabase < 0) {
                throw new \InvalidArgumentException('Redis database index is outside the supported integer range');
            }
            $database = $parsedDatabase;
        }

        $port = $parts['port'] ?? 6379;
        if ($port < 1) {
            throw new \InvalidArgumentException('Redis port must be between 1 and 65535');
        }

        $username = isset($parts['user']) ? rawurldecode($parts['user']) : null;
        if ($username === '') {
            $username = null;
        }

        return new self(
            $parts['host'],
            $port,
            $parts['scheme'] === 'rediss',
            $username,
            isset($parts['pass']) ? rawurldecode($parts['pass']) : null,
            $database,
        );
    }
}
