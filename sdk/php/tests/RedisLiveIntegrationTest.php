<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\Identity;
use Dilock\Options;
use Dilock\RedisEndpoint;
use Dilock\RedisLease;
use Dilock\RedisProvider;
use Dilock\ReleaseOutcome;
use PHPUnit\Framework\TestCase;

/** Opt-in integration coverage against a real Redis authority and phpredis. */
final class RedisLiveIntegrationTest extends TestCase
{
    public function testIndependentProvidersContentionHandoffExpiryAndStaleRelease(): void
    {
        $endpoint = getenv('DILOCK_TEST_REDIS_ENDPOINT');
        if (!is_string($endpoint) || $endpoint === '') {
            self::markTestSkipped('Set DILOCK_TEST_REDIS_ENDPOINT to run live Redis integration.');
        }
        if (!class_exists(\Redis::class)) {
            self::fail('DILOCK_TEST_REDIS_ENDPOINT is set, but ext-redis is not installed.');
        }

        $namespace = 'it-' . bin2hex(random_bytes(16));
        $epoch = 'it-' . bin2hex(random_bytes(16));
        $metadataKey = 'dilock:namespace:' . $namespace . ':meta';
        $metadata = json_encode([
            'protocol_major' => 1,
            'identity_scheme' => Identity::SCHEME,
            'profile' => 'redis-single-authority-v1',
            'coordination_epoch' => $epoch,
            'fixed_lease_ms' => null,
        ], JSON_THROW_ON_ERROR);

        $admin = $this->connect($endpoint);
        $providerA = null;
        $providerB = null;
        try {
            // The TTL bounds residue if the test process is terminated before its finally block.
            if ($admin->set($metadataKey, $metadata, ['nx', 'px' => 60_000]) !== true) {
                self::fail('Unique test namespace unexpectedly already exists; refusing to overwrite metadata.');
            }
            $providerA = RedisProvider::connect($endpoint, $epoch, $namespace);
            $providerB = RedisProvider::connect($endpoint, $epoch, $namespace);
            $short = new Options(0.5, 0.1, 0.2);
            $long = new Options(4.0, 0.2, 0.2);

            $first = $providerA->lock('handoff')->tryAcquire($long);
            self::assertInstanceOf(RedisLease::class, $first, 'Provider A should acquire the free resource.');
            self::assertNull($providerB->lock('handoff')->tryAcquire($long), 'Provider B should observe confirmed contention.');
            self::assertSame(ReleaseOutcome::Released, $first->release());
            $successor = $providerB->lock('handoff')->tryAcquire($long);
            self::assertInstanceOf(RedisLease::class, $successor, 'Provider B should acquire after A releases.');
            self::assertSame(ReleaseOutcome::Released, $successor->release());

            $expiredOwner = $providerA->lock('expiry-and-stale-release')->tryAcquire($short);
            self::assertInstanceOf(RedisLease::class, $expiredOwner, 'Provider A should acquire the expiring resource.');
            usleep(700_000);
            self::assertNotSame('held', $expiredOwner->state()->value, 'The local validity budget should have elapsed.');
            $newOwner = $providerB->lock('expiry-and-stale-release')->tryAcquire($long);
            self::assertInstanceOf(RedisLease::class, $newOwner, 'Redis TTL expiry should let provider B acquire.');
            self::assertSame(ReleaseOutcome::AlreadyNotOwned, $expiredOwner->release(), 'A stale owner release must not delete the successor.');
            self::assertSame('held', $newOwner->state()->value, 'The successor should remain locally held after stale release.');
            self::assertSame(ReleaseOutcome::Released, $newOwner->release());
        } finally {
            // Each provider tracks leases and releases only with their owner token.
            try {
                $providerB?->close();
            } finally {
                try {
                    $providerA?->close();
                } finally {
                    try {
                        // Remove only our metadata value with a bounded Redis-side atomic compare.
                        $admin->setOption(\Redis::OPT_READ_TIMEOUT, 0.5);
                        $admin->eval(
                            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
                            [$metadataKey, $metadata],
                            1,
                        );
                    } finally {
                        $admin->close();
                    }
                }
            }
        }
    }

    private function connect(string $endpoint): \Redis
    {
        $parsed = RedisEndpoint::parse($endpoint);
        $client = new \Redis();
        $host = ($parsed->tls ? 'tls://' : '') . $parsed->host;
        if (!$client->connect($host, $parsed->port, 0.5, null, 0, 0.5)) {
            self::fail('Could not connect the integration metadata client to Redis.');
        }
        $client->setOption(\Redis::OPT_READ_TIMEOUT, 0.5);
        if (($parsed->username !== null || $parsed->password !== null)) {
            $auth = $parsed->username !== null
                ? [$parsed->username, $parsed->password ?? '']
                : ($parsed->password ?? '');
            if (!$client->auth($auth)) {
                self::fail('Redis integration metadata client authentication failed.');
            }
        }
        if ($parsed->database !== null && !$client->select($parsed->database)) {
            self::fail('Redis integration metadata client could not select the configured database.');
        }
        return $client;
    }
}
