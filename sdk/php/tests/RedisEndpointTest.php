<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\RedisEndpoint;
use PHPUnit\Framework\TestCase;

final class RedisEndpointTest extends TestCase
{
    public function testDefaultAndRootPathDoNotSelectADatabase(): void
    {
        foreach (['redis://localhost', 'redis://localhost/'] as $uri) {
            $endpoint = RedisEndpoint::parse($uri);
            self::assertSame('localhost', $endpoint->host);
            self::assertSame(6379, $endpoint->port);
            self::assertNull($endpoint->database);
            self::assertFalse($endpoint->tls);
        }
    }

    public function testNumericDatabasePathIsParsed(): void
    {
        $endpoint = RedisEndpoint::parse('rediss://user:p%40ss@example.test:6380/000');

        self::assertSame('example.test', $endpoint->host);
        self::assertSame(6380, $endpoint->port);
        self::assertTrue($endpoint->tls);
        self::assertSame('user', $endpoint->username);
        self::assertSame('p@ss', $endpoint->password);
        self::assertSame(0, $endpoint->database);
    }

    public function testPasswordOnlyAuthenticationUsesDefaultRedisUser(): void
    {
        $endpoint = RedisEndpoint::parse('redis://:secret@localhost');

        self::assertNull($endpoint->username);
        self::assertSame('secret', $endpoint->password);
    }

    public function testMalformedEndpointComponentsAreRejected(): void
    {
        foreach ([
            'http://localhost',
            'redis://localhost/not-a-database',
            'redis://localhost/999999999999999999999999999999999',
            'redis://localhost:70000',
            'redis://localhost?database=1',
        ] as $uri) {
            try {
                RedisEndpoint::parse($uri);
                self::fail('invalid Redis endpoint was accepted: ' . $uri);
            } catch (\InvalidArgumentException) {
                self::assertTrue(true);
            }
        }
    }
}
