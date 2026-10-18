<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\AcquireOptions;
use Dilock\PostgresProvider;
use Dilock\ReleaseOutcome;
use Dilock\Options;
use PHPUnit\Framework\TestCase;

final class PostgresLiveIntegrationTest extends TestCase
{
    public function testContentionRenewReleaseAndHandoffAgainstPostgres(): void
    {
        $dsn = getenv('DILOCK_POSTGRES_DSN');
        if (!is_string($dsn) || $dsn === '') {
            self::markTestSkipped('DILOCK_POSTGRES_DSN is not set');
        }

        $namespace = 'php' . bin2hex(random_bytes(6));
        $epoch = 'php-postgres-' . bin2hex(random_bytes(8));
        $resource = 'php-postgres-live-' . bin2hex(random_bytes(8));
        $username = getenv('DILOCK_POSTGRES_USER') ?: null;
        $password = getenv('DILOCK_POSTGRES_PASSWORD') ?: null;
        $leaseOptions = new Options(10.0, 1.0, 1.0);
        $acquisition = new AcquireOptions(5.0, $leaseOptions);

        self::assertSame('created', PostgresProvider::provisionNamespace(
            $dsn, $epoch, $namespace, $username, $password));
        $first = PostgresProvider::connect(
            $dsn, $epoch, $namespace, $username, $password, defaults: $acquisition);
        $second = PostgresProvider::connect(
            $dsn, $epoch, $namespace, $username, $password, defaults: $acquisition);
        try {
            $firstLease = $first->lock($resource)->tryAcquire($leaseOptions);
            self::assertNotNull($firstLease);
            self::assertNull($second->lock($resource)->tryAcquire($leaseOptions));
            self::assertTrue($firstLease->renew());
            self::assertSame(ReleaseOutcome::Released, $firstLease->release());

            $successor = $second->lock($resource)->tryAcquire($leaseOptions);
            self::assertNotNull($successor);
            self::assertSame(ReleaseOutcome::Released, $successor->release());
        } finally {
            $first->close();
            $second->close();
        }
    }
}
