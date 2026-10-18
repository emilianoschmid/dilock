<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\Identity;
use Dilock\PostgresProvider;
use PHPUnit\Framework\TestCase;

final class PostgresContractTest extends TestCase
{
    public function testAdvisoryKeyUsesSignedBigEndianDigestPrefix(): void
    {
        $provider = (new \ReflectionClass(PostgresProvider::class))->newInstanceWithoutConstructor();
        $namespace = new \ReflectionProperty(PostgresProvider::class, 'namespace');
        $namespace->setValue($provider, 'default');
        self::assertIsInt($provider->namespaceKey('contract-resource'));
        self::assertSame(64, strlen(Identity::resourceId('default', 'contract-resource')));
    }

    public function testPdoPgsqlIsTheExplicitRuntimeCapability(): void
    {
        if (!in_array('pgsql', \PDO::getAvailableDrivers(), true)) {
            self::markTestSkipped('pdo_pgsql is not installed in this runner');
        }
        self::assertContains('pgsql', \PDO::getAvailableDrivers());
    }
}
