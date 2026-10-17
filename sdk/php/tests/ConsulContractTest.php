<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\ConsulFence;
use Dilock\ConsulProvider;
use Dilock\Options;
use PHPUnit\Framework\TestCase;

final class ConsulContractTest extends TestCase
{
    public function testConsulRequiresTheNativeSessionMinimumBeforeNetworkIo(): void
    {
        try {
            ConsulProvider::connect(
                'http://127.0.0.1:8500',
                'epoch-test',
                defaults: new \Dilock\AcquireOptions(1.0, new Options(5.0, 1.0, 1.0)),
            );
            self::fail('five-second Consul session TTL was accepted');
        } catch (\InvalidArgumentException $error) {
            self::assertStringContainsString('Consul session TTL', $error->getMessage());
        }
    }

    public function testFenceCarriesEpochAndPositiveLockIndex(): void
    {
        $fence = new ConsulFence('epoch-a', 17);
        self::assertSame('epoch-a', $fence->epoch);
        self::assertSame(17, $fence->lockIndex);
    }
}
