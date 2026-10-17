<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\ConsulProvider;
use Dilock\Options;
use Dilock\ReleaseOutcome;
use PHPUnit\Framework\TestCase;

final class ConsulLiveTest extends TestCase
{
    public function testContentionRenewalAndMonotonicLockIndex(): void
    {
        $endpoint = getenv('DILOCK_CONSUL_ENDPOINT');
        $namespace = getenv('DILOCK_CONSUL_NAMESPACE');
        $epoch = getenv('DILOCK_CONSUL_EPOCH');
        if (!is_string($endpoint) || $endpoint === '' || !is_string($namespace) || $namespace === ''
            || !is_string($epoch) || $epoch === '') {
            self::markTestSkipped('set DILOCK_CONSUL_ENDPOINT, DILOCK_CONSUL_NAMESPACE and DILOCK_CONSUL_EPOCH');
        }

        $token = getenv('CONSUL_HTTP_TOKEN');
        $token = is_string($token) && $token !== '' ? $token : null;
        ConsulProvider::provisionNamespace($endpoint, $epoch, $namespace, $token);
        $provider = ConsulProvider::connect($endpoint, $epoch, $namespace, $token);
        try {
            $handle = $provider->lock('php/live/' . bin2hex(random_bytes(12)));
            $options = new Options(10.0, 2.0, 2.0, true);
            $first = $handle->tryAcquire($options);
            self::assertNotNull($first);
            self::assertSame($epoch, $first->fence()->epoch);
            self::assertGreaterThan(0, $first->lockIndex);
            self::assertNull($handle->tryAcquire($options));
            self::assertTrue($first->renew());
            self::assertSame(ReleaseOutcome::Released, $first->release());

            $next = $handle->tryAcquire($options);
            self::assertNotNull($next);
            self::assertGreaterThan($first->lockIndex, $next->lockIndex);
            self::assertSame(ReleaseOutcome::Released, $next->release());
        } finally {
            $provider->close();
        }
    }
}
