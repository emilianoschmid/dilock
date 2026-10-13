<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\AcquireOptions;
use Dilock\EtcdLease;
use Dilock\EtcdProvider;
use Dilock\LeaseLost;
use Dilock\Options;
use Dilock\ReleaseOutcome;
use PHPUnit\Framework\TestCase;

/** Opt-in real etcd transport test; it never provisions or rewrites metadata. */
final class EtcdLiveIntegrationTest extends TestCase
{
    public function testNativeLockQueueRenewalAndFenceHandoff(): void
    {
        $endpoint = getenv('DILOCK_TEST_ETCD_ENDPOINT');
        $namespace = getenv('DILOCK_TEST_NAMESPACE');
        $epoch = getenv('DILOCK_TEST_EPOCH');
        if (!is_string($endpoint) || $endpoint === '' || !is_string($namespace) || $namespace === ''
            || !is_string($epoch) || $epoch === '') {
            self::markTestSkipped('Set DILOCK_TEST_ETCD_ENDPOINT, DILOCK_TEST_NAMESPACE and DILOCK_TEST_EPOCH for live etcd.');
        }

        $policy = new Options(leaseSeconds: 4, rpcSeconds: 1, cleanupSeconds: 1, requireFencing: true);
        $defaults = new AcquireOptions(waitSeconds: 8, lease: $policy);
        $firstProvider = EtcdProvider::connect($endpoint, $epoch, $namespace, $defaults);
        $secondProvider = null;
        $first = null;
        $successor = null;
        try {
            $secondProvider = EtcdProvider::connect($endpoint, $epoch, $namespace, $defaults);
            $resource = 'php-live-' . bin2hex(random_bytes(12));
            $firstLock = $firstProvider->lock($resource);
            $secondLock = $secondProvider->lock($resource);

            $first = $firstLock->acquire();
            self::assertSame($epoch, $first->fence()['epoch']);
            self::assertGreaterThan(0, (int) $first->fenceRevision);
            self::assertTrue($first->renew(), 'native LeaseKeepAlive must renew the confirmed lease');
            self::assertNull($secondLock->tryAcquire($policy), 'second provider must observe native lock contention');

            self::assertSame(ReleaseOutcome::Released, $first->release());
            $successor = $secondLock->acquire();
            self::assertSame($epoch, $successor->fence()['epoch']);
            self::assertGreaterThan((int) $first->fenceRevision, (int) $successor->fenceRevision);
            self::assertSame(ReleaseOutcome::Released, $successor->release());
        } finally {
            if ($successor !== null) {
                $successor->release();
            }
            if ($first !== null) {
                $first->release();
            }
            $secondProvider?->close();
            $firstProvider->close();
        }
    }

    public function testWithLeaseReportsLostOwnershipAfterItsEtcdLeaseExpires(): void
    {
        $endpoint = getenv('DILOCK_TEST_ETCD_ENDPOINT');
        $namespace = getenv('DILOCK_TEST_NAMESPACE');
        $epoch = getenv('DILOCK_TEST_EPOCH');
        if (!is_string($endpoint) || $endpoint === '' || !is_string($namespace) || $namespace === ''
            || !is_string($epoch) || $epoch === '') {
            self::markTestSkipped('Set DILOCK_TEST_ETCD_ENDPOINT, DILOCK_TEST_NAMESPACE and DILOCK_TEST_EPOCH for live etcd.');
        }

        // etcd lease TTL is integral seconds and includes the acquisition wait budget.
        // Keep the combined request below one second so the server grants a one-second lease.
        $leasePolicy = new Options(leaseSeconds: 0.1, rpcSeconds: 0.02, cleanupSeconds: 0.5, requireFencing: true);
        $acquisition = new AcquireOptions(waitSeconds: 0.1, lease: $leasePolicy);
        $provider = EtcdProvider::connect($endpoint, $epoch, $namespace, $acquisition);
        try {
            try {
                $provider->lock('php-lost-scope-' . bin2hex(random_bytes(12)))->withLease(
                    static function (EtcdLease $lease): void {
                        $remainingMilliseconds = $lease->effectiveLeaseMilliseconds();
                        self::assertGreaterThan(0, $remainingMilliseconds, 'the confirmed etcd lease should initially be locally valid');
                        usleep(($remainingMilliseconds + 100) * 1_000);
                        self::assertSame('lost', $lease->state()->value, 'local lease validity must expire during the scope');
                    },
                    $acquisition,
                );
                self::fail('scoped work must report that etcd ownership expired before cleanup');
            } catch (LeaseLost) {
                self::assertTrue(true);
            }
        } finally {
            $provider->close();
        }
    }

    public function testNaturalEtcdExpiryIsNotReportedAsAnExplicitRelease(): void
    {
        $endpoint = getenv('DILOCK_TEST_ETCD_ENDPOINT');
        $namespace = getenv('DILOCK_TEST_NAMESPACE');
        $epoch = getenv('DILOCK_TEST_EPOCH');
        if (!is_string($endpoint) || $endpoint === '' || !is_string($namespace) || $namespace === ''
            || !is_string($epoch) || $epoch === '') {
            self::markTestSkipped('Set DILOCK_TEST_ETCD_ENDPOINT, DILOCK_TEST_NAMESPACE and DILOCK_TEST_EPOCH for live etcd.');
        }

        // etcd grants integral-second leases. Keep acquisition below one second, then
        // let the returned lease expire without calling renew before release.
        $policy = new Options(leaseSeconds: 0.1, rpcSeconds: 0.02, cleanupSeconds: 1, requireFencing: true);
        $acquisition = new AcquireOptions(waitSeconds: 0.1, lease: $policy);
        $provider = EtcdProvider::connect($endpoint, $epoch, $namespace, $acquisition);
        $lease = null;
        try {
            $lease = $provider->lock('php-expired-release-' . bin2hex(random_bytes(12)))->acquire();
            $remainingMilliseconds = $lease->effectiveLeaseMilliseconds();
            self::assertGreaterThan(0, $remainingMilliseconds);
            // Allow etcd's lease-expiration sweep to run after the conservative local
            // deadline; the server's effective grant is rounded to whole seconds.
            usleep(($remainingMilliseconds + 1_000) * 1_000);

            self::assertSame('lost', $lease->state()->value);
            self::assertSame(ReleaseOutcome::AlreadyNotOwned, $lease->release());
        } finally {
            if ($lease !== null) {
                $lease->release();
            }
            $provider->close();
        }
    }
}
