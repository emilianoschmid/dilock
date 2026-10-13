<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\AcquireOptions;
use Dilock\AcquisitionUnknown;
use Dilock\BackendUnavailable;
use Dilock\CleanupError;
use Dilock\LeaseLost;
use Dilock\LeaseState;
use Dilock\Options;
use Dilock\ProtocolMismatch;
use Dilock\RedisCommands;
use Dilock\RedisLease;
use Dilock\RedisProvider;
use Dilock\ReleaseOutcome;
use Dilock\WaitTimeout;
use PHPUnit\Framework\TestCase;

final class RedisLifecycleTest extends TestCase
{
    public function testRedisRetryBackoffNeverSchedulesPastRemainingWaitBudget(): void
    {
        self::assertSame(7_500, Options::boundedRetryDelayMicroseconds(7_500, 20_000));
        self::assertSame(50_000, Options::boundedRetryDelayMicroseconds(100_000, 50_000));
        self::assertSame(0, Options::boundedRetryDelayMicroseconds(0, 50_000));
    }

    public function testOwnerCheckedReleaseAndSuccessorProtection(): void
    {
        $commands = new MemoryRedis();
        $provider = $this->provider($commands);
        $lock = $provider->lock('camera/1');
        $first = $lock->tryAcquire(new Options(2.0, 0.2, 0.2));
        if (!$first instanceof RedisLease) {
            self::fail('first acquisition failed');
        }
        self::assertNull($lock->tryAcquire(new Options(2.0, 0.2, 0.2)));
        $commands->expire('dilock:{' . \Dilock\Identity::resourceId('default', 'camera/1') . '}:lease');
        $second = $lock->tryAcquire(new Options(2.0, 0.2, 0.2));
        if (!$second instanceof RedisLease) {
            self::fail('successor acquisition failed');
        }
        self::assertSame(ReleaseOutcome::AlreadyNotOwned, $first->release());
        self::assertSame(LeaseState::Held, $second->state());
        self::assertSame(ReleaseOutcome::Released, $second->release());
        $provider->close();
    }

    public function testUnknownAcquisitionIsNotContention(): void
    {
        $commands = new MemoryRedis();
        $commands->failSetAfterCommit = true;
        $provider = $this->provider($commands);
        try {
            $provider->lock('uncertain')->tryAcquire(new Options(2.0, 0.2, 0.2));
            self::fail('ambiguous operation returned normally');
        } catch (AcquisitionUnknown) {
            self::assertTrue(true);
        }
    }

    public function testAcquireWaitBudgetExhaustionUsesWaitTimeout(): void
    {
        $commands = new MemoryRedis();
        $provider = $this->provider($commands);
        $handle = $provider->lock('wait-timeout-contract');
        $holder = $handle->tryAcquire(new Options(2.0, 0.2, 0.2));
        self::assertNotNull($holder);

        try {
            $handle->acquire(new AcquireOptions(0.04, new Options(2.0, 0.02, 0.2)));
            self::fail('confirmed contention must exhaust the caller wait budget');
        } catch (WaitTimeout) {
            self::assertSame(ReleaseOutcome::Released, $holder->release());
        } finally {
            $provider->close();
        }
    }

    public function testRedisTypeFailureIsBackendUnavailableNotProtocolMismatch(): void
    {
        $commands = new MemoryRedis();
        $provider = $this->provider($commands);
        $handle = $provider->lock('type-failure');
        $holder = $handle->tryAcquire(new Options(2.0, 0.2, 0.2));
        self::assertNotNull($holder);
        $commands->failType = true;

        try {
            $handle->tryAcquire(new Options(2.0, 0.2, 0.2));
            self::fail('TYPE backend failure was treated as a valid contention response');
        } catch (BackendUnavailable) {
            self::assertTrue(true);
        }

        $commands->failType = false;
        $holder->release();
        $provider->close();
    }

    public function testWithLeasePreservesWorkExceptionAndContainsObserverFailure(): void
    {
        $commands = new MemoryRedis();
        $commands->failRelease = true;
        $provider = $this->provider(
            $commands,
            new AcquireOptions(0.5, new Options(2.0, 0.2, 0.2)),
            static function (): never {
                throw new \LogicException('observer must not escape');
            }
        );
        $workError = new \DomainException('work failed');
        try {
            $provider->lock('scoped')->withLease(static function () use ($workError): never {
                throw $workError;
            });
            self::fail('work exception was swallowed');
        } catch (\DomainException $error) {
            self::assertSame($workError, $error);
        }
    }

    public function testWithLeaseSurfacesUnacknowledgedCleanupAfterSuccessfulWork(): void
    {
        $commands = new MemoryRedis();
        $commands->failRelease = true;
        $provider = $this->provider($commands);

        try {
            $provider->lock('scope-cleanup-unknown')->withLease(static fn (): string => 'completed');
            self::fail('successful scoped work must not hide an unknown release outcome');
        } catch (CleanupError) {
            self::assertSame(1, $commands->releaseCalls, 'cleanup is dispatched once');
        }
    }

    public function testWithLeaseStillAttemptsOwnerCleanupAfterLeaseWasLost(): void
    {
        $commands = new MemoryRedis();
        $provider = $this->provider($commands);
        $resource = 'scope-lost-lease-cleanup';
        $key = 'dilock:{' . \Dilock\Identity::resourceId('default', $resource) . '}:lease';

        try {
            $provider->lock($resource)->withLease(static function (RedisLease $lease) use ($commands, $key): string {
                $commands->expire($key);
                self::assertFalse($lease->renew(), 'missing owner makes renewal fail closed');
                self::assertSame(LeaseState::Lost, $lease->state());
                return 'completed';
            });
            self::fail('scope must not report success after release says the owner is gone');
        } catch (LeaseLost) {
            self::assertSame(1, $commands->releaseCalls, 'lost leases still attempt exact-owner cleanup');
        }
    }

    public function testUnexpectedReleaseReplyRemainsUnknown(): void
    {
        $commands = new MemoryRedis();
        $provider = $this->provider($commands);
        $commands->releaseReply = false;
        $lease = $provider->lock('unknown-release')->tryAcquire(new Options(2.0, 0.2, 0.2));
        self::assertNotNull($lease);
        self::assertSame(ReleaseOutcome::Unknown, $lease->release());
        self::assertSame(ReleaseOutcome::Unknown, $lease->release());
        self::assertSame(LeaseState::ReleasedUnknown, $lease->state());
        self::assertSame(1, $commands->releaseCalls, 'an ambiguous release must not be dispatched again');
    }

    public function testRedisAcquisitionSharesOneRpcBudgetAcrossSetAndContentionType(): void
    {
        $commands = new MemoryRedis();
        $provider = $this->provider($commands);
        $handle = $provider->lock('rpc-budget');
        $holder = $handle->tryAcquire(new Options(2.0, 0.2, 0.2));
        self::assertNotNull($holder);

        self::assertNull($handle->tryAcquire(new Options(2.0, 0.2, 0.2)));
        self::assertCount(2, $commands->setTimeouts);
        self::assertCount(1, $commands->typeTimeouts);
        self::assertGreaterThan(0.0, $commands->setTimeouts[1]);
        self::assertLessThanOrEqual(0.2, $commands->setTimeouts[1]);
        self::assertGreaterThan(0.0, $commands->typeTimeouts[0]);
        self::assertLessThan($commands->setTimeouts[1], $commands->typeTimeouts[0]);

        $holder->release();
        $provider->close();
    }

    public function testLateAcquisitionWithUnknownCleanupIsNotReportedAsWaitTimeout(): void
    {
        $commands = new MemoryRedis();
        $commands->setAcknowledgementDelayMicroseconds = 70_000;
        $commands->failRelease = true;
        $provider = $this->provider($commands);
        $handle = $provider->lock('late-acquisition-unknown-cleanup');

        try {
            $handle->acquire(new AcquireOptions(0.05, new Options(1.0, 0.04, 0.1)));
            self::fail('a late acquisition with unconfirmed cleanup must not be reported as ordinary timeout');
        } catch (AcquisitionUnknown) {
            self::assertSame(1, $commands->releaseCalls, 'late owner cleanup should be attempted exactly once');
        }

        $provider->close();
    }

    private function provider(MemoryRedis $commands, ?AcquireOptions $defaults = null, ?callable $observer = null): RedisProvider
    {
        $factory = \Closure::bind(static fn (): RedisProvider => new RedisProvider(
            $commands,
            $commands,
            'default',
            'test',
            $defaults ?? new AcquireOptions(),
            $observer === null ? null : \Closure::fromCallable($observer),
        ), null, RedisProvider::class);
        self::assertNotNull($factory);
        return $factory();
    }
}

/** Test-only command behavior; production always uses the phpredis adapter. */
final class MemoryRedis implements RedisCommands
{
    /** @var array<string, string> */
    private array $values = [];
    public bool $failSetAfterCommit = false;
    public bool $failType = false;
    public bool $failRelease = false;
    public mixed $releaseReply = null;
    public int $releaseCalls = 0;
    public int $setAcknowledgementDelayMicroseconds = 0;
    /** @var list<float> */
    public array $setTimeouts = [];
    /** @var list<float> */
    public array $typeTimeouts = [];

    public function expire(string $key): void
    {
        unset($this->values[$key]);
    }

    public function get(string $key): string|false
    {
        return $this->values[$key] ?? false;
    }
    public function type(string $key, float $timeoutSeconds): string
    {
        $this->typeTimeouts[] = $timeoutSeconds;
        if ($this->failType) {
            throw new \RuntimeException('simulated Redis TYPE failure');
        }
        return isset($this->values[$key]) ? 'string' : 'none';
    }
    public function setNxPx(string $key, string $owner, int $ttlMilliseconds, float $timeoutSeconds): bool
    {
        $this->setTimeouts[] = $timeoutSeconds;
        if (isset($this->values[$key])) {
            return false;
        }
        $this->values[$key] = $owner;
        // Simulate a scheduler stall after Redis committed but before PHP observes the reply.
        if ($this->setAcknowledgementDelayMicroseconds > 0) {
            usleep($this->setAcknowledgementDelayMicroseconds);
        }
        if ($this->failSetAfterCommit) {
            throw new \RuntimeException('simulated dropped SET acknowledgement');
        }
        return true;
    }
    public function releaseOwner(string $key, string $owner, float $timeoutSeconds): mixed
    {
        ++$this->releaseCalls;
        if ($this->failRelease) {
            throw new \RuntimeException('simulated dropped release acknowledgement');
        }
        if ($this->releaseReply !== null) {
            return $this->releaseReply;
        }
        if (($this->values[$key] ?? null) !== $owner) {
            return 0;
        }
        unset($this->values[$key]);
        return 1;
    }
    public function renewOwner(string $key, string $owner, int $ttlMilliseconds, float $timeoutSeconds): mixed
    {
        return ($this->values[$key] ?? null) === $owner ? 1 : 0;
    }
    public function close(): void
    {
    }
}
