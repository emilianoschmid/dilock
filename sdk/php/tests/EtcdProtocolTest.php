<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\EtcdProvider;
use Dilock\AcquisitionUnknown;
use Dilock\ProtocolMismatch;
use Dilock\ReleaseOutcome;
use Dilock\WaitTimeout;
use Etcdserverpb\LeaseGrantResponse;
use Etcdserverpb\LeaseKeepAliveRequest;
use Etcdserverpb\LeaseKeepAliveResponse;
use Mvccpb\KeyValue;
use PHPUnit\Framework\TestCase;

final class EtcdProtocolTest extends TestCase
{
    public function testGrantedAndKeepaliveLeaseValuesRoundTripThroughProtobuf(): void
    {
        $grant = new LeaseGrantResponse(['ID' => 123, 'TTL' => 30]);
        $decodedGrant = new LeaseGrantResponse();
        $decodedGrant->mergeFromString($grant->serializeToString());

        self::assertSame(123, $decodedGrant->getID());
        self::assertSame(30, $decodedGrant->getTTL());

        $keepAlive = new LeaseKeepAliveRequest(['ID' => $decodedGrant->getID()]);
        $decodedRequest = new LeaseKeepAliveRequest();
        $decodedRequest->mergeFromString($keepAlive->serializeToString());
        self::assertSame(123, $decodedRequest->getID());

        $response = new LeaseKeepAliveResponse(['ID' => 123, 'TTL' => 29]);
        $decodedResponse = new LeaseKeepAliveResponse();
        $decodedResponse->mergeFromString($response->serializeToString());
        self::assertSame(123, $decodedResponse->getID());
        self::assertSame(29, $decodedResponse->getTTL());
    }

    public function testLeaseIntegerNormalizationRejectsZeroMalformedAndOverflowValues(): void
    {
        self::assertSame(1, EtcdProvider::positiveInt(1));
        self::assertSame(123, EtcdProvider::positiveInt('123'));
        self::assertNull(EtcdProvider::positiveInt(0));
        self::assertNull(EtcdProvider::positiveInt('0'));
        self::assertNull(EtcdProvider::positiveInt('-1'));
        self::assertNull(EtcdProvider::positiveInt('not-an-integer'));
        self::assertNull(EtcdProvider::positiveInt('9223372036854775808'));
    }

    public function testOwnershipEvidenceRequiresTheExactReturnedKeyLeaseAndPositiveInt64Revision(): void
    {
        self::assertSame('9223372036854775807', EtcdProvider::verifiedOwnershipRevision(
            '/dilock/locks/resource/123',
            '/dilock/locks/resource/123',
            '123',
            123,
            '9223372036854775807'
        ));

        foreach ([
            ['/dilock/locks/resource/123', '/dilock/locks/other/123', '123', '123', '7'],
            ['/dilock/locks/resource/123', '/dilock/locks/resource/123', '124', '123', '7'],
            ['/dilock/locks/resource/123', '/dilock/locks/resource/123', '123', '123', '0'],
            ['/dilock/locks/resource/123', '/dilock/locks/resource/123', '123', '123', '-1'],
            ['/dilock/locks/resource/123', '/dilock/locks/resource/123', '123', '123', '9223372036854775808'],
        ] as [$requested, $returned, $actualLease, $expectedLease, $revision]) {
            try {
                EtcdProvider::verifiedOwnershipRevision($requested, $returned, $actualLease, $expectedLease, $revision);
                self::fail('malformed or mismatched exact-key ownership evidence must fail closed');
            } catch (ProtocolMismatch) {
                self::assertTrue(true);
            }
        }
    }

    public function testCleanupBudgetNeverRestartsAfterDeadline(): void
    {
        self::assertSame(0.75, EtcdProvider::remainingBudget(10.0, 9.25));
        self::assertSame(0.0, EtcdProvider::remainingBudget(10.0, 10.0));
        self::assertSame(0.0, EtcdProvider::remainingBudget(10.0, 12.0));
    }

    public function testEtcdLockIsNotDispatchedAfterWaitDeadline(): void
    {
        self::assertSame(0.25, EtcdProvider::lockWaitBudget(10.0, 2.0, 9.75));
        try {
            EtcdProvider::lockWaitBudget(10.0, 2.0, 10.0);
            self::fail('late lease grant must not start a Lock RPC with a synthetic millisecond budget');
        } catch (WaitTimeout) {
            self::assertTrue(true);
        }
    }

    public function testOwnershipVerificationUsesRemainingWaitBudgetAndClassifiesOnlyConfirmedTimeouts(): void
    {
        self::assertSame(0.25, EtcdProvider::lockWaitBudget(10.0, 2.0, 9.75));
        $rpcTimeout = new WaitTimeout('Range RPC deadline elapsed');

        self::assertInstanceOf(
            WaitTimeout::class,
            EtcdProvider::ownershipVerificationTimeoutFailure($rpcTimeout, true, true)
        );
        self::assertInstanceOf(
            AcquisitionUnknown::class,
            EtcdProvider::ownershipVerificationTimeoutFailure($rpcTimeout, false, true)
        );
        self::assertInstanceOf(
            AcquisitionUnknown::class,
            EtcdProvider::ownershipVerificationTimeoutFailure($rpcTimeout, true, false)
        );
    }

    public function testEtcdProtocolMismatchRequiresAcknowledgedAttemptCleanup(): void
    {
        $mismatch = new ProtocolMismatch('invalid ownership response');

        self::assertInstanceOf(
            AcquisitionUnknown::class,
            EtcdProvider::ownershipVerificationFailure($mismatch, false)
        );
        self::assertSame(
            $mismatch,
            EtcdProvider::ownershipVerificationFailure($mismatch, true)
        );
    }

    public function testMalformedEtcdGrantRequiresUsableIdAndAcknowledgedCleanupForProtocolMismatch(): void
    {
        self::assertInstanceOf(
            AcquisitionUnknown::class,
            EtcdProvider::malformedGrantFailure(false, false)
        );
        self::assertInstanceOf(
            AcquisitionUnknown::class,
            EtcdProvider::malformedGrantFailure(true, false)
        );
        self::assertInstanceOf(
            ProtocolMismatch::class,
            EtcdProvider::malformedGrantFailure(true, true)
        );
    }

    public function testAbsentEtcdLockKeyIsUnknownWhenLeaseRevokeWasNotAcknowledged(): void
    {
        self::assertSame(ReleaseOutcome::Unknown, EtcdProvider::releaseVerificationOutcome(true, false));
        self::assertSame(ReleaseOutcome::AlreadyNotOwned, EtcdProvider::releaseVerificationOutcome(true, true));
        self::assertSame(ReleaseOutcome::Unknown, EtcdProvider::releaseVerificationOutcome(false, true));
    }

    public function testReleaseAcknowledgementDistinguishesUnlockFromLeaseRevoke(): void
    {
        self::assertSame(ReleaseOutcome::Released, EtcdProvider::releaseAcknowledgementOutcome(true, true, true, false));
        self::assertSame(ReleaseOutcome::AlreadyNotOwned, EtcdProvider::releaseAcknowledgementOutcome(true, true, false, true));
        self::assertSame(ReleaseOutcome::AlreadyNotOwned, EtcdProvider::releaseAcknowledgementOutcome(true, false, true, false));
        self::assertSame(ReleaseOutcome::AlreadyNotOwned, EtcdProvider::releaseAcknowledgementOutcome(false, false, true, false));
        self::assertSame(ReleaseOutcome::Unknown, EtcdProvider::releaseAcknowledgementOutcome(true, false, false, false));
        self::assertSame(ReleaseOutcome::Unknown, EtcdProvider::releaseAcknowledgementOutcome(false, true, false, false));
    }

    public function testNotFoundRevocationConfirmsAnAlreadyExpiredAttemptLease(): void
    {
        self::assertSame('acknowledged', EtcdProvider::releaseRevocationStatus(\Grpc\STATUS_OK));
        self::assertSame('not_found', EtcdProvider::releaseRevocationStatus(\Grpc\STATUS_NOT_FOUND));
        self::assertSame('unknown', EtcdProvider::releaseRevocationStatus(\Grpc\STATUS_DEADLINE_EXCEEDED));
    }

    public function testRangeKeyValueGeneratedMessageRoundTrips(): void
    {
        $range = new \Etcdserverpb\RangeResponse([
            'kvs' => [new KeyValue(['key' => '/dilock/lock/example', 'lease' => 123])],
        ]);
        $decoded = new \Etcdserverpb\RangeResponse();
        $decoded->mergeFromString($range->serializeToString());

        self::assertCount(1, $decoded->getKvs());
        self::assertSame('/dilock/lock/example', $decoded->getKvs()[0]->getKey());
        self::assertSame(123, $decoded->getKvs()[0]->getLease());
    }
}
