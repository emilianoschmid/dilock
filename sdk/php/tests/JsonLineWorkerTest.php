<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\JsonLineWorker;
use PHPUnit\Framework\TestCase;

final class JsonLineWorkerTest extends TestCase
{
    public function testParsesAcquisitionAndManualRenewRequests(): void
    {
        self::assertSame(
            ['id' => 'a1', 'op' => 'acquire', 'resource' => 'orders', 'wait_ms' => 50, 'lease_ms' => 3000],
            JsonLineWorker::parseRequest('{"id":"a1","op":"acquire","resource":"orders","wait_ms":50,"lease_ms":3000}'),
        );
        self::assertSame(
            ['id' => 'r1', 'op' => 'renew', 'lease_id' => 'local-id'],
            JsonLineWorker::parseRequest('{"id":"r1","op":"renew","lease_id":"local-id"}'),
        );
    }

    public function testRejectsMalformedAndUnboundedRequestsWithoutEchoingInput(): void
    {
        foreach ([
            '{"id":"secret-token",',
            '{"id":"","op":"hello"}',
            '{"id":"x","op":"acquire","resource":"r"}',
            '{"id":"x","op":"try_acquire","resource":"r","wait_ms":-1}',
            '{"id":"x","op":"release","lease_id":"l","unknown":"secret"}',
        ] as $request) {
            try {
                JsonLineWorker::parseRequest($request);
                self::fail('Invalid request was accepted.');
            } catch (\InvalidArgumentException) {
                self::assertTrue(true);
            }
        }
        try {
            JsonLineWorker::parseRequest(str_repeat('x', 65_537));
            self::fail('Oversized request was accepted.');
        } catch (\InvalidArgumentException) {
            self::assertTrue(true);
        }
    }

    public function testEncodesExactlyOneUtf8JsonLine(): void
    {
        $line = JsonLineWorker::encodeResponse(['id' => 'ok', 'status' => 'acquired', 'resource' => 'café']);
        self::assertSame("\n", substr($line, -1));
        self::assertSame(['id' => 'ok', 'status' => 'acquired', 'resource' => 'café'], json_decode($line, true, 8, JSON_THROW_ON_ERROR));
        self::assertSame(1, substr_count($line, "\n"));
    }
}
