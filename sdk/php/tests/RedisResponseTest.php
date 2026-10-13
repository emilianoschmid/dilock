<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\RedisResponse;
use PHPUnit\Framework\TestCase;
use RedisException;

final class RedisResponseTest extends TestCase
{
    public function testTypeRequiresAnIntegerResponse(): void
    {
        self::assertSame(1, RedisResponse::typeCode(1));
        $this->expectException(RedisException::class);
        RedisResponse::typeCode('string');
    }

    public function testSetOutcomeRequiresTheBooleanContract(): void
    {
        self::assertTrue(RedisResponse::setNxPxResult(true));
        self::assertFalse(RedisResponse::setNxPxResult(false));
        $this->expectException(RedisException::class);
        RedisResponse::setNxPxResult('OK');
    }
}
