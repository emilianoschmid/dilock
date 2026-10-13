<?php

declare(strict_types=1);

namespace Dilock;

/** @internal Validate dynamic phpredis responses before classifying lock outcomes. */
final class RedisResponse
{
    public static function typeCode(mixed $value): int
    {
        if (!is_int($value)) {
            throw new \RedisException('Redis TYPE returned an invalid response type');
        }
        return $value;
    }

    public static function setNxPxResult(mixed $value): bool
    {
        if (!is_bool($value)) {
            throw new \RedisException('Redis SET returned an invalid response type');
        }
        return $value;
    }
}
