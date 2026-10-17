<?php

declare(strict_types=1);

namespace Dilock;

/** Epoch-scoped Consul LockIndex; safety requires retaining the lock KV key. */
final class ConsulFence
{
    public function __construct(public readonly string $epoch, public readonly int $lockIndex)
    {
        if ($epoch === '' || $lockIndex < 1) {
            throw new \InvalidArgumentException('Consul fence requires an epoch and positive LockIndex');
        }
    }
}
