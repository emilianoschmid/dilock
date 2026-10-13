<?php

declare(strict_types=1);

use Dilock\AcquireOptions;
use Dilock\Options;
use Dilock\RedisProvider;

require dirname(__DIR__) . '/vendor/autoload.php';

$provider = RedisProvider::connect(
    getenv('DILOCK_REDIS_URL') ?: 'redis://127.0.0.1:6379',
    getenv('DILOCK_EPOCH') ?: throw new RuntimeException('DILOCK_EPOCH is required'),
    getenv('DILOCK_NAMESPACE') ?: 'default',
);
try {
    // Defaults: 5 seconds to acquire and a 30-second lease.
    $provider->lock('quickstart/defaults')->withLease(static function ($lease): void {
        $lease->ensureHeld();
        echo "One short Redis operation completed.\n";
    });

    // Complete policy: all durations are seconds; Redis does not provide strong fencing.
    $configured = new AcquireOptions(3.0, new Options(
        leaseSeconds: 20.0,
        rpcSeconds: 1.0,
        cleanupSeconds: 1.0,
    ));
    $provider->lock('quickstart/configured')->withLease(static function ($lease): void {
        $lease->ensureHeld();
        echo "One configured Redis operation completed.\n";
    }, $configured);
} finally {
    $provider->close();
}
