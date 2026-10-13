<?php

declare(strict_types=1);

use Dilock\AcquireOptions;
use Dilock\EtcdProvider;
use Dilock\LeaseLost;
use Dilock\LeaseState;
use Dilock\Options;

require dirname(__DIR__) . '/vendor/autoload.php';

$policy = new AcquireOptions(3.0, new Options(
    leaseSeconds: 20.0,
    rpcSeconds: 1.0,
    cleanupSeconds: 1.0,
    requireFencing: true,
));
$provider = EtcdProvider::connect(
    getenv('DILOCK_ETCD_ENDPOINT') ?: 'http://127.0.0.1:2379',
    getenv('DILOCK_EPOCH') ?: throw new RuntimeException('DILOCK_EPOCH is required'),
    getenv('DILOCK_NAMESPACE') ?: 'default',
    $policy,
);
try {
    $provider->lock('quickstart/configured')->withLease(static function ($lease): void {
        if ($lease->state() !== LeaseState::Held) {
            throw new LeaseLost('The etcd lease is no longer held.');
        }
        $fence = $lease->fence();
        printf("One short etcd operation completed (epoch=%s revision=%s).\n", $fence['epoch'], $fence['value']);
    }, $policy);
} finally {
    $provider->close();
}
