<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\Identity;
use Dilock\Options;
use Dilock\RedisEndpoint;
use Dilock\RedisLease;
use Dilock\RedisProvider;
use PHPUnit\Framework\TestCase;

/** Opt-in process/IPC check using the native PHP Redis provider and a real Redis authority. */
final class JsonLineWorkerRedisLiveTest extends TestCase
{
    public function testWorkerRetainsRedisLeaseThroughRenewalAndCleansUpOnShutdown(): void
    {
        $endpoint = getenv('DILOCK_TEST_REDIS_ENDPOINT');
        if (!is_string($endpoint) || $endpoint === '') {
            self::markTestSkipped('Set DILOCK_TEST_REDIS_ENDPOINT to run the Redis worker integration.');
        }
        if (!class_exists(\Redis::class)) {
            self::fail('DILOCK_TEST_REDIS_ENDPOINT is set, but ext-redis is not installed.');
        }

        $namespace = 'worker-' . bin2hex(random_bytes(12));
        $epoch = 'worker-' . bin2hex(random_bytes(12));
        $metadataKey = 'dilock:namespace:' . $namespace . ':meta';
        $metadata = json_encode([
            'protocol_major' => 1,
            'identity_scheme' => Identity::SCHEME,
            'profile' => 'redis-single-authority-v1',
            'coordination_epoch' => $epoch,
            'fixed_lease_ms' => null,
        ], JSON_THROW_ON_ERROR);
        $admin = $this->connect($endpoint);
        $worker = null;
        $pipes = [];
        $configPath = tempnam(sys_get_temp_dir(), 'dilock-php-redis-worker-');
        if (!is_string($configPath)) {
            $admin->close();
            self::fail('Could not allocate a temporary worker config path.');
        }
        $metadataCreated = false;
        $provider = null;
        try {
            // Never overwrite a namespace record; the short TTL bounds residue after forced termination.
            $metadataCreated = $admin->set($metadataKey, $metadata, ['nx', 'px' => 60_000]) === true;
            self::assertTrue($metadataCreated, 'Unique test namespace unexpectedly already exists.');
            file_put_contents($configPath, json_encode([
                'backend' => 'redis', 'endpoint' => $endpoint, 'namespace' => $namespace, 'epoch' => $epoch,
            ], JSON_THROW_ON_ERROR));

            $worker = $this->startWorker($configPath, $pipes);
            $hello = $this->request($pipes, ['id' => 'hello', 'op' => 'hello']);
            self::assertSame('ok', $hello['status']);
            self::assertSame('redis', $hello['backend']);
            self::assertContains('renewal', $hello['capabilities']);

            $resource = 'process-lifecycle-' . bin2hex(random_bytes(8));
            $acquired = $this->request($pipes, ['id' => 'acquire', 'op' => 'acquire', 'resource' => $resource,
                'wait_ms' => 1_000, 'lease_ms' => 5_000, 'rpc_ms' => 1_000]);
            self::assertSame('acquired', $acquired['status']);
            self::assertIsString($acquired['lease_id']);
            $leaseId = $acquired['lease_id'];
            self::assertSame('held', $this->request($pipes, ['id' => 'inspect', 'op' => 'inspect', 'lease_id' => $leaseId])['state']);
            self::assertSame('renewed', $this->request($pipes, ['id' => 'renew', 'op' => 'renew', 'lease_id' => $leaseId,
                'lease_ms' => 5_000])['status']);
            self::assertSame('released', $this->request($pipes, ['id' => 'release', 'op' => 'release', 'lease_id' => $leaseId])['status']);

            $shutdownResource = 'process-shutdown-' . bin2hex(random_bytes(8));
            $shutdownLease = $this->request($pipes, ['id' => 'shutdown-acquire', 'op' => 'acquire', 'resource' => $shutdownResource,
                'wait_ms' => 1_000, 'lease_ms' => 5_000, 'rpc_ms' => 1_000]);
            self::assertSame('acquired', $shutdownLease['status']);
            $shutdown = $this->request($pipes, ['id' => 'shutdown', 'op' => 'shutdown']);
            self::assertSame('ok', $shutdown['status']);
            self::assertSame(0, $shutdown['outstanding_cleanup_attempts']);
            $this->stopWorker($worker, $pipes);
            $worker = null;

            $provider = RedisProvider::connect($endpoint, $epoch, $namespace, new \Dilock\AcquireOptions(1.0, new Options(5.0, 1.0, 1.0)));
            $successor = $provider->lock($shutdownResource)->tryAcquire();
            self::assertInstanceOf(RedisLease::class, $successor, 'A new provider should acquire after worker shutdown cleanup.');
            self::assertSame('released', $successor->release()->value);
        } finally {
            $provider?->close();
            if (is_resource($worker)) {
                $this->stopWorker($worker, $pipes);
            }
            @unlink($configPath);
            try {
                if ($metadataCreated) {
                    // Compare before deleting so cleanup cannot remove another test's record.
                    $admin->eval(
                        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0",
                        [$metadataKey, $metadata],
                        1,
                    );
                }
            } finally {
                $admin->close();
            }
        }
    }

    /** @return \Redis */
    private function connect(string $endpoint): \Redis
    {
        $parsed = RedisEndpoint::parse($endpoint);
        $client = new \Redis();
        $host = ($parsed->tls ? 'tls://' : '') . $parsed->host;
        if (!$client->connect($host, $parsed->port, 0.5, null, 0, 0.5)) {
            self::fail('Could not connect the worker integration metadata client to Redis.');
        }
        $client->setOption(\Redis::OPT_READ_TIMEOUT, 0.5);
        if ($parsed->username !== null || $parsed->password !== null) {
            $auth = $parsed->username !== null
                ? [$parsed->username, $parsed->password ?? '']
                : ($parsed->password ?? '');
            if (!$client->auth($auth)) {
                self::fail('Worker integration metadata client authentication failed.');
            }
        }
        if ($parsed->database !== null && !$client->select($parsed->database)) {
            self::fail('Worker integration metadata client could not select the configured database.');
        }
        return $client;
    }

    /** @param array<int, resource> $pipes @return resource */
    private function startWorker(string $configPath, array &$pipes)
    {
        $command = [PHP_BINARY, dirname(__DIR__) . '/bin/dilock-worker', $configPath];
        $process = proc_open($command, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, dirname(__DIR__));
        if (!is_resource($process)) {
            self::fail('Could not start the PHP Redis worker process.');
        }
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);
        return $process;
    }

    /** @param array<int, resource> $pipes @param array<string, mixed> $request */
    private function send(array $pipes, array $request): void
    {
        fwrite($pipes[0], json_encode($request, JSON_THROW_ON_ERROR) . "\n");
        fflush($pipes[0]);
    }

    /** @param array<int, resource> $pipes @param array<string, mixed> $request @return array<string, mixed> */
    private function request(array $pipes, array $request, float $timeout = 3.0): array
    {
        $this->send($pipes, $request);
        $read = [$pipes[1]];
        $write = [];
        $except = [];
        $selected = stream_select($read, $write, $except, (int) $timeout, (int) (($timeout - (int) $timeout) * 1_000_000));
        self::assertSame(1, $selected, 'Worker response exceeded its deadline.');
        $line = fgets($pipes[1]);
        self::assertIsString($line);
        $decoded = json_decode($line, true, 16, JSON_THROW_ON_ERROR);
        self::assertIsArray($decoded);
        return $decoded;
    }

    /** @param resource $worker @param array<int, resource> $pipes */
    private function stopWorker($worker, array &$pipes): void
    {
        foreach ([0, 1, 2] as $index) {
            if (isset($pipes[$index]) && is_resource($pipes[$index])) {
                fclose($pipes[$index]);
            }
        }
        $status = proc_get_status($worker);
        if ($status['running']) {
            proc_terminate($worker);
        }
        proc_close($worker);
    }
}
