<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\AcquireOptions;
use Dilock\EtcdProvider;
use Dilock\Options;
use PHPUnit\Framework\TestCase;

/** Opt-in process/IPC check using the native PHP gRPC provider and a real etcd authority. */
final class JsonLineWorkerEtcdLiveTest extends TestCase
{
    public function testReaderRemainsResponsiveAndChildRetainsLeaseUntilRelease(): void
    {
        $endpoint = getenv('DILOCK_TEST_ETCD_ENDPOINT');
        $namespace = getenv('DILOCK_TEST_NAMESPACE');
        $epoch = getenv('DILOCK_TEST_EPOCH');
        if (!is_string($endpoint) || $endpoint === '' || !is_string($namespace) || $namespace === ''
            || !is_string($epoch) || $epoch === '') {
            self::markTestSkipped('Set the etcd endpoint and pre-provisioned namespace metadata for the worker integration.');
        }

        $policy = new Options(leaseSeconds: 5, rpcSeconds: 1, cleanupSeconds: 1);
        $provider = EtcdProvider::connect($endpoint, $epoch, $namespace, new AcquireOptions(6, $policy));
        $worker = null;
        $pipes = [];
        $configPath = tempnam(sys_get_temp_dir(), 'dilock-php-worker-');
        if (!is_string($configPath)) {
            self::fail('Could not allocate a temporary worker config path.');
        }
        file_put_contents($configPath, json_encode([
            'backend' => 'etcd', 'endpoint' => $endpoint, 'namespace' => $namespace, 'epoch' => $epoch,
        ], JSON_THROW_ON_ERROR));
        try {
            $resource = 'php-worker-wait-' . bin2hex(random_bytes(8));
            $holder = $provider->lock($resource)->acquire();
            $worker = $this->startWorker($configPath, $pipes);
            $busy = $this->request($pipes, ['id' => 'busy-try', 'op' => 'try_acquire', 'resource' => $resource,
                'lease_ms' => 5_000, 'rpc_ms' => 1_000]);
            self::assertSame('busy', $busy['status']);
            self::assertSame(
                'ok',
                $this->request($pipes, ['id' => 'hello-after-busy', 'op' => 'hello'])['status'],
                'The parent must continue reading after it removes a child that reported busy.'
            );
            $this->send($pipes, ['id' => 'waiter', 'op' => 'acquire', 'resource' => $resource,
                'wait_ms' => 5_000, 'lease_ms' => 5_000, 'rpc_ms' => 1_000]);
            $hello = $this->request($pipes, ['id' => 'hello-while-waiting', 'op' => 'hello'], 1.0);
            $diagnostics = isset($pipes[2]) ? stream_get_contents($pipes[2]) : '';
            self::assertSame('ok', $hello['status'], 'Parent must answer while the child blocks in etcd Lock: '
                . json_encode($hello) . '; worker diagnostics: ' . (is_string($diagnostics) ? $diagnostics : 'unavailable'));
            $holder->release();

            $acquired = $this->readResponse($pipes, 7.0);
            self::assertSame('waiter', $acquired['id']);
            self::assertSame('acquired', $acquired['status']);
            self::assertIsArray($acquired['fence'] ?? null);
            self::assertSame($epoch, $acquired['fence']['epoch']);
            self::assertIsString($acquired['lease_id']);

            $leaseId = $acquired['lease_id'];
            self::assertSame('held', $this->request($pipes, ['id' => 'inspect', 'op' => 'inspect', 'lease_id' => $leaseId])['state']);
            self::assertSame('renewed', $this->request($pipes, ['id' => 'renew', 'op' => 'renew', 'lease_id' => $leaseId, 'lease_ms' => 5_000])['status']);
            self::assertSame('released', $this->request($pipes, ['id' => 'release', 'op' => 'release', 'lease_id' => $leaseId])['status']);
            self::assertSame('released', $this->request($pipes, ['id' => 'release-again', 'op' => 'release', 'lease_id' => $leaseId])['status']);
            self::assertSame('ok', $this->request($pipes, ['id' => 'shutdown', 'op' => 'shutdown'])['status']);
            $this->stopWorker($worker, $pipes);
            $worker = null;

            $cancelResource = 'php-worker-cancel-' . bin2hex(random_bytes(8));
            $cancelHolder = $provider->lock($cancelResource)->acquire();
            $worker = $this->startWorker($configPath, $pipes);
            $this->send($pipes, ['id' => 'pending-acquire', 'op' => 'acquire', 'resource' => $cancelResource,
                'wait_ms' => 5_000, 'lease_ms' => 1_000, 'rpc_ms' => 200]);
            $this->send($pipes, ['id' => 'cancel', 'op' => 'cancel_request', 'target_id' => 'pending-acquire']);
            $cancelled = $this->readResponse($pipes, 2.0);
            $cancelAck = $this->readResponse($pipes, 2.0);
            self::assertSame(['id' => 'pending-acquire', 'status' => 'cancelled', 'remote_outcome_unknown' => true], $cancelled);
            self::assertSame('cancel_requested', $cancelAck['status']);
            self::assertTrue($cancelAck['found']);
            $cancelHolder->release();
            self::assertSame('ok', $this->request($pipes, ['id' => 'cancel-shutdown', 'op' => 'shutdown'])['status']);
            $this->stopWorker($worker, $pipes);
            $worker = null;
        } finally {
            $provider->close();
            if (is_resource($worker)) {
                $this->stopWorker($worker, $pipes);
            }
            @unlink($configPath);
        }
    }

    /** @param array<int, resource> $pipes @return resource */
    private function startWorker(string $configPath, array &$pipes)
    {
        $command = [PHP_BINARY, dirname(__DIR__) . '/bin/dilock-worker', $configPath];
        $process = proc_open($command, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes, dirname(__DIR__));
        if (!is_resource($process)) {
            self::fail('Could not start the PHP worker process.');
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
        return $this->readResponse($pipes, $timeout);
    }

    /** @param array<int, resource> $pipes @return array<string, mixed> */
    private function readResponse(array $pipes, float $timeout): array
    {
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
        if (isset($pipes[0]) && is_resource($pipes[0])) {
            fclose($pipes[0]);
        }
        foreach ([1, 2] as $index) {
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
