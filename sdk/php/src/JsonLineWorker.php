<?php

declare(strict_types=1);

namespace Dilock;

final class JsonLineWorkerChild
{
    /** @var resource */
    public $process;
    /** @var resource */
    public $stdin;
    /** @var resource */
    public $stdout;
    public string $buffer = '';
    /** @var array<string, bool> */
    public array $activeIds = [];
    /** @var array<string, string> */
    public array $expectedOps;
    public ?string $leaseId = null;
    public ?string $terminalId = null;

    /**
     * @param resource $process
     * @param resource $stdin
     * @param resource $stdout
     * @param array<string, string> $expectedOps
     */
    public function __construct($process, $stdin, $stdout, public string $requestId, array $expectedOps)
    {
        $this->process = $process;
        $this->stdin = $stdin;
        $this->stdout = $stdout;
        $this->expectedOps = $expectedOps;
    }
}

/**
 * Process-isolated JSON Lines adapter used only by the shared conformance harness.
 * @phpstan-type Request array{id: string, op: 'hello'|'acquire'|'try_acquire'|'renew'|'release'|'inspect'|'cancel_request'|'shutdown', resource?: string, lease_id?: string, target_id?: string, wait_ms?: int, lease_ms?: int, rpc_ms?: int, auto_renew?: bool}
 */
final class JsonLineWorker
{
    private const MAX_LINE_BYTES = 65_536;
    private const MAX_CHILDREN = 64;
    private bool $stopping = false;

    /**
     * @param array<string, mixed> $config
     */
    public function __construct(
        private readonly array $config,
        private readonly ?string $configPath = null,
    ) {
    }

    /** @return Request */
    public static function parseRequest(string $line): array
    {
        if (strlen($line) > self::MAX_LINE_BYTES) {
            throw new \InvalidArgumentException('request line exceeds 65536 bytes');
        }
        try {
            $request = json_decode($line, true, 32, JSON_THROW_ON_ERROR);
        } catch (\JsonException $error) {
            throw new \InvalidArgumentException('request is not valid JSON', 0, $error);
        }
        if (!is_array($request) || array_is_list($request)) {
            throw new \InvalidArgumentException('request must be a JSON object');
        }
        /** @var array<string, mixed> $request */
        $request = $request;
        $allowed = ['id', 'op', 'resource', 'lease_id', 'target_id', 'wait_ms', 'lease_ms', 'rpc_ms', 'auto_renew'];
        if (array_diff(array_keys($request), $allowed) !== []) {
            throw new \InvalidArgumentException('request contains an unknown field');
        }
        $id = $request['id'] ?? null;
        $op = $request['op'] ?? null;
        if (!is_string($id) || $id === '' || strlen($id) > 128 || !is_string($op)) {
            throw new \InvalidArgumentException('request requires a bounded string id and string op');
        }
        if (!in_array($op, ['hello', 'acquire', 'try_acquire', 'renew', 'release', 'inspect', 'cancel_request', 'shutdown'], true)) {
            throw new \InvalidArgumentException('request operation is unknown');
        }
        foreach (['resource', 'lease_id', 'target_id'] as $field) {
            if (array_key_exists($field, $request) && !is_string($request[$field])) {
                throw new \InvalidArgumentException($field . ' must be a string');
            }
        }
        foreach (['wait_ms' => 0, 'lease_ms' => 1, 'rpc_ms' => 1] as $field => $minimum) {
            if (array_key_exists($field, $request)
                && (!is_int($request[$field]) || $request[$field] < $minimum || $request[$field] > 86_400_000)) {
                throw new \InvalidArgumentException($field . ' must be a bounded integer');
            }
        }
        if (array_key_exists('auto_renew', $request) && !is_bool($request['auto_renew'])) {
            throw new \InvalidArgumentException('auto_renew must be a boolean');
        }
        if (in_array($op, ['acquire', 'try_acquire'], true)
            && (!isset($request['resource']) || $request['resource'] === '')) {
            throw new \InvalidArgumentException('acquisition requires a nonempty resource');
        }
        if (in_array($op, ['release', 'inspect', 'renew'], true)
            && (!isset($request['lease_id']) || $request['lease_id'] === '')) {
            throw new \InvalidArgumentException($op . ' requires a nonempty lease_id');
        }
        if ($op === 'cancel_request' && (!isset($request['target_id']) || $request['target_id'] === '')) {
            throw new \InvalidArgumentException('cancel_request requires a nonempty target_id');
        }
        if ($op === 'acquire' && !array_key_exists('wait_ms', $request)) {
            throw new \InvalidArgumentException('acquire requires wait_ms');
        }
        /** @var array<string, mixed> $normalized */
        $normalized = ['id' => $id, 'op' => $op];
        foreach (['resource', 'lease_id', 'target_id'] as $field) {
            if (isset($request[$field]) && is_string($request[$field])) {
                $normalized[$field] = $request[$field];
            }
        }
        foreach (['wait_ms', 'lease_ms', 'rpc_ms'] as $field) {
            if (isset($request[$field]) && is_int($request[$field])) {
                $normalized[$field] = $request[$field];
            }
        }
        if (array_key_exists('auto_renew', $request)) {
            /** @var bool $autoRenew */
            $autoRenew = $request['auto_renew'];
            $normalized['auto_renew'] = $autoRenew;
        }
        /** @var Request $normalized */
        return $normalized;
    }

    /** @param array<string, mixed> $response */
    public static function encodeResponse(array $response): string
    {
        return json_encode($response, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES) . "\n";
    }

    /**
     * @param resource $input
     * @param resource $output
     */
    public function run($input, $output): void
    {
        if (!function_exists('proc_open')) {
            throw new \RuntimeException('PHP conformance worker requires proc_open');
        }
        stream_set_blocking($input, false);
        /** @var array<int, JsonLineWorkerChild> $children */
        $children = [];
        /** @var array<string, int> $leases */
        $leases = [];
        /** @var array<string, int> $pending */
        $pending = [];
        /** @var array<string, bool> $active */
        $active = [];
        $inputBuffer = '';
        $discardLine = false;
        $eof = false;
        try {
            while ((!$eof && !$this->stopping) || $children !== []) {
                /** @var resource[] $read */
                $read = [$input];
                foreach ($children as $child) {
                    $read[] = $child->stdout;
                }
                /** @var resource[] $write */
                $write = [];
                /** @var resource[] $except */
                $except = [];
                $selected = @stream_select($read, $write, $except, 0, 200_000);
                if ($selected === false) {
                    throw new \RuntimeException('worker select failed');
                }
                foreach ($read as $ready) {
                    if ($ready === $input) {
                        $chunk = fread($input, 16_384);
                        if ($chunk === false || ($chunk === '' && feof($input))) {
                            $eof = true;
                            continue;
                        }
                        if ($chunk === '') {
                            continue;
                        }
                        for ($offset = 0, $length = strlen($chunk); $offset < $length; ++$offset) {
                            $byte = $chunk[$offset];
                            if ($byte === "\n") {
                                if (!$discardLine) {
                                    $this->acceptLine(rtrim($inputBuffer, "\r"), $output, $children, $leases, $pending, $active);
                                } else {
                                    $this->write($output, ['id' => '', 'status' => 'invalid_argument']);
                                }
                                $inputBuffer = '';
                                $discardLine = false;
                            } elseif (!$discardLine) {
                                $inputBuffer .= $byte;
                                if (strlen($inputBuffer) > self::MAX_LINE_BYTES) {
                                    $inputBuffer = '';
                                    $discardLine = true;
                                }
                            }
                        }
                        continue;
                    }
                    $childKey = get_resource_id($ready);
                    if (!isset($children[$childKey])) {
                        continue;
                    }
                    $chunk = fread($ready, 16_384);
                    if ($chunk === false) {
                        $chunk = '';
                    }
                    if ($chunk !== '') {
                        $children[$childKey]->buffer .= $chunk;
                        if (strlen($children[$childKey]->buffer) > self::MAX_LINE_BYTES * 2) {
                            $this->finishChild($childKey, $children, $leases, $pending, $active);
                            continue;
                        }
                        while (isset($children[$childKey])
                            && ($newline = strpos($children[$childKey]->buffer, "\n")) !== false) {
                            $line = substr($children[$childKey]->buffer, 0, $newline);
                            $children[$childKey]->buffer = substr($children[$childKey]->buffer, $newline + 1);
                            try {
                                $response = json_decode($line, true, 32, JSON_THROW_ON_ERROR);
                            } catch (\Throwable) {
                                $response = ['id' => '', 'status' => 'internal_error'];
                            }
                            if (!is_array($response) || !is_string($response['id'] ?? null)) {
                                $response = ['id' => '', 'status' => 'internal_error'];
                            }
                            $responseId = is_string($response['id'] ?? null) ? $response['id'] : '';
                            /** @var array<string, mixed> $response */
                            $response = $response;
                            $expected = $children[$childKey]->expectedOps[$responseId] ?? null;
                            $validStatuses = match ($expected) {
                                'acquire', 'try_acquire' => ['acquired', 'busy', 'timeout', 'cancelled', 'acquisition_unknown', 'backend_unavailable', 'invalid_argument', 'unsupported_capability', 'protocol_mismatch', 'internal_error'],
                                'release' => ['released', 'already_not_owned', 'release_unknown', 'invalid_argument', 'backend_unavailable', 'unsupported_capability', 'internal_error'],
                                'inspect' => ['ok', 'invalid_argument', 'backend_unavailable', 'unsupported_capability', 'internal_error'],
                                'renew' => ['renewed', 'lost', 'invalid_argument', 'backend_unavailable', 'unsupported_capability', 'internal_error'],
                                'shutdown' => ['ok'],
                                default => [],
                            };
                            if ($expected === null || !in_array($response['status'] ?? null, $validStatuses, true)) {
                                foreach ($children[$childKey]->expectedOps as $expectedId => $expectedOp) {
                                    $failure = match ($expectedOp) {
                                        'acquire', 'try_acquire' => ['status' => 'acquisition_unknown', 'remote_outcome_unknown' => true],
                                        'release' => ['status' => 'release_unknown'],
                                        default => ['status' => 'backend_unavailable'],
                                    };
                                    $this->write($output, ['id' => $expectedId] + $failure);
                                    unset($pending[$expectedId], $active[$expectedId]);
                                }
                                $this->finishChild($childKey, $children, $leases, $pending, $active);
                                continue;
                            }
                            $response = $this->sanitizeChildResponse($responseId, $response);
                            if ($response === null) {
                                $operation = $expected;
                                $failure = match ($operation) {
                                    'acquire', 'try_acquire' => ['status' => 'acquisition_unknown', 'remote_outcome_unknown' => true],
                                    'release' => ['status' => 'release_unknown'],
                                    default => ['status' => 'backend_unavailable'],
                                };
                                $this->write($output, ['id' => $responseId] + $failure);
                                unset($pending[$responseId], $active[$responseId]);
                                $this->finishChild($childKey, $children, $leases, $pending, $active);
                                continue;
                            }
                            /** @var array<string, mixed> $safeResponse */
                            $safeResponse = $response;
                            $this->write($output, $safeResponse);
                            unset($children[$childKey]->expectedOps[$responseId], $children[$childKey]->activeIds[$responseId]);
                            if (($response['status'] ?? null) === 'acquired' && is_string($response['lease_id'] ?? null)) {
                                $leaseId = $response['lease_id'];
                                $children[$childKey]->leaseId = $leaseId;
                                $leases[$leaseId] = $childKey;
                                unset($pending[$responseId]);
                                unset($active[$responseId]);
                            } elseif (isset($pending[$responseId])) {
                                unset($pending[$responseId], $active[$responseId]);
                                $this->finishChild($childKey, $children, $leases, $pending, $active);
                            } elseif (($children[$childKey]->terminalId ?? null) === $responseId) {
                                $this->finishChild($childKey, $children, $leases, $pending, $active);
                            } else {
                                unset($active[$responseId]);
                            }
                        }
                    }
                    if (isset($children[$childKey]) && feof($ready)) {
                        foreach ($children[$childKey]->expectedOps as $requestId => $operation) {
                            $failure = match ($operation) {
                                'acquire', 'try_acquire' => ['status' => 'acquisition_unknown', 'remote_outcome_unknown' => true],
                                'release' => ['status' => 'release_unknown'],
                                default => ['status' => 'backend_unavailable'],
                            };
                            $this->write($output, ['id' => $requestId] + $failure);
                            unset($pending[$requestId], $active[$requestId]);
                        }
                        if (isset($children[$childKey]->leaseId) && !isset($children[$childKey]->terminalId)) {
                            $this->write($output, ['event' => 'lease_lost', 'lease_id' => $children[$childKey]->leaseId,
                                'reason' => 'lease_process_exited_before_confirmed_cleanup']);
                        }
                        $this->finishChild($childKey, $children, $leases, $pending, $active);
                    }
                }
                if ($eof && $inputBuffer !== '') {
                    if (!$discardLine) {
                        $this->acceptLine($inputBuffer, $output, $children, $leases, $pending, $active);
                    } else {
                        $this->write($output, ['id' => '', 'status' => 'invalid_argument']);
                    }
                    $inputBuffer = '';
                }
                if ($eof && $children !== []) {
                    $this->shutdownChildren($children, $leases, $pending, $active);
                }
            }
        } finally {
            foreach (array_keys($children) as $childKey) {
                $this->killChild($childKey, $children, $leases, $pending, $active);
            }
        }
    }

    /**
     * @param resource $output
     * @param array<int, JsonLineWorkerChild> $children
     * @param array<string, int> $leases
     * @param array<string, int> $pending
     * @param array<string, bool> $active
     * @param-out array<int, JsonLineWorkerChild> $children
     * @param-out array<string, int> $leases
     * @param-out array<string, int> $pending
     * @param-out array<string, bool> $active
     */
    private function acceptLine(string $line, $output, array &$children, array &$leases, array &$pending, array &$active): void
    {
        try {
            $request = self::parseRequest($line);
        } catch (\Throwable) {
            $this->write($output, ['id' => '', 'status' => 'invalid_argument']);
            return;
        }
        $id = $request['id'];
        if (isset($active[$id])) {
            $this->write($output, ['id' => $id, 'status' => 'invalid_argument']);
            return;
        }
        $active[$id] = true;
        if ($request['op'] === 'hello') {
            $backend = $this->backend();
            $capabilities = [$backend, 'renewal'];
            if ($backend === 'etcd') {
                $capabilities[] = 'fencing';
            }
            $client = $backend === 'redis'
                ? (phpversion('redis') ?: 'unknown phpredis version')
                : 'grpc/grpc ' . (defined('Grpc\\VERSION') ? constant('Grpc\\VERSION') : 'installed') . ', google/protobuf installed';
            $this->write($output, ['id' => $id, 'status' => 'ok', 'sdk' => 'php', 'version' => '0.1.0-dev',
                'protocol' => 1, 'backend' => $backend, 'client' => $client, 'server_version' => 'unknown', 'capabilities' => $capabilities]);
            unset($active[$id]);
            return;
        }
        if ($request['op'] === 'cancel_request') {
            $target = $request['target_id'] ?? '';
            $childKey = $pending[$target] ?? null;
            if ($childKey === null || !isset($children[$childKey])) {
                $this->write($output, ['id' => $id, 'status' => 'cancel_requested', 'found' => false]);
                unset($active[$id]);
                return;
            }
            $originalId = $children[$childKey]->requestId;
            $this->killChild($childKey, $children, $leases, $pending, $active);
            $this->write($output, ['id' => $originalId, 'status' => 'cancelled', 'remote_outcome_unknown' => true]);
            unset($active[$originalId]);
            $this->write($output, ['id' => $id, 'status' => 'cancel_requested', 'found' => true]);
            unset($active[$id]);
            return;
        }
        if ($request['op'] === 'shutdown') {
            foreach ($pending as $pendingId => $_childKey) {
                $this->write($output, ['id' => $pendingId, 'status' => 'cancelled', 'remote_outcome_unknown' => true]);
                unset($active[$pendingId]);
            }
            foreach ($children as $childKey => $child) {
                foreach ($child->expectedOps as $pendingId => $operation) {
                    if (in_array($operation, ['acquire', 'try_acquire'], true)) {
                        continue;
                    }
                    $status = match ($operation) {
                        'release' => 'release_unknown',
                        'renew', 'inspect' => 'backend_unavailable',
                        default => 'internal_error',
                    };
                    $this->write($output, ['id' => $pendingId, 'status' => $status]);
                    unset($active[$pendingId]);
                }
                $children[$childKey]->expectedOps = [];
            }
            $outstanding = $this->shutdownChildren($children, $leases, $pending, $active);
            $this->write($output, ['id' => $id, 'status' => 'ok', 'outstanding_cleanup_attempts' => $outstanding]);
            $children = [];
            $this->stopping = true;
            unset($active[$id]);
            return;
        }
        if (in_array($request['op'], ['acquire', 'try_acquire'], true)) {
            if (count($children) >= self::MAX_CHILDREN) {
                $this->write($output, ['id' => $id, 'status' => 'internal_error']);
                unset($active[$id]);
                return;
            }
            try {
                $child = $this->spawnChild($request);
            } catch (\Throwable $error) {
                $this->write($output, $this->errorResponse($id, $error));
                unset($active[$id]);
                return;
            }
            $childKey = get_resource_id($child->stdout);
            $child->activeIds[$id] = true;
            /** @var JsonLineWorkerChild $child */
            $children[$childKey] = $child;
            $pending[$id] = $childKey;
            return;
        }
        $childKey = $leases[$request['lease_id'] ?? ''] ?? null;
        if ($childKey === null || !isset($children[$childKey])) {
            $this->write($output, ['id' => $id, 'status' => 'invalid_argument']);
            unset($active[$id]);
            return;
        }
        if ($children[$childKey]->expectedOps !== []) {
            $this->write($output, ['id' => $id, 'status' => 'internal_error']);
            unset($active[$id]);
            return;
        }
        $children[$childKey]->activeIds[$id] = true;
        $children[$childKey]->expectedOps[$id] = $request['op'];
        $this->writeChild($children[$childKey]->stdin, $request);
    }

    /**
     * @param Request $request
     * @return JsonLineWorkerChild
     */
    private function spawnChild(array $request): JsonLineWorkerChild
    {
        if ($this->configPath === null || !is_file($this->configPath)) {
            throw new \RuntimeException('a readable config path is required for isolated lease processes');
        }
        $entrypoint = dirname(__DIR__) . '/bin/dilock-worker';
        $descriptors = [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['file', 'php://stderr', 'a']];
        $pipes = [];
        $process = proc_open([PHP_BINARY, $entrypoint, '--lease-child', $this->configPath], $descriptors, $pipes, dirname(__DIR__));
        if (!is_resource($process)) {
            throw new \RuntimeException('could not start isolated lease process');
        }
        if (!isset($pipes[0], $pipes[1]) || !is_resource($pipes[0]) || !is_resource($pipes[1])) {
            proc_terminate($process, 9);
            proc_close($process);
            throw new \RuntimeException('isolated lease process pipes were unavailable');
        }
        stream_set_blocking($pipes[0], false);
        stream_set_blocking($pipes[1], false);
        $child = new JsonLineWorkerChild($process, $pipes[0], $pipes[1], $request['id'], [$request['id'] => $request['op']]);
        $this->writeChild($child->stdin, $request);
        return $child;
    }

    /**
     * @param resource $input
     * @param resource $output
     */
    public function runLeaseChild($input, $output): void
    {
        $socket = $output;
        $request = $this->childRead($input);
        if ($request === null) {
            return;
        }
        if (!in_array($request['op'], ['acquire', 'try_acquire'], true) || !isset($request['resource'])) {
            $this->childWrite($socket, ['id' => $request['id'], 'status' => 'invalid_argument']);
            return;
        }
        $provider = null;
        $lease = null;
        try {
            if (($request['auto_renew'] ?? false) === true) {
                throw new UnsupportedCapability('PHP does not provide background auto-renewal');
            }
            $leaseMs = $request['lease_ms'] ?? $this->integerConfig('lease_ms', 30_000, 1, 86_400_000);
            $rpcMs = $request['rpc_ms'] ?? $this->integerConfig('rpc_ms', 2_000, 1, 86_400_000);
            $options = new Options($leaseMs / 1000, min($rpcMs / 1000, ($leaseMs / 1000) - 0.011), 2.0);
            $provider = $this->makeProvider($options);
            $handle = $provider->lock($request['resource']);
            $waitMs = $request['wait_ms'] ?? 0;
            if ($request['op'] === 'try_acquire') {
                $lease = $handle->tryAcquire($options);
            } elseif ($waitMs === 0) {
                $lease = $handle->tryAcquire($options);
                if ($lease === null) {
                    throw new WaitTimeout('zero-duration acquisition found the resource busy');
                }
            } else {
                $lease = $handle->acquire(new AcquireOptions($waitMs / 1000, $options));
            }
            if ($lease === null) {
                $this->childWrite($socket, ['id' => $request['id'], 'status' => 'busy']);
                return;
            }
            $leaseId = bin2hex(random_bytes(16));
            $response = ['id' => $request['id'], 'status' => 'acquired', 'lease_id' => $leaseId,
                'effective_lease_ms' => $lease->effectiveLeaseMilliseconds()];
            if ($lease instanceof EtcdLease) {
                $response['fence'] = $lease->fence();
            }
            $this->childWrite($socket, $response);
            while (($command = $this->childRead($input)) !== null) {
                $op = $command['op'];
                $id = $command['id'];
                try {
                    if ($op === 'inspect') {
                        $result = ['id' => $id, 'status' => 'ok', 'state' => $lease->state()->value];
                    } elseif ($op === 'renew') {
                        $ttl = $command['lease_ms'] ?? $leaseMs;
                        if ($ttl !== $leaseMs) {
                            throw new UnsupportedCapability('renewal retains the acquisition TTL');
                        }
                        $result = ['id' => $id, 'status' => $lease->renew() ? 'renewed' : 'lost'];
                    } elseif ($op === 'release') {
                        $result = ['id' => $id, 'status' => $lease->release()->value];
                        $this->childWrite($socket, $result);
                        continue;
                    } elseif ($op === 'shutdown') {
                        $status = $lease->release()->value;
                        $this->childWrite($socket, ['id' => $id, 'status' => 'ok', 'outstanding_cleanup_attempts' => $status === 'release_unknown' ? 1 : 0]);
                        return;
                    } else {
                        throw new \InvalidArgumentException('unsupported child operation');
                    }
                    $this->childWrite($socket, $result);
                } catch (\Throwable $error) {
                    $this->childWrite($socket, $this->errorResponse($id, $error));
                }
            }
        } catch (\Throwable $error) {
            $this->childWrite($socket, $this->errorResponse($request['id'], $error));
        } finally {
            if ($lease !== null && $lease->state() === LeaseState::Held) {
                try {
                    $lease->release();
                } catch (\Throwable) {
                    error_log('dilock php worker: lease cleanup failed');
                }
            }
            if ($provider !== null) {
                try {
                    $provider->close();
                } catch (\Throwable) {
                    error_log('dilock php worker: provider close failed');
                }
            }
        }
    }

    /**
     * @param resource $socket
     * @param Request $request
     */
    private function writeChild($socket, array $request): void
    {
        $line = self::encodeResponse($request);
        $deadline = microtime(true) + 1.0;
        $offset = 0;
        while ($offset < strlen($line) && microtime(true) < $deadline) {
            $written = @fwrite($socket, substr($line, $offset));
            if (is_int($written) && $written > 0) {
                $offset += $written;
                continue;
            }
            $read = [];
            $write = [$socket];
            $except = [];
            @stream_select($read, $write, $except, 0, 20_000);
        }
        if ($offset !== strlen($line)) {
            throw new \RuntimeException('bounded worker IPC write failed');
        }
    }

    /**
     * @param resource $socket
     * @param array<string, mixed> $message
     */
    private function childWrite($socket, array $message): void
    {
        $line = self::encodeResponse($message);
        $offset = 0;
        while ($offset < strlen($line)) {
            $written = fwrite($socket, substr($line, $offset));
            if ($written === false || $written === 0) {
                return;
            }
            $offset += $written;
        }
        fflush($socket);
    }

    /**
     * @param resource $socket
     * @return Request|null
     */
    private function childRead($socket): ?array
    {
        $line = fgets($socket, self::MAX_LINE_BYTES + 2);
        if ($line === false) {
            return null;
        }
        if (strlen($line) > self::MAX_LINE_BYTES || !str_ends_with($line, "\n")) {
            return null;
        }
        try {
            return self::parseRequest($line);
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * @param array<string, mixed> $response
     * @return array<string, mixed>|null
     */
    private function sanitizeChildResponse(string $id, array $response): ?array
    {
        $status = $response['status'] ?? null;
        if (!is_string($status)) {
            return null;
        }
        $result = ['id' => $id, 'status' => $status];
        if ($status === 'acquired') {
            if (!is_string($response['lease_id'] ?? null) || preg_match('/^[a-f0-9]{32}$/D', $response['lease_id']) !== 1
                || !is_int($response['effective_lease_ms'] ?? null) || $response['effective_lease_ms'] < 0) {
                return null;
            }
            $result['lease_id'] = $response['lease_id'];
            $result['effective_lease_ms'] = $response['effective_lease_ms'];
            if (isset($response['fence'])) {
                $fence = $response['fence'];
                if (!is_array($fence) || !is_string($fence['epoch'] ?? null) || !is_string($fence['value'] ?? null)
                    || preg_match('/^[0-9]+$/D', $fence['value']) !== 1) {
                    return null;
                }
                $result['fence'] = ['epoch' => $fence['epoch'], 'value' => $fence['value']];
            }
            return $result;
        }
        if (in_array($status, ['acquisition_unknown', 'cancelled'], true)) {
            if (!is_bool($response['remote_outcome_unknown'] ?? null)) {
                return null;
            }
            $result['remote_outcome_unknown'] = $response['remote_outcome_unknown'];
        }
        if ($status === 'ok' && isset($response['state'])) {
            if (!is_string($response['state']) || !in_array($response['state'], ['held', 'releasing', 'released', 'released_unknown', 'lost'], true)) {
                return null;
            }
            $result['state'] = $response['state'];
        }
        if ($status === 'unsupported_capability' && is_string($response['capability'] ?? null)) {
            $result['capability'] = $response['capability'];
        }
        return $result;
    }

    /**
     * @param array<int, JsonLineWorkerChild> $children
     * @param array<string, int> $leases
     * @param array<string, int> $pending
     * @param array<string, bool> $active
     * @param-out array<int, JsonLineWorkerChild> $children
     * @param-out array<string, int> $leases
     * @param-out array<string, int> $pending
     * @param-out array<string, bool> $active
     */
    private function finishChild(int $key, array &$children, array &$leases, array &$pending, array &$active): void
    {
        if (!isset($children[$key])) {
            return;
        }
        if (isset($children[$key]->leaseId)) {
            unset($leases[$children[$key]->leaseId]);
        }
        foreach (array_keys($pending) as $id) {
            if ($pending[$id] === $key) {
                unset($pending[$id], $active[$id]);
            }
        }
        if (is_resource($children[$key]->stdin)) {
            fclose($children[$key]->stdin);
        }
        if (is_resource($children[$key]->stdout)) {
            fclose($children[$key]->stdout);
        }
        proc_close($children[$key]->process);
        unset($children[$key]);
    }

    /**
     * @param array<int, JsonLineWorkerChild> $children
     * @param array<string, int> $leases
     * @param array<string, int> $pending
     * @param array<string, bool> $active
     * @param-out array<int, JsonLineWorkerChild> $children
     * @param-out array<string, int> $leases
     * @param-out array<string, int> $pending
     * @param-out array<string, bool> $active
     */
    private function killChild(int $key, array &$children, array &$leases, array &$pending, array &$active): void
    {
        if (!isset($children[$key])) {
            return;
        }
        proc_terminate($children[$key]->process);
        $deadline = microtime(true) + 0.5;
        do {
            $status = proc_get_status($children[$key]->process);
            if (!$status['running']) {
                break;
            }
            usleep(10_000);
        } while (microtime(true) < $deadline);
        if ($status['running']) {
            proc_terminate($children[$key]->process, 9);
        }
        $this->finishChild($key, $children, $leases, $pending, $active);
    }

    /**
     * @param array<int, JsonLineWorkerChild> $children
     * @param array<string, int> $leases
     * @param array<string, int> $pending
     * @param array<string, bool> $active
     * @param-out array<int, JsonLineWorkerChild> $children
     * @param-out array<string, int> $leases
     * @param-out array<string, int> $pending
     * @param-out array<string, bool> $active
     */
    private function shutdownChildren(array &$children, array &$leases, array &$pending, array &$active): int
    {
        $deadline = microtime(true) + 2.5;
        $unknown = 0;
        foreach ($children as $key => $child) {
            if (isset($child->leaseId)) {
                if ($child->expectedOps !== []) {
                    ++$unknown;
                    $this->killChild($key, $children, $leases, $pending, $active);
                    continue;
                }
                $this->writeChild($child->stdin, ['id' => 'worker-shutdown-' . $key, 'op' => 'shutdown']);
                $children[$key]->terminalId = 'worker-shutdown-' . $key;
            } else {
                ++$unknown;
                $this->killChild($key, $children, $leases, $pending, $active);
            }
        }
        while ($children !== [] && microtime(true) < $deadline) {
            /** @var resource[] $read */
            $read = array_map(static fn (JsonLineWorkerChild $child) => $child->stdout, $children);
            /** @var resource[] $write */
            $write = [];
            /** @var resource[] $except */
            $except = [];
            @stream_select($read, $write, $except, 0, 50_000);
            foreach ($read as $socket) {
                $key = get_resource_id($socket);
                $data = fread($socket, 4096);
                if (isset($children[$key]) && $data !== false && $data !== '') {
                    $children[$key]->buffer .= $data;
                    if (str_contains($children[$key]->buffer, "\n")) {
                        $line = strtok($children[$key]->buffer, "\n");
                        $response = json_decode($line ?: '', true);
                        if (is_array($response) && ($response['id'] ?? null) === ($children[$key]->terminalId ?? null)
                            && ($response['status'] ?? null) === 'ok') {
                            $attempts = $response['outstanding_cleanup_attempts'] ?? null;
                            if (is_int($attempts) && $attempts >= 0) {
                                $unknown += $attempts;
                            } else {
                                ++$unknown;
                            }
                        } else {
                            ++$unknown;
                        }
                        $this->finishChild($key, $children, $leases, $pending, $active);
                    }
                }
            }
        }
        foreach (array_keys($children) as $key) {
            ++$unknown;
            $this->killChild($key, $children, $leases, $pending, $active);
        }
        return $unknown;
    }

    /**
     * @param resource $output
     * @param array<string, mixed> $response
     */
    private function write($output, array $response): void
    {
        try {
            $line = self::encodeResponse($response);
            if (fwrite($output, $line) !== strlen($line)) {
                throw new \RuntimeException('short write');
            }
            fflush($output);
        } catch (\Throwable) {
            error_log('dilock php worker: response output failed');
        }
    }

    private function backend(): string
    {
        $backend = $this->config['backend'] ?? 'redis';
        if (!is_string($backend) || !in_array($backend, ['redis', 'etcd'], true)) {
            throw new \InvalidArgumentException('backend must be redis or etcd');
        }
        return $backend;
    }

    private function makeProvider(Options $options): RedisProvider|EtcdProvider
    {
        $endpoint = $this->config['endpoint'] ?? null;
        $namespace = $this->config['namespace'] ?? null;
        $epoch = $this->config['epoch'] ?? null;
        if (!is_string($endpoint) || !is_string($namespace) || !is_string($epoch)) {
            throw new \InvalidArgumentException('config requires string endpoint, namespace and epoch');
        }
        $defaults = new AcquireOptions(
            $this->integerConfig('wait_ms', 5_000, 1, 86_400_000) / 1000,
            $options,
        );
        return $this->backend() === 'redis'
            ? RedisProvider::connect($endpoint, $epoch, $namespace, $defaults)
            : EtcdProvider::connect($endpoint, $epoch, $namespace, $defaults);
    }

    private function integerConfig(string $name, int $default, int $minimum, int $maximum): int
    {
        $value = $this->config[$name] ?? $default;
        if (!is_int($value) || $value < $minimum || $value > $maximum) {
            throw new \InvalidArgumentException('config ' . $name . ' is outside its supported range');
        }
        return $value;
    }

    /** @return array<string, mixed> */
    private function errorResponse(string $id, \Throwable $error): array
    {
        $status = match (true) {
            $error instanceof AcquisitionUnknown => 'acquisition_unknown',
            $error instanceof BackendUnavailable => 'backend_unavailable',
            $error instanceof ProtocolMismatch => 'protocol_mismatch',
            $error instanceof UnsupportedCapability => 'unsupported_capability',
            $error instanceof WaitTimeout => 'timeout',
            $error instanceof \InvalidArgumentException, $error instanceof \OutOfBoundsException => 'invalid_argument',
            $error instanceof LeaseLost => 'backend_unavailable',
            default => 'internal_error',
        };
        if ($status === 'internal_error') {
            error_log('dilock php worker: operation failed (' . $error::class . ')');
        }
        $response = ['id' => $id, 'status' => $status];
        if ($error instanceof AcquisitionUnknown) {
            $response['remote_outcome_unknown'] = true;
        }
        if ($error instanceof WaitTimeout) {
            $response['remote_outcome_unknown'] = false;
        }
        return $response;
    }
}
