<?php

declare(strict_types=1);

namespace Dilock;

/** One Consul HTTP authority; lock KV entries are retained for LockIndex safety. */
final class ConsulProvider
{
    private bool $closed = false;
    /** @var array<int, ConsulLease> */
    private array $leases = [];

    private function __construct(
        private readonly string $endpoint,
        private readonly string $namespace,
        private readonly string $epoch,
        private readonly ?string $token,
        public readonly AcquireOptions $defaults,
        private readonly ?\Closure $cleanupObserver,
    ) {
    }

    public static function connect(
        string $endpoint,
        string $epoch,
        string $namespace = 'default',
        ?string $token = null,
        ?AcquireOptions $defaults = null,
        ?callable $cleanupObserver = null,
    ): self {
        self::validateConfiguration($endpoint, $epoch, $namespace, $token);
        $defaults ??= new AcquireOptions();
        self::validateConsulTtl($defaults->lease->leaseSeconds);
        $provider = new self(
            rtrim($endpoint, '/'),
            $namespace,
            $epoch,
            $token,
            $defaults,
            $cleanupObserver === null ? null : \Closure::fromCallable($cleanupObserver),
        );
        $provider->validateMetadata($defaults->lease->rpcSeconds);
        return $provider;
    }

    /** Creates namespace metadata only with CAS=0; compatible existing data is untouched. */
    public static function provisionNamespace(
        string $endpoint,
        string $epoch,
        string $namespace = 'default',
        ?string $token = null,
        float $timeoutSeconds = 2.0,
    ): void {
        self::validateConfiguration($endpoint, $epoch, $namespace, $token);
        if (!is_finite($timeoutSeconds) || $timeoutSeconds <= 0) {
            throw new \InvalidArgumentException('Consul timeout must be finite and positive');
        }
        $provider = new self(rtrim($endpoint, '/'), $namespace, $epoch, $token, new AcquireOptions(), null);
        $metadata = json_encode([
            'protocol_major' => 1,
            'identity_scheme' => Identity::SCHEME,
            'profile' => 'consul-kv-session-v1',
            'coordination_epoch' => $epoch,
            'fixed_lease_ms' => null,
        ], JSON_THROW_ON_ERROR);
        $result = $provider->request(
            'PUT',
            '/v1/kv/dilock/namespaces/' . $namespace . '/meta?cas=0',
            $metadata,
            $timeoutSeconds,
            true,
        );
        if ($result === true) {
            return;
        }
        if ($result !== false) {
            throw new ProtocolMismatch('Consul metadata CAS response was not boolean');
        }
        $provider->validateMetadata($timeoutSeconds);
    }

    private static function validateConfiguration(string $endpoint, string $epoch, string $namespace, ?string $token): void
    {
        Identity::resourceId($namespace, 'validation');
        $parts = parse_url($endpoint);
        if (!is_array($parts) || !in_array($parts['scheme'] ?? '', ['http', 'https'], true)
            || !isset($parts['host']) || isset($parts['user']) || isset($parts['pass'])
            || (isset($parts['path']) && !in_array($parts['path'], ['', '/'], true))
            || isset($parts['query']) || isset($parts['fragment'])) {
            throw new \InvalidArgumentException('endpoint must be an http(s) authority without credentials or path');
        }
        if ($epoch === '' || strlen($epoch) > 1024 || preg_match('//u', $epoch) !== 1 || str_contains($epoch, "\0")) {
            throw new \InvalidArgumentException('epoch must be nonempty UTF-8, at most 1024 bytes and contain no NUL');
        }
        if ($token !== null && ($token === '' || preg_match('/^[!-~]+$/D', $token) !== 1)) {
            throw new \InvalidArgumentException('Consul token must contain printable non-space ASCII only');
        }
    }

    private static function validateConsulTtl(float $seconds): int
    {
        if (!is_finite($seconds) || $seconds < 10.0 || $seconds > 86400.0) {
            throw new \InvalidArgumentException('Consul session TTL must be between 10 seconds and 24 hours');
        }
        $effective = (int) ceil($seconds);
        if ($effective > 86400) {
            throw new \InvalidArgumentException('rounded Consul session TTL exceeds 24 hours');
        }
        return $effective;
    }

    public function lock(string $resource): ConsulHandle
    {
        $this->ensureOpen();
        return new ConsulHandle($this, 'dilock/locks/' . Identity::resourceId($this->namespace, $resource));
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        foreach (array_values($this->leases) as $lease) {
            $lease->release();
        }
        $this->closed = true;
    }

    public function ensureOpen(): void
    {
        if ($this->closed) {
            throw new \LogicException('Consul provider is closed');
        }
    }

    public function addLease(ConsulLease $lease): void { $this->leases[spl_object_id($lease)] = $lease; }
    public function removeLease(ConsulLease $lease): void { unset($this->leases[spl_object_id($lease)]); }
    public function epoch(): string { return $this->epoch; }

    public function observeCleanup(ReleaseOutcome $outcome, string $reason): void
    {
        try {
            if ($this->cleanupObserver !== null) {
                ($this->cleanupObserver)(new CleanupObservation('consul', $outcome, $reason));
            } else {
                error_log(sprintf('dilock consul scope cleanup outcome=%s reason=%s', $outcome->value, $reason));
            }
        } catch (\Throwable) {
        }
    }

    private function validateMetadata(float $timeout): void
    {
        $key = 'dilock/namespaces/' . $this->namespace . '/meta';
        $rows = $this->request('GET', '/v1/kv/' . $key . '?consistent', null, $timeout, false);
        if (!is_array($rows) || count($rows) !== 1 || !is_array($rows[0]) || ($rows[0]['Key'] ?? null) !== $key
            || !isset($rows[0]['Value']) || !is_string($rows[0]['Value'])) {
            throw new ProtocolMismatch('Consul namespace metadata is missing or malformed');
        }
        $raw = base64_decode($rows[0]['Value'], true);
        if (!is_string($raw) || $raw === '' || strlen($raw) > 8192) {
            throw new ProtocolMismatch('Consul namespace metadata is invalid base64 or too large');
        }
        try {
            $metadata = json_decode($raw, true, 16, JSON_THROW_ON_ERROR);
        } catch (\JsonException $error) {
            throw new ProtocolMismatch('Consul namespace metadata is not valid JSON', 0, $error);
        }
        if (!is_array($metadata) || count($metadata) !== 5
            || ($metadata['protocol_major'] ?? null) !== 1
            || ($metadata['identity_scheme'] ?? null) !== Identity::SCHEME
            || ($metadata['profile'] ?? null) !== 'consul-kv-session-v1'
            || ($metadata['coordination_epoch'] ?? null) !== $this->epoch
            || !array_key_exists('fixed_lease_ms', $metadata) || $metadata['fixed_lease_ms'] !== null) {
            throw new ProtocolMismatch('Consul namespace metadata is incompatible with this authority');
        }
    }

    /** @return mixed */
    public function request(string $method, string $path, ?string $body, float $timeout, bool $mutation): mixed
    {
        $headers = ['Accept: application/json'];
        if ($body !== null) { $headers[] = 'Content-Type: application/json'; }
        if ($this->token !== null) { $headers[] = 'X-Consul-Token: ' . $this->token; }
        $context = stream_context_create(['http' => [
            'method' => $method,
            'header' => implode("\r\n", $headers),
            'content' => $body ?? '',
            'timeout' => $timeout,
            'ignore_errors' => true,
            'follow_location' => 0,
            'max_redirects' => 0,
            'protocol_version' => 1.1,
        ], 'ssl' => ['verify_peer' => true, 'verify_peer_name' => true]]);
        $url = $this->endpoint . $path;
        $response = @file_get_contents($url, false, $context);
        /** @var list<string> $http_response_header */
        $statusLine = $http_response_header[0] ?? '';
        preg_match('/\s(\d{3})\s/', $statusLine, $matches);
        $status = isset($matches[1]) ? (int) $matches[1] : 0;
        if ($response === false || $status < 200 || $status >= 300) {
            if (!$mutation && $status === 404) {
                throw new ProtocolMismatch('Consul metadata or owner key is missing');
            }
            $cause = new \RuntimeException('Consul HTTP request was not acknowledged (status ' . $status . ')');
            if ($mutation) { throw new AcquisitionUnknown('Consul mutation acknowledgement is unknown', 0, $cause); }
            throw new BackendUnavailable('Consul HTTP request failed', 0, $cause);
        }
        if (strlen($response) > 65536) { throw new ProtocolMismatch('Consul response exceeds 64 KiB'); }
        if ($response === '') { return null; }
        try { return json_decode($response, true, 32, JSON_THROW_ON_ERROR); }
        catch (\JsonException $error) {
            if ($mutation) { throw new AcquisitionUnknown('Consul mutation response could not be decoded', 0, $error); }
            throw new ProtocolMismatch('Consul response is not valid JSON', 0, $error);
        }
    }

    public function createSession(int $ttlSeconds, float $timeout): array
    {
        $sent = hrtime(true) / 1_000_000_000;
        $body = json_encode(['Name' => 'dilock lease', 'Behavior' => 'release', 'LockDelay' => '0s', 'TTL' => $ttlSeconds . 's', 'Checks' => []], JSON_THROW_ON_ERROR);
        $response = $this->request('PUT', '/v1/session/create', $body, $timeout, true);
        if (!is_array($response) || !is_string($response['ID'] ?? null) || $response['ID'] === '') {
            throw new AcquisitionUnknown('Consul may have created a session without returning its identifier');
        }
        return [$response['ID'], $sent];
    }

    public function renewSession(string $session, float $timeout): float
    {
        $sent = hrtime(true) / 1_000_000_000;
        $response = $this->request('PUT', '/v1/session/renew/' . rawurlencode($session), null, $timeout, true);
        if (!is_array($response) || count($response) !== 1 || !is_array($response[0]) || ($response[0]['ID'] ?? null) !== $session) {
            throw new ProtocolMismatch('Consul session renewal response is malformed');
        }
        return $sent;
    }

    /** @return array{0:?string,1:int} */
    public function owner(string $key, float $timeout): array
    {
        $rows = $this->request('GET', '/v1/kv/' . $key . '?consistent', null, $timeout, false);
        if (!is_array($rows) || count($rows) !== 1 || !is_array($rows[0]) || ($rows[0]['Key'] ?? null) !== $key
            || !is_int($rows[0]['LockIndex'] ?? null) || $rows[0]['LockIndex'] < 0
            || (($rows[0]['Session'] ?? null) !== null && !is_string($rows[0]['Session']))) {
            throw new ProtocolMismatch('Consul KV owner response is malformed');
        }
        return [$rows[0]['Session'] ?? null, $rows[0]['LockIndex']];
    }

    public function abandon(string $key, string $session, float $timeout): bool
    {
        try {
            $released = $this->request('PUT', '/v1/kv/' . $key . '?release=' . rawurlencode($session), '', $timeout, true);
            $destroyed = $this->request('PUT', '/v1/session/destroy/' . rawurlencode($session), null, $timeout, true);
            return is_bool($released) && $destroyed === true;
        } catch (\Throwable) {
            return false;
        }
    }
}
