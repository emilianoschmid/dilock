<?php
// Local-only fixture: no TTL, network, renewal, fencing or wire protocol.
declare(strict_types=1);
namespace Dilock\Style;

enum ReleaseOutcome { case Released; case AlreadyNotOwned; }

/** Single request/event-loop local store; not a thread-safe backend client. */
final class MemoryStore
{
    /** @var array<string, object> */
    private array $owners = [];
    public function claim(string $resource, object $owner): bool
    {
        if (isset($this->owners[$resource])) { return false; }
        $this->owners[$resource] = $owner;
        return true;
    }
    public function release(string $resource, object $owner): ReleaseOutcome
    {
        if (($this->owners[$resource] ?? null) !== $owner) { return ReleaseOutcome::AlreadyNotOwned; }
        unset($this->owners[$resource]);
        return ReleaseOutcome::Released;
    }
    public function expireForTest(string $resource): void { unset($this->owners[$resource]); }
}

/** No network work in destructors: consumer cleanup is explicit or scoped. */
final class Lease
{
    private bool $closed = false;
    public function __construct(
        private readonly MemoryStore $store,
        private readonly string $resource,
        private readonly object $owner,
    ) {}
    public function release(): ReleaseOutcome
    {
        if ($this->closed) { return ReleaseOutcome::AlreadyNotOwned; }
        $this->closed = true;
        return $this->store->release($this->resource, $this->owner);
    }
}
final class ResourceLock
{
    public function __construct(private readonly MemoryStore $store, private readonly string $resource)
    {
        if ($resource === '') { throw new \InvalidArgumentException('empty resource'); }
    }
    public function tryAcquire(): ?Lease
    {
        $owner = new \stdClass();
        return $this->store->claim($this->resource, $owner) ? new Lease($this->store, $this->resource, $owner) : null;
    }
    /** @template T @param callable(Lease):T $work @return T */
    public function withLease(callable $work): mixed
    {
        $lease = $this->tryAcquire() ?? throw new \RuntimeException('busy');
        try { return $work($lease); }
        finally { $lease->release(); }
    }
}
final class Provider
{
    public function __construct(private readonly MemoryStore $store) {}
    public function lock(string $resource): ResourceLock { return new ResourceLock($this->store, $resource); }
}
function check(bool $condition): void
{
    if (!$condition) { throw new \LogicException('style assertion failed'); }
}
$store = new MemoryStore();
$lock = (new Provider($store))->lock('example/resource');
$first = $lock->tryAcquire() ?? throw new \LogicException('first');
check($lock->tryAcquire() === null);
$store->expireForTest('example/resource');
$second = $lock->tryAcquire() ?? throw new \LogicException('second');
check($first->release() === ReleaseOutcome::AlreadyNotOwned);
check($lock->tryAcquire() === null);
check($second->release() === ReleaseOutcome::Released);
check($second->release() === ReleaseOutcome::AlreadyNotOwned);
try {
    $lock->withLease(static function (Lease $lease): never { throw new \DomainException('consumer failure'); });
} catch (\DomainException) {}
check($lock->withLease(static fn(Lease $lease): int => 42) === 42);
echo "PASS php: busy, stale owner, idempotence, finally/block\n";
