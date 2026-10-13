/** Local-only style example. No TTL, remote backend, renewal or fencing. */
export type ReleaseOutcome = "released" | "already-not-owned";

/** Single JS event-loop fixture; methods mutate before any suspension. */
export class MemoryStore {
  readonly #owners = new Map<string, object>();
  claim(resource: string, owner: object): boolean {
    if (this.#owners.has(resource)) return false;
    this.#owners.set(resource, owner);
    return true;
  }
  release(resource: string, owner: object): ReleaseOutcome {
    if (this.#owners.get(resource) !== owner) return "already-not-owned";
    this.#owners.delete(resource);
    return "released";
  }
  /** Test-only hook. Do not expose force-unlock in the production API. */
  expireForTest(resource: string): void { this.#owners.delete(resource); }
}

/** One acquisition; release is awaited even though this local store is immediate. */
export class Lease {
  #closed = false;
  constructor(private readonly store: MemoryStore,
              private readonly resource: string, private readonly owner: object) {}
  async release(): Promise<ReleaseOutcome> {
    if (this.#closed) return "already-not-owned";
    this.#closed = true;
    return this.store.release(this.resource, this.owner);
  }
}

export class ResourceLock {
  constructor(private readonly store: MemoryStore, private readonly resource: string) {
    if (resource.length === 0) throw new TypeError("empty resource");
  }
  /** undefined means busy. Cancellation is an error, not contention. */
  async tryAcquire(signal?: AbortSignal): Promise<Lease | undefined> {
    signal?.throwIfAborted();
    const owner = {}; // Identity for this process only; not a wire token.
    if (!this.store.claim(this.resource, owner)) return undefined;
    return new Lease(this.store, this.resource, owner);
  }
}

export class Provider {
  constructor(private readonly store: MemoryStore) {}
  lock(resource: string): ResourceLock { return new ResourceLock(this.store, resource); }
}

function check(value: unknown): asserts value {
  if (!value) throw new Error("style assertion failed");
}

async function main(): Promise<void> {
  const store = new MemoryStore();
  const resource = new Provider(store).lock("example/resource");
  const first = await resource.tryAcquire(); check(first);
  check((await resource.tryAcquire()) === undefined);
  store.expireForTest("example/resource");
  const second = await resource.tryAcquire(); check(second);
  check(await first.release() === "already-not-owned");
  check((await resource.tryAcquire()) === undefined);
  check(await second.release() === "released");
  check(await second.release() === "already-not-owned");
  const workError = new Error("consumer failure");
  try {
    const held = await resource.tryAcquire(); check(held);
    try { throw workError; }
    finally { await held.release(); }
  } catch (error) { check(error === workError); }
  const final = await resource.tryAcquire(); check(final); await final.release();
  const controller = new AbortController(); controller.abort();
  let cancelled = false;
  try { await resource.tryAcquire(controller.signal); }
  catch { cancelled = true; }
  check(cancelled);
  console.log("PASS typescript: busy, stale owner, idempotence, finally, cancellation");
}
await main();
