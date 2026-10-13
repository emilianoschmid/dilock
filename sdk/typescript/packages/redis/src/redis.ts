import { randomUUID } from "node:crypto";
import { createClient, type RedisClientType } from "redis";
import { identity, ownerToken, redisKey } from "@dilock/core";
import { AcquisitionTimeoutError, AcquisitionUnknownError, BackendUnavailableError, InvalidArgumentError, LeaseLostError, ProtocolMismatchError, ReleaseUnknownError, UnsupportedCapabilityError } from "@dilock/core";
import { parseMetadata } from "./internal/metadata.js";

const ACQUIRE_PROFILE = "redis-single-authority-v1";
const IDENTITY_SCHEME = "sha256-framed-utf8-v1";
export const RELEASE_LUA = `-- Dilock draft 1 reference. KEYS[1] = lease key; ARGV[1] = full owner value.\n-- Atomic owner check prevents a stale releaser from deleting a successor.\nif redis.call('GET', KEYS[1]) == ARGV[1] then\n    return redis.call('DEL', KEYS[1])\nend\nreturn 0\n`;
export const RENEW_LUA = `-- Dilock draft 1 reference. KEYS[1] = lease key.\n-- ARGV[1] = full owner value; ARGV[2] = positive integer TTL in milliseconds.\nlocal ttl = tonumber(ARGV[2])\nif not ttl or ttl ~= math.floor(ttl) or ttl <= 0 or ttl > 86400000 then\n    return redis.error_reply('DILOCK_INVALID_TTL')\nend\nif redis.call('GET', KEYS[1]) == ARGV[1] then\n    return redis.call('PEXPIRE', KEYS[1], ttl)\nend\nreturn 0\n`;

export interface RedisProviderOptions {
  readonly endpoint: string;
  readonly namespace?: string;
  readonly epoch: string;
  readonly rpcTimeoutMs?: number;
  readonly cleanupTimeoutMs?: number;
  readonly defaultAcquisition?: RedisAcquisitionDefaults;
  readonly cleanupObserver?: (observation: CleanupObservation) => void;
}
export interface CleanupObservation { readonly backend: "redis" | "etcd"; readonly outcome: "unknown"; readonly reason: string }
/** Complete immutable provider policy used only when an operation omits its own policy. */
export interface RedisAcquisitionDefaults {
  readonly waitMs: number;
  readonly leaseMs: number;
  readonly rpcTimeoutMs: number;
}
export interface AcquireOptions {
  /** Complete per-acquisition wait/lease/RPC policy. */
  readonly waitMs: number;
  readonly leaseMs: number;
  readonly rpcTimeoutMs: number;
  readonly signal?: AbortSignal;
  readonly requireFencing?: boolean;
}
export interface TryAcquireOptions extends Omit<AcquireOptions, "waitMs"> {}
type ResolvedAcquireOptions = AcquireOptions;
export type ReleaseOutcome = "released" | "already-not-owned";
const owners = new WeakMap<RedisLease, string>();
function leaseOwner(lease: RedisLease): string {
  const owner = owners.get(lease);
  if (!owner) throw new InvalidArgumentError("lease owner state is unavailable");
  return owner;
}

function integer(value: number, name: string, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new InvalidArgumentError(`${name} must be an integer from ${minimum} through ${maximum}`);
  return value;
}
function commandSignal(timeoutMs: number, caller?: AbortSignal): { signal: AbortSignal; dispose: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new DOMException("Redis command timed out", "TimeoutError")), timeoutMs);
  const abort = () => controller.abort(caller?.reason ?? new DOMException("Aborted", "AbortError"));
  caller?.addEventListener("abort", abort, { once: true });
  return { signal: controller.signal, dispose: () => { clearTimeout(timer); caller?.removeEventListener("abort", abort); } };
}

export class RedisProvider {
  readonly #client: RedisClientType;
  readonly #namespace: string;
  readonly #epoch: string;
  readonly #rpcMs: number;
  readonly #cleanupMs: number;
  readonly #defaultAcquisition: Readonly<RedisAcquisitionDefaults>;
  readonly #cleanupObserver: RedisProviderOptions["cleanupObserver"];
  readonly #leases = new Map<string, RedisLease>();
  readonly #cleanups = new Set<Promise<unknown>>();
  readonly #shutdown = new AbortController();
  readonly #activeAttempts = new Set<Promise<RedisLease | undefined>>();
  #closed = false;
  #closePromise: Promise<void> | undefined;

  private constructor(client: RedisClientType, options: RedisProviderOptions) {
    this.#client = client;
    this.#namespace = options.namespace ?? "default";
    this.#epoch = options.epoch;
    this.#rpcMs = integer(options.rpcTimeoutMs ?? 2000, "rpcTimeoutMs", 1, 60_000);
    this.#cleanupMs = integer(options.cleanupTimeoutMs ?? 2000, "cleanupTimeoutMs", 1, 60_000);
    this.#defaultAcquisition = Object.freeze({
      waitMs: integer(options.defaultAcquisition?.waitMs ?? 5000, "defaultAcquisition.waitMs", 1, 86_400_000),
      leaseMs: integer(options.defaultAcquisition?.leaseMs ?? 30_000, "defaultAcquisition.leaseMs", 1, 86_400_000),
      rpcTimeoutMs: integer(options.defaultAcquisition?.rpcTimeoutMs ?? this.#rpcMs, "defaultAcquisition.rpcTimeoutMs", 1, 60_000),
    });
    this.#cleanupObserver = options.cleanupObserver;
  }

  static async connect(options: RedisProviderOptions): Promise<RedisProvider> {
    try {
      const namespace = options.namespace ?? "default";
      identity(namespace, "validation");
      if (typeof options.epoch !== "string" || options.epoch.length === 0 || Buffer.byteLength(options.epoch) > 1024) throw new InvalidArgumentError("epoch must be a non-empty string up to 1024 UTF-8 bytes");
      const rpcTimeoutMs = integer(options.rpcTimeoutMs ?? 2000, "rpcTimeoutMs", 1, 60_000);
      integer(options.cleanupTimeoutMs ?? 2000, "cleanupTimeoutMs", 1, 60_000);
      integer(options.defaultAcquisition?.waitMs ?? 5000, "defaultAcquisition.waitMs", 1, 86_400_000);
      const defaultLeaseMs = integer(options.defaultAcquisition?.leaseMs ?? 30_000, "defaultAcquisition.leaseMs", 1, 86_400_000);
      const defaultRpcMs = integer(options.defaultAcquisition?.rpcTimeoutMs ?? rpcTimeoutMs, "defaultAcquisition.rpcTimeoutMs", 1, 60_000);
      if (defaultRpcMs + Math.max(10, Math.ceil(defaultLeaseMs * 0.01)) >= defaultLeaseMs) throw new InvalidArgumentError("default acquisition RPC timeout plus safety margin must be less than the lease");
      const client = createClient({
        url: options.endpoint,
        disableOfflineQueue: true,
        socket: { connectTimeout: rpcTimeoutMs, reconnectStrategy: false },
      });
      // node-redis requires an error listener even when all command failures are awaited.
      client.on("error", () => undefined);
      await client.connect();
      const provider = new RedisProvider(client, options);
      try { await provider.#validateMetadata(); }
      catch (error) { await client.close().catch(() => client.destroy()); throw error; }
      return provider;
    } catch (error) {
      if (error instanceof InvalidArgumentError || error instanceof ProtocolMismatchError) throw error;
      throw new BackendUnavailableError("could not connect to or validate the Redis authority", { cause: error });
    }
  }

  lock(resource: string): RedisLock { this.#ensureOpen(); return new RedisLock(this, resource); }

  defaultAcquireOptions(): Readonly<RedisAcquisitionDefaults> { return this.#defaultAcquisition; }
  observeCleanup(reason: string): void {
    try {
      if (this.#cleanupObserver) this.#cleanupObserver({ backend: "redis", outcome: "unknown", reason });
      else process.emitWarning(`Dilock Redis cleanup outcome is unknown (${reason})`);
    }
    catch { /* A diagnostic callback cannot replace work or cleanup results. */ }
  }

  async #validateMetadata(): Promise<void> {
    let raw: string | null;
    const bounded = commandSignal(this.#rpcMs);
    try { raw = await this.#client.withAbortSignal(bounded.signal).get(`dilock:namespace:${this.#namespace}:meta`); }
    catch (error) { throw new BackendUnavailableError("failed reading namespace metadata", { cause: error }); }
    finally { bounded.dispose(); }
    if (raw === null) throw new ProtocolMismatchError("namespace metadata is missing; provision it explicitly before use");
    parseMetadata(raw, this.#epoch);
  }

  #ensureOpen(): void { if (this.#closed) throw new InvalidArgumentError("provider is closed"); }

  async #tryAcquire(resource: string, leaseMs: number, rpcMs: number, signal?: AbortSignal): Promise<RedisLease | undefined> {
    this.#ensureOpen();
    const operationSignal = signal ? AbortSignal.any([signal, this.#shutdown.signal]) : this.#shutdown.signal;
    operationSignal.throwIfAborted();
    const id = identity(this.#namespace, resource);
    const owner = ownerToken();
    const key = redisKey(id);
    const margin = Math.max(10, Math.ceil(leaseMs * 0.01));
    if (rpcMs + margin >= leaseMs) throw new InvalidArgumentError("RPC timeout plus safety margin must be less than the lease");
    const sentAt = performance.now();
    let response: string | null;
    const bounded = commandSignal(rpcMs, operationSignal);
    try {
      // One SET NX PX command per logical attempt; this client has offline queuing and reconnect retries disabled.
      response = await this.#client.withAbortSignal(bounded.signal).set(key, owner, { NX: true, PX: leaseMs });
    } catch (error) {
      if (error instanceof Error && error.name === "SimpleError") throw new BackendUnavailableError("Redis rejected the acquisition command", { cause: error });
      const cleanup = this.#cleanupOwner(key, owner);
      this.#cleanups.add(cleanup);
      void cleanup.finally(() => this.#cleanups.delete(cleanup));
      throw new AcquisitionUnknownError("Redis acquisition response was not confirmed; owner-specific cleanup is tracked", { cause: error });
    } finally { bounded.dispose(); }
    if (operationSignal.aborted) {
      if (response === "OK") {
        const cleanup = this.#cleanupOwner(key, owner);
        this.#cleanups.add(cleanup);
        void cleanup.finally(() => this.#cleanups.delete(cleanup));
        throw new AcquisitionUnknownError("acquisition succeeded as cancellation arrived; owner-specific cleanup is tracked", { cause: operationSignal.reason });
      }
      operationSignal.throwIfAborted();
    }
    if (response === null) {
      const typeBounded = commandSignal(rpcMs, operationSignal);
      let keyType: string;
      try { keyType = await this.#client.withAbortSignal(typeBounded.signal).type(key); }
      catch (error) {
        if (operationSignal.aborted) operationSignal.throwIfAborted();
        throw new BackendUnavailableError("Redis could not classify a contended lock key", { cause: error });
      } finally { typeBounded.dispose(); }
      if (operationSignal.aborted) operationSignal.throwIfAborted();
      if (keyType !== "string" && keyType !== "none")
        throw new ProtocolMismatchError(`Redis lock key has unexpected type '${keyType}'`);
      return undefined;
    }
    const deadline = sentAt + leaseMs - margin;
    if (performance.now() >= deadline) {
      const cleanup = this.#cleanupOwner(key, owner);
      this.#cleanups.add(cleanup);
      void cleanup.finally(() => this.#cleanups.delete(cleanup));
      throw new AcquisitionUnknownError("Redis confirmed acquisition after the conservative local deadline");
    }
    const lease = new RedisLease(this, key, owner, leaseMs, deadline, rpcMs, this.#cleanupMs);
    this.#leases.set(lease.id, lease);
    return lease;
  }

  async #cleanupOwner(key: string, owner: string): Promise<boolean> {
    const bounded = commandSignal(this.#cleanupMs);
    try {
      const result = Number(await this.#client.withAbortSignal(bounded.signal).eval(RELEASE_LUA, { keys: [key], arguments: [owner] }));
      return result === 0 || result === 1;
    }
    catch { /* Lease expiry is the final bound if owner-specific cleanup cannot be confirmed. */ return false; }
    finally { bounded.dispose(); }
  }

  /** @internal Clean up an acquisition that arrived after its caller's wait deadline. */
  async cleanupLateAcquisition(lease: RedisLease): Promise<void> {
    const confirmed = await this.#cleanupOwner(lease.key, leaseOwner(lease));
    this.#leases.delete(lease.id);
    lease.markLost();
    if (!confirmed) throw new AcquisitionUnknownError("late Redis acquisition cleanup was not confirmed; its lease will expire by TTL");
  }

  async #release(lease: RedisLease, rpcMs: number): Promise<ReleaseOutcome> {
    try {
      const bounded = commandSignal(rpcMs);
      let result: unknown;
      try { result = await this.#client.withAbortSignal(bounded.signal).eval(RELEASE_LUA, { keys: [lease.key], arguments: [leaseOwner(lease)] }); }
      finally { bounded.dispose(); }
      this.#leases.delete(lease.id);
      return Number(result) === 1 ? "released" : "already-not-owned";
    } catch (error) { throw new ReleaseUnknownError("Redis did not confirm lease release", { cause: error }); }
  }

  async #renew(lease: RedisLease, leaseMs: number, rpcMs: number): Promise<boolean> {
    const sentAt = performance.now();
    try {
      const bounded = commandSignal(rpcMs);
      let result: unknown;
      try { result = await this.#client.withAbortSignal(bounded.signal).eval(RENEW_LUA, { keys: [lease.key], arguments: [leaseOwner(lease), String(leaseMs)] }); }
      finally { bounded.dispose(); }
      if (Number(result) !== 1) return false;
      const margin = Math.max(10, Math.ceil(leaseMs * 0.01));
      return lease.confirmRenewal(sentAt + leaseMs - margin);
    } catch (error) { throw new BackendUnavailableError("Redis lease renewal failed", { cause: error }); }
  }

  async close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closed = true;
    this.#shutdown.abort(new DOMException("Provider is closing", "AbortError"));
    this.#closePromise = this.#finishClose();
    return this.#closePromise;
  }

  async #finishClose(): Promise<void> {
    await Promise.allSettled([...this.#activeAttempts]);
    const releases = [...this.#leases.values()].map(lease => lease.release().catch(() => { this.observeCleanup("provider-close"); }));
    await Promise.race([Promise.allSettled(releases), new Promise(resolve => setTimeout(resolve, this.#cleanupMs))]);
    if (this.#cleanups.size) await Promise.race([Promise.allSettled([...this.#cleanups]), new Promise(resolve => setTimeout(resolve, this.#cleanupMs))]);
    await this.#client.close().catch(() => this.#client.destroy());
  }

  // Internal bridge keeps the consumer surface small while leases remain separate objects.
  acquire(resource: string, leaseMs: number, rpcMs: number, signal?: AbortSignal): Promise<RedisLease | undefined> {
    this.#ensureOpen();
    const attempt = this.#tryAcquire(resource, leaseMs, rpcMs, signal);
    this.#activeAttempts.add(attempt);
    void attempt.finally(() => this.#activeAttempts.delete(attempt)).catch(() => undefined);
    return attempt;
  }
  release(lease: RedisLease, rpcMs: number): Promise<ReleaseOutcome> { return this.#release(lease, rpcMs); }
  renew(lease: RedisLease, leaseMs: number, rpcMs: number): Promise<boolean> { return this.#renew(lease, leaseMs, rpcMs); }
}

export class RedisLock {
  readonly #provider: RedisProvider;
  readonly #resource: string;
  constructor(provider: RedisProvider, resource: string) {
    identity("default", resource); // Validate the resource before any network call; namespace is checked again at use.
    this.#provider = provider;
    this.#resource = resource;
  }
  async tryAcquire(options?: TryAcquireOptions): Promise<RedisLease | undefined> {
    const effective: TryAcquireOptions = options ?? { ...this.#provider.defaultAcquireOptions() };
    if (effective.requireFencing) throw new UnsupportedCapabilityError("Redis single-authority profile does not provide strong fencing");
    const leaseMs = integer(effective.leaseMs, "leaseMs", 1, 86_400_000);
    const rpcMs = integer(effective.rpcTimeoutMs, "rpcTimeoutMs", 1, 60_000);
    effective.signal?.throwIfAborted();
    return this.#provider.acquire(this.#resource, leaseMs, rpcMs, effective.signal);
  }
  async acquire(options?: AcquireOptions): Promise<RedisLease> {
    const effective: ResolvedAcquireOptions = options ?? this.#provider.defaultAcquireOptions();
    if (effective.requireFencing) throw new UnsupportedCapabilityError("Redis single-authority profile does not provide strong fencing");
    const waitMs = integer(effective.waitMs, "waitMs", 1, 86_400_000);
    const end = performance.now() + waitMs;
    const configuredRpcMs = integer(effective.rpcTimeoutMs, "rpcTimeoutMs", 1, 60_000);
    for (;;) {
      effective.signal?.throwIfAborted();
      const remaining = end - performance.now();
      if (remaining < 1) throw new AcquisitionTimeoutError("timed out waiting for Redis lease contention to clear");
      const attemptRpcMs = Math.max(1, Math.min(configuredRpcMs, Math.floor(remaining)));
      const lease = await this.tryAcquire({ ...effective, rpcTimeoutMs: attemptRpcMs });
      if (lease) {
        if (performance.now() >= end) {
          await this.#provider.cleanupLateAcquisition(lease);
          throw new AcquisitionTimeoutError("Redis acquisition completed after the overall wait budget");
        }
        return lease;
      }
      const afterAttempt = end - performance.now();
      if (afterAttempt <= 0) throw new AcquisitionTimeoutError("timed out waiting for Redis lease contention to clear");
      await new Promise<void>((resolve, reject) => {
        const done = () => effective.signal?.removeEventListener("abort", abort);
        const timer = setTimeout(() => { done(); resolve(); }, Math.min(afterAttempt, 25 + Math.random() * 50));
        const abort = () => { clearTimeout(timer); done(); reject(effective.signal?.reason ?? new DOMException("Aborted", "AbortError")); };
        effective.signal?.addEventListener("abort", abort, { once: true });
      });
    }
  }

  /**
   * Acquires once, runs the callback, and awaits bounded release. A release
   * failure rejects successful work; if work already failed, its error remains
   * primary and unknown cleanup is reported to the provider observer.
  */
  async withLease<T>(work: (lease: RedisLease) => Promise<T> | T, options?: AcquireOptions): Promise<T> {
    if (typeof work !== "function") throw new InvalidArgumentError("work must be a function");
    const lease = await this.acquire(options);
    let workFailed = false;
    let workError: unknown;
    let value: T | undefined;
    try { value = await work(lease); }
    catch (error) { workFailed = true; workError = error; }

    try {
      const outcome = await lease.release();
      if (outcome !== "released") throw new LeaseLostError("Redis lease was no longer owned when its scope ended");
    }
    catch (error) {
      this.#provider.observeCleanup(workFailed ? "with-lease-after-work-error" : "with-lease");
      if (!workFailed) throw error;
    }
    if (workFailed) throw workError;
    return value as T;
  }
}


export class RedisLease {
  readonly id = randomUUID();
  readonly key: string;
  readonly #provider: RedisProvider;
  readonly #leaseMs: number;
  readonly #rpcMs: number;
  readonly #cleanupMs: number;
  #deadline: number;
  #state: "held" | "releasing" | "released" | "release-unknown" | "lost" = "held";
  #releasePromise: Promise<ReleaseOutcome> | undefined;

  constructor(provider: RedisProvider, key: string, owner: string, leaseMs: number, deadline: number, rpcMs: number, cleanupMs: number) {
    this.#provider = provider; this.key = key; owners.set(this, owner); this.#leaseMs = leaseMs; this.#deadline = deadline; this.#rpcMs = rpcMs; this.#cleanupMs = cleanupMs;
  }
  get state(): string { this.#expireLocally(); return this.#state; }
  assertValid(): void { this.#expireLocally(); if (this.#state !== "held") throw new LeaseLostError("lease is no longer locally valid"); }
  #expireLocally(): void { if (this.#state === "held" && performance.now() >= this.#deadline) this.#state = "lost"; }
  /** @internal Terminal local state after owner-specific late-acquisition cleanup. */
  markLost(): void { this.#expireLocally(); if (this.#state === "held") this.#state = "lost"; }
  confirmRenewal(deadline: number): boolean {
    this.#expireLocally();
    if (this.#state !== "held" || performance.now() >= deadline) { this.#state = "lost"; return false; }
    this.#deadline = deadline; return true;
  }
  async renew(leaseMs = this.#leaseMs): Promise<void> {
    this.assertValid();
    integer(leaseMs, "leaseMs", 1, 86_400_000);
    if (this.#rpcMs + Math.max(10, Math.ceil(leaseMs * 0.01)) >= leaseMs) throw new InvalidArgumentError("RPC timeout plus safety margin must be less than the lease");
    try {
      if (!await this.#provider.renew(this, leaseMs, this.#rpcMs)) { this.#state = "lost"; throw new LeaseLostError("Redis no longer recognizes this owner"); }
    } catch (error) {
      // The EVAL may have committed before its reply was lost. End local reliance
      // instead of allowing another call to treat this generation as confirmed.
      if (error instanceof BackendUnavailableError) this.#state = "lost";
      throw error;
    }
  }
  async release(): Promise<ReleaseOutcome> {
    this.#expireLocally();
    if (this.#state === "released") return "already-not-owned";
    // Local expiry/loss does not prove that Redis removed this owner's value. Still run the
    // atomic owner comparison so a live orphan can be cleaned without touching a successor.
    if (this.#state === "release-unknown") throw new ReleaseUnknownError("a previous Redis release acknowledgement was unknown");
    if (this.#state === "releasing") return this.#releasePromise ?? Promise.reject(new ReleaseUnknownError("release is already in progress without a tracked acknowledgement"));
    this.#state = "releasing";
    this.#releasePromise = this.#provider.release(this, this.#rpcMs).then(result => {
      this.#state = result === "released" ? "released" : "lost";
      return result;
    }).catch(error => {
      this.#state = "release-unknown";
      throw error;
    });
    return this.#releasePromise;
  }
}

export async function provisionNamespace(client: RedisClientType, namespace: string, epoch: string): Promise<"created" | "compatible-existing"> {
  identity(namespace, "validation");
  if (!epoch || Buffer.byteLength(epoch, "utf8") > 1024) throw new InvalidArgumentError("epoch must be a non-empty string up to 1024 UTF-8 bytes");
  const metadata = JSON.stringify({ protocol_major: 1, identity_scheme: IDENTITY_SCHEME, profile: ACQUIRE_PROFILE, coordination_epoch: epoch, fixed_lease_ms: null });
  const key = `dilock:namespace:${namespace}:meta`;
  let created: string | null;
  const setBound = commandSignal(2000);
  try { created = await client.withAbortSignal(setBound.signal).set(key, metadata, { NX: true }); }
  catch (error) { throw new BackendUnavailableError("failed provisioning Redis namespace metadata", { cause: error }); }
  finally { setBound.dispose(); }
  let current: string | null;
  const getBound = commandSignal(2000);
  try { current = await client.withAbortSignal(getBound.signal).get(key); }
  catch (error) { throw new BackendUnavailableError("failed verifying Redis namespace metadata", { cause: error }); }
  finally { getBound.dispose(); }
  if (current === null) throw new ProtocolMismatchError("namespace metadata disappeared during provisioning");
  parseMetadata(current, epoch);
  return created === "OK" ? "created" : "compatible-existing";
}



