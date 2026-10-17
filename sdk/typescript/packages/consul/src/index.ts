import { identity, AcquisitionTimeoutError, AcquisitionUnknownError, BackendUnavailableError,
  InvalidArgumentError, LeaseLostError, ProtocolMismatchError, ReleaseUnknownError } from "@dilock/core";

const PROFILE = "consul-kv-session-v1";
const SCHEME = "sha256-framed-utf8-v1";
const MIN_TTL_MS = 10_000;
const MAX_TTL_MS = 86_400_000;

export interface ConsulProviderOptions {
  readonly endpoint: string;
  readonly namespace?: string;
  readonly epoch: string;
  readonly token?: string;
  readonly requestTimeoutMs?: number;
  readonly cleanupTimeoutMs?: number;
  readonly defaultAcquisition?: { readonly waitMs: number; readonly leaseMs: number };
}
export interface ConsulAcquireOptions { readonly waitMs: number; readonly leaseMs: number; readonly signal?: AbortSignal }
export type ConsulReleaseOutcome = "released" | "already-not-owned";

function integer(value: number, name: string, min: number, max: number): number {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new InvalidArgumentError(`${name} must be an integer from ${min} through ${max}`);
  return value;
}
function ttlSeconds(value: number): number {
  integer(value, "leaseMs", MIN_TTL_MS, MAX_TTL_MS);
  return Math.ceil(value / 1000);
}
function parseJson(raw: string, label: string): any {
  try { return JSON.parse(raw) as any; }
  catch (cause) { throw new ProtocolMismatchError(`Consul ${label} response was invalid JSON`, { cause }); }
}

export class ConsulProvider {
  readonly #endpoint: URL;
  readonly #namespace: string;
  readonly #epoch: string;
  readonly #token: string | undefined;
  readonly #requestMs: number;
  readonly #cleanupMs: number;
  readonly #defaults: Readonly<{ waitMs: number; leaseMs: number }>;
  readonly #leases = new Set<ConsulLease>();
  #closed = false;

  private constructor(options: ConsulProviderOptions, endpoint: URL) {
    this.#endpoint = endpoint;
    this.#namespace = options.namespace ?? "default";
    this.#epoch = options.epoch;
    this.#token = options.token;
    this.#requestMs = integer(options.requestTimeoutMs ?? 2000, "requestTimeoutMs", 1, 60_000);
    this.#cleanupMs = integer(options.cleanupTimeoutMs ?? 2000, "cleanupTimeoutMs", 1, 60_000);
    this.#defaults = Object.freeze({
      waitMs: integer(options.defaultAcquisition?.waitMs ?? 5000, "defaultAcquisition.waitMs", 1, MAX_TTL_MS),
      leaseMs: ttlSeconds(options.defaultAcquisition?.leaseMs ?? 30_000) * 1000,
    });
  }

  static async connect(options: ConsulProviderOptions): Promise<ConsulProvider> {
    const namespace = options.namespace ?? "default";
    identity(namespace, "validation");
    if (typeof options.epoch !== "string" || !options.epoch || Buffer.byteLength(options.epoch, "utf8") > 1024 || options.epoch.includes("\0"))
      throw new InvalidArgumentError("epoch must be non-empty and at most 1024 UTF-8 bytes");
    let endpoint: URL;
    try { endpoint = new URL(options.endpoint); }
    catch (cause) { throw new InvalidArgumentError("invalid Consul endpoint", { cause }); }
    if (!["http:", "https:"].includes(endpoint.protocol) || endpoint.username || endpoint.password || endpoint.pathname !== "/" || endpoint.search || endpoint.hash)
      throw new InvalidArgumentError("Consul endpoint must be an http(s) authority without credentials, path, query, or fragment");
    if (options.token !== undefined && (!options.token || !/^[\x21-\x7e]+$/.test(options.token)))
      throw new InvalidArgumentError("token must contain printable ASCII header characters");
    const provider = new ConsulProvider(options, endpoint);
    await provider.#validateMetadata();
    return provider;
  }

  lock(resource: string): ConsulLock {
    this.#ensureOpen();
    return new ConsulLock(this, `dilock/locks/${identity(this.#namespace, resource)}`);
  }
  get epoch(): string { return this.#epoch; }
  get defaults(): Readonly<{ waitMs: number; leaseMs: number }> { return this.#defaults; }
  #ensureOpen(): void { if (this.#closed) throw new InvalidArgumentError("provider is closed"); }

  async #request(method: string, path: string, options: { body?: string; timeoutMs: number; mutation?: boolean; missing?: boolean; signal?: AbortSignal }): Promise<Response | undefined> {
    this.#ensureOpen();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new DOMException("Consul request timed out", "TimeoutError")), options.timeoutMs);
    const abort = () => controller.abort(options.signal?.reason ?? new DOMException("Aborted", "AbortError"));
    options.signal?.addEventListener("abort", abort, { once: true });
    const headers = new Headers({ Accept: "application/json" });
    if (options.body !== undefined) headers.set("Content-Type", "application/json");
    if (this.#token) headers.set("X-Consul-Token", this.#token);
    try {
      const response = await fetch(new URL(path, this.#endpoint), {
        method, headers, redirect: "manual", signal: controller.signal,
        ...(options.body === undefined ? {} : { body: options.body }),
      });
      if (options.missing && response.status === 404) return undefined;
      if (!response.ok) throw new (options.mutation && response.status >= 500 ? AcquisitionUnknownError : BackendUnavailableError)(`Consul HTTP request rejected (${response.status})`);
      return response;
    } catch (cause) {
      if (cause instanceof BackendUnavailableError || cause instanceof AcquisitionUnknownError) throw cause;
      if (options.mutation) throw new AcquisitionUnknownError("Consul mutation acknowledgement is unknown", { cause });
      if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("Aborted", "AbortError");
      throw new BackendUnavailableError("Consul request failed", { cause });
    } finally {
      clearTimeout(timer); options.signal?.removeEventListener("abort", abort);
    }
  }

  async #validateMetadata(): Promise<void> {
    const key = `dilock/namespaces/${this.#namespace}/meta`;
    const response = await this.#request("GET", `/v1/kv/${key}?consistent`, { timeoutMs: this.#requestMs, missing: true });
    if (!response) throw new ProtocolMismatchError("Consul namespace metadata is missing; provision it explicitly");
    const rows = parseJson(await response.text(), "metadata");
    if (!Array.isArray(rows) || rows.length !== 1 || rows[0]?.Key !== key || typeof rows[0]?.Value !== "string")
      throw new ProtocolMismatchError("Consul namespace metadata response is malformed");
    let meta: any;
    try { meta = JSON.parse(Buffer.from(rows[0].Value, "base64").toString("utf8")); }
    catch (cause) { throw new ProtocolMismatchError("Consul namespace metadata is invalid", { cause }); }
    if (!meta || typeof meta !== "object" || Array.isArray(meta) || Object.keys(meta).length !== 5 || meta.protocol_major !== 1
        || meta.identity_scheme !== SCHEME || meta.profile !== PROFILE || meta.coordination_epoch !== this.#epoch || meta.fixed_lease_ms !== null)
      throw new ProtocolMismatchError("Consul namespace metadata is incompatible with draft 1");
  }

  async createSession(ttl: number): Promise<{ id: string; sentAt: number }> {
    const sentAt = performance.now();
    const response = await this.#request("PUT", "/v1/session/create", { timeoutMs: this.#requestMs, mutation: true,
      body: JSON.stringify({ Name: "dilock lease", Behavior: "release", LockDelay: "0s", TTL: `${ttl}s`, Checks: [] }) });
    const result = parseJson(await response!.text(), "session create");
    if (typeof result?.ID !== "string" || !result.ID || result.ID.length > 128) throw new AcquisitionUnknownError("Consul returned an invalid session id");
    return { id: result.ID, sentAt };
  }
  async renewSession(session: string): Promise<number> {
    const sentAt = performance.now();
    const response = await this.#request("PUT", `/v1/session/renew/${encodeURIComponent(session)}`, { timeoutMs: this.#requestMs, mutation: true });
    const result = parseJson(await response!.text(), "session renewal");
    if (!Array.isArray(result) || result.length !== 1 || result[0]?.ID !== session) throw new LeaseLostError("Consul session renewal was not confirmed");
    return sentAt;
  }
  async acquireKey(key: string, session: string, signal?: AbortSignal): Promise<boolean> {
    const response = await this.#request("PUT", `/v1/kv/${key}?acquire=${encodeURIComponent(session)}`, { timeoutMs: this.#requestMs, mutation: true, ...(signal ? { signal } : {}), body: "" });
    const value = parseJson(await response!.text(), "KV acquire");
    if (typeof value !== "boolean") throw new AcquisitionUnknownError("Consul acquire response was not boolean");
    return value;
  }
  async releaseKey(key: string, session: string): Promise<boolean> {
    const response = await this.#request("PUT", `/v1/kv/${key}?release=${encodeURIComponent(session)}`, { timeoutMs: this.#cleanupMs, mutation: true, body: "" });
    const value = parseJson(await response!.text(), "KV release");
    if (typeof value !== "boolean") throw new ReleaseUnknownError("Consul release response was not boolean");
    return value;
  }
  async owner(key: string): Promise<{ session: string | null; lockIndex: number } | undefined> {
    const response = await this.#request("GET", `/v1/kv/${key}?consistent`, { timeoutMs: this.#requestMs, missing: true });
    if (!response) return undefined;
    const rows = parseJson(await response.text(), "KV read");
    if (!Array.isArray(rows) || rows.length !== 1 || rows[0]?.Key !== key || !Number.isSafeInteger(rows[0]?.LockIndex) || rows[0].LockIndex < 0 || !(rows[0].Session === null || typeof rows[0].Session === "string"))
      throw new ProtocolMismatchError("Consul KV ownership response is malformed");
    return { session: rows[0].Session, lockIndex: rows[0].LockIndex };
  }
  async destroySession(session: string): Promise<void> {
    const response = await this.#request("PUT", `/v1/session/destroy/${encodeURIComponent(session)}`, { timeoutMs: this.#cleanupMs, mutation: true });
    parseJson(await response!.text(), "session destroy");
  }
  addLease(lease: ConsulLease): void { this.#leases.add(lease); }
  removeLease(lease: ConsulLease): void { this.#leases.delete(lease); }
  async close(): Promise<void> {
    if (this.#closed) return;
    for (const lease of [...this.#leases]) { try { await lease.release(); } catch { /* bounded TTL is fallback */ } }
    this.#closed = true;
  }
}

export class ConsulLock {
  readonly #provider: ConsulProvider;
  readonly #key: string;
  constructor(provider: ConsulProvider, key: string) { this.#provider = provider; this.#key = key; }
  async tryAcquire(options?: Omit<ConsulAcquireOptions, "waitMs">): Promise<ConsulLease | undefined> {
    const defaults = this.#provider.defaults;
    return this.#acquire({ waitMs: 0, leaseMs: options?.leaseMs ?? defaults.leaseMs, ...(options?.signal ? { signal: options.signal } : {}) });
  }
  async acquire(options?: ConsulAcquireOptions): Promise<ConsulLease> {
    const defaults = this.#provider.defaults;
    const lease = await this.#acquire(options ?? { ...defaults });
    if (!lease) throw new AcquisitionTimeoutError("timed out waiting for Consul KV lock");
    return lease;
  }
  async #acquire(options: ConsulAcquireOptions): Promise<ConsulLease | undefined> {
    integer(options.waitMs, "waitMs", 0, MAX_TTL_MS);
    const ttl = ttlSeconds(options.leaseMs);
    const created = await this.#provider.createSession(ttl);
    const lease = new ConsulLease(this.#provider, this.#key, created.id, ttl * 1000, created.sentAt);
    this.#provider.addLease(lease); lease.startRenewal();
    const deadline = performance.now() + options.waitMs;
    try {
      do {
        if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("Aborted", "AbortError");
        if (lease.state === "lost") throw new LeaseLostError("Consul session expired while waiting");
        if (await this.#provider.acquireKey(this.#key, created.id, options.signal)) {
          const owner = await this.#provider.owner(this.#key);
          if (!owner || owner.session !== created.id || owner.lockIndex < 1) throw new AcquisitionUnknownError("Consul linearizable read did not confirm owner and LockIndex");
          lease.activate(owner.lockIndex); return lease;
        }
        if (options.waitMs === 0 || performance.now() >= deadline) break;
        await new Promise(resolve => setTimeout(resolve, Math.min(50, Math.max(1, deadline - performance.now()))));
      } while (true);
      await lease.abandon(); return undefined;
    } catch (cause) {
      await lease.abandon();
      if (options.signal?.aborted) throw new AcquisitionUnknownError("Consul acquisition was cancelled; cleanup was attempted", { cause });
      throw cause;
    }
  }
  async withLease<T>(work: (lease: ConsulLease) => Promise<T> | T, options?: ConsulAcquireOptions): Promise<T> {
    if (typeof work !== "function") throw new InvalidArgumentError("work must be a function");
    const lease = await this.acquire(options);
    let failed = false; let error: unknown; let value: T | undefined;
    try { value = await work(lease); } catch (cause) { failed = true; error = cause; }
    try { if (await lease.release() !== "released") throw new LeaseLostError("Consul lease was no longer owned when its scope ended"); }
    catch (cleanup) { if (!failed) throw cleanup; }
    if (failed) throw error;
    return value as T;
  }
}

export class ConsulLease {
  readonly #provider: ConsulProvider;
  readonly #key: string;
  readonly #session: string;
  readonly effectiveTtlMs: number;
  #state: "waiting" | "held" | "releasing" | "released" | "lost" = "waiting";
  #deadline: number;
  #timer: NodeJS.Timeout | undefined;
  #lockIndex = 0;
  #renewing: Promise<void> | undefined;
  constructor(provider: ConsulProvider, key: string, session: string, effectiveTtlMs: number, sentAt: number) {
    this.#provider = provider; this.#key = key; this.#session = session; this.effectiveTtlMs = effectiveTtlMs;
    this.#deadline = sentAt + effectiveTtlMs - Math.max(100, effectiveTtlMs * 0.01);
  }
  get state(): string { this.#expire(); return this.#state; }
  get fence(): { readonly epoch: string; readonly lockIndex: number } { this.assertValid(); return { epoch: this.#provider.epoch, lockIndex: this.#lockIndex }; }
  assertValid(): void { this.#expire(); if (this.#state !== "held") throw new LeaseLostError("Consul lease is no longer locally valid"); }
  activate(index: number): void { this.#expire(); if (this.#state !== "waiting") throw new AcquisitionUnknownError("Consul session expired before lock activation"); this.#lockIndex = index; this.#state = "held"; }
  startRenewal(): void {
    this.#timer = setInterval(() => { this.#renewing ??= this.#renew().finally(() => { this.#renewing = undefined; }); }, Math.max(1000, this.effectiveTtlMs / 3));
  }
  async #renew(): Promise<void> {
    if (this.#state !== "waiting" && this.#state !== "held") return;
    if (performance.now() >= this.#deadline) { this.#state = "lost"; this.#stop(); return; }
    try {
      const sentAt = await this.#provider.renewSession(this.#session);
      if (this.#state === "held") {
        const owner = await this.#provider.owner(this.#key);
        if (owner?.session !== this.#session || owner.lockIndex !== this.#lockIndex) { this.#state = "lost"; this.#stop(); return; }
      }
      const deadline = sentAt + this.effectiveTtlMs - Math.max(100, this.effectiveTtlMs * 0.01);
      if (performance.now() >= this.#deadline || performance.now() >= deadline) { this.#state = "lost"; this.#stop(); return; }
      this.#deadline = deadline;
    } catch { this.#state = "lost"; this.#stop(); }
  }
  #expire(): void { if ((this.#state === "waiting" || this.#state === "held") && performance.now() >= this.#deadline) { this.#state = "lost"; this.#stop(); } }
  #stop(): void { if (this.#timer) clearInterval(this.#timer); this.#timer = undefined; }
  async release(): Promise<ConsulReleaseOutcome> {
    this.#expire();
    if (this.#state === "released") return "already-not-owned";
    if (this.#state === "releasing") throw new ReleaseUnknownError("Consul release is already in progress");
    this.#state = "releasing"; this.#stop();
    let outcome: ConsulReleaseOutcome | undefined;
    let failed = false;
    let failure: unknown;
    try {
      await this.#renewing;
      const released = await this.#provider.releaseKey(this.#key, this.#session);
      if (!released) {
        const owner = await this.#provider.owner(this.#key);
        if (owner?.session === this.#session) throw new ReleaseUnknownError("Consul rejected release while the session still owns the lock");
      }
      outcome = released ? "released" : "already-not-owned";
    } catch (cause) {
      failed = true;
      failure = cause;
    }
    let cleanupConfirmed = false;
    try { await this.#provider.destroySession(this.#session); cleanupConfirmed = true; }
    catch (cause) { if (!failed) failure = cause; failed = true; }
    this.#provider.removeLease(this);
    if (failed || !cleanupConfirmed || outcome === undefined) {
      this.#state = "lost";
      if (failure instanceof ReleaseUnknownError) throw failure;
      throw new ReleaseUnknownError("Consul release or session cleanup was not confirmed", { cause: failure });
    }
    this.#state = "released";
    return outcome;
  }
  async abandon(): Promise<void> {
    this.#stop();
    await this.#renewing;
    try { await this.#provider.releaseKey(this.#key, this.#session); } catch { }
    try { await this.#provider.destroySession(this.#session); } catch { }
    this.#state = "lost"; this.#provider.removeLease(this);
  }
}
