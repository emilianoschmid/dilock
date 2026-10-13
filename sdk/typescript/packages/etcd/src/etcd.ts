import { resolve } from "node:path";
import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";
import { identity } from "@dilock/core";
import {
  AcquisitionTimeoutError, AcquisitionUnknownError, BackendUnavailableError,
  InvalidArgumentError, LeaseLostError, ProtocolMismatchError, ReleaseUnknownError, UnsupportedCapabilityError,
} from "@dilock/core";
import { boundedEtcdCleanup, classifyEtcdLockFailure, classifyEtcdRelease, classifyEtcdReleaseOwnership, etcdCleanupConfirmed, type EtcdReleaseOwnership } from "./internal/etcd-lock-failure.js";
import { closeOnce } from "./internal/close-once.js";
import { endpointTransport } from "./internal/endpoint.js";
import { parseMetadata } from "./internal/metadata.js";

const require = createRequire(import.meta.url);
const grpc = require("@grpc/grpc-js") as any;
const protoLoader = require("@grpc/proto-loader") as any;
const PROTO = resolve(import.meta.dirname, "../proto/etcd-v3.7.2/v3lock.proto");
const definition = protoLoader.loadSync(PROTO, { keepCase: true, longs: String, enums: String, defaults: false, oneofs: true });
const etcd = grpc.loadPackageDefinition(definition) as any;
const MAX_TTL_MS = 86_400_000;

export interface EtcdProviderOptions {
  /** A gRPC endpoint such as `http://127.0.0.1:2379`, `https://etcd.example:2379`, or `127.0.0.1:2379`. HTTPS uses system trust roots; custom roots and client certificates are not configurable in this profile. */
  endpoint: string;
  /** A pre-provisioned draft 1 namespace; runtime clients never create metadata. */
  namespace?: string;
  /** Stable operator-approved coordinator incarnation identifier. */
  epoch: string;
  rpcTimeoutMs?: number;
  /** Total monotonic budget shared by sequential cleanup RPCs. */
  cleanupTimeoutMs?: number;
  defaultAcquisition?: EtcdAcquisitionDefaults;
  cleanupObserver?: (observation: { readonly backend: "etcd"; readonly outcome: "unknown"; readonly reason: string }) => void;
}

/** Complete immutable provider policy used only when an operation omits its own policy. */
export interface EtcdAcquisitionDefaults {
  readonly waitMs: number;
  readonly leaseMs: number;
  readonly rpcTimeoutMs: number;
  readonly autoRenew: boolean;
}

export interface EtcdAcquireOptions {
  /** Total monotonic wait budget; a late confirmed Lock is cleaned up before timeout is returned. */
  waitMs: number;
  leaseMs: number;
  rpcTimeoutMs: number;
  /** Keep the lease renewed after acquisition; waiting attempts always renew. Defaults to false. */
  autoRenew: boolean;
  signal?: AbortSignal;
}
export interface EtcdTryAcquireOptions extends Omit<EtcdAcquireOptions, "waitMs"> {}

interface Reply { [key: string]: any }

export class EtcdAcquisitionCancelledError extends AcquisitionUnknownError {
  readonly remoteOutcomeUnknown: boolean;
  constructor(remoteOutcomeUnknown: boolean, message: string, cause?: unknown) {
    super(message, cause === undefined ? undefined : { cause });
    this.remoteOutcomeUnknown = remoteOutcomeUnknown;
  }
}

function integer(value: number, name: string, min: number, max: number): number {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new InvalidArgumentError(`${name} must be an integer from ${min} through ${max}`);
  return value;
}

function booleanOption(value: unknown, name: string, fallback: boolean): boolean {
  if (value === undefined) return fallback;
  if (typeof value !== "boolean") throw new InvalidArgumentError(`${name} must be a boolean`);
  return value;
}

/** @internal */
function unary(client: any, method: string, request: Reply, timeoutMs: number, signal?: AbortSignal): Promise<Reply> {
  if (signal?.aborted) return Promise.reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
  return new Promise((resolvePromise, rejectPromise) => {
    let settled = false;
    let call: any;
    const cleanup = () => signal?.removeEventListener("abort", abort);
    const finish = (error: unknown, value?: Reply) => {
      if (settled) return;
      settled = true; cleanup();
      if (error) rejectPromise(error); else resolvePromise(value ?? {});
    };
    const abort = () => { call?.cancel(); finish(signal?.reason ?? new DOMException("Aborted", "AbortError")); };
    signal?.addEventListener("abort", abort, { once: true });
    const deadline = new Date(Date.now() + timeoutMs);
    try { call = client[method](request, { deadline }, (error: unknown, value: Reply) => finish(error, value)); }
    catch (error) { finish(error); }
  });
}

function grpcErrorCode(error: unknown): number | undefined {
  return error && typeof error === "object" && "code" in error ? Number((error as { code: unknown }).code) : undefined;
}

/** @internal Uses the monotonic clock selected by the acquisition caller. */
export function isEtcdWaitBudgetExpired(deadline: number, now: number): boolean { return now >= deadline; }

/** @internal Returns the remaining part of one monotonic acquisition wait budget. */
export function remainingEtcdWaitBudget(waitMs: number, startedAt: number, now: number): number {
  return Math.max(0, waitMs - (now - startedAt));
}

export class EtcdProvider {
  readonly #kv: any;
  readonly #leasesRpc: any;
  readonly #locks: any;
  readonly #namespace: string;
  readonly #epoch: string;
  readonly #rpcMs: number;
  readonly #cleanupMs: number;
  readonly #defaultAcquisition: Readonly<EtcdAcquisitionDefaults>;
  readonly #cleanupObserver: EtcdProviderOptions["cleanupObserver"];
  readonly #leases = new Map<string, EtcdLease>();
  readonly #shutdown = new AbortController();
  readonly #activeAttempts = new Set<Promise<void>>();
  readonly #closeOperation: () => Promise<void>;
  #closed = false;

  private constructor(options: EtcdProviderOptions) {
    this.#namespace = options.namespace ?? "default";
    this.#epoch = options.epoch;
    this.#rpcMs = integer(options.rpcTimeoutMs ?? 2000, "rpcTimeoutMs", 1, 60_000);
    this.#cleanupMs = integer(options.cleanupTimeoutMs ?? 2000, "cleanupTimeoutMs", 1, 60_000);
    this.#defaultAcquisition = Object.freeze({
      waitMs: integer(options.defaultAcquisition?.waitMs ?? 5000, "defaultAcquisition.waitMs", 1, MAX_TTL_MS),
      leaseMs: integer(options.defaultAcquisition?.leaseMs ?? 30_000, "defaultAcquisition.leaseMs", 1, MAX_TTL_MS),
      rpcTimeoutMs: integer(options.defaultAcquisition?.rpcTimeoutMs ?? this.#rpcMs, "defaultAcquisition.rpcTimeoutMs", 1, 60_000),
      autoRenew: booleanOption(options.defaultAcquisition?.autoRenew, "defaultAcquisition.autoRenew", false),
    });
    if (this.#defaultAcquisition.rpcTimeoutMs + Math.max(10, Math.ceil(this.#defaultAcquisition.leaseMs * 0.01)) >= this.#defaultAcquisition.leaseMs) {
      throw new InvalidArgumentError("default acquisition RPC timeout plus safety margin must be less than the lease");
    }
    this.#cleanupObserver = options.cleanupObserver;
    const endpoint = endpointTransport(options.endpoint);
    const credentials = endpoint.secure ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();
    this.#kv = new etcd.etcdserverpb.KV(endpoint.target, credentials);
    this.#leasesRpc = new etcd.etcdserverpb.Lease(endpoint.target, credentials);
    this.#locks = new etcd.v3lockpb.Lock(endpoint.target, credentials);
    this.#closeOperation = closeOnce(() => this.#finishClose());
  }

  /** Connects to one etcd v3 authority, validating metadata through a linearizable Range. */
  static async connect(options: EtcdProviderOptions): Promise<EtcdProvider> {
    identity(options.namespace ?? "default", "validation");
    if (typeof options.epoch !== "string" || options.epoch.length === 0 || Buffer.byteLength(options.epoch, "utf8") > 1024) throw new InvalidArgumentError("epoch must be non-empty and at most 1024 UTF-8 bytes");
    const provider = new EtcdProvider(options);
    try { await provider.#validateMetadata(); return provider; }
    catch (error) { provider.#closeClients(); throw error; }
  }

  /** Returns a reusable resource handle; it is not a local mutex. */
  lock(resource: string): EtcdLock { this.#ensureOpen(); identity(this.#namespace, resource); return new EtcdLock(this, resource); }

  defaultAcquireOptions(): Readonly<EtcdAcquisitionDefaults> { return this.#defaultAcquisition; }
  observeCleanup(reason: string): void {
    try {
      if (this.#cleanupObserver) this.#cleanupObserver({ backend: "etcd", outcome: "unknown", reason });
      else process.emitWarning(`Dilock etcd cleanup outcome is unknown (${reason})`);
    }
    catch { /* A diagnostic callback cannot replace work or cleanup results. */ }
  }

  async #validateMetadata(): Promise<void> {
    const key = Buffer.from(`/dilock/namespaces/${this.#namespace}/meta`, "utf8");
    let result: Reply;
    try { result = await unary(this.#kv, "range", { key }, this.#rpcMs); }
    catch (cause) { throw new BackendUnavailableError("failed reading etcd namespace metadata", { cause }); }
    const value = result.kvs?.[0]?.value;
    if (!value) throw new ProtocolMismatchError("namespace metadata is missing; provision it explicitly before use");
    parseMetadata(Buffer.from(value), this.#namespace, this.#epoch);
  }

  #ensureOpen(): void { if (this.#closed) throw new InvalidArgumentError("provider is closed"); }

  async acquire(resource: string, options: Required<Pick<EtcdAcquireOptions, "waitMs" | "leaseMs" | "rpcTimeoutMs" | "autoRenew">> & Pick<EtcdAcquireOptions, "signal">): Promise<EtcdLease> {
    let finish!: () => void;
    const active = new Promise<void>(resolvePromise => { finish = resolvePromise; });
    this.#activeAttempts.add(active);
    const signal = options.signal ? AbortSignal.any([options.signal, this.#shutdown.signal]) : this.#shutdown.signal;
    try { return await this.#acquireCore(resource, { ...options, signal }); }
    finally { finish(); this.#activeAttempts.delete(active); }
  }

  async #acquireCore(resource: string, options: Required<Pick<EtcdAcquireOptions, "waitMs" | "leaseMs" | "rpcTimeoutMs" | "autoRenew">> & { signal: AbortSignal }): Promise<EtcdLease> {
    this.#ensureOpen();
    options.signal?.throwIfAborted();
    const resourceId = identity(this.#namespace, resource);
    const ttlSeconds = Math.ceil(options.leaseMs / 1000);
    const leaseMsLimit = integer(options.leaseMs, "leaseMs", 1, MAX_TTL_MS);
    const margin = Math.max(10, Math.ceil(leaseMsLimit * 0.01));
    if (options.rpcTimeoutMs + margin >= leaseMsLimit) throw new InvalidArgumentError("RPC timeout plus safety margin must be less than the lease");

    const acquisitionStartedAt = performance.now();
    const waitDeadline = acquisitionStartedAt + options.waitMs;
    const grantTimeoutMs = Math.min(options.rpcTimeoutMs, remainingEtcdWaitBudget(options.waitMs, acquisitionStartedAt, performance.now()));
    if (grantTimeoutMs <= 0) throw new AcquisitionTimeoutError("acquisition wait budget expired before etcd lease grant");
    const grantSentAt = performance.now();
    let granted: Reply;
    try { granted = await unary(this.#leasesRpc, "leaseGrant", { TTL: ttlSeconds, ID: "0" }, grantTimeoutMs, options.signal); }
    catch (cause) {
      if (options.signal?.aborted) throw new AcquisitionUnknownError("cancellation occurred during lease grant; the remote lease outcome is unknown", { cause });
      throw new AcquisitionUnknownError("etcd did not confirm lease grant; an unknown lease may expire by its server TTL", { cause });
    }
    const leaseId = String(granted.ID);
    const grantedTtlSeconds = Number(granted.TTL);
    if (!leaseId || leaseId === "0" || !Number.isSafeInteger(grantedTtlSeconds) || grantedTtlSeconds <= 0) throw new AcquisitionUnknownError("etcd returned an invalid lease grant");
    const lease = new EtcdLease(this, leaseId, grantedTtlSeconds * 1000, grantSentAt + grantedTtlSeconds * 1000 - Math.max(10, Math.ceil(grantedTtlSeconds * 10)));
    this.#leases.set(lease.id, lease);
    lease.startKeepAlive();
    if (!lease.isConservativelyValid()) {
      await this.#revokeAndForget(lease);
      throw new AcquisitionUnknownError("keepalive could not be established before the conservative lease deadline");
    }

    const lockName = Buffer.from(`/dilock/locks/${resourceId}`, "utf8");
    let locked: Reply;
    const lockStartedAt = performance.now();
    const lockTimeoutMs = remainingEtcdWaitBudget(options.waitMs, acquisitionStartedAt, lockStartedAt);
    if (lockTimeoutMs <= 0) {
      const revoked = await this.#revoke(leaseId);
      lease.stopKeepAlive(); this.#leases.delete(lease.id); lease.markLost();
      if (!revoked) throw new AcquisitionUnknownError("acquisition wait budget expired after lease grant and revocation was not confirmed");
      throw new AcquisitionTimeoutError("acquisition wait budget expired after lease grant; revocation was confirmed");
    }
    try {
      locked = await unary(this.#locks, "lock", { name: lockName, lease: leaseId }, lockTimeoutMs, options.signal);
    } catch (cause) {
      const lockElapsedMs = performance.now() - lockStartedAt;
      const revoked = await this.#revoke(leaseId);
      lease.stopKeepAlive(); this.#leases.delete(lease.id); lease.markLost();
      const disposition = classifyEtcdLockFailure(
        cause, lockElapsedMs, lockTimeoutMs, Boolean(options.signal?.aborted), revoked,
      );
      if (disposition === "cancelled") {
        throw new EtcdAcquisitionCancelledError(!revoked, revoked ? "acquisition cancelled after send; the attempt lease was revoked" : "cancellation cleanup could not confirm lease revocation", options.signal?.reason);
      }
      if (disposition === "unknown") throw new AcquisitionUnknownError("native Lock failed and lease revocation was not confirmed", { cause });
      if (disposition === "timeout") throw new AcquisitionTimeoutError("timed out waiting for etcd native Lock", { cause });
      throw new BackendUnavailableError("etcd native Lock RPC failed; its attempt lease was revoked", { cause });
    }

    if (options.signal?.aborted) {
      const cleanup = await this.#unlockAndRevoke(lease, Buffer.from(locked.key));
      throw new EtcdAcquisitionCancelledError(!cleanup, cleanup ? "cancellation arrived after native Lock succeeded; Unlock and Revoke were confirmed" : "cancellation arrived after native Lock succeeded; cleanup acknowledgement is unknown", options.signal.reason);
    }
    const opaqueKey = Buffer.from(locked.key);
    if (!opaqueKey.length) {
      await this.#revokeAndForget(lease);
      throw new AcquisitionUnknownError("etcd native Lock returned an empty opaque key");
    }
    if (isEtcdWaitBudgetExpired(waitDeadline, performance.now())) {
      const cleanup = await this.#unlockAndRevoke(lease, opaqueKey);
      if (!cleanup) throw new AcquisitionUnknownError("etcd native Lock completed after the acquisition wait budget and cleanup was not confirmed");
      throw new AcquisitionTimeoutError("etcd native Lock completed after the acquisition wait budget; Unlock and Revoke were confirmed");
    }
    let range: Reply;
    const verifyTimeoutMs = Math.min(options.rpcTimeoutMs, remainingEtcdWaitBudget(options.waitMs, acquisitionStartedAt, performance.now()));
    if (verifyTimeoutMs <= 0) {
      const cleanup = await this.#unlockAndRevoke(lease, opaqueKey);
      if (!cleanup) throw new AcquisitionUnknownError("acquisition wait budget expired before ownership verification and cleanup was not confirmed");
      throw new AcquisitionTimeoutError("acquisition wait budget expired before ownership verification; cleanup was confirmed");
    }
    try { range = await unary(this.#kv, "range", { key: opaqueKey }, verifyTimeoutMs, options.signal); }
    catch (cause) {
      const cleanup = await this.#unlockAndRevoke(lease, opaqueKey);
      if (options.signal?.aborted) throw new EtcdAcquisitionCancelledError(!cleanup, cleanup ? "cancelled during lock-key verification; cleanup was confirmed" : "cancelled during lock-key verification; cleanup acknowledgement is unknown", options.signal.reason);
      if (cleanup && isEtcdWaitBudgetExpired(waitDeadline, performance.now()))
        throw new AcquisitionTimeoutError("etcd ownership verification exceeded the acquisition wait budget; cleanup was confirmed", { cause });
      throw new AcquisitionUnknownError("could not verify the exact native lock key; cleanup was attempted", { cause });
    }
    const kv = range.kvs?.[0];
    if (!kv || !Buffer.from(kv.key).equals(opaqueKey) || String(kv.lease) !== leaseId || !/^[1-9][0-9]*$/.test(String(kv.create_revision))) {
      await this.#unlockAndRevoke(lease, opaqueKey);
      throw new AcquisitionUnknownError("linearizable Range did not confirm the exact lock key attached to this attempt lease");
    }
    if (!lease.isConservativelyValid()) {
      await this.#unlockAndRevoke(lease, opaqueKey);
      throw new AcquisitionUnknownError("verification exceeded the conservative etcd lease budget");
    }
    if (isEtcdWaitBudgetExpired(waitDeadline, performance.now())) {
      const cleanup = await this.#unlockAndRevoke(lease, opaqueKey);
      if (!cleanup) throw new AcquisitionUnknownError("etcd lock verification completed after the acquisition wait budget and cleanup was not confirmed");
      throw new AcquisitionTimeoutError("etcd lock verification completed after the acquisition wait budget; Unlock and Revoke were confirmed");
    }
    lease.activate(opaqueKey, String(kv.create_revision));
    if (!options.autoRenew) lease.stopKeepAlive();
    return lease;
  }

  async #revoke(leaseId: string): Promise<boolean> {
    try { await unary(this.#leasesRpc, "leaseRevoke", { ID: leaseId }, this.#cleanupMs); return true; }
    catch { return false; }
  }

  async #revokeAndForget(lease: EtcdLease): Promise<void> {
    lease.stopKeepAlive(); await this.#revoke(lease.id); this.#leases.delete(lease.id); lease.markLost();
  }

  async #unlockAndRevoke(lease: EtcdLease, key: Buffer): Promise<boolean> {
    lease.stopKeepAlive();
    const cleanup = await boundedEtcdCleanup(this.#cleanupMs, () => performance.now(),
      timeoutMs => unary(this.#locks, "unlock", { key }, timeoutMs),
      timeoutMs => unary(this.#leasesRpc, "leaseRevoke", { ID: lease.id }, timeoutMs));
    this.#leases.delete(lease.id);
    lease.markLost();
    return etcdCleanupConfirmed(cleanup.unlockAcknowledged, cleanup.revokeAcknowledged);
  }

  async release(lease: EtcdLease, key: Buffer): Promise<"released" | "already-not-owned"> {
    lease.stopKeepAlive();
    const cleanupStartedAt = performance.now();
    const remaining = () => Math.floor(this.#cleanupMs - (performance.now() - cleanupStartedAt));
    let ownership: EtcdReleaseOwnership = "unknown";
    const verifyBudget = remaining();
    if (key.length > 0 && verifyBudget > 0) {
      try {
        const range = await unary(this.#kv, "range", { key }, verifyBudget);
        ownership = classifyEtcdReleaseOwnership(range.kvs, key, lease.id);
      } catch {
        ownership = "unknown";
      }
    } else if (key.length === 0) {
      ownership = "not-owned";
    }

    let unlockAcknowledged = false;
    let unlockError: unknown;
    const unlockBudget = remaining();
    if (ownership === "owned" && unlockBudget > 0) {
      try { await unary(this.#locks, "unlock", { key }, unlockBudget); unlockAcknowledged = true; }
      catch (error) { unlockError = error; }
    }

    let revokeAcknowledged = false;
    let revokeError: unknown;
    const revokeBudget = remaining();
    if (revokeBudget > 0) {
      try { await unary(this.#leasesRpc, "leaseRevoke", { ID: lease.id }, revokeBudget); revokeAcknowledged = true; }
      catch (error) {
        revokeError = error;
        if (grpcErrorCode(error) === 5) revokeAcknowledged = true;
      }
    }
    this.#leases.delete(lease.id);
    const outcome = classifyEtcdRelease(ownership, unlockAcknowledged, revokeAcknowledged);
    if (outcome === "released") return outcome;
    if (outcome === "already-not-owned") return outcome;
    if (unlockError) {
      throw new ReleaseUnknownError("etcd did not confirm native Unlock; lease revocation was attempted", { cause: unlockError });
    }
    if (revokeError) throw new ReleaseUnknownError("etcd could not confirm exact-key ownership and cleanup; lease revocation was attempted", { cause: revokeError });
    throw new ReleaseUnknownError("etcd could not confirm exact-key ownership and both cleanup acknowledgements");
  }

  close(): Promise<void> {
    return this.#closeOperation();
  }

  async #finishClose(): Promise<void> {
    this.#closed = true;
    this.#shutdown.abort(new DOMException("Provider is closing", "AbortError"));
    await Promise.allSettled([...this.#activeAttempts]);
    const leases = [...this.#leases.values()];
    const results = await Promise.allSettled(leases.map(lease => lease.release()));
    results.forEach(result => { if (result.status === "rejected") this.observeCleanup("provider-close"); });
    this.#closeClients();
  }

  #closeClients(): void { this.#kv.close(); this.#leasesRpc.close(); this.#locks.close(); }

  /** @internal */
  epochForLease(): string { return this.#epoch; }

  /** @internal */
  openKeepAlive(leaseId: string, onResponse: (response: Reply) => void, onError: () => void): any {
    const stream = this.#leasesRpc.leaseKeepAlive();
    stream.on("data", onResponse);
    stream.on("error", onError);
    stream.on("end", onError);
    return stream;
  }
}

export class EtcdLock {
  readonly #provider: EtcdProvider;
  readonly #resource: string;
  constructor(provider: EtcdProvider, resource: string) { this.#provider = provider; this.#resource = resource; }
  async tryAcquire(_options?: EtcdTryAcquireOptions): Promise<EtcdLease> {
    const effective = _options ?? this.#provider.defaultAcquireOptions();
    integer(effective.leaseMs, "leaseMs", 1, MAX_TTL_MS);
    integer(effective.rpcTimeoutMs, "rpcTimeoutMs", 1, 60_000);
    throw new UnsupportedCapabilityError("etcd native Lock does not provide a safe try-acquire operation");
  }
  async acquire(options?: EtcdAcquireOptions): Promise<EtcdLease> {
    const effective = options ?? this.#provider.defaultAcquireOptions();
    const autoRenew = booleanOption(effective.autoRenew, "autoRenew", false);
    return this.#provider.acquire(this.#resource, { waitMs: integer(effective.waitMs, "waitMs", 1, MAX_TTL_MS), leaseMs: integer(effective.leaseMs, "leaseMs", 1, MAX_TTL_MS), rpcTimeoutMs: integer(effective.rpcTimeoutMs, "rpcTimeoutMs", 1, 60_000), autoRenew, ...(options?.signal ? { signal: options.signal } : {}) });
  }

  /**
   * Acquires once, runs the callback, and awaits bounded release. A release
   * failure rejects successful work; if work already failed, its error remains
   * primary and unknown cleanup is reported to the provider observer.
  */
  async withLease<T>(work: (lease: EtcdLease) => Promise<T> | T, options?: EtcdAcquireOptions): Promise<T> {
    if (typeof work !== "function") throw new InvalidArgumentError("work must be a function");
    const lease = await this.acquire(options);
    let workFailed = false;
    let workError: unknown;
    let value: T | undefined;
    try { value = await work(lease); }
    catch (error) { workFailed = true; workError = error; }

    try {
      const outcome = await lease.release();
      if (outcome !== "released") throw new LeaseLostError("etcd lease was no longer owned when its scope ended");
    }
    catch (error) {
      this.#provider.observeCleanup(workFailed ? "with-lease-after-work-error" : "with-lease");
      if (!workFailed) throw error;
    }
    if (workFailed) throw workError;
    return value as T;
  }
}

export class EtcdLease {
  readonly id: string;
  readonly effectiveLeaseMs: number;
  readonly #provider: EtcdProvider;
  #key: Buffer | undefined;
  #createRevision: string | undefined;
  #deadline: number;
  #state: "waiting" | "held" | "releasing" | "released" | "release-unknown" | "lost" = "waiting";
  #stoppingKeepAlive = false;
  #stream: any;
  #timer: NodeJS.Timeout | undefined;
  #release: Promise<"released" | "already-not-owned"> | undefined;
  #releaseOutcome: "released" | "already-not-owned" | undefined;
  readonly #keepAliveSentAt: number[] = [];

  constructor(provider: EtcdProvider, id: string, effectiveLeaseMs: number, deadline: number) {
    this.#provider = provider; this.id = id; this.effectiveLeaseMs = effectiveLeaseMs; this.#deadline = deadline;
  }
  get state(): string { this.#expire(); return this.#state; }
  get createRevision(): string | undefined { return this.#createRevision; }
  get fence(): { epoch: string; value: string } | undefined { return this.#createRevision ? { epoch: this.#providerEpoch(), value: this.#createRevision } : undefined; }
  #providerEpoch(): string { return this.#provider.epochForLease(); }
  startKeepAlive(): void {
    const intervalMs = Math.max(200, Math.floor(this.effectiveLeaseMs / 3));
    const open = () => {
      if (this.#state === "lost" || this.#state === "released" || this.#state === "release-unknown") return;
      try {
        this.#stream = this.#provider.openKeepAlive(this.id, response => {
          if (this.#stoppingKeepAlive) return;
          // A delayed acknowledgement must not restore local permission after
          // the last server-confirmed validity budget has elapsed. Check the
          // old deadline before replacing it with the deadline from this reply.
          const now = performance.now();
          if (now >= this.#deadline) {
            this.#state = "lost";
            this.stopKeepAlive();
            return;
          }
          const ttl = Number(response.TTL);
          if (!Number.isSafeInteger(ttl) || ttl <= 0) { this.#state = "lost"; this.stopKeepAlive(); return; }
          const sentAt = this.#keepAliveSentAt.shift();
          if (sentAt !== undefined) {
            const renewedDeadline = sentAt + ttl * 1000 - Math.max(10, Math.ceil(ttl * 10));
            if (now >= renewedDeadline) {
              this.#state = "lost";
              this.stopKeepAlive();
              return;
            }
            this.#deadline = renewedDeadline;
          }
        }, () => { if (!this.#stoppingKeepAlive) this.#state = "lost"; });
        this.#timer = setInterval(() => {
          if (this.#stoppingKeepAlive) return;
          this.#expire();
          if (this.#state === "lost") return this.stopKeepAlive();
          try { this.#keepAliveSentAt.push(performance.now()); this.#stream.write({ ID: this.id }); }
          catch { this.#state = "lost"; this.stopKeepAlive(); }
        }, intervalMs);
      } catch { this.#state = "lost"; }
    };
    open();
  }
  activate(key: Buffer, createRevision: string): void { this.#expire(); if (this.#state === "lost") throw new LeaseLostError("etcd lease validity expired before activation"); this.#key = key; this.#createRevision = createRevision; this.#state = "held"; }
  isConservativelyValid(): boolean { this.#expire(); return this.#state !== "lost" && performance.now() < this.#deadline; }
  assertValid(): void { this.#expire(); if (this.#state !== "held") throw new LeaseLostError("etcd lease is no longer locally valid"); }
  #expire(): void { if ((this.#state === "held" || this.#state === "waiting") && performance.now() >= this.#deadline) this.#state = "lost"; }
  stopKeepAlive(): void { this.#stoppingKeepAlive = true; if (this.#timer) clearInterval(this.#timer); this.#timer = undefined; this.#stream?.cancel(); this.#stream = undefined; }
  markLost(): void { this.stopKeepAlive(); this.#state = "lost"; }
  async release(): Promise<"released" | "already-not-owned"> {
    this.#expire();
    if (this.#releaseOutcome !== undefined) return this.#releaseOutcome;
    if (this.#state === "released" || (this.#state === "lost" && !this.#key)) {
      this.#releaseOutcome = "already-not-owned";
      return this.#releaseOutcome;
    }
    // Local expiry/loss does not prove that etcd removed this native key. If a verified key exists,
    // attempt owner-specific Unlock and revoke; lease expiry remains the fallback.
    if (this.#state === "release-unknown") throw new ReleaseUnknownError("a previous etcd release acknowledgement was unknown");
    if (this.#state === "releasing") return this.#release!;
    if (!this.#key) throw new InvalidArgumentError("lease has no verified native lock key");
    this.#state = "releasing";
    this.#release = this.#provider.release(this, this.#key).then(outcome => {
      this.#releaseOutcome = outcome;
      this.#state = outcome === "released" ? "released" : "lost";
      return outcome;
    }).catch(error => { this.#state = "release-unknown"; throw error; });
    return this.#release;
  }
}

