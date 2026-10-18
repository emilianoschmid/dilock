import { randomUUID } from "node:crypto";
import {
  AcquisitionUnknownError,
  AcquisitionTimeoutError,
  BackendUnavailableError,
  InvalidArgumentError,
  LeaseLostError,
  ProtocolMismatchError,
  ReleaseUnknownError,
  UnsupportedCapabilityError,
  identity,
} from "@dilock/core";

const ACQUIRE_PROFILE = "postgres-advisory-session-v1";
const IDENTITY_SCHEME = "sha256-framed-utf8-v1";
const MAX_TIMEOUT_MS = 60_000;
const MAX_OPERATION_MS = 86_400_000;

export interface PostgresProviderOptions {
  readonly connectionString: string;
  readonly namespace?: string;
  readonly epoch: string;
  readonly rpcTimeoutMs?: number;
  readonly cleanupTimeoutMs?: number;
}
export interface AcquireOptions {
  readonly waitMs: number;
  readonly leaseMs: number;
  readonly rpcTimeoutMs: number;
  readonly signal?: AbortSignal;
  readonly requireFencing?: boolean;
  readonly autoRenew?: boolean;
}
export interface TryAcquireOptions extends Omit<AcquireOptions, "waitMs"> {}
interface ClientConfig { connectionString?: string; connectionTimeoutMillis?: number }
interface QueryResult<Row = Record<string, unknown>> { rows: Row[] }
interface QueryConfig { text: string; values?: readonly unknown[]; signal?: AbortSignal }
interface ClientLike {
  connect(): Promise<void>;
  end(): Promise<void>;
  on?(event: "error", listener: (error: unknown) => void): unknown;
  query<Row = Record<string, unknown>>(text: string, values?: readonly unknown[]): Promise<QueryResult<Row>>;
  query<Row = Record<string, unknown>>(config: QueryConfig): Promise<QueryResult<Row>>;
}
type ClientConstructor = new (config?: ClientConfig) => ClientLike;
export type ReleaseOutcome = "released" | "already-not-owned";

function integer(value: number, name: string, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new InvalidArgumentError(`${name} must be an integer from ${minimum} through ${maximum}`);
  return value;
}

/** The first 8 bytes of the Dilock identity digest, interpreted as signed int64. */
export function advisoryKey(resourceId: string): bigint {
  if (typeof resourceId !== "string" || !/^[0-9a-f]{64}$/.test(resourceId)) throw new InvalidArgumentError("resourceId must be a lowercase SHA-256 hex digest");
  const unsigned = BigInt(`0x${resourceId.slice(0, 16)}`);
  return unsigned >= 0x8000000000000000n ? unsigned - 0x1_0000_0000_0000_0000n : unsigned;
}

type MetadataRow = { protocol_major: number; identity_scheme: string; profile: string; coordination_epoch: string; fixed_lease_ms: number | null };
function validateMetadata(row: MetadataRow | undefined, epoch: string): void {
  if (!row || row.protocol_major !== 1 || row.identity_scheme !== IDENTITY_SCHEME || row.profile !== ACQUIRE_PROFILE || row.coordination_epoch !== epoch || row.fixed_lease_ms !== null) {
    throw new ProtocolMismatchError("namespace metadata is incompatible with PostgreSQL advisory-session draft 1");
  }
}

function operationSignal(caller: AbortSignal | undefined, timeoutMs: number): { signal: AbortSignal; dispose: () => void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new DOMException("PostgreSQL command timed out", "TimeoutError")), timeoutMs);
  const abort = () => controller.abort(caller?.reason ?? new DOMException("Aborted", "AbortError"));
  caller?.addEventListener("abort", abort, { once: true });
  return { signal: controller.signal, dispose: () => { clearTimeout(timer); caller?.removeEventListener("abort", abort); } };
}

async function createClient(config: ClientConfig): Promise<ClientLike> {
  // @ts-ignore -- pg's runtime package has no declarations in this release.
  const module = await import("pg") as unknown as { Client: ClientConstructor };
  const client = new module.Client(config);
  // pg emits asynchronous errors for an idle connection whose PostgreSQL
  // session was terminated.  Query callers still receive the operation error;
  // this listener prevents Node from treating the later event as unhandled
  // and crashing before the provider can report an unknown outcome.
  client.on?.("error", () => undefined);
  return client;
}

async function queryBounded<T>(client: ClientLike, sql: string, values: readonly unknown[], timeoutMs: number, caller?: AbortSignal): Promise<T[]> {
  const bounded = operationSignal(caller, timeoutMs);
  try {
    bounded.signal.throwIfAborted();
    const result = await client.query<T>({ text: sql, values, signal: bounded.signal });
    bounded.signal.throwIfAborted();
    return result.rows;
  } finally { bounded.dispose(); }
}

export class PostgresProvider {
  readonly #options: Required<Pick<PostgresProviderOptions, "connectionString" | "epoch">> & { namespace: string; rpcTimeoutMs: number; cleanupTimeoutMs: number };
  #closed = false;
  #leases = new Set<PostgresLease>();
  #closePromise: Promise<void> | undefined;

  private constructor(options: PostgresProviderOptions) {
    this.#options = {
      connectionString: options.connectionString,
      epoch: options.epoch,
      namespace: options.namespace ?? "default",
      rpcTimeoutMs: integer(options.rpcTimeoutMs ?? 2000, "rpcTimeoutMs", 1, MAX_TIMEOUT_MS),
      cleanupTimeoutMs: integer(options.cleanupTimeoutMs ?? 2000, "cleanupTimeoutMs", 1, MAX_TIMEOUT_MS),
    };
  }

  static async connect(options: PostgresProviderOptions): Promise<PostgresProvider> {
    if (typeof options.connectionString !== "string" || options.connectionString.length === 0) throw new InvalidArgumentError("connectionString must be non-empty");
    identity(options.namespace ?? "default", "validation");
    if (typeof options.epoch !== "string" || options.epoch.length === 0 || Buffer.byteLength(options.epoch) > 1024) throw new InvalidArgumentError("epoch must be a non-empty string up to 1024 UTF-8 bytes");
    const provider = new PostgresProvider(options);
    const client = await provider.#newClient();
    try {
      await client.connect();
      const rows = await queryBounded<MetadataRow>(client, "SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = $1", [provider.#options.namespace], provider.#options.rpcTimeoutMs);
      validateMetadata(rows[0], provider.#options.epoch);
      return provider;
    } catch (error) {
      if (error instanceof InvalidArgumentError || error instanceof ProtocolMismatchError) throw error;
      throw new BackendUnavailableError("could not connect to or validate the PostgreSQL authority", { cause: error });
    } finally { await client.end().catch(() => undefined); }
  }

  async #newClient(): Promise<ClientLike> { return createClient({ connectionString: this.#options.connectionString, connectionTimeoutMillis: this.#options.rpcTimeoutMs }); }
  #ensureOpen(): void { if (this.#closed) throw new InvalidArgumentError("provider is closed"); }
  lock(resource: string): PostgresLock { this.#ensureOpen(); return new PostgresLock(this, resource); }
  async close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closed = true;
    this.#closePromise = (async () => {
      const releases = [...this.#leases].map(lease => lease.release().catch(() => undefined));
      await Promise.all(releases);
    })();
    return this.#closePromise;
  }
  get #rpcMs(): number { return this.#options.rpcTimeoutMs; }
  get #cleanupMs(): number { return this.#options.cleanupTimeoutMs; }

  async #tryAcquire(resource: string, leaseMs: number, rpcMs: number, signal?: AbortSignal): Promise<PostgresLease | undefined> {
    this.#ensureOpen();
    if (signal?.aborted) throw signal.reason;
    const id = identity(this.#options.namespace, resource);
    const client = await this.#newClient();
    try {
      await client.connect();
      const rows = await queryBounded<{ acquired: boolean }>(client, "SELECT pg_try_advisory_lock($1::bigint) AS acquired", [advisoryKey(id).toString()], rpcMs, signal);
      if (rows.length !== 1 || typeof rows[0]?.acquired !== "boolean") throw new AcquisitionUnknownError("PostgreSQL returned no valid advisory-lock acknowledgement");
      if (!rows[0].acquired) { await client.end(); return undefined; }
      const lease = new PostgresLease(this, client, resource, leaseMs, rpcMs);
      this.#leases.add(lease);
      return lease;
    } catch (error) {
      await client.end().catch(() => undefined);
      if (error instanceof InvalidArgumentError || error instanceof UnsupportedCapabilityError) throw error;
      if (error instanceof AcquisitionUnknownError) throw error;
      if (signal?.aborted) throw new AcquisitionUnknownError("PostgreSQL acquisition was cancelled; the advisory-lock outcome is unknown", { cause: error });
      throw new AcquisitionUnknownError("PostgreSQL did not confirm the advisory-lock acquisition outcome", { cause: error });
    }
  }

  async acquire(resource: string, options: AcquireOptions): Promise<PostgresLease> {
    if (options.requireFencing || options.autoRenew) throw new UnsupportedCapabilityError("PostgreSQL advisory-session profile provides neither fencing nor TTL auto-renewal");
    const waitMs = integer(options.waitMs, "waitMs", 1, MAX_OPERATION_MS);
    const end = performance.now() + waitMs;
    for (;;) {
      options.signal?.throwIfAborted();
      const remaining = end - performance.now();
      if (remaining < 1) throw new AcquisitionTimeoutError("timed out waiting for PostgreSQL advisory lock contention to clear");
      const lease = await this.#tryAcquire(resource, integer(options.leaseMs, "leaseMs", 1, MAX_OPERATION_MS), integer(options.rpcTimeoutMs, "rpcTimeoutMs", 1, MAX_TIMEOUT_MS), options.signal);
      if (lease) return lease;
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, Math.min(remaining, 25 + Math.random() * 50));
        const abort = () => { clearTimeout(timer); options.signal?.removeEventListener("abort", abort); reject(options.signal?.reason); };
        options.signal?.addEventListener("abort", abort, { once: true });
      });
    }
  }

  async tryAcquire(resource: string, options: TryAcquireOptions): Promise<PostgresLease | undefined> {
    if (options.requireFencing || options.autoRenew) throw new UnsupportedCapabilityError("PostgreSQL advisory-session profile provides neither fencing nor TTL auto-renewal");
    const leaseMs = integer(options.leaseMs, "leaseMs", 1, MAX_OPERATION_MS);
    const rpcMs = integer(options.rpcTimeoutMs, "rpcTimeoutMs", 1, MAX_TIMEOUT_MS);
    return this.#tryAcquire(resource, leaseMs, rpcMs, options.signal);
  }

  async release(lease: PostgresLease): Promise<ReleaseOutcome> {
    let outcome: ReleaseOutcome;
    let failure: unknown;
    try {
      const rows = await queryBounded<{ released: boolean }>(lease.client, "SELECT pg_advisory_unlock($1::bigint) AS released", [advisoryKey(identity(this.#options.namespace, lease.resource)).toString()], this.#cleanupMs);
      if (rows.length !== 1 || typeof rows[0]?.released !== "boolean") throw new Error("PostgreSQL returned no valid advisory-unlock acknowledgement");
      outcome = rows[0].released ? "released" : "already-not-owned";
    } catch (error) { failure = error; }
    try { await lease.client.end(); }
    catch (error) { failure ??= error; }
    this.#leases.delete(lease);
    if (failure !== undefined) throw new ReleaseUnknownError("PostgreSQL did not confirm advisory-lock release", { cause: failure });
    return outcome!;
  }

  async liveness(lease: PostgresLease): Promise<void> {
    try { await queryBounded(lease.client, "SELECT 1", [], lease.rpcMs); }
    catch (error) { throw new BackendUnavailableError("PostgreSQL advisory-lock session liveness failed", { cause: error }); }
  }

  get namespace(): string { return this.#options.namespace; }
}

export class PostgresLock {
  readonly #provider: PostgresProvider;
  readonly #resource: string;
  constructor(provider: PostgresProvider, resource: string) { identity(provider.namespace, resource); this.#provider = provider; this.#resource = resource; }
  async tryAcquire(options: TryAcquireOptions): Promise<PostgresLease | undefined> {
    return this.#provider.tryAcquire(this.#resource, options);
  }
  async acquire(options: AcquireOptions): Promise<PostgresLease> {
    if (options.requireFencing || options.autoRenew) throw new UnsupportedCapabilityError("PostgreSQL advisory-session profile provides neither fencing nor TTL auto-renewal");
    return this.#provider.acquire(this.#resource, options);
  }
  async withLease<T>(work: (lease: PostgresLease) => Promise<T> | T, options: AcquireOptions): Promise<T> {
    if (typeof work !== "function") throw new InvalidArgumentError("work must be a function");
    const lease = await this.acquire(options);
    try { return await work(lease); }
    finally { await lease.release(); }
  }
}

export class PostgresLease {
  readonly id = randomUUID();
  readonly #provider: PostgresProvider;
  readonly client: ClientLike;
  readonly resource: string;
  readonly #leaseMs: number;
  readonly #rpcMs: number;
  #state: "held" | "releasing" | "released" | "release-unknown" | "lost" = "held";
  #releasePromise: Promise<ReleaseOutcome> | undefined;
  #releaseOutcome: ReleaseOutcome | undefined;
  constructor(provider: PostgresProvider, client: ClientLike, resource: string, leaseMs: number, rpcMs: number) { this.#provider = provider; this.client = client; this.resource = resource; this.#leaseMs = leaseMs; this.#rpcMs = rpcMs; }
  get rpcMs(): number { return this.#rpcMs; }
  get state(): string { return this.#state; }
  assertValid(): void { if (this.#state !== "held") throw new LeaseLostError("lease is no longer locally valid"); }
  async renew(): Promise<void> {
    this.assertValid();
    try { await this.#provider.liveness(this); }
    catch (error) { this.#state = "lost"; throw error; }
  }
  async release(): Promise<ReleaseOutcome> {
    if (this.#releaseOutcome !== undefined) return this.#releaseOutcome;
    if (this.#state === "release-unknown") throw new ReleaseUnknownError("a previous PostgreSQL release acknowledgement was unknown");
    if (this.#state === "releasing") return this.#releasePromise ?? Promise.reject(new ReleaseUnknownError("PostgreSQL release is already in progress without a tracked acknowledgement"));
    this.#state = "releasing";
    this.#releasePromise = this.#provider.release(this).then(outcome => {
      this.#releaseOutcome = outcome;
      this.#state = outcome === "released" ? "released" : "lost";
      return outcome;
    }).catch(error => { this.#state = "release-unknown"; throw error; });
    return this.#releasePromise;
  }
}

export async function provisionNamespace(connectionString: string, namespace: string, epoch: string): Promise<"created" | "compatible-existing"> {
  identity(namespace, "validation");
  if (!epoch || Buffer.byteLength(epoch, "utf8") > 1024) throw new InvalidArgumentError("epoch must be a non-empty string up to 1024 UTF-8 bytes");
  const client = await createClient({ connectionString });
  try {
    await client.connect();
    await client.query("BEGIN");
    try {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", ["dilock:namespace-meta:v1"]);
      await client.query("CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)");
      const insert = await client.query("INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES ($1, 1, $2, $3, $4, NULL) ON CONFLICT (namespace) DO NOTHING RETURNING namespace", [namespace, IDENTITY_SCHEME, ACQUIRE_PROFILE, epoch]);
      await client.query("COMMIT");
      const rows = await client.query<MetadataRow>("SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = $1", [namespace]);
      validateMetadata(rows.rows[0], epoch);
      return insert.rows.length ? "created" : "compatible-existing";
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    }
  } catch (error) {
    if (error instanceof ProtocolMismatchError || error instanceof InvalidArgumentError) throw error;
    throw new BackendUnavailableError("failed provisioning PostgreSQL namespace metadata", { cause: error });
  } finally { await client.end().catch(() => undefined); }
}
