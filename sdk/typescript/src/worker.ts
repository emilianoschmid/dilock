import { readFile } from "node:fs/promises";
import { RedisLease, RedisProvider } from "@dilock/redis";
import { EtcdAcquisitionCancelledError, EtcdLease, EtcdProvider } from "@dilock/etcd";
import { AcquisitionTimeoutError, AcquisitionUnknownError, BackendUnavailableError, InvalidArgumentError, LeaseLostError, ProtocolMismatchError, ReleaseUnknownError, UnsupportedCapabilityError } from "@dilock/core";

type Request = { id: string; op: string; resource?: string; lease_id?: string; target_id?: string; wait_ms?: number; lease_ms?: number; rpc_ms?: number; auto_renew?: boolean };
const MAX_LINE_BYTES = 64 * 1024;

function status(error: unknown, tryOnly = false): string {
  if (tryOnly && error instanceof AcquisitionTimeoutError) return "busy";
  if (error instanceof AcquisitionTimeoutError) return "timeout";
  if (error instanceof AcquisitionUnknownError) return error.cause instanceof Error && error.cause.name === "AbortError" ? "cancelled" : "acquisition_unknown";
  if (error instanceof BackendUnavailableError || error instanceof ReleaseUnknownError) return error instanceof ReleaseUnknownError ? "release_unknown" : "backend_unavailable";
  if (error instanceof InvalidArgumentError || error instanceof TypeError) return "invalid_argument";
  if (error instanceof ProtocolMismatchError) return "protocol_mismatch";
  if (error instanceof UnsupportedCapabilityError) return "unsupported_capability";
  if (error instanceof LeaseLostError) return "already_not_owned";
  if (error instanceof Error && error.name === "AbortError") return "cancelled";
  return "internal_error";
}

async function main(): Promise<void> {
  const configPath = process.argv[2];
  if (!configPath) throw new Error("usage: npm run worker -- <config.json>");
  const config = JSON.parse(await readFile(configPath, "utf8")) as { backend: "redis" | "etcd"; endpoint: string; namespace: string; epoch: string };
  if (config.backend !== "redis" && config.backend !== "etcd") throw new Error("backend must be redis or etcd");
  const provider = config.backend === "redis"
    ? await RedisProvider.connect({ endpoint: config.endpoint, namespace: config.namespace, epoch: config.epoch })
    : await EtcdProvider.connect({ endpoint: config.endpoint, namespace: config.namespace, epoch: config.epoch });
  const leases = new Map<string, RedisLease | EtcdLease>();
  const released = new Map<string, string>();
  const activeIds = new Set<string>();
  const aborts = new Map<string, AbortController>();
  const tasks = new Set<Promise<void>>();
  let stopping = false;
  const write = (value: unknown) => process.stdout.write(JSON.stringify(value) + "\n");

  const handle = async (message: Request): Promise<void> => {
    try {
      switch (message.op) {
        case "hello":
          write({ id: message.id, status: "ok", sdk_version: "0.1.0", protocol: 1, backend: config.backend, capabilities: { renewal: true, auto_renew: config.backend === "etcd", fencing: config.backend === "etcd", try_acquire: config.backend === "redis" } });
          break;
        case "try_acquire":
        case "acquire": {
          if (typeof message.resource !== "string") throw new InvalidArgumentError("resource is required");
          if (config.backend === "redis" && message.auto_renew) throw new UnsupportedCapabilityError("automatic renewal is not supported by the TypeScript Redis adapter");
          const controller = new AbortController(); aborts.set(message.id, controller);
          const options = { waitMs: message.wait_ms ?? 5000, leaseMs: message.lease_ms ?? 30_000, rpcTimeoutMs: message.rpc_ms ?? 2000, autoRenew: message.auto_renew ?? false, signal: controller.signal };
          const tryOnly = message.op === "try_acquire";
          const lease = config.backend === "redis"
            ? tryOnly
              ? await provider.lock(message.resource).tryAcquire(options)
              : await provider.lock(message.resource).acquire(options)
            : tryOnly
              ? await provider.lock(message.resource).tryAcquire(options)
              : await provider.lock(message.resource).acquire(options);
          aborts.delete(message.id);
          if (!lease) { write({ id: message.id, status: "busy" }); break; }
          leases.set(lease.id, lease);
          const fence = lease instanceof EtcdLease ? lease.fence : undefined;
          write({ id: message.id, status: "acquired", lease_id: lease.id, effective_lease_ms: lease instanceof EtcdLease ? lease.effectiveLeaseMs : message.lease_ms ?? 30_000, ...(fence ? { fence } : {}) });
          break;
        }
        case "release": {
          if (!message.lease_id) throw new InvalidArgumentError("lease_id is required");
          const prior = released.get(message.lease_id);
          if (prior) { write({ id: message.id, status: prior }); break; }
          const lease = leases.get(message.lease_id);
          if (!lease) throw new InvalidArgumentError("unknown lease_id");
          const outcome = await lease.release();
          if (outcome === "released" || outcome === "already-not-owned") {
            leases.delete(message.lease_id);
            const status = outcome === "released" ? "released" : "already_not_owned";
            released.set(message.lease_id, status);
            write({ id: message.id, status });
          } else write({ id: message.id, status: "release_unknown" });
          break;
        }
        case "inspect": {
          if (!message.lease_id) throw new InvalidArgumentError("lease_id is required");
          const lease = leases.get(message.lease_id);
          if (!lease) throw new InvalidArgumentError("unknown lease_id");
          write({ id: message.id, status: "ok", state: lease.state });
          break;
        }
        case "cancel_request": {
          if (!message.target_id) throw new InvalidArgumentError("target_id is required");
          const controller = aborts.get(message.target_id);
          if (controller) controller.abort();
          write({ id: message.id, status: "ok", cancelled: Boolean(controller) });
          break;
        }
        case "shutdown":
          stopping = true;
          write({ id: message.id, status: "ok", outstanding_cleanup_attempts: 0 });
          break;
        default: throw new InvalidArgumentError(`unknown op: ${message.op}`);
      }
    } catch (error) {
      const response: Record<string, unknown> = { id: message.id, status: status(error, message.op === "try_acquire") };
      if (response.status === "cancelled" || response.status === "acquisition_unknown") response.remote_outcome_unknown = error instanceof EtcdAcquisitionCancelledError ? error.remoteOutcomeUnknown : error instanceof AcquisitionUnknownError;
      write(response);
    } finally { activeIds.delete(message.id); aborts.delete(message.id); }
  };

  const acceptLine = (line: string) => {
    if (stopping) return;
    let message: Request;
    try { message = JSON.parse(line) as Request; }
    catch { process.stderr.write("worker: malformed JSON input\n"); return; }
    if (!message || typeof message.id !== "string" || !message.id || typeof message.op !== "string") { process.stderr.write("worker: malformed request envelope\n"); return; }
    if (activeIds.has(message.id)) { write({ id: message.id, status: "invalid_argument" }); return; }
    activeIds.add(message.id);
    const task = handle(message).finally(() => tasks.delete(task));
    tasks.add(task);
  };
  process.stdin.setEncoding("utf8");
  await new Promise<void>(resolve => {
    let line = "";
    let lineBytes = 0;
    let dropping = false;
    process.stdin.on("data", chunk => {
      const text = typeof chunk === "string" ? chunk : chunk.toString("utf8");
      for (const character of text) {
        if (character === "\n") {
          if (dropping) process.stderr.write("worker: input line exceeds 65536 bytes\n");
          else acceptLine(line.endsWith("\r") ? line.slice(0, -1) : line);
          line = ""; lineBytes = 0; dropping = false;
        } else if (!dropping) {
          lineBytes += Buffer.byteLength(character, "utf8");
          if (lineBytes > MAX_LINE_BYTES) { line = ""; dropping = true; }
          else line += character;
        }
      }
    });
    process.stdin.on("end", () => {
      if (line.length && !dropping) acceptLine(line);
      else if (dropping) process.stderr.write("worker: input line exceeds 65536 bytes\n");
      resolve();
    });
  });
  for (const controller of aborts.values()) controller.abort();
  await Promise.allSettled([...tasks]);
  for (const lease of leases.values()) await lease.release().catch(() => undefined);
  await provider.close();
  if (!stopping) process.exitCode = 1;
}

main().catch(error => { process.stderr.write(`worker startup failed: ${error instanceof Error ? error.message : "unknown error"}\n`); process.exitCode = 1; });

