/** Internal disposition for a failed native Lock RPC after bounded cleanup. */
export type EtcdLockFailureDisposition =
  | "cancelled"
  | "unknown"
  | "timeout"
  | "backend-unavailable";

/** Either acknowledged cleanup RPC proves this attempt no longer owns its lock key. */
export function etcdCleanupConfirmed(unlockAcknowledged: boolean, revokeAcknowledged: boolean): boolean {
  return unlockAcknowledged || revokeAcknowledged;
}

export interface EtcdCleanupAcknowledgements {
  readonly unlockAcknowledged: boolean;
  readonly revokeAcknowledged: boolean;
  readonly unlockError?: unknown;
  readonly revokeError?: unknown;
}

/** Runs Unlock and Revoke sequentially inside one monotonic cleanup budget. */
export async function boundedEtcdCleanup(
  timeoutMs: number,
  now: () => number,
  unlock: (timeoutMs: number) => Promise<unknown>,
  revoke: (timeoutMs: number) => Promise<unknown>,
): Promise<EtcdCleanupAcknowledgements> {
  const deadline = now() + timeoutMs;
  const remaining = () => Math.floor(deadline - now());
  let unlockAcknowledged = false;
  let revokeAcknowledged = false;
  let unlockError: unknown;
  let revokeError: unknown;

  let budgetMs = remaining();
  if (budgetMs > 0) {
    try { await unlock(budgetMs); unlockAcknowledged = true; }
    catch (error) { unlockError = error; }
  }

  budgetMs = remaining();
  if (budgetMs > 0) {
    try { await revoke(budgetMs); revokeAcknowledged = true; }
    catch (error) { revokeError = error; }
  }

  return { unlockAcknowledged, revokeAcknowledged, unlockError, revokeError };
}

/** Classifies acknowledged cleanup without claiming which RPC performed it. */
export function classifyEtcdReleaseAcknowledgements(
  unlockAcknowledged: boolean,
  revokeAcknowledged: boolean,
): "released" | "already-not-owned" | "unknown" {
  if (unlockAcknowledged) return "released";
  if (revokeAcknowledged) return "already-not-owned";
  return "unknown";
}

export type EtcdReleaseOwnership = "owned" | "not-owned" | "unknown";

/** Classifies a successful exact-key Range response against the attempt lease. */
export function classifyEtcdReleaseOwnership(
  kvs: readonly { readonly key?: Uint8Array; readonly lease?: unknown }[] | undefined,
  exactKey: Uint8Array,
  expectedLeaseId: string,
): EtcdReleaseOwnership {
  if (exactKey.byteLength === 0) return "not-owned";
  if (!kvs || kvs.length === 0) return "not-owned";
  if (kvs.length !== 1) return "not-owned";
  const kv = kvs[0];
  if (!kv || !kv.key || !Buffer.from(kv.key).equals(Buffer.from(exactKey)) || String(kv.lease) !== expectedLeaseId)
    return "not-owned";
  return "owned";
}

/**
 * A release is reported as successful only when the exact opaque key was
 * observed on this attempt's lease and both native cleanup RPCs were
 * acknowledged. A confirmed revoke can prove an already-unowned key is gone,
 * but cannot repair an uncertain ownership observation.
 */
export function classifyEtcdRelease(
  ownership: EtcdReleaseOwnership,
  unlockAcknowledged: boolean,
  revokeAcknowledged: boolean,
): "released" | "already-not-owned" | "unknown" {
  if (ownership === "owned" && unlockAcknowledged && revokeAcknowledged) return "released";
  if (ownership !== "unknown" && revokeAcknowledged) return "already-not-owned";
  return "unknown";
}

function grpcErrorCode(error: unknown): number | undefined {
  return error && typeof error === "object" && "code" in error
    ? Number((error as { code: unknown }).code)
    : undefined;
}

/**
 * Classify a failed native Lock only after its attempt lease cleanup is known.
 * grpc-js can surface its own elapsed call deadline as CANCELLED (1), so that
 * status is a timeout only when the monotonic local Lock budget also elapsed.
 */
export function classifyEtcdLockFailure(
  error: unknown,
  elapsedMs: number,
  timeoutMs: number,
  callerAborted: boolean,
  revokeConfirmed: boolean,
): EtcdLockFailureDisposition {
  if (callerAborted) return "cancelled";
  if (!revokeConfirmed) return "unknown";
  const code = grpcErrorCode(error);
  if (code === 4 || (code === 1 && elapsedMs >= timeoutMs)) return "timeout";
  return "backend-unavailable";
}
