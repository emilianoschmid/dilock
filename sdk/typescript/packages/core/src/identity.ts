import { createHash, randomBytes } from "node:crypto";

export function validateNamespace(namespace: string): void {
  const match = typeof namespace === "string"
    ? /^[a-z0-9][a-z0-9_.-]{0,63}$/.exec(namespace)
    : null;
  // JavaScript's `$` may match before a final line terminator. Require the
  // matched ASCII token to consume the whole input rather than accepting a
  // namespace such as `default\n`.
  if (match === null || match[0] !== namespace) {
    throw new TypeError("namespace must match [a-z0-9][a-z0-9_.-]{0,63}");
  }
}

export function identity(namespace: string, resource: string): string {
  validateNamespace(namespace);
  if (typeof resource !== "string" || resource.length === 0 || resource.includes("\0")) {
    throw new TypeError("resource must be a non-empty Unicode scalar string without NUL");
  }
  // TextEncoder replaces unpaired UTF-16 surrogates, so reject them before encoding.
  for (let i = 0; i < resource.length; i++) {
    const code = resource.charCodeAt(i);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = resource.charCodeAt(i + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError("resource contains an unpaired surrogate");
      i++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new TypeError("resource contains an unpaired surrogate");
    }
  }
  const ns = Buffer.from(namespace, "utf8");
  const value = Buffer.from(resource, "utf8");
  if (value.length < 1 || value.length > 1024) throw new TypeError("resource UTF-8 encoding must contain 1 to 1024 bytes");
  const domain = Buffer.from("dilock.lock.identity\0", "ascii");
  const frame = Buffer.allocUnsafe(domain.length + 4 + ns.length + 4 + value.length);
  domain.copy(frame);
  let offset = domain.length;
  frame.writeUInt32BE(ns.length, offset); offset += 4;
  ns.copy(frame, offset); offset += ns.length;
  frame.writeUInt32BE(value.length, offset); offset += 4;
  value.copy(frame, offset);
  return createHash("sha256").update(frame).digest("hex");
}

export function redisKey(resourceId: string): string { return `dilock:{${resourceId}}:lease`; }
export function ownerToken(): string { return `dilock/1/${randomBytes(32).toString("hex")}`; }
