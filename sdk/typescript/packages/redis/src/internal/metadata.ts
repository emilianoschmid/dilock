import { ProtocolMismatchError } from "@dilock/core";

const IDENTITY_SCHEME = "sha256-framed-utf8-v1";
const ACQUIRE_PROFILE = "redis-single-authority-v1";
type Metadata = { protocol_major: number; identity_scheme: string; profile: string; coordination_epoch: string; fixed_lease_ms: number | null };

function hasExactProtocolMajorOne(raw: string): boolean {
  let index = 0;
  const whitespace = () => { while (/\s/.test(raw[index] ?? "")) index++; };
  const stringEnd = (start: number): number => {
    let escaped = false;
    for (let cursor = start + 1; cursor < raw.length; cursor++) {
      const char = raw[cursor];
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') return cursor + 1;
    }
    return raw.length;
  };
  whitespace();
  if (raw[index++] !== "{") return false;
  let count = 0;
  let exact = false;
  while (index < raw.length) {
    whitespace();
    if (raw[index] === "}") break;
    if (raw[index] !== '"') return false;
    const keyStart = index;
    index = stringEnd(index);
    const key = JSON.parse(raw.slice(keyStart, index)) as string;
    whitespace();
    if (raw[index++] !== ":") return false;
    whitespace();
    if (key === "protocol_major") {
      count++;
      exact = /^1(?=\s*[,}])/.test(raw.slice(index));
    }
    let depth = 0;
    let inString = false;
    let escaped = false;
    for (; index < raw.length; index++) {
      const char = raw[index];
      if (inString) {
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') inString = false;
      } else if (char === '"') inString = true;
      else if (char === "{" || char === "[") depth++;
      else if (char === "}" || char === "]") {
        if (depth === 0) break;
        depth--;
      } else if (char === "," && depth === 0) break;
    }
    if (raw[index] === ",") index++;
    else if (raw[index] === "}") break;
    else return false;
  }
  return count === 1 && exact;
}

/** @internal */
export function parseMetadata(raw: string, epoch: string): void {
  if (Buffer.byteLength(raw, "utf8") > 8192) throw new ProtocolMismatchError("namespace metadata exceeds 8192 bytes");
  let value: unknown;
  try { value = JSON.parse(raw) as unknown; } catch (cause) { throw new ProtocolMismatchError("namespace metadata is not valid JSON", { cause }); }
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new ProtocolMismatchError("namespace metadata must be a JSON object");
  const metadata = value as Partial<Metadata>;
  if (!hasExactProtocolMajorOne(raw) || metadata.protocol_major !== 1 || metadata.identity_scheme !== IDENTITY_SCHEME || metadata.profile !== ACQUIRE_PROFILE || typeof metadata.coordination_epoch !== "string" || metadata.coordination_epoch.length === 0 || metadata.fixed_lease_ms !== null) {
    throw new ProtocolMismatchError("namespace metadata is incompatible with Redis draft 1");
  }
  if (metadata.coordination_epoch !== epoch) throw new ProtocolMismatchError("namespace coordination epoch does not match configuration");
}
