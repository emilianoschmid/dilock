import { InvalidArgumentError } from "@dilock/core";

export function endpointTransport(value: string): { target: string; secure: boolean } {
  let parsed: URL;
  try { parsed = new URL(value.includes("://") ? value : `http://${value}`); }
  catch (cause) { throw new InvalidArgumentError("invalid etcd endpoint", { cause }); }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new InvalidArgumentError("etcd endpoint must use http:// or https://");
  if (!parsed.hostname || parsed.username || parsed.password || parsed.pathname !== "/" || parsed.search || parsed.hash) throw new InvalidArgumentError("etcd endpoint must be a host and port without credentials or path");
  return {
    target: `${parsed.hostname}:${parsed.port || "2379"}`,
    secure: parsed.protocol === "https:",
  };
}
