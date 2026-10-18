import { createInterface } from "node:readline";
import { parseArgs } from "node:util";

const { values: args } = parseArgs({
  options: {
    dsn: { type: "string" },
    namespace: { type: "string" },
    epoch: { type: "string" },
    resource: { type: "string", default: "cross-language-resource" },
    mode: { type: "string" },
  },
});

if (!args.dsn || !args.namespace || !args.epoch || !args.mode) throw new Error("--dsn, --namespace, --epoch and --mode are required");

const { PostgresProvider } = await import("../sdk/typescript/packages/postgres/dist/index.js");
const options = { leaseMs: 5_000, rpcTimeoutMs: 1_000 };
const provider = await PostgresProvider.connect({
  connectionString: args.dsn,
  namespace: args.namespace,
  epoch: args.epoch,
});
const lock = provider.lock(args.resource);
let lease;
let input;

try {
  if (args.mode === "try") {
    lease = await lock.tryAcquire(options);
    if (lease !== undefined) throw new Error("cross-language contender unexpectedly acquired the held lock");
    process.stdout.write("BUSY\n");
  } else if (args.mode === "acquire-release") {
    lease = await lock.acquire({ waitMs: 5_000, ...options });
    await lease.renew();
    process.stdout.write("NODE_ACQUIRED\n");
    await lease.release();
    lease = undefined;
    process.stdout.write("NODE_RELEASED\n");
  } else if (args.mode === "hold") {
    lease = await lock.acquire({ waitMs: 5_000, ...options });
    process.stdout.write("READY\n");
    input = createInterface({ input: process.stdin });
    for await (const line of input) {
      if (line.trim() === "release") break;
    }
    input.close();
    await lease.release();
    lease = undefined;
    process.stdout.write("RELEASED\n");
  } else {
    throw new Error(`unknown mode: ${args.mode}`);
  }
} finally {
  if (lease !== undefined) await lease.release().catch(() => undefined);
  input?.close();
  await provider.close();
}
