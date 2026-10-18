/** Run the PostgreSQL advisory-session profile through the TypeScript/Node route. */
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { parseArgs } from "node:util";

const require = createRequire(new URL("../sdk/typescript/packages/postgres/dist/index.js", import.meta.url));
const { Client } = require("pg");
const { values: args } = parseArgs({
  options: {
    dsn: { type: "string" },
    namespace: { type: "string", default: "ci-postgres-typescript" },
    epoch: { type: "string", default: "ci-postgres-typescript-epoch" },
    output: { type: "string" },
  },
});

if (!args.dsn) throw new Error("--dsn is required");

const {
  PostgresProvider,
  advisoryKey,
  provisionNamespace,
} = await import("../sdk/typescript/packages/postgres/dist/index.js");

const checks = [];
const report = { status: "failed", checks, postgres_version: null };

function variantNamespace(suffix) {
  const room = 63 - suffix.length - 1;
  return `${args.namespace.slice(0, room)}-${suffix}`;
}

async function withClient(work) {
  const client = new Client({ connectionString: args.dsn });
  await client.connect();
  try {
    return await work(client);
  } finally {
    await client.end().catch(() => undefined);
  }
}

async function main() {
  await provisionNamespace(args.dsn, args.namespace, args.epoch);
  await assert.rejects(
    provisionNamespace(args.dsn, args.namespace, `${args.epoch}-conflict`),
    /conflict|mismatch|compatible/i,
  );
  checks.push("metadata-immutable-on-conflict");

  report.postgres_version = await withClient(async (client) => {
    const result = await client.query("SELECT version() AS version");
    const mismatchNamespace = variantNamespace("mismatch");
    await client.query(
      `INSERT INTO dilock_namespace_meta
         (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms)
       VALUES ($1, 1, 'sha256-framed-utf8-v1', 'postgres-advisory-session-v1', 'wrong-epoch', NULL)
       ON CONFLICT (namespace) DO UPDATE SET coordination_epoch = EXCLUDED.coordination_epoch`,
      [mismatchNamespace],
    );
    return result.rows[0].version;
  });

  for (const [namespace, epoch] of [
    [variantNamespace("missing"), args.epoch],
    [variantNamespace("mismatch"), args.epoch],
  ]) {
    await assert.rejects(
      PostgresProvider.connect({ connectionString: args.dsn, namespace, epoch }),
      /metadata|mismatch|namespace/i,
    );
  }
  await withClient((client) => client.query("DELETE FROM dilock_namespace_meta WHERE namespace = $1", [variantNamespace("mismatch")]));
  checks.push("metadata-missing-rejected", "metadata-mismatch-rejected");

  await withClient(async (first) => {
    await withClient(async (second) => {
      const shared = advisoryKey((await import("../sdk/typescript/packages/core/dist/index.js")).identity(args.namespace, "shared-resource")).toString();
      const other = advisoryKey((await import("../sdk/typescript/packages/core/dist/index.js")).identity(args.namespace, "different-resource")).toString();
      assert.equal((await first.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [shared])).rows[0].acquired, true);
      assert.equal((await second.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [shared])).rows[0].acquired, false);
      assert.equal((await second.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [other])).rows[0].acquired, true);
      assert.equal((await second.query("SELECT pg_advisory_unlock($1::bigint) AS released", [other])).rows[0].released, true);
      assert.equal((await first.query("SELECT pg_advisory_unlock($1::bigint) AS released", [shared])).rows[0].released, true);
      assert.equal((await second.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [shared])).rows[0].acquired, true);
      assert.equal((await second.query("SELECT pg_advisory_unlock($1::bigint) AS released", [shared])).rows[0].released, true);
    });
  });
  checks.push("same-resource-contention", "different-resource-progress", "same-session-release-handoff");

  await withClient(async (oldSession) => {
    await withClient(async (successor) => {
      const { identity } = await import("../sdk/typescript/packages/core/dist/index.js");
      const key = advisoryKey(identity(args.namespace, "old-session-release")).toString();
      assert.equal((await oldSession.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [key])).rows[0].acquired, true);
      assert.equal((await oldSession.query("SELECT pg_advisory_unlock($1::bigint) AS released", [key])).rows[0].released, true);
      assert.equal((await successor.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [key])).rows[0].acquired, true);
      assert.equal((await oldSession.query("SELECT pg_advisory_unlock($1::bigint) AS released", [key])).rows[0].released, false);
      assert.equal((await successor.query("SELECT pg_advisory_unlock($1::bigint) AS released", [key])).rows[0].released, true);
    });
  });
  checks.push("old-session-release-rejected");

  await withClient(async (abandoned) => {
    await withClient(async (successor) => {
      const { identity } = await import("../sdk/typescript/packages/core/dist/index.js");
      const key = advisoryKey(identity(args.namespace, "session-loss")).toString();
      assert.equal((await abandoned.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [key])).rows[0].acquired, true);
      await abandoned.end();
      let acquired = false;
      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline) {
        if ((await successor.query("SELECT pg_try_advisory_lock($1::bigint) AS acquired", [key])).rows[0].acquired) {
          acquired = true;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      assert.equal(acquired, true);
      assert.equal((await successor.query("SELECT pg_advisory_unlock($1::bigint) AS released", [key])).rows[0].released, true);
    });
  });
  checks.push("session-loss-cleanup");

  const first = await PostgresProvider.connect({ connectionString: args.dsn, namespace: args.namespace, epoch: args.epoch });
  const second = await PostgresProvider.connect({ connectionString: args.dsn, namespace: args.namespace, epoch: args.epoch });
  let lease;
  try {
    const sameProcess = first.lock("same-process-non-reentrant");
    const sameProcessLease = await sameProcess.tryAcquire({ leaseMs: 1_000, rpcTimeoutMs: 500 });
    assert.ok(sameProcessLease);
    assert.equal(await sameProcess.tryAcquire({ leaseMs: 1_000, rpcTimeoutMs: 500 }), undefined);
    assert.equal(await sameProcessLease.release(), "released");
    checks.push("same-process-non-reentrancy");

    const handle = first.lock("typescript-provider");
    const contender = second.lock("typescript-provider");
    lease = await handle.tryAcquire({ leaseMs: 1_000, rpcTimeoutMs: 500 });
    assert.ok(lease);
    await lease.renew();
    assert.equal(await contender.tryAcquire({ leaseMs: 1_000, rpcTimeoutMs: 500 }), undefined);
    await assert.rejects(
      contender.acquire({ waitMs: 150, leaseMs: 1_000, rpcTimeoutMs: 500 }),
      /timed out waiting/,
    );
    await assert.rejects(
      contender.tryAcquire({ leaseMs: 1_000, rpcTimeoutMs: 500, requireFencing: true }),
      /neither fencing/,
    );
    await assert.rejects(
      contender.tryAcquire({ leaseMs: 1_000, rpcTimeoutMs: 500, autoRenew: true }),
      /neither fencing|auto-renewal/,
    );
    const controller = new AbortController();
    const cancelled = contender.acquire({ waitMs: 5_000, leaseMs: 1_000, rpcTimeoutMs: 500, signal: controller.signal });
    setTimeout(() => controller.abort(new DOMException("aborted", "AbortError")), 50);
    await assert.rejects(cancelled, /aborted|AbortError/i);
    assert.equal(await lease.release(), "released");

    const lostRelease = await first.lock("lost-release-ack").tryAcquire({ leaseMs: 1_000, rpcTimeoutMs: 500 });
    assert.ok(lostRelease);
    const pidRows = await lostRelease.client.query("SELECT pg_backend_pid()");
    assert.equal(pidRows.rows.length, 1);
    await withClient((administrator) => administrator.query("SELECT pg_terminate_backend($1)", [pidRows.rows[0].pg_backend_pid]));
    await assert.rejects(lostRelease.release(), /did not confirm|release/i);
    assert.equal(lostRelease.state, "release-unknown");
    checks.push("lost-release-acknowledgement");
  } finally {
    if (lease) await lease.release().catch(() => undefined);
    await first.close();
    await second.close();
  }
  checks.push("typescript-provider-route", "timeout", "capability-rejection", "cancellation");

  await assert.rejects(
    PostgresProvider.connect({
      connectionString: args.dsn.replace("postgres:postgres@", "postgres:wrong-password@"),
      namespace: args.namespace,
      epoch: args.epoch,
    }),
    /connection|authentication|password|backend/i,
  );
  checks.push("authentication-failure");
  report.status = "passed";
}

try {
  await main();
} catch (error) {
  report.error = String(error?.stack ?? error);
  throw error;
} finally {
  if (args.output) {
    await mkdir(dirname(resolve(args.output)), { recursive: true });
    await writeFile(args.output, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  }
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
