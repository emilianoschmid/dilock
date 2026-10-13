import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const sdkRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(sdkRoot, "../..");
const tsRoot = path.join(repoRoot, "sdk", "typescript");
const npmCli = process.env.npm_execpath;
const node = process.execPath;
const npm = process.platform === "win32" ? "npm.cmd" : path.join(path.dirname(node), "npm");
const persistentScratch = process.env.DILOCK_PERSIST_SCRATCH_DIR;
const scratch = persistentScratch
  ? path.resolve(persistentScratch)
  : await mkdtemp(path.join(os.tmpdir(), "dilock-javascript-packed-"));
await mkdir(scratch, { recursive: true });
const archiveDir = path.join(scratch, "archives");
await mkdir(archiveDir, { recursive: true });

function run(command, args, cwd, env = process.env) {
  const result = spawnSync(command, args, {
    cwd,
    env,
    encoding: "utf8",
    windowsHide: true,
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  assert.equal(result.error, undefined, `${command} ${args.join(" ")} (cwd exists=${existsSync(cwd)} at ${cwd}): ${result.error ?? ""}`);
  assert.equal(result.status, 0, `${command} ${args.join(" ")} exited ${result.status}`);
}

function runNpm(args, cwd, env) {
  if (process.platform === "win32" && npmCli) run(node, [npmCli, ...args], cwd, env);
  else run(npm, args, cwd, env);
}

function packageEnv() {
  return {
    ...process.env,
    PATH: [path.dirname(process.execPath), process.env.PATH].filter(Boolean).join(path.delimiter),
    npm_config_cache: process.env.npm_config_cache ?? path.join(repoRoot, ".tools", "npm-cache"),
    npm_config_audit: "false",
    npm_config_fund: "false",
    npm_config_update_notifier: "false",
  };
}

const env = packageEnv();
const artifactHashes = {};
const mode = process.env.DILOCK_PACKED_CONSUMER_MODE;
assert.ok(mode === undefined || ["install-only", "consume-only"].includes(mode), "DILOCK_PACKED_CONSUMER_MODE must be install-only or consume-only");
try {
  const prepackedDir = process.env.DILOCK_PREPACKED_ARCHIVE_DIR;
  if (!prepackedDir) {
    for (const unit of ["core", "redis", "etcd"]) {
      runNpm(["pack", `--workspace=@dilock/${unit}`, "--pack-destination", archiveDir], tsRoot, env);
    }
  }

  const tarballs = {};
  const tarballIntegrity = {};
  for (const unit of ["core", "redis", "etcd"]) {
    const archive = path.join(prepackedDir ?? archiveDir, `dilock-${unit}-0.1.0.tgz`);
    const bytes = await readFile(archive);
    tarballs[unit] = archive;
    artifactHashes[unit] = createHash("sha256").update(bytes).digest("hex");
    tarballIntegrity[unit] = `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
  }

  const fixtures = {
    core: `import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { identity, redisKey, AcquisitionTimeoutError } from "@dilock/core";
const require = createRequire(import.meta.url);
assert.equal(redisKey(identity("default", "javascript/packed")), "dilock:{" + identity("default", "javascript/packed") + "}:lease");
assert.equal(typeof AcquisitionTimeoutError, "function");
for (const name of ["redis", "@grpc/grpc-js", "@grpc/proto-loader"]) assert.throws(() => require.resolve(name), { code: "MODULE_NOT_FOUND" });
console.log("PASS JavaScript core tarball consumer");
`,
    redis: `import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { RedisProvider } from "@dilock/redis";
import { createClient } from "redis";
const require = createRequire(import.meta.url);
assert.equal(typeof RedisProvider.connect, "function");
require.resolve("redis");
for (const name of ["@grpc/grpc-js", "@grpc/proto-loader"]) assert.throws(() => require.resolve(name), { code: "MODULE_NOT_FOUND" });
const endpoint = process.env.DILOCK_TEST_REDIS_URL;
if (endpoint) {
  const suffix = randomUUID();
  const namespace = "javascript-" + suffix;
  const epoch = "local-" + suffix;
  const metadataKey = "dilock:namespace:" + namespace + ":meta";
  const admin = createClient({ url: endpoint, disableOfflineQueue: true, socket: { reconnectStrategy: false } });
  admin.on("error", () => undefined);
  let firstProvider;
  let secondProvider;
  try {
    await admin.connect();
    const { provisionNamespace } = await import("@dilock/redis");
    assert.equal(await provisionNamespace(admin, namespace, epoch), "created");
    await admin.pExpire(metadataKey, 120_000);
    firstProvider = await RedisProvider.connect({ endpoint, namespace, epoch });
    secondProvider = await RedisProvider.connect({ endpoint, namespace, epoch });
    const resource = "packed-js/" + suffix;
    const firstLock = firstProvider.lock(resource);
    const secondLock = secondProvider.lock(resource);
    const first = await firstLock.acquire({ waitMs: 2000, leaseMs: 1600, rpcTimeoutMs: 100 });
    assert.equal(await secondLock.tryAcquire({ leaseMs: 1600, rpcTimeoutMs: 100 }), undefined);
    const waiting = secondLock.acquire({ waitMs: 4000, leaseMs: 1600, rpcTimeoutMs: 100 });
    await delay(100);
    assert.equal(await first.release(), "released");
    const successor = await waiting;
    assert.equal(successor.state, "held");
    assert.equal(await successor.release(), "released");
    const { identity } = await import("@dilock/core");
    assert.equal(await admin.exists("dilock:{" + identity(namespace, resource) + "}:lease"), 0);
    console.log("PASS live JavaScript Redis tarball lifecycle: contention, queued handoff, release, zero lock key");
  } finally {
    await Promise.allSettled([firstProvider?.close(), secondProvider?.close()].filter(Boolean));
    if (admin.isOpen) {
      const raw = await admin.get(metadataKey).catch(() => null);
      if (raw !== null) {
        await admin.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0", { keys: [metadataKey], arguments: [raw] }).catch(() => undefined);
      }
      await admin.close().catch(() => admin.destroy());
    }
  }
}
console.log("PASS JavaScript Redis tarball consumer");
`,
    etcd: `import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { EtcdProvider } from "@dilock/etcd";
const require = createRequire(import.meta.url);
assert.equal(typeof EtcdProvider.connect, "function");
require.resolve("@grpc/grpc-js");
require.resolve("@grpc/proto-loader");
assert.throws(() => require.resolve("redis"), { code: "MODULE_NOT_FOUND" });
assert.equal(require("@grpc/grpc-js/package.json").version, "1.14.5");
const endpoint = process.env.DILOCK_TEST_ETCD_ENDPOINT;
const namespace = process.env.DILOCK_TEST_ETCD_NAMESPACE;
const epoch = process.env.DILOCK_TEST_ETCD_EPOCH;
if (endpoint && namespace && epoch) {
  const suffix = randomUUID();
  const firstProvider = await EtcdProvider.connect({ endpoint, namespace, epoch });
  const secondProvider = await EtcdProvider.connect({ endpoint, namespace, epoch });
  try {
    const resource = "packed-js/" + suffix;
    const firstLock = firstProvider.lock(resource);
    const secondLock = secondProvider.lock(resource);
    const first = await firstLock.acquire({ waitMs: 2000, leaseMs: 3000, rpcTimeoutMs: 500, autoRenew: true });
    await assert.rejects(firstLock.tryAcquire(), { name: "UnsupportedCapabilityError" });
    const waiting = secondLock.acquire({ waitMs: 5000, leaseMs: 3000, rpcTimeoutMs: 500, autoRenew: false });
    await delay(150);
    await first.release();
    const successor = await waiting;
    assert.ok(BigInt(successor.fence.value) > BigInt(first.fence.value));
    assert.equal(await successor.release(), "released");
    console.log("PASS live JavaScript etcd tarball lifecycle: native queue, revision fence handoff, release");
  } finally {
    await Promise.allSettled([firstProvider.close(), secondProvider.close()]);
  }
}
console.log("PASS JavaScript etcd tarball consumer");
`,
  };
  for (const unit of Object.keys(fixtures)) {
    const consumer = path.join(scratch, `consumer-${unit}`);
    await mkdir(consumer, { recursive: true });
    const consumerDependencies = Object.fromEntries(
      (unit === "core" ? ["core"] : ["core", unit]).map((dependency) => [
        `@dilock/${dependency}`,
        `file:${tarballs[dependency]}`,
      ]),
    );
    const consumerManifest = {
      name: `dilock-javascript-${unit}-tarball-consumer`,
      private: true,
      type: "module",
      dependencies: consumerDependencies,
    };
    await writeFile(path.join(consumer, "package.json"), JSON.stringify(consumerManifest, null, 2) + "\n");
    // Reuse the exact lockfile dependency closure used by the TypeScript
    // workspace. This keeps offline consumers from asking npm for registry
    // packuments when a tarball is already in the local cache.
    const workspaceLock = JSON.parse(await readFile(path.join(tsRoot, "package-lock.json"), "utf8"));
    const availablePackages = Object.fromEntries(
      Object.entries(workspaceLock.packages).filter(([name, value]) =>
        name.startsWith("node_modules/") && !value.link,
      ),
    );
    const packages = {};
    const pendingPackages = [];
    for (const dependency of Object.keys(consumerDependencies)) {
      const shortName = dependency.slice("@dilock/".length);
      const packageManifest = JSON.parse(await readFile(path.join(tsRoot, "packages", shortName, "package.json"), "utf8"));
      const packageKey = `node_modules/${dependency}`;
      packages[packageKey] = {
        version: "0.1.0",
        resolved: `file:${tarballs[shortName]}`,
        integrity: tarballIntegrity[shortName],
        dependencies: packageManifest.dependencies,
      };
      pendingPackages.push(packageKey);
    }
    function resolveLockedDependency(fromPackage, dependency) {
      for (let base = fromPackage; base && base !== "."; base = path.posix.dirname(base)) {
        const candidate = `${base}/node_modules/${dependency}`;
        if (packages[candidate] || availablePackages[candidate]) return candidate;
      }
      const topLevel = `node_modules/${dependency}`;
      if (packages[topLevel] || availablePackages[topLevel]) return topLevel;
      return undefined;
    }
    while (pendingPackages.length > 0) {
      const packageKey = pendingPackages.pop();
      const packageMetadata = packages[packageKey] ?? availablePackages[packageKey];
      for (const dependency of Object.keys(packageMetadata.dependencies ?? {})) {
        const dependencyKey = resolveLockedDependency(packageKey, dependency);
        assert.ok(dependencyKey, `Missing locked production dependency ${dependency} required by ${packageKey}`);
        if (!(dependencyKey in packages)) {
          packages[dependencyKey] = availablePackages[dependencyKey];
          pendingPackages.push(dependencyKey);
        }
      }
      for (const dependency of Object.keys(packageMetadata.optionalDependencies ?? {})) {
        const dependencyKey = resolveLockedDependency(packageKey, dependency);
        if (dependencyKey && !(dependencyKey in packages)) {
          packages[dependencyKey] = availablePackages[dependencyKey];
          pendingPackages.push(dependencyKey);
        }
      }
    }
    const consumerLock = {
      name: consumerManifest.name,
      version: "0.1.0",
      lockfileVersion: 3,
      requires: true,
      packages: {
        "": {
          name: consumerManifest.name,
          version: "0.1.0",
          dependencies: consumerDependencies,
        },
        ...packages,
      },
    };
    await writeFile(path.join(consumer, "package-lock.json"), JSON.stringify(consumerLock, null, 2) + "\n");
    await writeFile(path.join(consumer, "main.mjs"), fixtures[unit]);
    if (mode !== "consume-only") {
      runNpm(["ci", "--offline", "--ignore-scripts", "--no-audit", "--no-fund", "--omit=dev"], consumer, env);
    }
    if (mode !== "install-only") run(node, ["main.mjs"], consumer, env);
  }
} finally {
  if (!persistentScratch) await rm(scratch, { recursive: true, force: true });
}

console.log(`${mode === "install-only" ? "PASS OFFLINE INSTALL" : mode === "consume-only" ? "PASS CONSUMER" : "PASS source-hidden ESM consumers"} for Core, Redis and etcd; exact tarball SHA-256: ${JSON.stringify(artifactHashes)}`);
