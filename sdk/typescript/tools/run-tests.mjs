import { cp, mkdir, readdir } from "node:fs/promises";
import { resolve } from "node:path";

// Import test modules into one process to support restricted Windows runners
// where node --test cannot spawn its worker processes.
const directory = resolve("target/src/tests");
// The aggregate TypeScript build emits package source under target/packages.
// Keep etcd's runtime proto beside that emitted source, as it is in the package build.
const etcdOutput = resolve("target/packages/etcd");
await mkdir(etcdOutput, { recursive: true });
await cp(resolve("packages/etcd/proto"), resolve(etcdOutput, "proto"), { recursive: true, force: true });
// Root tests load compiled files under target/, whose relative spec paths need a local copy.
const specOutput = resolve("target/spec");
await mkdir(specOutput, { recursive: true });
await cp(resolve("../../spec"), specOutput, { recursive: true, force: true });
const files = (await readdir(directory)).filter((name) => name.endsWith(".test.js")).sort();
if (files.length === 0) throw new Error(`no compiled tests found in ${directory}`);
for (const file of files) await import(new URL(`../target/src/tests/${file}`, import.meta.url));
