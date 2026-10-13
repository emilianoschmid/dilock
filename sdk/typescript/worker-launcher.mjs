import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const sdk = dirname(fileURLToPath(import.meta.url));
if (process.argv.length !== 3) {
  process.stderr.write("usage: node sdk/typescript/worker-launcher.mjs <config.json>\n");
  process.exit(2);
}
if (!existsSync(resolve(sdk, "target/src/worker.js"))) {
  const build = spawnSync("npm", ["run", "build"], { cwd: sdk, stdio: "inherit", shell: process.platform === "win32" });
  if (build.status !== 0) process.exit(build.status ?? 1);
}
const worker = spawnSync(process.execPath, [resolve(sdk, "target/src/worker.js"), process.argv[2]], { stdio: "inherit" });
process.exit(worker.status ?? 1);
