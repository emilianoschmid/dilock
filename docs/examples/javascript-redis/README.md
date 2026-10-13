# JavaScript with Redis

`quickstart/main.mjs` uses the public JavaScript entry point backed by the TypeScript Node.js runtime. Requirements: Node.js 22+ and the local workspace package dependencies.

From the repository root, build the shared runtime and syntax-check this sample:

```powershell
npm --prefix sdk/javascript run build:runtime
node --check docs/examples/javascript-redis/quickstart/main.mjs
```

Set `REDIS_URL`, `DILOCK_NAMESPACE` (defaults to `default`), and `DILOCK_COORDINATION_EPOCH`, then run `node docs/examples/javascript-redis/quickstart/main.mjs`. Redis namespace metadata must already exist and contain the matching epoch; this example never provisions metadata. The JavaScript SDK reuses the TypeScript engine. For complete connection and acquisition settings, see [the TypeScript SDK guide](../../../sdk/typescript/README.md). Two lock handles from one provider reuse its connection; they still acquire independently.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
