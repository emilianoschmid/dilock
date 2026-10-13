# JavaScript with etcd

`quickstart/main.mjs` uses the public JavaScript entry point backed by the TypeScript Node.js runtime. Requirements: Node.js 22+ and the local workspace package dependencies.

From the repository root, build the shared runtime and syntax-check this sample:

```powershell
npm --prefix sdk/javascript run build:runtime
node --check docs/examples/javascript-etcd/quickstart/main.mjs
```

Set `ETCD_URL`, `DILOCK_NAMESPACE` (defaults to `default`), and `DILOCK_COORDINATION_EPOCH`, then run `node docs/examples/javascript-etcd/quickstart/main.mjs`. etcd namespace metadata must already exist with profile `etcd-native-lock-v1` and the matching epoch. The protected resource must atomically enforce the lease's epoch/revision fence. This is the TypeScript SDK runtime exposed through JavaScript. For complete settings, see [the TypeScript SDK guide](../../../sdk/typescript/README.md); separate handles from one provider reuse its connection but do not combine acquisitions atomically.

These two example acquisitions run sequentially through the same provider connection. Sharing a provider does not combine them into an atomic operation.
