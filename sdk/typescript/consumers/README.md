# TypeScript package consumers

`package_consumer.py` builds the three private npm workspace archives, installs the exact tarballs offline into isolated consumers and checks dependency separation:

- Core imports identity and public errors with neither Redis nor gRPC installed.
- Redis installs core plus `@dilock/redis` and node-redis, with no gRPC packages.
- etcd installs core plus `@dilock/etcd` and gRPC, with no Redis package.

From `sdk/typescript`, run:

```powershell
$env:NPM_CONFIG_CACHE = 'C:\codex\dilock\.tools\npm-cache'
npm run test:packages
```

The script records exact tarball hashes and all command exit codes in `.tools/optional-package-consumer-report.json`. It uses `npm --offline`; it does not access a registry or publish packages. Package names remain candidates pending scope ownership verification.
