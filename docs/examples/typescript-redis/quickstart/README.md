# TypeScript Redis quickstart

Provision the namespace metadata once using the operator workflow; the SDK only validates it. The default path expects namespace `default` and approved epoch `dev-1`. From the repository root, build the private local packages and install the file-based consumer offline:

```powershell
$repo = (Get-Location).Path
Push-Location sdk\typescript
$env:NPM_CONFIG_CACHE = Join-Path $repo ".tools\npm-cache"
npm install --offline --ignore-scripts --no-audit --no-fund
npm run build
Pop-Location
Push-Location docs\examples\typescript-redis\quickstart
npm install --offline --ignore-scripts --no-audit --no-fund
$env:DILOCK_REDIS_URL = "redis://127.0.0.1:6379/"
$env:DILOCK_EPOCH = "dev-1"
npm start
node quickstart.mjs --configured
node quickstart.mjs --shared-provider
Pop-Location
```

No flags acquires one resource with 5 s wait and 30 s lease defaults. `--configured` applies a complete 10 s/60 s policy plus 2 s RPC/cleanup budgets and a cleanup observer. `--shared-provider` holds two different resources using one provider/client pool; it is not an atomic multi-resource acquisition. `withLease` awaits cleanup and preserves callback errors; TTL does not extend scope lifetime or stop stale work.

This example uses plaintext Redis. The URI/client can carry approved TLS and authentication configuration; protect credentials and never commit them. Redis single-authority does not promise strong fencing across unsafe failover.
