# TypeScript etcd quickstart

Provision namespace metadata administratively before connecting; the SDK validates only. The default sample expects namespace `default` and approved epoch `dev-1`. From the repository root, build the local candidate packages and install the file-based consumer offline:

```powershell
$repo = (Get-Location).Path
Push-Location sdk\typescript
$env:NPM_CONFIG_CACHE = Join-Path $repo ".tools\npm-cache"
npm install --offline --ignore-scripts --no-audit --no-fund
npm run build
Pop-Location
Push-Location docs\examples\typescript-etcd\quickstart
npm install --offline --ignore-scripts --no-audit --no-fund
$env:DILOCK_ETCD_ENDPOINT = "http://127.0.0.1:2379"
$env:DILOCK_EPOCH = "dev-1"
npm start
node quickstart.mjs --configured
node quickstart.mjs --shared-provider
Pop-Location
```

No flags acquires one resource with 5 s wait and 30 s lease defaults. `--configured` supplies complete 10 s/60 s policy and 2 s RPC/cleanup budgets. `--shared-provider` holds two independent resources through one channel; their acquisitions are not atomic. The native etcd Lock API does not offer safe immediate try-acquire. Helpers await cleanup; expiry cannot stop stale application work, and downstream fencing must atomically enforce `(epoch, create_revision)`.

The current quickstart passes a plaintext HTTP endpoint. The TypeScript provider supports HTTPS with the platform's default root trust, but does not accept inline credentials or demonstrate custom trust/client certificates. Configure the authenticated TLS endpoint and verify trust settings separately before production use.
