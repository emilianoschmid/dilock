# Zig Redis quickstart

`main.zig` uses the Zig typed binding and `defer` for local handle cleanup. Redis coordination is performed by the existing C++ Redis engine through Dilock C ABI; Zig is a facade, not an independent backend. Experimental/internal candidate; no stable package is implied.

Prerequisites: the Zig version pinned in `sdk/zig/build.zig.zon` (0.17.0), shared C ABI with Redis, Redis, and pre-provisioned `billing` metadata matching the source epoch. Configure endpoint/epoch in the source to match; provision metadata separately.

Build/install the C ABI first as in `sdk/c/README.md`. From the repository root, build the SDK project with the install prefix as documented in `sdk/zig/README.md`:

```sh
cd sdk/zig
zig build -Dc-include=../../.local/dilock-c/include -Dc-lib=../../.local/dilock-c/lib
```

This compiles the project examples but does not run them. Runtime loading must find `libdilock_c` and native dependencies. Run only against a disposable Redis namespace.

The source is the easy default path. For complete provider configuration use `RedisProvider.connectWithOptions(RedisProviderOptions{ .host = ..., .namespace = ..., .coordination_epoch = ..., ... })`; a single provider can create multiple independent handles with `provider.lock(...)`. Acquiring those handles sequentially does not make the operation atomic. See `sdk/zig/README.md` for every exposed option.
