import { readFile } from "node:fs/promises";
import { test } from "node:test";
import assert from "node:assert/strict";
import { identity, redisKey } from "@dilock/core";
import { RELEASE_LUA, RENEW_LUA } from "../../packages/redis/src/redis.js";

test("draft 1 valid identity vectors map to canonical Redis keys", async () => {
  const vectors = JSON.parse(await readFile("../../spec/vectors/identity.json", "utf8")) as { valid: { namespace: string; resource: string; sha256: string; redis_key: string }[] };
  for (const vector of vectors.valid) {
    const digest = identity(vector.namespace, vector.resource);
    assert.equal(digest, vector.sha256);
    assert.equal(redisKey(digest), vector.redis_key);
  }
});

test("invalid draft 1 identity vectors are rejected before network calls", async () => {
  const vectors = JSON.parse(await readFile("../../spec/vectors/identity.json", "utf8")) as { invalid: { namespace: unknown; resource: unknown }[] };
  for (const vector of vectors.invalid) {
    assert.throws(() => identity(vector.namespace as string, vector.resource as string));
  }
});

test("strict UTF-8 accepts F1-F3 four-byte scalars and rejects lone surrogates", () => {
  assert.equal(identity("default", "\u{40000}").length, 64);
  assert.equal(identity("default", "\u{fffff}").length, 64);
  assert.throws(() => identity("default", "\ud800"));
  assert.throws(() => identity("default", "\udfff"));
});

test("namespace grammar rejects a trailing line terminator", () => {
  for (const namespace of ["default\n", "default\r", "default\u2028", "default\u2029"]) {
    assert.throws(() => identity(namespace, "resource"));
  }
});

test("bundled Redis Lua matches canonical spec source", async () => {
  const release = (await readFile("../../spec/backends/redis-release.lua", "utf8")).replaceAll("\r\n", "\n");
  const renew = (await readFile("../../spec/backends/redis-renew.lua", "utf8")).replaceAll("\r\n", "\n");
  assert.equal(RELEASE_LUA, release);
  assert.equal(RENEW_LUA, renew);
});
