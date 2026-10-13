import assert from "node:assert/strict";
import test from "node:test";
import { ProtocolMismatchError } from "@dilock/core";
import { parseMetadata as parseRedisMetadata } from "../../packages/redis/src/internal/metadata.js";
import { parseMetadata as parseEtcdMetadata } from "../../packages/etcd/src/internal/metadata.js";

const metadata = (protocolMajor: string, profile = "redis-single-authority-v1") => `{
  "protocol_major": ${protocolMajor},
  "identity_scheme": "sha256-framed-utf8-v1",
  "profile": "${profile}",
  "coordination_epoch": "epoch-1",
  "fixed_lease_ms": null,
  "future_note": "the text protocol_major: 1.0 is not a field",
  "future_extension": { "protocol_major": 1.0 }
}`;

test("Redis and etcd metadata require protocol_major to be the JSON integer token 1", () => {
  parseRedisMetadata(metadata("1"), "epoch-1");
  parseEtcdMetadata(Buffer.from(metadata("1", "etcd-native-lock-v1")), "test", "epoch-1");

  for (const token of ["1.0", "1e0", "1.5", "\"1\"", "true", "null", "4294967297"]) {
    assert.throws(() => parseRedisMetadata(metadata(token), "epoch-1"), ProtocolMismatchError, `Redis accepted ${token}`);
    assert.throws(() => parseEtcdMetadata(Buffer.from(metadata(token, "etcd-native-lock-v1")), "test", "epoch-1"), ProtocolMismatchError, `etcd accepted ${token}`);
  }
});

test("Redis and etcd reject null and non-object namespace metadata as protocol mismatches", () => {
  for (const raw of ["null", "[]", "\"metadata\""]) {
    assert.throws(() => parseRedisMetadata(raw, "epoch-1"), ProtocolMismatchError);
    assert.throws(() => parseEtcdMetadata(Buffer.from(raw), "test", "epoch-1"), ProtocolMismatchError);
  }
});

test("duplicate top-level protocol_major fields are rejected consistently", () => {
  const redis = metadata("1").replace('"protocol_major": 1,', '"protocol_major": 1, "protocol_major": 1,');
  const etcd = metadata("1", "etcd-native-lock-v1").replace('"protocol_major": 1,', '"protocol_major": 1, "protocol_major": 1,');
  assert.throws(() => parseRedisMetadata(redis, "epoch-1"), ProtocolMismatchError);
  assert.throws(() => parseEtcdMetadata(Buffer.from(etcd), "test", "epoch-1"), ProtocolMismatchError);
});
