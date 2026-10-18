import { strict as assert } from "node:assert";
import test from "node:test";
import { advisoryKey } from "../../packages/postgres/src/postgres.js";
import { InvalidArgumentError } from "@dilock/core";

test("PostgreSQL advisory key uses the signed first int64 digest bytes", () => {
  assert.equal(advisoryKey("0000000000000000000000000000000000000000000000000000000000000000"), 0n);
  assert.equal(advisoryKey("7fffffffffffffff000000000000000000000000000000000000000000000000"), 9_223_372_036_854_775_807n);
  assert.equal(advisoryKey("8000000000000000000000000000000000000000000000000000000000000000"), -9_223_372_036_854_775_808n);
});

test("PostgreSQL advisory key rejects non-canonical identities", () => {
  assert.throws(() => advisoryKey("not-a-digest"), InvalidArgumentError);
});
