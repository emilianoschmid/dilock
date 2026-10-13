/** Run against a Redis namespace whose Dilock metadata was provisioned out of band. */
import { randomUUID } from "node:crypto";
import { RedisLease, RedisProvider } from "@dilock/redis";

const endpoint = process.env.DILOCK_REDIS_URL;
const namespace = process.env.DILOCK_NAMESPACE;
const epoch = process.env.DILOCK_EPOCH;

if (!endpoint || !namespace || !epoch) {
  throw new Error(
    "Set DILOCK_REDIS_URL, DILOCK_NAMESPACE and DILOCK_EPOCH for a pre-provisioned Redis namespace.",
  );
}

const resource = `docs-example/${randomUUID()}`;
const owner = await RedisProvider.connect({ endpoint, namespace, epoch });
let contender: RedisProvider | undefined;
let ownerLease: RedisLease | undefined;
let successor: RedisLease | undefined;

try {
  contender = await RedisProvider.connect({ endpoint, namespace, epoch });
  const ownerLock = owner.lock(resource);
  const contenderLock = contender.lock(resource);

  ownerLease = await ownerLock.tryAcquire({ leaseMs: 5_000, rpcTimeoutMs: 1_000 });
  if (!ownerLease) throw new Error("The unique example resource was unexpectedly busy.");

  const busy = await contenderLock.tryAcquire({ leaseMs: 5_000, rpcTimeoutMs: 1_000 });
  if (busy) {
    await busy.release();
    throw new Error("A second owner acquired an actively held resource.");
  }
  console.log("contender: acknowledged busy");

  const outcome = await ownerLease.release();
  ownerLease = undefined;
  if (outcome !== "released") {
    throw new Error(`Owner release was not acknowledged: ${outcome}`);
  }

  successor = await contenderLock.tryAcquire({ leaseMs: 5_000, rpcTimeoutMs: 1_000 });
  if (!successor) throw new Error("Contender remained busy after acknowledged release.");
  successor.assertValid();
  console.log("contender: acquired after owner release");
} finally {
  try {
    if (successor) await successor.release();
  } finally {
    try {
      if (ownerLease) await ownerLease.release();
    } finally {
      try {
        if (contender) await contender.close();
      } finally {
        await owner.close();
      }
    }
  }
}
