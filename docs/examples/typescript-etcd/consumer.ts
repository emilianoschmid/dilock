import { randomUUID } from "node:crypto";
import { EtcdLease, EtcdProvider } from "@dilock/etcd";

const endpoint = process.env.DILOCK_ETCD_ENDPOINT;
const namespace = process.env.DILOCK_NAMESPACE;
const epoch = process.env.DILOCK_EPOCH;

if (!endpoint || !namespace || !epoch) {
  throw new Error(
    "Set DILOCK_ETCD_ENDPOINT, DILOCK_NAMESPACE and DILOCK_EPOCH for an operator-provisioned etcd namespace.",
  );
}

const resource = `docs-example/typescript-etcd/${randomUUID()}`;
const owner = await EtcdProvider.connect({ endpoint, namespace, epoch });
let successor: EtcdProvider | undefined;
let ownerLease: EtcdLease | undefined;
let successorLease: EtcdLease | undefined;

try {
  successor = await EtcdProvider.connect({ endpoint, namespace, epoch });

  ownerLease = await owner.lock(resource).acquire({
    waitMs: 5_000,
    leaseMs: 10_000,
    rpcTimeoutMs: 2_000,
  });
  ownerLease.assertValid();
  const ownerFence = ownerLease.fence;
  if (!ownerFence) throw new Error("The acquired etcd lease did not expose its verified fence.");
  console.log(`owner: acquired fence ${ownerFence.epoch}/${ownerFence.value}`);

  const released = await ownerLease.release();
  if (released !== "released") {
    throw new Error(`Owner release was not acknowledged: ${released}`);
  }
  ownerLease = undefined;
  console.log("owner: release acknowledged");

  successorLease = await successor.lock(resource).acquire({
    waitMs: 5_000,
    leaseMs: 10_000,
    rpcTimeoutMs: 2_000,
  });
  successorLease.assertValid();
  const successorFence = successorLease.fence;
  if (!successorFence) throw new Error("The successor etcd lease did not expose its verified fence.");
  console.log(`successor: acquired fence ${successorFence.epoch}/${successorFence.value}`);

  const successorReleased = await successorLease.release();
  if (successorReleased !== "released") {
    throw new Error(`Successor release was not acknowledged: ${successorReleased}`);
  }
  successorLease = undefined;
  console.log("successor: release acknowledged");
} finally {
  try {
    if (successorLease) await successorLease.release();
  } finally {
    try {
      if (ownerLease) await ownerLease.release();
    } finally {
      try {
        if (successor) await successor.close();
      } finally {
        await owner.close();
      }
    }
  }
}
