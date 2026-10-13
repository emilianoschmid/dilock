"""Native JSONL conformance worker; stdout contains only protocol messages."""
import asyncio
import json
import sys
import uuid

from dilock import (AcquisitionUnknown, BackendUnavailable, Options, ProtocolMismatch,
                    UnsupportedCapability, WaitTimeout)
from dilock.redis import RedisProvider


async def main():
    config = json.loads(open(sys.argv[1], encoding="utf-8").read())
    backend = config.get("backend", "redis")
    if backend == "redis":
        provider = RedisProvider(config["endpoint"], config["namespace"], config["epoch"])
    elif backend == "etcd":
        from dilock.etcd import EtcdProvider
        provider = EtcdProvider(config["endpoint"], config["namespace"], config["epoch"])
    else:
        raise ValueError("unsupported backend")
    leases, pending, seen = {}, {}, set()

    def write(message):
        print(json.dumps(message), flush=True)

    async def process(r):
        id = r["id"]
        try:
            op = r["op"]
            if op == "hello":
                write(dict(id=id, status="ok", sdk="python", version="0.1.0.dev1",
                           protocol=1, capabilities=[backend, "renewal", "auto_renew", "fencing"]
                           if backend == "etcd" else [backend, "renewal", "auto_renew"]))
            elif op in ("try_acquire", "acquire"):
                options = Options(lease=r.get("lease_ms", 30000)/1000,
                                  rpc=r.get("rpc_ms", 200)/1000,
                                  auto_renew=r.get("auto_renew", False),
                                  require_fencing=r.get("require_fencing", False))
                handle = provider.lock(r["resource"])
                lease = (await handle.try_acquire(options) if op == "try_acquire"
                         else await handle.acquire(r["wait_ms"]/1000, options))
                if lease is None:
                    write(dict(id=id, status="busy"))
                else:
                    local = uuid.uuid4().hex
                    leases[local] = lease
                    response = dict(id=id, status="acquired", lease_id=local,
                                    effective_lease_ms=lease.effective_lease_ms)
                    if backend == "etcd":
                        response["fence"] = {"epoch": lease.fence[0],
                                             "value": str(lease.fence_revision)}
                    write(response)
            elif op == "release":
                write(dict(id=id, status=(await leases[r["lease_id"]].release()).value))
            elif op == "inspect":
                write(dict(id=id, status="ok", state=leases[r["lease_id"]].state.value))
            elif op == "renew":
                write(dict(id=id, status="renewed" if await leases[r["lease_id"]].renew()
                           else "lost"))
            elif op == "cancel_request":
                target = pending.get(r["target_id"])
                if target is not None:
                    target.cancel()
                write(dict(id=id, status="ok"))
            else:
                write(dict(id=id, status="invalid_argument"))
        except asyncio.CancelledError:
            write(dict(id=id, status="cancelled", remote_outcome_unknown=False))
        except (AcquisitionUnknown, BackendUnavailable, ProtocolMismatch,
                UnsupportedCapability, WaitTimeout) as e:
            status = {AcquisitionUnknown: "acquisition_unknown", BackendUnavailable:
                      "backend_unavailable", ProtocolMismatch: "protocol_mismatch",
                      UnsupportedCapability: "unsupported_capability", WaitTimeout: "timeout"}
            write(dict(id=id, status=status[type(e)],
                       remote_outcome_unknown=isinstance(e, AcquisitionUnknown)))
        except (ValueError, TypeError, KeyError):
            write(dict(id=id, status="invalid_argument"))
        finally:
            pending.pop(id, None)

    try:
        while True:
            line = await asyncio.to_thread(sys.stdin.readline, 65538)
            if not line:
                break
            try:
                if len(line) > 65536:
                    raise ValueError()
                r = json.loads(line)
                if not isinstance(r["id"], str) or r["id"] in seen:
                    raise ValueError()
                seen.add(r["id"])
            except (ValueError, KeyError, TypeError):
                write(dict(id="", status="invalid_argument"))
                continue
            if r["op"] == "shutdown":
                for task in tuple(pending.values()):
                    task.cancel()
                await asyncio.gather(*tuple(pending.values()), return_exceptions=True)
                await provider.aclose()
                write(dict(id=r["id"], status="ok", outstanding_cleanup=0))
                break
            pending[r["id"]] = asyncio.create_task(process(r))
    finally:
        for task in tuple(pending.values()):
            task.cancel()
        await asyncio.gather(*tuple(pending.values()), return_exceptions=True)
        await provider.aclose()


asyncio.run(main())
