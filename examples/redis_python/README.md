# Redis command adapter — Python

A small actual command adapter, separate from the fifteen in-memory style samples.
It implements atomic `SET ... NX PX`, owner-checked Lua release and owner-checked
Lua renewal through an injected `redis.asyncio.Redis`-compatible command port.
Scripts and identities come from the repository's canonical specification.

**This is not a complete lease provider or a production SDK.** Its tests use a
scripted port and do not execute Redis/Lua. No real Redis server was tested in this
bundle. Do not mistake the `ClaimReply.accepted` acknowledgement for permission to
run work: the lifecycle layer still needs validity/deadline checks and metadata.

Run the six unit tests from this directory:

```shell
python -m unittest -v test_primitives
```

Illustrative client injection after independently installing a reviewed supported
redis-py version:

```python
# client is a configured redis.asyncio.Redis owned by the application.
adapter = RedisPrimitiveAdapter(client)
identity = adapter.new_identity("service", "example/resource")
reply = await adapter.claim(identity, ttl_ms=10_000)
# Do NOT start work based on accepted alone. Hand acknowledgement + elapsed time
# to the lifecycle engine that validates remaining budget and metadata.
```

Client connection setup must explicitly bound command timeouts and disable hidden
mutation retries/offline queues. Credentials and TLS come from the application.
Do not test against production. No integration runner that changes a remote
keyspace is executed by these unit tests.

The outer lifecycle engine must classify lost replies as outcome-unknown,
serialize renew/release, manage bounded cleanup, enforce terminal lease states,
notify loss, negotiate namespace profiles and test server failover assumptions.
Those are deliberately outside this small adapter, not optional safety features.
The runtime SDK must package canonical scripts rather than depend on repository
paths or `sys.path` manipulation used by this standalone reference.

Primary Redis pattern: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
