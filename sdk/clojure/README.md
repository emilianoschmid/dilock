# Dilock Clojure facade

This thin facade provides Clojure constructors and `with-open` macros over the
Java SDK. Redis, etcd, and Consul protocol logic stays in the Java modules; this project
does not implement a second backend. The dependency on both adapters is
intentional for this combined facade. Applications that need only one backend
can depend on the Java adapter directly.

The quick path uses the Java defaults (5 seconds of acquisition wait and a 30
second lease):

```clojure
(require '[dilock.clojure.core :as dilock])

(dilock/with-provider [provider
                       (dilock/redis-provider "redis://127.0.0.1:6379"
                                              "billing" "redis-epoch-1")]
  (dilock/with-lease [lease (.lock provider "invoice/42/reconcile")]
    (.assertValid lease)
    ;; Protect the actual resource with idempotency or resource-side fencing.
    ))
```

`with-provider` expands to a Java `AutoCloseable` scope. `with-lease` calls the
Java lease's explicit bounded `release()` and propagates an unacknowledged
release on normal work; if work already failed, the cleanup exception is
attached as suppressed while the original exception remains primary. This is
deliberate because Java `close()` is best-effort and reports an unknown outcome
only through its optional observer. The macro accepts only the typed `RELEASED`
outcome from the Java Redis, etcd, or Consul adapter; another value with the same name
does not confirm cleanup. Keep the scope short. A lease is not a
local mutex and expiry cannot stop a paused process.

For explicit wait and lease durations, call
`(dilock/acquisition-options waitMillis leaseMillis)` and pass the result as
the fourth binding value to `with-redis-lock` or `with-etcd-lock`, or as the
third binding value to `with-lease`. The provider remains caller-owned. The
following complete example also sets the
provider's RPC/cleanup bounds and closes the lease and provider in their
respective `with-open` scopes:

```clojure
(require '[dilock.clojure.core :as dilock])
(import '[java.time Duration]
        '[org.dilock.redis AcquireOptions RedisProvider RedisProviderOptions])

(let [policy (AcquireOptions. 12000 45000)
      options (RedisProviderOptions.
               "redis://127.0.0.1:6379" "billing" "prod-redis-epoch"
               (Duration/ofSeconds 2) (Duration/ofSeconds 3) policy nil)]
  (dilock/with-provider [provider (RedisProvider/connect options)]
  (dilock/with-redis-lock [lease provider "invoice/42/reconcile" policy]
        (.assertValid lease)
        ;; Perform bounded, idempotent work here; the macro awaits release.
        ))
```

For an immediate Redis attempt,
`(.tryAcquire lock)` uses the configured default lease, while
`(.tryAcquire lock leaseMillis)` sets only the lease duration; it is not an
acquisition-wait timeout. etcd fences are
available from `.fence`; the protected resource must atomically reject stale
fences. Redis does not offer
strong fencing. `tryAcquire` semantics and exceptions are those of the Java
adapter, including etcd's explicit unsupported-capability error.

Held-lease renewal is an etcd-only opt-in. Use `with-etcd-renewing-lock` when
work may outlast the initial TTL; it accepts either the provider's default
policy or an explicit `AcquireOptions` value:

```clojure
(dilock/with-provider [provider
                       (dilock/etcd-provider "http://127.0.0.1:2379"
                                              "billing" "prod-etcd-epoch")]
  (dilock/with-etcd-renewing-lock [lease provider "invoice/42/reconcile" true]
    (.assertValid lease)
    (let [fence (.fence lease)]
      ;; The protected resource must atomically reject stale fences.
      (str (.epoch fence) ":" (.value fence)))))
```

The five-element binding is `[lease provider resource options auto-renew?]`;
pass `false` to opt out while retaining explicit timings. Renewal keeps the
same server lease alive while this process can service keepalive. It cannot
stop paused work after lease loss, so resource-side fencing remains necessary.
Redis does not expose this held-lease renewal option.

For an explicit wait and lease policy:

```clojure
(dilock/with-etcd-renewing-lock
  [lease provider "invoice/42/reconcile"
   (dilock/acquisition-options 12000 45000) true]
  (.assertValid lease)
  ;; Keep the protected operation bounded and enforce its etcd fence.
  )
```

## Local execution

Install the Java reactor into a local Maven repository first, then from this
directory run `clojure -M:test`. To build the local source JAR and embedded POM
against that monorepo repository, run:

```sh
clojure -T:build jar :local-maven-repo "/absolute/path/to/maven-repository"
```

The temporary repository is used only to resolve this checkout's Java
artifacts; it is intentionally omitted from the generated POM. The root
`LICENSE` is included in the JAR as `META-INF/LICENSE`. The candidate
coordinate is `org.dilock/dilock-clojure:0.1.0`; the final public group identity
and registry remain owner decisions. The examples under `examples/` are executable
against explicitly provisioned local authorities. They contact Redis or etcd
and should only be run when those local services have the expected namespace
metadata.
