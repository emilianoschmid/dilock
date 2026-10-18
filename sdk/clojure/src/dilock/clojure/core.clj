(ns dilock.clojure.core
  "Small Clojure scope helpers over Dilock's Java SDK. Backend behavior and
  wire semantics remain in the Java Redis, etcd, and Consul modules."
  (:import [org.dilock.redis RedisProvider RedisLease$ReleaseOutcome AcquireOptions DilockException DilockException$Kind]
           [org.dilock.etcd EtcdProvider EtcdLease$ReleaseOutcome]
           [org.dilock.consul ConsulProvider ConsulLease$ReleaseOutcome]
           [org.dilock.postgres PostgresProvider PostgresLease$ReleaseOutcome]))

(defn redis-provider
  "Connect to one Redis authority and validate its pre-provisioned namespace.
  The returned Java provider is caller-owned and must be closed."
  ([endpoint namespace epoch]
   (RedisProvider/connect endpoint namespace epoch))
  ([endpoint epoch]
   (RedisProvider/connect endpoint epoch)))

(defn etcd-provider
  "Connect to one etcd authority and validate its pre-provisioned namespace.
  The returned Java provider is caller-owned and must be closed."
  ([endpoint namespace epoch]
   (EtcdProvider/connect endpoint namespace epoch))
  ([endpoint epoch]
   (EtcdProvider/connect endpoint epoch)))

(defn consul-provider
  "Connect to one Consul agent and validate explicitly provisioned metadata."
  ([endpoint namespace epoch] (ConsulProvider/connect endpoint namespace epoch))
  ([endpoint epoch] (ConsulProvider/connect endpoint epoch)))

(defn postgres-provider
  "Connect to a pre-provisioned PostgreSQL advisory-session namespace."
  ([jdbc-url namespace epoch] (PostgresProvider/connect jdbc-url namespace epoch))
  ([jdbc-url epoch] (PostgresProvider/connect jdbc-url epoch)))

(defmacro with-resource
  "Evaluate body with a Java AutoCloseable bound to binding; close it on every exit."
  [[binding resource-expression] & body]
  `(with-open [~binding ~resource-expression]
     ~@body))

(defmacro with-provider
  "Scope any Dilock Java provider. Provider close performs bounded cleanup of its leases."
  [[binding provider-expression] & body]
  `(with-resource [~binding ~provider-expression]
     ~@body))

(declare scope-with-release)

(defmacro with-lease
  "Acquire and scope a lease from a Java lock handle; release is acknowledged.
  The two-element binding uses the handle's configured default policy. The
  three-element binding supplies one Java AcquireOptions value."
  [binding-form & body]
  (let [[binding lock-expression options :as form] binding-form]
    (when-not (and (vector? form) (#{2 3} (count form)))
      (throw (IllegalArgumentException.
              "with-lease binding must be [binding lock] or [binding lock options]")))
    (if (= 2 (count form))
      `(let [~binding (.acquire ~lock-expression)]
         (scope-with-release ~binding (fn [lease#] (.release lease#))
                             (fn [] ~@body)))
      `(let [~binding (.acquire ~lock-expression ~options)]
         (scope-with-release ~binding (fn [lease#] (.release lease#))
                             (fn [] ~@body))))))

(defn ^:no-doc scope-with-release
  "Run body and explicitly await the Java lease release acknowledgement.
  Keep a body failure primary and attach a simultaneous cleanup failure."
  [lease release-fn body]
  (let [body-result (try
                      {:value (body)}
                      (catch Throwable error {:error error}))
        cleanup-result (try
                         {:outcome (release-fn lease)}
                         (catch Throwable error {:error error}))
        cleanup-error (or (:error cleanup-result)
                          (when-not (or (= RedisLease$ReleaseOutcome/RELEASED (:outcome cleanup-result))
                                        (= EtcdLease$ReleaseOutcome/RELEASED (:outcome cleanup-result))
                                        (= ConsulLease$ReleaseOutcome/RELEASED (:outcome cleanup-result))
                                        (= PostgresLease$ReleaseOutcome/RELEASED (:outcome cleanup-result)))
                            (DilockException.
                             DilockException$Kind/LEASE_LOST
                             (str "Scoped lease release did not return RELEASED: "
                                  (:outcome cleanup-result)))))]
    (if-let [body-error (:error body-result)]
      (do
        (when (and cleanup-error (not (identical? body-error cleanup-error)))
          (.addSuppressed ^Throwable body-error ^Throwable cleanup-error))
        (throw body-error))
      (if cleanup-error
        (throw cleanup-error)
        (:value body-result)))))

(defmacro with-redis-lock
  "Acquire a Redis lock; provider remains caller-owned. The three-element
  binding uses the Java default policy; a fourth element supplies AcquireOptions."
  [binding-form & body]
  (let [[binding provider resource options :as form] binding-form]
    (when-not (and (vector? form) (#{3 4} (count form)))
      (throw (IllegalArgumentException.
              "with-redis-lock binding must be [binding provider resource] or [binding provider resource options]")))
    (if (= 3 (count form))
      `(with-lease [~binding (.lock ~provider ~resource)]
         ~@body)
      `(with-lease [~binding (.lock ~provider ~resource) ~options]
         ~@body))))

(defmacro with-etcd-lock
  "Acquire an etcd native lock; provider remains caller-owned. The three-element
  binding uses the Java default policy; a fourth element supplies AcquireOptions."
  [binding-form & body]
  (let [[binding provider resource options :as form] binding-form]
    (when-not (and (vector? form) (#{3 4} (count form)))
      (throw (IllegalArgumentException.
              "with-etcd-lock binding must be [binding provider resource] or [binding provider resource options]")))
    (if (= 3 (count form))
      `(with-lease [~binding (.lock ~provider ~resource)]
         ~@body)
      `(with-lease [~binding (.lock ~provider ~resource) ~options]
         ~@body))))

(defmacro with-consul-lock
  "Acquire and release one Consul KV session lock; provider remains caller-owned."
  [[binding provider resource & [options]] & body]
  (let [lock `(.lock ~provider ~resource)]
    (if options
      `(with-lease [~binding ~lock ~options] ~@body)
      `(with-lease [~binding ~lock] ~@body))))

(defmacro with-postgres-lock
  "Acquire and release one PostgreSQL session advisory lock."
  [[binding provider resource & [options]] & body]
  (let [lock `(.lock ~provider ~resource)]
    (if options
      `(with-lease [~binding ~lock ~options] ~@body)
      `(with-lease [~binding ~lock] ~@body))))

(defmacro with-etcd-renewing-lock
  "Acquire and scope an etcd lock with automatic renewal while held.
  Bindings are [binding provider resource auto-renew?] or
  [binding provider resource options auto-renew?]. Renewal keeps the same
  etcd lease alive while this process can service keepalive; it cannot stop a
  paused process after lease loss. The provider remains caller-owned."
  [binding-form & body]
  (let [[binding provider resource options auto-renew? :as form] binding-form]
    (when-not (and (vector? form) (#{4 5} (count form)))
      (throw (IllegalArgumentException.
              "with-etcd-renewing-lock binding must be [binding provider resource auto-renew?] or [binding provider resource options auto-renew?]")))
    (let [policy (if (= 4 (count form)) 'org.dilock.redis.AcquireOptions/DEFAULT options)
          renew? (if (= 4 (count form)) options auto-renew?)]
      `(let [lock# (.lock ~provider ~resource)
             policy# ~policy
             ~binding (.acquire lock# (.waitMillis policy#) (.leaseMillis policy#) ~renew?)]
         (scope-with-release ~binding (fn [lease#] (.release lease#))
                             (fn [] ~@body))))))

(defn acquisition-options
  "Create the Java SDK's immutable policy from positive whole milliseconds.
  Reject fractions before conversion so Clojure's numeric coercion cannot
  silently shorten a requested wait or lease."
  [wait-millis lease-millis]
  (when-not (and (integer? wait-millis)
                 (<= 1 wait-millis 86400000)
                 (integer? lease-millis)
                 (<= 1 lease-millis 86400000))
    (throw (IllegalArgumentException.
            "wait and lease must be integer milliseconds from 1 through 86400000")))
  (AcquireOptions. (long wait-millis) (long lease-millis)))
