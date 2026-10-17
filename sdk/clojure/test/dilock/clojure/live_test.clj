(ns dilock.clojure.live-test
  "Opt-in real-backend checks for the public Clojure scope macros."
  (:require [clojure.test :refer [deftest is]]
            [dilock.clojure.test-config :as config]
            [dilock.clojure.core :as dilock])
  (:import [org.dilock.redis RedisProvider AcquireOptions]
           [org.dilock.consul ConsulProvider]
           [redis.clients.jedis RedisClient]
           [java.util.concurrent Executors TimeUnit]))

(defn- test-id [prefix]
  (str "clj-live-" prefix "-" (System/currentTimeMillis)))

(when (config/configured-value? (System/getenv "DILOCK_REDIS_URL"))
  (deftest redis-live-two-provider-scope
    (let [endpoint (System/getenv "DILOCK_REDIS_URL")
          namespace (test-id "redis")
          epoch (test-id "epoch")
          resource (str "resource/" namespace)]
      (with-open [client (RedisClient/create endpoint)]
        (RedisProvider/provisionNamespace client namespace epoch))
      (dilock/with-provider [first-provider (dilock/redis-provider endpoint namespace epoch)]
        (dilock/with-provider [second-provider (dilock/redis-provider endpoint namespace epoch)]
          (let [first-lock (.lock first-provider resource)
                second-lock (.lock second-provider resource)
                options (AcquireOptions. 1000 5000)
                first-lease (.acquire first-lock options)]
            (try
              (.assertValid first-lease)
              (is (.isEmpty (.tryAcquire second-lock 5000))
                  "an independent provider must observe ordinary contention")
              (is (= "RELEASED" (str (.release first-lease))))
              (dilock/with-redis-lock [successor second-provider resource]
                (.assertValid successor)
                (is (.isEmpty (.tryAcquire first-lock 5000))
                    "the successor must remain held while its scope is open"))
              (finally
                (.close first-lease)))))))))

(when (config/configured-value? (System/getenv "DILOCK_ETCD_URL"))
  (deftest etcd-live-two-provider-scope
    (let [endpoint (System/getenv "DILOCK_ETCD_URL")
          namespace (System/getenv "DILOCK_NAMESPACE")
          epoch (System/getenv "DILOCK_COORDINATION_EPOCH")
          resource (str "resource/" (test-id "etcd"))]
      (when-not (and (config/configured-value? namespace)
                     (config/configured-value? epoch))
        (throw (IllegalStateException. "DILOCK_NAMESPACE and DILOCK_COORDINATION_EPOCH are required")))
      (dilock/with-provider [first-provider (dilock/etcd-provider endpoint namespace epoch)]
        (dilock/with-provider [second-provider (dilock/etcd-provider endpoint namespace epoch)]
          (let [first-lock (.lock first-provider resource)
                second-lock (.lock second-provider resource)
                options (AcquireOptions. 5000 5000)
                first-lease (.acquire first-lock options)
                executor (Executors/newSingleThreadExecutor)]
            (try
              (let [first-fence (.fence first-lease)
                    queued (.submit executor ^java.util.concurrent.Callable
                                    (reify java.util.concurrent.Callable
                                      (call [_] (.acquire second-lock options))))]
                (is (= epoch (.epoch first-fence)))
                    (is (pos? (.value first-fence)))
                (Thread/sleep 250)
                (is (not (.isDone queued)) "the second provider should queue behind the owner")
                (is (= "RELEASED" (str (.release first-lease))))
                (with-open [successor (.get queued 7 TimeUnit/SECONDS)]
                  (let [next-fence (.fence successor)]
                    (is (= epoch (.epoch next-fence)))
                    (is (> (.value next-fence) (.value first-fence))))))
              (finally
                (.close first-lease)
                (.shutdownNow executor)))))))))

(when (config/configured-value? (System/getenv "DILOCK_CONSUL_ENDPOINT"))
  (deftest consul-live-two-provider-scope-and-fence
    (let [endpoint (System/getenv "DILOCK_CONSUL_ENDPOINT")
          namespace (System/getenv "DILOCK_NAMESPACE")
          epoch (System/getenv "DILOCK_COORDINATION_EPOCH")
          resource (str "resource/" (test-id "consul"))]
      (when-not (and (config/configured-value? namespace)
                     (config/configured-value? epoch))
        (throw (IllegalStateException. "DILOCK_NAMESPACE and DILOCK_COORDINATION_EPOCH are required")))
      (dilock/with-provider [first-provider (dilock/consul-provider endpoint namespace epoch)]
        (dilock/with-provider [second-provider (dilock/consul-provider endpoint namespace epoch)]
          (let [first-lock (.lock first-provider resource)
                second-lock (.lock second-provider resource)
                options (AcquireOptions. 2000 10000)
                first-lease (.acquire first-lock options)]
            (try
              (let [first-fence (.fence first-lease)]
                (is (= epoch (.epoch first-fence)))
                (is (pos? (.lockIndex first-fence)))
                (is (.isEmpty (.tryAcquire second-lock))
                    "an independent provider must observe ordinary contention")
                (is (= "RELEASED" (str (.release first-lease))))
                (dilock/with-consul-lock [successor second-provider resource options]
                  (let [next-fence (.fence successor)]
                    (is (= epoch (.epoch next-fence)))
                    (is (> (.lockIndex next-fence) (.lockIndex first-fence))))))
              (finally
                (.close first-lease)))))))))

(when (config/configured-value? (System/getenv "DILOCK_ETCD_URL"))
  (deftest etcd-live-renewing-scope-retains-lock-past-initial-ttl
    (let [endpoint (System/getenv "DILOCK_ETCD_URL")
          namespace (System/getenv "DILOCK_NAMESPACE")
          epoch (System/getenv "DILOCK_COORDINATION_EPOCH")
          resource (str "resource/" (test-id "renewal"))]
      (dilock/with-provider [owner (dilock/etcd-provider endpoint namespace epoch)]
        (dilock/with-provider [contender (dilock/etcd-provider endpoint namespace epoch)]
          (let [owner-lock (.lock owner resource)
                contender-lock (.lock contender resource)
                options (AcquireOptions. 5000 3000)
                executor (Executors/newSingleThreadExecutor)
                queued (atom nil)]
            (try
              (dilock/with-etcd-renewing-lock [lease owner resource options true]
                (.assertValid lease)
                (reset! queued
                        (.submit executor ^java.util.concurrent.Callable
                                 (reify java.util.concurrent.Callable
                                   (call [_] (.acquire contender-lock options)))))
                ;; The initial server TTL is three seconds. A queued successor
                ;; remaining blocked beyond it demonstrates active renewal.
                (Thread/sleep 3500)
                (.assertValid lease)
                (is (not (.isDone @queued))
                    "the contender must remain queued after the initial TTL"))
              (with-open [successor (.get @queued 7 TimeUnit/SECONDS)]
                (.assertValid successor)
                (is (pos? (.value (.fence successor)))
                    "the successor must receive a verified etcd fence"))
              (finally
                (when-let [waiter @queued] (.cancel waiter true))
                (.shutdownNow executor)))))))))
