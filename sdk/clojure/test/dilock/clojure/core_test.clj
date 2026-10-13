(ns dilock.clojure.core-test
  (:require [clojure.test :refer [deftest is]]
            [dilock.clojure.test-config :as config]
            [dilock.clojure.core :as dilock])
  (:import [java.lang AutoCloseable]
           [java.util.concurrent.atomic AtomicBoolean AtomicInteger]
           [org.dilock.redis RedisLease$ReleaseOutcome]
           [org.dilock.etcd EtcdLease$ReleaseOutcome]))

(deftest with-resource-closes-on-normal-return
  (let [closed (AtomicBoolean. false)
        resource (reify AutoCloseable
                   (close [_] (.set closed true)))
        result (dilock/with-resource [value resource]
                 (identical? value resource))]
    (is result)
    (is (.get closed))))

(deftest with-resource-closes-when-body-throws
  (let [closed (AtomicBoolean. false)
        resource (reify AutoCloseable
                   (close [_] (.set closed true)))]
    (is (thrown-with-msg? Exception #"expected"
          (dilock/with-resource [_ resource]
            (throw (Exception. "expected")))))
    (is (.get closed))))

(deftest lease-scope-surfaces-an-unacknowledged-release
  (let [scope (var-get (ns-resolve 'dilock.clojure.core 'scope-with-release))
        release-error (IllegalStateException. "release acknowledgement unknown")
        observed (try
                   (scope :lease (fn [_] (throw release-error)) (fn [] :completed))
                   nil
                   (catch Throwable error error))]
    (is (identical? release-error observed))))

(deftest lease-scope-does-not-return-before-release-is-acknowledged
  (let [scope (var-get (ns-resolve 'dilock.clojure.core 'scope-with-release))
        release-count (AtomicInteger. 0)
        result (scope :lease
                      (fn [_]
                        (.incrementAndGet release-count)
                        RedisLease$ReleaseOutcome/RELEASED)
                      (fn [] :completed))]
    (is (= :completed result))
    (is (= 1 (.get release-count)))))

(deftest lease-scope-preserves-body-error-and-attaches-cleanup-error
  (let [scope (var-get (ns-resolve 'dilock.clojure.core 'scope-with-release))
        body-error (IllegalArgumentException. "work failed")
        cleanup-error (IllegalStateException. "release acknowledgement unknown")
        observed (try
                   (scope :lease (fn [_] (throw cleanup-error)) (fn [] (throw body-error)))
                   nil
                   (catch Throwable error error))]
    (is (identical? body-error observed))
    (is (= [cleanup-error] (vec (.getSuppressed observed))))))

(deftest lease-scope-rejects-already-not-owned-outcome
  (let [scope (var-get (ns-resolve 'dilock.clojure.core 'scope-with-release))
        observed (try
                   (scope :lease (fn [_] RedisLease$ReleaseOutcome/ALREADY_NOT_OWNED)
                          (fn [] :completed))
                   nil
                   (catch Throwable error error))]
    (is (instance? org.dilock.redis.DilockException observed))
    (is (= org.dilock.redis.DilockException$Kind/LEASE_LOST (.kind observed)))))

(deftest lease-scope-classifies-native-etcd-release-outcomes
  (let [scope (var-get (ns-resolve 'dilock.clojure.core 'scope-with-release))
        body-runs (AtomicBoolean. false)
        released (scope :lease
                        (fn [_] EtcdLease$ReleaseOutcome/RELEASED)
                        (fn [] (.set body-runs true) :completed))
        already-not-owned (try
                            (scope :lease
                                   (fn [_] EtcdLease$ReleaseOutcome/ALREADY_NOT_OWNED)
                                   (fn [] :completed))
                            nil
                            (catch Throwable error error))
        unknown-release (org.dilock.redis.DilockException.
                         org.dilock.redis.DilockException$Kind/RELEASE_UNKNOWN
                         "etcd cleanup acknowledgement was unknown")
        observed-unknown (try
                           (scope :lease (fn [_] (throw unknown-release))
                                  (fn [] :completed))
                           nil
                           (catch Throwable error error))]
    (is (= :completed released))
    (is (.get body-runs))
    (is (instance? org.dilock.redis.DilockException already-not-owned))
    (is (= org.dilock.redis.DilockException$Kind/LEASE_LOST
           (.kind ^org.dilock.redis.DilockException already-not-owned)))
    (is (identical? unknown-release observed-unknown))
    (is (= org.dilock.redis.DilockException$Kind/RELEASE_UNKNOWN
           (.kind ^org.dilock.redis.DilockException observed-unknown)))))

(deftest lease-scope-rejects-a-non-dilock-release-value
  (let [scope (var-get (ns-resolve 'dilock.clojure.core 'scope-with-release))
        observed (try
                   (scope :lease (fn [_] "RELEASED") (fn [] :completed))
                   nil
                   (catch Throwable error error))]
    (is (instance? org.dilock.redis.DilockException observed))
    (is (= org.dilock.redis.DilockException$Kind/LEASE_LOST (.kind observed)))))

(deftest lease-scope-preserves-body-error-when-release-is-already-not-owned
  (let [scope (var-get (ns-resolve 'dilock.clojure.core 'scope-with-release))
        body-error (IllegalArgumentException. "work failed")
        observed (try
                   (scope :lease (fn [_] RedisLease$ReleaseOutcome/ALREADY_NOT_OWNED)
                          (fn [] (throw body-error)))
                   nil
                   (catch Throwable error error))]
    (is (identical? body-error observed))
    (is (= org.dilock.redis.DilockException$Kind/LEASE_LOST
           (.kind ^org.dilock.redis.DilockException (first (.getSuppressed observed)))))))

(deftest backend-scope-macros-expand-to-reusable-lock-scopes
  (let [redis (macroexpand-1
                '(dilock.clojure.core/with-redis-lock [lease provider resource] :body))
        etcd (macroexpand-1
               '(dilock.clojure.core/with-etcd-lock [lease provider resource] :body))]
    (is (= 'dilock.clojure.core/with-lease (first redis)))
    (is (= '[lease (.lock provider resource)] (second redis)))
    (is (= 'dilock.clojure.core/with-lease (first etcd)))
    (is (= '[lease (.lock provider resource)] (second etcd)))))

(deftest backend-scope-macros-accept-explicit-acquisition-options
  (let [redis (macroexpand-1
                '(dilock.clojure.core/with-redis-lock [lease provider resource options] :body))
        etcd (macroexpand-1
               '(dilock.clojure.core/with-etcd-lock [lease provider resource options] :body))
        lease (macroexpand-1
                '(dilock.clojure.core/with-lease [lease lock options] :body))]
    (is (= 'dilock.clojure.core/with-lease (first redis)))
    (is (= '[lease (.lock provider resource) options] (second redis)))
    (is (= 'dilock.clojure.core/with-lease (first etcd)))
    (is (= '[lease (.lock provider resource) options] (second etcd)))
    (is (= 'clojure.core/let (first lease)))
    (is (= '[lease (.acquire lock options)] (second lease)))
    (is (= 'dilock.clojure.core/scope-with-release
           (first (nth lease 2))))))

(deftest etcd-renewing-scope-forwards-default-and-explicit-policy
  (let [default-policy (macroexpand-1
                        '(dilock.clojure.core/with-etcd-renewing-lock
                           [lease provider resource renew?] :body))
        explicit-policy (macroexpand-1
                         '(dilock.clojure.core/with-etcd-renewing-lock
                            [lease provider resource options renew?] :body))
        acquire-call (fn [expanded]
                       (first (filter #(and (seq? %) (= '.acquire (first %)))
                                      (tree-seq coll? seq expanded))))]
    (is (= 'org.dilock.redis.AcquireOptions/DEFAULT
           (nth (second default-policy) 3)))
    (is (= 5 (count (acquire-call default-policy))))
    (is (= 'renew? (nth (acquire-call default-policy) 4)))
    (is (= 'options (nth (second explicit-policy) 3)))
    (is (= 'renew? (nth (acquire-call explicit-policy) 4)))
    (is (= '.waitMillis (first (nth (acquire-call default-policy) 2))))
    (is (= '.leaseMillis (first (nth (acquire-call default-policy) 3))))))

(deftest optional-backend-settings-require-nonblank-values
  (is (false? (config/configured-value? nil)))
  (is (false? (config/configured-value? "")))
  (is (false? (config/configured-value? "  \t")))
  (is (true? (config/configured-value? "redis://127.0.0.1:6379"))))

(deftest acquisition-options-require-whole-in-range-milliseconds
  (let [options (dilock/acquisition-options 1500 30000)]
    (is (= 1500 (.waitMillis options)))
    (is (= 30000 (.leaseMillis options))))
  (doseq [[wait-millis lease-millis] [[1.5 1000]
                                       [1000 1.5]
                                       [0 1000]
                                       [1000 86400001]
                                       ["1000" 1000]]]
    (is (thrown-with-msg? IllegalArgumentException
                          #"integer milliseconds"
                          (dilock/acquisition-options wait-millis lease-millis)))))
