(ns dilock.clojure.test-runner
  (:require [clojure.test :as test]
            [dilock.clojure.test-config :as config]
            [dilock.clojure.core-test]
            [dilock.clojure.live-test]))

(defn -main [& _]
  (let [namespaces (cond-> ['dilock.clojure.core-test]
                     (or (config/configured-value? (System/getenv "DILOCK_REDIS_URL"))
                         (config/configured-value? (System/getenv "DILOCK_ETCD_URL")))
                     (conj 'dilock.clojure.live-test))
        {:keys [fail error]} (apply test/run-tests namespaces)]
    (when (pos? (+ fail error))
      (System/exit 1))))
