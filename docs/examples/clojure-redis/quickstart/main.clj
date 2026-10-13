(require '[dilock.clojure.core :as dilock])

(defn rebuild-daily-report []
  ;; Replace with one bounded, idempotent unit of work.
  nil)

(defn rebuild-monthly-report [] nil)

(let [epoch (or (System/getenv "DILOCK_COORDINATION_EPOCH")
                (throw (ex-info "Set DILOCK_COORDINATION_EPOCH to the provisioned epoch" {})))
      endpoint (or (System/getenv "DILOCK_REDIS_URL") "redis://127.0.0.1:6379")
      namespace (or (System/getenv "DILOCK_NAMESPACE") "default")]
  ;; Defaults: five-second wait and thirty-second lease. Both scopes close
  ;; through the Java engine shared by this facade.
  (dilock/with-provider [provider (dilock/redis-provider endpoint namespace epoch)]
    (dilock/with-lease [lease (.lock provider "reports/daily")]
      (.assertValid lease)
      (rebuild-daily-report))
    ;; Sequential second resource on the same provider, with explicit timings.
    (dilock/with-lease [lease (.lock provider "reports/monthly")
                        (dilock/acquisition-options 12000 45000)]
      (.assertValid lease)
      (rebuild-monthly-report))))
