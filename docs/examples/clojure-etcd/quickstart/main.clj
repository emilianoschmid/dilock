(require '[dilock.clojure.core :as dilock])

(defn rebuild-daily-report [epoch revision]
  ;; Atomically reject stale epoch/revision fences at the protected resource.
  (println "Applying work under fence" epoch revision))

(let [epoch (or (System/getenv "DILOCK_COORDINATION_EPOCH")
                (throw (ex-info "Set DILOCK_COORDINATION_EPOCH to the provisioned epoch" {})))
      endpoint (or (System/getenv "DILOCK_ETCD_ENDPOINT") "http://127.0.0.1:2379")
      namespace (or (System/getenv "DILOCK_NAMESPACE") "default")]
  (dilock/with-provider [provider (dilock/etcd-provider endpoint namespace epoch)]
    (dilock/with-lease [lease (.lock provider "reports/daily")]
      (.assertValid lease)
      (let [fence (.fence lease)]
        (rebuild-daily-report (.epoch fence) (.value fence))))
    ;; Sequential second resource reuses the provider with explicit timings.
    (dilock/with-lease [lease (.lock provider "reports/monthly")
                        (dilock/acquisition-options 12000 45000)]
      (.assertValid lease)
      (let [fence (.fence lease)]
        (rebuild-daily-report (.epoch fence) (.value fence))))))
