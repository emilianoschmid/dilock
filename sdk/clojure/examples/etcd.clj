(require '[dilock.clojure.core :as dilock])

(dilock/with-provider [provider (dilock/etcd-provider "http://127.0.0.1:2379" "billing" "local-etcd-epoch")]
  (dilock/with-lease [lease (.lock provider "invoice/42/reconcile")]
    (.assertValid lease)
    (let [fence (.fence lease)]
      ;; The protected resource must atomically compare and record this fence.
      (println "etcd fence" (.epoch fence) (.value fence)))))
