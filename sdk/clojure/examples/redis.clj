(require '[dilock.clojure.core :as dilock])

;; Simple path: standard 5 s wait and 30 s lease; the provider and lease both
;; close even if the protected operation throws.
(dilock/with-provider [provider (dilock/redis-provider "redis://127.0.0.1:6379" "billing" "local-redis-epoch")]
  (dilock/with-lease [lease (.lock provider "invoice/42/reconcile")]
    (.assertValid lease)
    (println "Redis lease acquired" (.id lease))))

;; Fully configured path: Java engine still owns protocol and backend operations.
(dilock/with-provider [provider (dilock/redis-provider "redis://127.0.0.1:6379" "billing" "local-redis-epoch")]
  (dilock/with-resource [lease (.acquire (.lock provider "invoice/42/reconcile")
                                        (dilock/acquisition-options 12000 45000))]
    (.assertValid lease)
    (println "Configured Redis lease acquired" (.id lease))))
