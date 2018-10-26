(ns blackfish-metrics.sync
  (:require [blackfish-metrics.lightspeed :as ls]
            [blackfish-metrics.logging :as log]
            [blackfish-metrics.schema :as schema]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; API to psql

(defn fetch-missing [get-fn parser latest-id]
  (assert (pos-int? latest-id))
  (loop [acc []
         iteration 0]
    (let [records (parser (:body (get-fn {:offset (* 100 iteration)})))
          lowest-id (apply min (map :id records))]
      (log/info (format "  Lowest ID received: %s, looking for: %s" lowest-id latest-id))
      (if (or (> iteration 10) (< lowest-id latest-id))
        (concat acc records)
        (recur (concat acc records) (inc iteration))))))

(defn- import-missing!* [db type]
  (log/info "SYNC:" (name type))
  (let [schema (schema/get-schema type)
        fetch (::schema/api-fetch schema)
        parse (schema/make-parser type)
        persist! (schema/make-persister type)
        latest-id (schema/latest-id db (schema/table type))
        records (fetch-missing fetch parse latest-id)]
    (persist! db records)))

(defn import-missing! [db]
  (ls/refresh-access-token!)
  (doseq [type [:data/sales :data/items :data/sale-lines]]
    (import-missing!* db type)))

(comment

  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (import-missing! db))

  )
