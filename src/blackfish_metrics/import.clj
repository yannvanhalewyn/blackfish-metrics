(ns blackfish-metrics.import
  (:require [blackfish-metrics.schema :as schema]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; API to psql

(defn fetch-latest [get-fn parser latest-id]
  (assert (pos-int? latest-id))
  (loop [acc []
         iteration 0]
    (let [records (parser (:body (get-fn {:offset (* 100 iteration)})))
          lowest-id (apply min (map :id records))]
      (println (format "Lowest ID received: %s, looking for: %s" lowest-id latest-id))
      (if (or (> iteration 10) (< lowest-id latest-id))
        (concat acc records)
        (recur (concat acc records) (inc iteration))))))

(defn import-missing! [db type]
  (println "Running sync for " (name type))
  (let [schema (schema/get-schema type)
        fetch (::schema/api-fetch schema)
        parse (schema/make-parser type)
        persist! (schema/make-persister type)
        latest-id (schema/latest-id db (schema/table type))
        records (fetch-latest fetch parse latest-id)]
    (persist! db records)))

(comment
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (doseq [type [:data/sales :data/items :data/sale-lines]]
      (import-latest! db type)))

  )
