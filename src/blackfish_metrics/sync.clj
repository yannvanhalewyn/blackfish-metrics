(ns blackfish-metrics.sync
  (:require [blackfish-metrics.lightspeed :as ls]
            [blackfish-metrics.logging :as log]
            [blackfish-metrics.schema :as schema]
            [clojure.java.jdbc :as jdbc])
  (:import java.sql.BatchUpdateException))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers

(defn- get-latest-id [db table]
  (:id (first (jdbc/query db [(format "select id from %s order by id desc limit 1" table)]))))

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
        latest-id (get-latest-id db (schema/table type))
        records (fetch-missing fetch parse latest-id)]
    (persist! db records)))

(defn- import-recents! [db types]
  (doseq [type types]
    (import-missing!* db type)))

(defn import-sales-with-relations-when-necessary! [db]
  (ls/refresh-access-token!)
  (try
    (import-recents! db [:data/sales :data/sale-lines])
    (catch BatchUpdateException e
      (log/info "PSQL exception caught. Attempting to fetch missing relations.")
      (import-recents! db [:data/manufacturers :data/vendors :data/categories :data/items])
      (import-recents! db [:data/sales :data/sale-lines]))))

(comment

  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (import-sales-with-relations-when-necessary! db))

  )
