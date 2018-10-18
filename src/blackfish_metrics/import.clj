(ns blackfish-metrics.import
  (:require [blackfish-metrics.lightspeed :as ls]
            [blackfish-metrics.schema :as schema]
            [cheshire.core :as json]
            [clojure.java.io :as io]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; JSON (initial load)

(defn- write-json-file [data filename]
  (json/generate-stream data (io/writer filename) {:pretty true}))

(defn- fetch-and-write [get-fn offset dir]
  (write-json-file (:body (get-fn {:offset offset}))
                   (str dir offset ".json")))

(defn- download-all-to-json! []
  (doseq [i (range 31)]
    (fetch-and-write #'ls/get-sales (* 100 i) "resources/data/sales/"))

  (doseq [i (range 9)]
    (fetch-and-write #'ls/get-items (* 100 i) "resources/data/items/"))

  (doseq [i (range 29)]
    (fetch-and-write #'ls/get-sale-lines (* 100 i) "resources/data/sale_lines/")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Straight to db (tx)

(defn fetch-missing [get-fn parser latest-id]
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
        latest-id (schema/latest-id db (::schema/table (schema/get-schema type)))
        records (fetch-missing fetch parse latest-id)]
    (persist! db records)))

(comment
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (doseq [type [:data/sales :data/items :data/sale-lines]]
      (import-missing! db type))))
