(ns blackfish-metrics.core
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reading

(defn read-json [filename]
  (json/read (io/reader (io/resource (str "data/" filename)))
             :key-fn keyword))

(defn read-data []
  {:data/sales (:Sale (read-json "sales.json"))
   :data/sale-lines (:SaleLine (read-json "sale_lines.json"))
   :data/items (:Item (read-json "items.json"))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Persisting

(def SCHEMA
  [{:schema/table "sales"
    :schema/data-key :data/sales
    :schema/attrs {:id (comp parse-int :saleID)
                   :created-at (comp parse-date :createTime)
                   :completed (comp parse-bool :completed)
                   :total (comp double->cents :total)}}])

(defn transform [schema-attrs item]
  (map-vals #(% item) schema-attrs))

(defn persist! [db table coll]
  (jdbc/insert-multi! db table coll))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core

(defn import! [db]
  (let [data (read-data)]
    (for [{:schema/keys [table data-key attrs]} SCHEMA]
      (jdbc/insert-multi!
       db table
       (map (partial transform attrs) (get data data-key))
       {:entities unkeywordize}))))

(comment
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (import! db)))
