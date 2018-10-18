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

(defn- price-type [type sale-line]
  (->> (get-in sale-line [:Prices :ItemPrice])
       (filter (comp #{type} :useType))
       first :amount double->cents))

(def SCHEMA
  [{:schema/table "sales"
    :schema/data-key :data/sales
    :schema/attrs {:id (comp parse-int :saleID)
                   :created-at (comp parse-date :createTime)
                   :completed (comp parse-bool :completed)
                   :total (comp double->cents :total)}}
   {:schema/table "items"
    :schema/data-key :data/items
    :schema/attrs {:id (comp parse-int :itemID)
                   :created-at (comp parse-date :createTime)
                   :sku :systemSku
                   :description :description
                   :msrp (partial price-type "MSRP")
                   :online_price (partial price-type "Online")
                   :default_price (partial price-type "Default")}}
   {:schema/table "sale_lines"
    :schema/data-key :data/sale-lines
    :schema/attrs {:id (comp parse-int :saleLineID)
                   :sale-id (comp parse-int :saleID)
                   :item-id (comp parse-int :itemID)
                   :created-at (comp parse-date :createTime)
                   :unit-qty (comp parse-int :unitQuantity)
                   :unit-price (comp double->cents :unitPrice)
                   :price (comp double->cents :calcTotal)
                   :discount (comp double->cents :discountAmount)}}])

(defn transform [schema-attrs item]
  (map-vals #(% item) schema-attrs))

(defn persist! [db table coll]
  (jdbc/insert-multi! db table coll))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core

(defn import! [db]
  (jdbc/execute! db ["truncate sales, sale_lines, items"])
  (let [data (read-data)]
    (doseq [{:schema/keys [table data-key attrs]} SCHEMA]
      (let [coll (map (partial transform attrs) (get data data-key))]
        (println (format "Persisting %s - %s records" table (count coll)))
        (jdbc/insert-multi! db table coll {:entities unkeywordize})))))

(comment
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (import! db))
  )
