(ns blackfish-metrics.core
  (:require [blackfish-metrics.utils
             :refer
             [double->cents map-vals parse-bool parse-date parse-int unkeywordize]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reading

(defn read-json [file]
  (json/read file :key-fn keyword))

(def xml-legacy-vector #(if (map? %) (vector %) %))

(defn join-json-files [dir key]
  (->> (file-seq (io/file dir))
       (filter #(str/ends-with? (.getName %) ".json"))
       (map (comp read-json io/reader))
       (mapcat (comp xml-legacy-vector key))))

(defn read-data []
  {:data/sales (join-json-files "resources/data/sales" :Sale)
   :data/sale-lines (join-json-files "resources/data/sale_lines" :SaleLine)
   :data/items (join-json-files "resources/data/items" :Item)})

(defn- all-ids [db table]
  (set (map :id (jdbc/query db [(format "select id from %s" table)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Persisting

(defn- price-type [type sale-line]
  (->> (get-in sale-line [:Prices :ItemPrice])
       (filter (comp #{type} :useType))
       first :amount double->cents))

(defn- stub-missing-relations [db sale-lines]
  (let [item-ids (all-ids db "items")
        sale-ids (all-ids db "sales")]
    (map #(assoc %
            :item-id (get item-ids (:item-id %) 0)
            :sale-id (get sale-ids (:sale-id %) 0))
         sale-lines)))

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
    :before-fn #'stub-missing-relations
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Core

(defn- insert-dummy-item! [db]
  (jdbc/insert! db "items"
                {:id 0
                 :created-at (java.sql.Timestamp. 1506816000000)
                 :sku "0"
                 :description "Dummy -- Item was deleted"
                 :msrp 0
                 :online-price 0
                 :default-price 0}
                {:entities unkeywordize}))

(defn- insert-dummy-sale! [db]
  (jdbc/insert!
   db "sales"
   {:id 0 :created-at (java.sql.Timestamp. 1506816000000) :completed false :total 0}
   {:entities unkeywordize}))

(defn import! [db]
  (jdbc/execute! db ["truncate sales, sale_lines, items"])
  (insert-dummy-item! db)
  (insert-dummy-sale! db)
  (let [data (read-data)]
    (doseq [{:schema/keys [table data-key attrs] :keys [before-fn]} SCHEMA]
      (let [coll (map (partial transform attrs) (get data data-key))
            coll (if before-fn (before-fn db coll) coll)]
        (println (format "Persisting %s - %s records" table (count coll)))
        (jdbc/insert-multi! db table coll {:entities unkeywordize})))))

(comment
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (import! db))

  )
