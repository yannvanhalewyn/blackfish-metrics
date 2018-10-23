(ns blackfish-metrics.schema
  (:require [blackfish-metrics.db :as db]
            [blackfish-metrics.lightspeed :as ls]
            [blackfish-metrics.logging :as log]
            [blackfish-metrics.utils :as u]
            [clojure.java.jdbc :as jdbc]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers

(defn- price-type [type sale-line]
  (->> (get-in sale-line [:Prices :ItemPrice])
       (filter (comp #{type} :useType))
       first :amount u/double->cents))

(defn- all-ids [db table]
  (set (map :id (jdbc/query db [(format "select id from %s" table)]))))

(defn latest-id [db table]
  (:id (first (jdbc/query db [(format "select id from %s order by id desc limit 1" table)]))))

(defn- stub-missing-sale-line-relations [db sale-lines]
  (let [item? (all-ids db "items")
        sale? (all-ids db "sales")]
    (map #(cond-> %
            (not (item? (:item-id %))) (assoc :item-id nil)
            (not (sale? (:sale-id %))) (assoc :sale-id nil))
         sale-lines)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema

(def SCHEMA
  [{::table "sales"
    ::data-key :data/sales
    ::api-root :Sale
    ::api-fetch #'ls/get-sales
    ::attrs {:id (comp u/parse-int :saleID)
             :created-at (comp u/parse-date :createTime)
             :completed (comp u/parse-bool :completed)
             :total (comp u/double->cents :total)}}
   {::table "items"
    ::data-key :data/items
    ::api-root :Item
    ::api-fetch #'ls/get-items
    ::attrs {:id (comp u/parse-int :itemID)
             :created-at (comp u/parse-date :createTime)
             :sku :systemSku
             :manufacturer_id (comp u/zero->nil u/parse-int :manufacturerID)
             :category_id (comp u/zero->nil u/parse-int :categoryID)
             :description :description
             :msrp (partial price-type "MSRP")
             :online_price (partial price-type "Online")
             :default_price (partial price-type "Default")
             :archived (comp u/parse-bool :archived)}}
   {::table "sale_lines"
    ::data-key :data/sale-lines
    ::api-root :SaleLine
    ::api-fetch #'ls/get-sale-lines
    ::before-persist #'stub-missing-sale-line-relations
    ::attrs {:id (comp u/parse-int :saleLineID)
             :sale-id (comp u/parse-int :saleID)
             :item-id (comp u/parse-int :itemID)
             :created-at (comp u/parse-date :createTime)
             :qty (comp u/parse-int :unitQuantity)
             :unit-price (comp u/double->cents :unitPrice)
             :total (comp u/double->cents :calcTotal)
             :subtotal (comp u/double->cents :calcSubtotal)
             :fifo-price (comp u/double->cents :fifoCost)
             :discount (comp u/double->cents :discountAmount)}}])

(def SCHEMA_BY_KEY (u/key-by ::data-key SCHEMA))

(defn make-parser [key]
  (fn [coll]
    (map (fn [e] (u/map-vals #(% e) (get-in SCHEMA_BY_KEY [key ::attrs])))
         (u/vectorize (get coll (get-in SCHEMA_BY_KEY [key ::api-root]))))))

(defn get-schema [key]
  (get SCHEMA_BY_KEY key))

(def table (comp ::table get-schema))

(defn make-persister [data-key]
  (fn [db coll]
    (let [{::keys [table before-persist]} (get-schema data-key)
          records (if before-persist (before-persist db coll) coll)
          new-records (remove (comp (all-ids db table) :id) records)]
      (log/info (format "PERSIST: %s - %s new records | %s upserts"
                        table (count new-records) (count records)))
      (db/upsert! db table records {:entities u/unkeywordize}))))
