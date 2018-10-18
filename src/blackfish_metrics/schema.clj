(ns blackfish-metrics.schema
  (:require [blackfish-metrics.lightspeed :as ls]
            [blackfish-metrics.utils :as u]
            [clojure.java.jdbc :as jdbc]))

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
             :description :description
             :msrp (partial price-type "MSRP")
             :online_price (partial price-type "Online")
             :default_price (partial price-type "Default")}}
   {::table "sale_lines"
    ::data-key :data/sale-lines
    ::api-root :SaleLine
    ::api-fetch #'ls/get-sale-lines
    :before-fn #'stub-missing-relations
    ::attrs {:id (comp u/parse-int :saleLineID)
             :sale-id (comp u/parse-int :saleID)
             :item-id (comp u/parse-int :itemID)
             :created-at (comp u/parse-date :createTime)
             :unit-qty (comp u/parse-int :unitQuantity)
             :unit-price (comp u/double->cents :unitPrice)
             :price (comp u/double->cents :calcTotal)
             :discount (comp u/double->cents :discountAmount)}}])

(def SCHEMA_BY_KEY (u/key-by ::data-key SCHEMA))

(defn- price-type [type sale-line]
  (->> (get-in sale-line [:Prices :ItemPrice])
       (filter (comp #{type} :useType))
       first :amount u/double->cents))

(defn- all-ids [db table]
  (set (map :id (jdbc/query db [(format "select id from %s" table)]))))

(defn latest-id [db table]
  (:id (first (jdbc/query db [(format "select id from %s order by id desc limit 1" table)]))))

(defn- stub-missing-relations [db sale-lines]
  (let [item-ids (all-ids db "items")
        sale-ids (all-ids db "sales")]
    (map #(assoc %
            :item-id (get item-ids (:item-id %) 0)
            :sale-id (get sale-ids (:sale-id %) 0))
         sale-lines)))

(defn make-parser [key]
  (fn [coll]
    (map (fn [e] (u/map-vals #(% e) (get-in SCHEMA_BY_KEY [key ::attrs])))
         (get coll (get-in SCHEMA_BY_KEY [key ::api-root])))))

(defn get-schema [key]
  (get SCHEMA_BY_KEY key))

(defn make-persister [key]
  (fn [db coll]
    (let [table (::table (get-schema key))
          new-records (remove (comp (all-ids db table) :id) coll)]
      (println (format "Persisting %s new records into %s" (count new-records) table))
      (jdbc/insert-multi! db (table-name key) new-records
                          {:entities u/unkeywordize}))))
