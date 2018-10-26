(ns blackfish-metrics.schema
  (:require [blackfish-metrics.db :as db]
            [blackfish-metrics.lightspeed :as ls]
            [blackfish-metrics.logging :as log]
            [blackfish-metrics.utils :as u]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers

(defn- category-gender [{:keys [fullPathName] :as category}]
  (str/lower-case (first (str/split fullPathName #"/" 2))))

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
  [{::table "manufacturers"
    ::data-key :data/manufacturers
    ::api-root :Manufacturer
    ::api-fetch (ls/fetcher "Manufacturer.json")
    ::attrs {:id (comp u/parse-int :manufacturerID)
             :name :name}}
   {::table "categories"
    ::data-key :data/categories
    ::api-root :Category
    ::api-fetch (ls/fetcher "Category.json")
    ::attrs {:id (comp u/parse-int :categoryID)
             :name :name
             :gender category-gender}}
   {::table "vendors"
    ::data-key :data/vendors
    ::api-root :Vendor
    ::api-fetch (ls/fetcher "Vendor.json")
    ::attrs {:id (comp u/parse-int :vendorID)
             :name :name}}
   {::table "items"
    ::data-key :data/items
    ::api-root :Item
    ::api-fetch #'ls/get-items
    ::attrs {:id (comp u/parse-int :itemID)
             :created-at (comp u/parse-date :createTime)
             :sku :systemSku
             :manufacturer-id (comp u/zero->nil u/parse-int :manufacturerID)
             :vendor-id (comp u/zero->nil u/parse-int :defaultVendorID)
             :category-id (comp u/zero->nil u/parse-int :categoryID)
             :description :description
             :msrp (partial price-type "MSRP")
             :online-price (partial price-type "Online")
             :default-price (partial price-type "Default")
             :archived (comp u/parse-bool :archived)}}
   {::table "sales"
    ::data-key :data/sales
    ::api-root :Sale
    ::api-fetch #'ls/get-sales
    ::attrs {:id (comp u/parse-int :saleID)
             :created-at (comp u/parse-date :createTime)
             :completed (comp u/parse-bool :completed)
             :total (comp u/double->cents :total)}}
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
(def ALL_KEYS (map ::data-key SCHEMA))

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
