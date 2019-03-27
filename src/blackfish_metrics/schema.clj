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

(defn- sale-method
  "Wether the sale was online or at the store"
  [{:keys [registerID]}]
  (if (= "1" registerID) "Store" "Online"))

(defn- price-type [type sale-line]
  (->> (get-in sale-line [:Prices :ItemPrice])
       (filter (comp #{type} :useType))
       first :amount u/parse-double))

(defn- all-ids [db table]
  (set (map :id (jdbc/query db [(format "select id from %s" table)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema

(defn- subtotal-with-discount [line]
  (- (u/parse-double (:calcSubtotal line)) (u/parse-double (:calcLineDiscount line))))

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
    ::api-fetch (ls/fetcher "Vendor.json" (assoc ls/DEFAULT-QUERY-PARAMS
                                            :orderby "timeStamp"))
    ::attrs {:id (comp u/parse-int :vendorID)
             :name :name}}
   {::table "items"
    ::data-key :data/items
    ::api-root :Item
    ::api-fetch (ls/fetcher "Item.json")
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
    ::api-fetch (ls/fetcher "Sale.json")
    ::attrs {:id (comp u/parse-int :saleID)
             :created-at (comp u/parse-date :createTime)
             :completed (comp u/parse-bool :completed)
             :method sale-method
             :total (comp u/parse-double :total)}}
   {::table "sale_lines"
    ::data-key :data/sale-lines
    ::api-root :SaleLine
    ::api-fetch (ls/fetcher "SaleLine.json")
    ::attrs {:id (comp u/parse-int :saleLineID)
             :sale-id (comp u/zero->nil u/parse-int :saleID)
             :item-id (comp u/zero->nil u/parse-int :itemID)
             :created-at (comp u/parse-date :createTime)
             :qty (comp u/parse-int :unitQuantity)
             :unit-price (comp u/parse-double :unitPrice)
             :total (comp u/parse-double :calcTotal)
             :subtotal subtotal-with-discount
             :fifo-price (comp u/parse-double :fifoCost)
             :discount (comp u/parse-double :calcLineDiscount)}}])

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
    (let [{::keys [table]} (get-schema data-key)
          records (remove (comp (all-ids db table) :id) coll)]
      (log/info (format "PERSIST: %s - %s new records | %s upserts"
                        table (count records) (count coll)))
      (db/upsert! db table coll {:entities u/unkeywordize}))))
