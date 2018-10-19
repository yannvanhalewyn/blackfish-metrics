(ns blackfish-metrics.initial-load
  (:require [blackfish-metrics.lightspeed :as ls]
            [blackfish-metrics.schema :as schema]
            [blackfish-metrics.utils :as u]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Lightspeed to json files

(defn- fetch-and-write [get-fn offset dir]
  (json/generate-stream
   (:body (get-fn {:offset offset :orderby nil :orderby_desc nil}))
   (io/writer (str dir offset ".json"))
   {:pretty true}))

(defn- download-all-to-json! [dir]
  (doseq [i (range 31)]
    (fetch-and-write #'ls/get-sales (* 100 i) (str dir "sales/")))

  (doseq [i (range 9)]
    (fetch-and-write #'ls/get-items (* 100 i) (str dir "items/")))

  (doseq [i (range 29)]
    (fetch-and-write #'ls/get-sale-lines (* 100 i) (str dir "sale_lines/"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Stubbing data

(defn- insert-dummy-item! [db]
  (jdbc/insert! db "items"
                {:id 0
                 :created-at (java.sql.Timestamp. 1506816000000)
                 :sku "0"
                 :description "Dummy -- Item was deleted"
                 :msrp 0
                 :online-price 0
                 :default-price 0}
                {:entities u/unkeywordize}))

(defn- insert-dummy-sale! [db]
  (jdbc/insert!
   db "sales"
   {:id 0 :created-at (java.sql.Timestamp. 1506816000000) :completed false :total 0}
   {:entities u/unkeywordize}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Json to psql

(defn read-json [file]
  (json/parse-stream (io/reader file) keyword))

(defn join-json-files [dir key]
  {key (->> (file-seq (io/file dir))
            (filter #(str/ends-with? (.getName %) ".json"))
            (map (comp read-json io/reader))
            (mapcat (comp u/vectorize key)))})

(defn read-jsons []
  {:data/sales (join-json-files "resources/data/sales" :Sale)
   :data/sale-lines (join-json-files "resources/data/sale_lines" :SaleLine)
   :data/items (join-json-files "resources/data/items" :Item)})

(sc.api/defsc 2)

(defn initialize-from-jsons! [db]
  (jdbc/execute! db ["truncate sales, sale_lines, items"])
  (insert-dummy-item! db)
  (insert-dummy-sale! db)
  (let [data (read-jsons)]
    (doseq [key [:data/sales :data/items :data/sale-lines]]
      (let [parse (schema/make-parser key)
            persist! (schema/make-persister key)
            coll (parse (get data key))]
        (sc.api/spy key)
        (println (format "Persisting %s - %s records" (::table (schema/get-schema key)) (count coll)))
        (persist! db coll)))))

(comment

  ;; Download all data to json files
  (download-all-to-json! "resources/data/")

  ;; Read json files and plug them in a database
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (initialize-from-jsons! db))

  )
