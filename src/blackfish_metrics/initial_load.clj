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

(defn insert-manufacturers! [db]
  (let [manufacturers (:Manufacturer (read-json "resources/data/manufacturers.json"))]
    (jdbc/insert-multi!
     db "manufacturers" [:id :name]
     (map (juxt (comp u/parse-int :manufacturerID) :name) manufacturers))))

(defn- gender [{:keys [fullPathName] :as category}]
  (str/lower-case (first (str/split fullPathName #"/" 2))))

(defn insert-categories! [db]
  (let [categories (:Category (read-json "resources/data/categories.json"))]
    (jdbc/insert-multi!
     db "categories" [:id :name :gender]
     (map (juxt (comp u/parse-int :categoryID) :name gender) categories))))

(defn initialize-from-jsons! [db]
  (jdbc/execute! db ["truncate sales, sale_lines, items, manufacturers, categories"])
  (insert-manufacturers! db)
  (insert-categories! db)
  (let [data (read-jsons)]
    (doseq [schema-key [:data/sales :data/items :data/sale-lines]]
      (let [parse (schema/make-parser schema-key)
            persist! (schema/make-persister schema-key)
            coll (parse (get data schema-key))]
        (persist! db coll)))))

(comment

  ;; Download all data to json files
  (download-all-to-json! "resources/data/")

  ;; Read json files and plug them in a database
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (initialize-from-jsons! db))

  )
