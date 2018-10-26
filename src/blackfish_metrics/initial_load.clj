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

(defn- fetch-and-write! [get-fn offset dir]
  (let [{:keys [body]} (get-fn {:offset offset :orderby nil :orderby_desc nil})]
    (json/generate-stream body (io/writer (format "%s%04d%s" dir offset ".json"))
                          {:pretty true})
    body))

(defn- more-records?
  "Based on the response, are there more records to be fetched?"
  [body]
  (let [{:keys [count offset limit]} (get body (keyword "@attributes"))]
    (< (+ (u/parse-int offset) (u/parse-int limit)) (u/parse-int count))))

(defn- download-all-to-json! [dir]
  (ls/refresh-access-token!)
  (doseq [{::schema/keys [api-fetch table]}
          (map schema/get-schema [:data/sales :data/items
                                  :data/sale-lines :data/vendors])]
    (io/make-parents (str dir table "/0.json"))
    (loop [offset 0]
      (let [result (fetch-and-write! api-fetch offset (str dir table "/"))]
        (if (more-records? result)
          (recur (+ offset 100))
          :done)))))

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
