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
          (map schema/get-schema [:data/manufacturers :data/categories :data/vendors
                                  :data/sales :data/items :data/sale-lines])]
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

(defn initialize-from-jsons! [db]
  (jdbc/execute! db [(str "truncate " (str/join ", " (map ::schema/table schema/SCHEMA)))])
  (doseq [{::schema/keys [table data-key api-root]} schema/SCHEMA]
    (let [parse (schema/make-parser data-key)
          persist! (schema/make-persister data-key)
          data (parse (join-json-files (str "resources/data/" table) api-root))]
      (persist! db data))))

(comment

  ;; Download all data to json files
  (download-all-to-json! "resources/data/")

  ;; Read json files and plug them in a database
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (initialize-from-jsons! db))

  )
