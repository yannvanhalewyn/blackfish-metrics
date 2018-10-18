(ns blackfish-metrics.import
  (:require [blackfish-metrics.lightspeed :as ls]
            [cheshire.core :as json]
            [clojure.java.io :as io]))

(defn- write-json-file [data filename]
  (json/generate-stream data (io/writer filename)))

(defn fetch-and-write [get-fn offset dir]
  (write-json-file (:body (get-fn {:offset offset}))
                   (str dir offset ".json")))

(comment
  (doseq [i (range 5 31)]
    (fetch-and-write #'ls/get-sales (* 100 i) "resources/data/sales/"))

  )
