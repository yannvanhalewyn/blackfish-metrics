(ns blackfish-metrics.config
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn- keywordize [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "." "-")
      (keyword)))

(defn- read-system-env []
  (->> (System/getenv)
       (map (fn [[k v]] [(keywordize k) v]))
       (into {})))

(defn- read-env-file [f]
  (if-let [env-file (io/file f)]
    (if (.exists env-file)
      (edn/read-string (slurp env-file)))))

(def env
  (merge
   (read-env-file (io/resource "secrets.edn"))
   (read-system-env)))
