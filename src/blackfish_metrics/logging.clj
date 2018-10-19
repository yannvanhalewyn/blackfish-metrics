(ns blackfish-metrics.logging
  (:require [clj-time.format :as f]
            [clojure.string :as str]))

(defn info [& msgs]
  (println
   (format "[%s] INFO %s"
           (f/unparse (f/formatter "YYYY-MM-dd hh:mm:ss") (clj-time.core/now))
           (str/join " " msgs))))
