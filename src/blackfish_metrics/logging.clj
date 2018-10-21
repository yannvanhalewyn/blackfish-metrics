(ns blackfish-metrics.logging
  (:require [blackfish-metrics.utils :as u]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.string :as str]))

(defn info [& msgs]
  (println
   (format "[%s] INFO %s"
           (f/unparse (f/formatter "YYYY-MM-dd HH:mm:ss" u/TZ)
                      (t/now))
           (str/join " " msgs))))
