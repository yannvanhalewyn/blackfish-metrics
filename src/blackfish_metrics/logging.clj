(ns blackfish-metrics.logging
  (:require [clj-time.format :as f]
            [clj-time.core :as t]
            [clojure.string :as str]))

(defn info [& msgs]
  (println
   (format "[%s] INFO %s"
           (f/unparse (f/formatter "YYYY-MM-dd HH:mm:ss" (t/time-zone-for-id "Europe/Amsterdam"))
                      (t/now))
           (str/join " " msgs))))
