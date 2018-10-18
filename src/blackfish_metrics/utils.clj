(ns blackfish-metrics.utils
  (:require [clojure.string :as str])
  (:import java.sql.Timestamp))

(defn parse-num [s parse-fn]
  (when-let [x (re-find #"-?\d+" (str s))]
    (parse-fn x)))

(defn parse-int
  "Safely attempts to extract an integer from a string. Will return
  the first found integer, without decimals. nil if none found."
  [s]
  (when-let [x (re-find #"-?\d+" (str s))]
    (Integer. x)))

(defn parse-double [s]
  (when-let [x (first (re-find #"-?\d+(\.?\d+)?" (str s)))]
    (Double/parseDouble x)))

(defn parse-date [s]
  (when-let [[_ date time] (re-find #"(.+)T(.+)\+" s)]
    (Timestamp/valueOf (str date " " time))))

(def parse-bool #(= "true" %))

(defn map-vals
  "Applies function f to each item in the map s and returns a map."
  [f coll]
  (persistent!
   (reduce-kv #(assoc! %1 %2 (f %3)) (transient {}) coll)))

(defn unkeywordize
  [kw]
  (str/replace (name kw) #"-" "_"))

(def in-cents #(int (* 100 %)))
(def double->cents (comp in-cents parse-double))
