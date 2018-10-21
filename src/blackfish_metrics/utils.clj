(ns blackfish-metrics.utils
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.string :as str]))

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
  (when-let [[_ x] (re-find #"(.+)\+" s)]
    (t/to-time-zone
     (f/parse (:date-hour-minute-second f/formatters) x)
     (t/time-zone-for-id "Europe/Amsterdam"))))

(def parse-bool #(= "true" %))

(defn map-vals
  "Applies function f to each item in the map s and returns a map."
  [f coll]
  (persistent!
   (reduce-kv #(assoc! %1 %2 (f %3)) (transient {}) coll)))

(defn key-by
  "Returns a map of the elements of coll keyed by the value of
  applying f to each element in coll.

  @example
  (def users [{:name \"John\" :id 2} {:name \"Jeff\" :id 3}])
  (key-by :id users) ;; => {2 {:name \"John\" :id 2} 3 {:name \"Jeff\" :id 3}}"
  [f coll]
  (into {} (for [r coll] [(f r) r])))

(defn unkeywordize
  [kw]
  (str/replace (name kw) #"-" "_"))

(defn vectorize [x]
  (if (map? x) (vector x) x))

(defn zero->nil [x]
  (if (= 0 x) nil x))

(def in-cents #(int (* 100 %)))
(def double->cents (comp in-cents parse-double))
