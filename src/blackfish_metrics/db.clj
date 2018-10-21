(ns blackfish-metrics.db
  (:require [blackfish-metrics.utils :as u]
            [clj-time.coerce :refer [to-sql-time]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]))

(extend-protocol jdbc/ISQLValue
  org.joda.time.DateTime
  (sql-value [value] (to-sql-time value)))

(defn- upsert-sql [table columns values entities]
  (let [nc (count columns)
        vcs (map count values)]
    (if-not (and (or (zero? nc) (= nc (first vcs))) (apply = vcs))
      (throw (IllegalArgumentException. "insert called with inconsistent number of columns / values"))
      (into [(str "INSERT INTO " table
                  (when (seq columns)
                    (str " ( " (str/join ", " (map entities columns)) " )"))
                  " VALUES ( "
                  (str/join ", " (repeat (first vcs) "?"))
                  " ) "
                  "ON CONFLICT (id) DO UPDATE SET "
                  (str/join
                   ", "
                   (map #(format "%s = EXCLUDED.%s" (entities %) (entities %))
                        (disj (set columns) :id))))]
            values))))

(defn upsert! [db table coll {:keys [entities]}]
  (jdbc/db-do-prepared db (upsert-sql table (keys (first coll)) (map vals coll) entities)
                       {:multi? true}))
