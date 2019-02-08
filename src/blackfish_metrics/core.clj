(ns blackfish-metrics.core
  (:gen-class)
  (:require [blackfish-metrics.config :refer [env]]
            [blackfish-metrics.scheduler :as schedule]
            [clojure.core.async :as a]))

(defn -main [& args]
  (println "Use subtotals with subtracted discount")
  (a/<!! (schedule/start! (:database-url env))))
