(ns blackfish-metrics.core
  (:gen-class)
  (:require [blackfish-metrics.config :refer [env]]
            [blackfish-metrics.scheduler :as schedule]))

(defn -main [& args]
  (schedule/start! (:database-url env)))
