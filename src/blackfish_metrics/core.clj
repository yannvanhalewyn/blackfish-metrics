(ns blackfish-metrics.core
  (:gen-class)
  (:require [blackfish-metrics.config :refer [env]]
            [blackfish-metrics.scheduler :as schedule]
            [clojure.core.async :as a]))

(defn -main [& args]
  (a/<!! (schedule/start! (:database-url env))))
