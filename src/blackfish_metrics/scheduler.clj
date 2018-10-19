(ns blackfish-metrics.scheduler
  (:require [blackfish-metrics.logging :as log]
            [blackfish-metrics.sync :as sync]
            [chime :refer [chime-at]]
            [clj-time.core :as t]
            [clj-time.periodic :refer [periodic-seq]]))

(defn- work-hour? [d] ;; Sadly utc
  (<= 5 (t/hour d) 23))

(defonce scheduler (atom nil))

(defn start! [db]
  (log/info "Starting scheduler")
  (if @scheduler
    (log/info "Scheduler already started")
    (reset! scheduler
            (chime-at (filter work-hour? (periodic-seq (t/now) (t/minutes 2)))
                      (fn [_] (sync/import-missing! db))))))

(defn stop! []
  (log/info "Stopping scheduler")
  (if @scheduler
    (do (@scheduler) (reset! scheduler nil))
    (log/info "No scheduler active")))

(comment
  (let [db "postgresql://localhost:5432/blackfish_metrics"]
    (start! db))

  (stop!)
  )
