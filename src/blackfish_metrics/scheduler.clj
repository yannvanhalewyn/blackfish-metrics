(ns blackfish-metrics.scheduler
  (:require [blackfish-metrics.logging :as log]
            [blackfish-metrics.sync :as sync]
            [blackfish-metrics.utils :as u]
            [chime :refer [chime-at]]
            [clj-time.core :as t]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.async :as a]))

(defn- work-hour? [d]
  (<= 7 (t/hour (u/to-tz d )) 22))

(defonce scheduler (atom nil))

(defn start! [db]
  (log/info "Starting scheduler")
  (assert (string? db))
  (if @scheduler
    (log/info "Scheduler already started")
    (let [done-ch (a/chan)]
      (reset! scheduler
              (chime-at (filter work-hour? (periodic-seq (t/now) (t/minutes 2)))
                        (fn [_] (sync/import-missing! db))
                        {:on-finished #(a/put! done-ch :done)}))
      done-ch)))

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
