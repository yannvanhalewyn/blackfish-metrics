(ns blackfish-metrics.scheduler
  (:require [chime :refer [chime-at]]
            [clj-time.core :as t]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.async :as a :refer [<! go-loop]]
            [blackfish-metrics.logging :as log]
            [blackfish-metrics.lightspeed :as ls]))

(defn- beginning-of-minute [d]
  (t/date-time (t/year d) (t/month d) (t/day d)
               (t/hour d) (t/minute d) 0))

(defn- every-two-minutes-during-workhours []
  (filter #(<= 5 (t/hour %) 23) ;; Actually UTC :(
          (periodic-seq (beginning-of-minute (t/now)) (t/minutes 2))))

(defonce scheduler (atom nil))

(defn start! []
  (log/info "Starting scheduler")
  (if @scheduler
    (log/info "Scheduler already started")
    (reset! scheduler
            (chime-at (every-two-minutes-during-workhours)
                      log/info))))

(defn stop! []
  (log/info "Stopping scheduler")
  (if @scheduler
    (do (@scheduler) (reset! scheduler nil))
    (log/info "No scheduler active")))

(comment
  (start!)
  (stop!)
  )
