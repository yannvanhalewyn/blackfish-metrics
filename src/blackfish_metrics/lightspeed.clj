(ns blackfish-metrics.lightspeed
  (:require [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [blackfish-metrics.logging :as log]))

(def config (edn/read-string (slurp (io/resource "secrets.edn"))))
(defonce access-token (atom nil))

(defn- api-uri [endpoint]
  (format "https://api.lightspeedapp.com/API/Account/%s/%s"
          (:ls/account-id config) endpoint))

(defn- request [{:keys [uri query-params]}]
  (http/get uri {:headers {"Authorization" (str "Bearer " @access-token)}
                 :query-params query-params
                 :as :json}))

(defn refresh-access-token! []
  (log/info "FETCH: Refreshing access token")
  (let [token (get-in
               (http/post "https://cloud.lightspeedapp.com/oauth/access_token.php"
                          {:form-params
                           {:client_id (:ls/client-id config)
                            :client_secret (:ls/client-secret config)
                            :refresh_token (:ls/refresh-token config)
                            "grant_type" "refresh_token"}
                           :as :json})
               [:body :access_token])]
    (when (and (string? token) (not= token @access-token))
      (log/info "  New access token received")
      (reset! access-token token))
    token))

(def DEFAULT-QUERY-PARAMS
  {:offset 0
   :orderby "createTime"
   :archived true
   :orderby_desc 1})

(defn get-sales [params]
  (log/info "FETCH: sales" (:offset params))
  (request
   {:uri (api-uri "Sale.json")
    :query-params (merge DEFAULT-QUERY-PARAMS params)}))

(defn get-items [params]
  (log/info "FETCH: items" (:offset params))
  (request
   {:uri (api-uri "Item.json")
    :query-params (merge DEFAULT-QUERY-PARAMS params)}))

(defn get-sale-lines [params]
  (log/info "FETCH: sale lines" (:offset params))
  (request
   {:uri (api-uri "SaleLine.json")
    :query-params (merge DEFAULT-QUERY-PARAMS params)}))
