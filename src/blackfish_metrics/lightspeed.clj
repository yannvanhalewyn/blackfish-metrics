(ns blackfish-metrics.lightspeed
  (:require [blackfish-metrics.config :refer [env]]
            [blackfish-metrics.logging :as log]
            [blackfish-metrics.utils :as u]
            [clj-http.client :as http]
            [clojure.string :as str]))

(defonce access-token (atom nil))

(defn- api-uri [endpoint]
  (let [account-id (u/parse-int (:ls-account-id env))]
    (assert (nat-int? account-id))
    (format "https://api.lightspeedapp.com/API/Account/%s/%s"
            account-id endpoint)))

(defn- request [{:keys [uri query-params]}]
  (http/get uri {:headers {"Authorization" (str "Bearer " @access-token)}
                 :query-params query-params
                 :as :json}))

(defn refresh-access-token! []
  (log/info "FETCH: Refreshing access token")
  (assert (every? string? (map env [:ls-client-id :ls-client-secret :ls-refresh-token])))
  (let [token (get-in
               (http/post "https://cloud.lightspeedapp.com/oauth/access_token.php"
                          {:form-params
                           {:client_id (:ls-client-id env)
                            :client_secret (:ls-client-secret env)
                            :refresh_token (:ls-refresh-token env)
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

(defn fetcher [endpoint]
  (fn [params]
    (log/info "FETCH:" endpoint (:offset params))
    (request
     {:uri (api-uri endpoint)
      :query-params (merge DEFAULT-QUERY-PARAMS params)})))
