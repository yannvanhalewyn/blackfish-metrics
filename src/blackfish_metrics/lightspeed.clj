(ns blackfish-metrics.lightspeed
  (:require [clj-http.client :as http]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

(def config (edn/read-string (slurp (io/resource "secrets.edn"))))
(defonce access-token (atom nil))

(defn- api-uri [endpoint]
  (format "https://api.lightspeedapp.com/API/Account/%s/%s"
          (:ls/account-id config) endpoint))

(defn- request [{:keys [uri query-params]}]
  (http/get uri {:headers {"Authorization" (str "Bearer " @access-token)}
                 :query-params query-params
                 :as :json}))

(defn- refresh-access-token []
  (let [token (get-in
               (http/post "https://cloud.lightspeedapp.com/oauth/access_token.php"
                          {:form-params
                           {:client_id (:ls/client-id config)
                            :client_secret (:ls/client-secret config)
                            :refresh_token (:ls/refresh-token config)
                            "grant_type" "refresh_token"}
                           :as :json})
               [:body :access_token])]
    (when (string? token)
      (reset! access-token token))
    token))

(defn- ordered-query-params [offset]
  {:offset offset
   :orderby "createTime"
   :orderby_desc 1})

(defn get-sales [{:keys [offset]}]
  (println "Fetching sales" offset)
  (request
   {:uri (api-uri "Sale.json")
    :query-params (ordered-query-params offset)}))

(defn get-items [{:keys [offset]}]
  (println "Fetching items" offset)
  (request
   {:uri (api-uri "Item.json")
    :query-params (ordered-query-params offset)}))

(defn get-sale-lines [{:keys [offset]}]
  (println "Fetching sale lines" offset)
  (request
   {:uri (api-uri "SaleLine.json")
    :query-params (ordered-query-params offset)}))
