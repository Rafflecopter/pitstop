(ns pitstop.storage.mongo
  (:require [pitstop.core :as p]
            [clojure.string :as string]
            [clojure.core.async :refer (go go-loop chan alt! <! close!) :as async]
            [monger (core :as mg)
                    (collection :as mc)
                    joda-time]
            [clj-time.core :as t]
            [qb.util :as qbu])
  (:import [org.joda.time DateTimeZone]
           [java.util UUID]))

;; set default time zone that a org.joda.time.DateTime instances
;; will use
(DateTimeZone/setDefault DateTimeZone/UTC)

;; Connection stuff

(defn- ensure-indices! [{:keys [db coll]}]
  (mc/ensure-index db coll (array-map :ready -1 :locked -1)))

(defn- connect [{:keys [host port hosts options dbname user pass coll]}]
  (let [addrs (->> (or hosts [(str host ":" port)])
                   (map #(string/split % #":"))
                   (map #(mg/server-address (nth % 0) (-> % (nth 1) Integer/parseInt))))
        options (mg/mongo-options (merge {:w 1} options))
        conn (mg/connect addrs options)
        dbref (mg/get-db conn dbname)]
    (if (and user pass)
        (mg/authenticate dbref user (.toCharArray pass)))
    (doto {:db dbref :coll coll}
      (ensure-indices!))))

;; Util

(defn- make-id [] (str (UUID/randomUUID)))

(defn- resulting-operation [op]
  (let [rc (qbu/result-chan)]
    (go (try (if-let [res (op)]
                     (qbu/error rc res)
                     (qbu/success rc))
          (catch Exception e (qbu/error rc {:error (.getMessage e)}))))
    rc))

(defn- interval->rand-ms
  "Converts a joda time interval into a random
  number of milliseconds averaging to that interval"
  [interval]
  (let [ms (t/in-millis interval)]
    (+ (/ ms 2) (rand-int ms))))

(def ^:private unlocked-val (t/minus (t/now) (t/hours 1)))
(def ^:private neverending-val nil)

(def interval->ms t/in-millis)
(def ms->interval t/millis)

;; Wrap messages

(defn- wrap-deferred-msg [{id :id :as msg} when]
  {:_id (or id (make-id))
   :ready when
   :msg msg
   :locked unlocked-val})

(defn- wrap-recurring-msg [{id :id :as msg} every starting ending]
  {:_id (or id (make-id))
   :ready starting
   :recur (interval->ms every)
   :msg msg
   :locked unlocked-val
   :expire ending})

;; DB Operations

(defn- update-msg! [{:keys [db coll]} {:keys [_id] :as full-msg}]
  (resulting-operation
    #(if (= 0 (-> (mc/update-by-id db coll _id full-msg {:upsert true}) .getN))
         {:error "update failed to upsert"})))

(defn- remove-msg! [{:keys [db coll]} id]
  (resulting-operation
    #(do (mc/remove-by-id db coll id) nil)))

(defn- re-recur-msg! [{:keys [db coll]} id intv]
  (resulting-operation
    #(do (mc/update db coll {:_id id} {"$set" {:ready (t/plus (t/now) intv)
                                               :locked unlocked-val}})
         nil)))

(defn- get-and-lock-ready-msgs! [{:keys [db coll lock-time]}]
  (let [locked-by (make-id)
        result (mc/update db coll
                          {:locked {"$lte" (t/now)}
                           :ready {"$lte" (t/now)}}
                          {"$set" {:locked (t/plus (t/now) lock-time)
                                   :locked-by locked-by}}
                          {:upsert false :multi true})]
    (if (= 0 (.getN result))
        []
        (mc/find-maps db coll {:locked-by locked-by}))))

;; Higher level stuff

(defn- unexpired-msg-check [inst {:keys [_id expire]}]
  (if (and expire (t/after? (t/now) expire))
      (do (go (remove-msg! inst _id))
          false)
      true))

(defn- on-result-success [inst {:keys [_id recur]}]
  (if recur (re-recur-msg! inst _id (ms->interval recur))
            (remove-msg! inst _id)))

(defn- on-result-error [inst {:keys [_id] :as msg} error]
  (update-msg! inst (assoc msg :last-error error)))

(defn- message-xform [inst]
  (comp (filter (partial unexpired-msg-check inst))
        (qbu/wrap-result-chan-xf (partial on-result-success inst)
                                 (partial on-result-error inst))
        (map #(assoc (:msg %) :result (:result %)))
        (map #(select-keys % [:msg :result]))))

(defn- listener-loop [{:keys [loop-time] :as inst} stopper]
  (let [c (chan)
        mktimeout #(async/timeout (interval->rand-ms loop-time))]
    (go-loop [tmout (mktimeout)]
      (<! (async/onto-chan c (get-and-lock-ready-msgs! inst) false))
      (alt! stopper ([_] (close! c))
            tmout ([_] (recur (mktimeout)))))
    c))

(defn- listen [inst]
  (let [stopper (chan)]
      {:stop stopper
       :data (async/pipe (listener-loop inst stopper)
                         (chan 1 (message-xform inst)))}))

;; Storage implementation

(defrecord MongoStorage [db coll loop-time lock-time]
  p/Storage
  (listen [inst] (listen inst))
  (store! [inst msg when]
    (update-msg! inst (wrap-deferred-msg msg when)))
  (store! [inst msg start end every]
    (update-msg! inst (wrap-recurring-msg msg every start end)))
  (remove! [inst id] (remove-msg! inst id)))

(defmethod p/init! :mongo
  [{:keys [lock-time loop-time] :as cfg}]
  (let [{:keys [db coll]} (connect cfg)]
    (MongoStorage. db coll
                   (or loop-time (t/minutes 1))
                   (or lock-time (t/minutes 15)))))