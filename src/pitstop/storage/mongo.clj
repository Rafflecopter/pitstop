(ns pitstop.storage.mongo
  (:require [pitstop.core :as p]
            [clojure.string :as string]
            [clojure.core.async :refer (go go-loop chan alt! <! close!) :as async]
            [monger (collection :as mc)
                    (connect :as mconn)
                    joda-time]
            [clj-time.core :as t]
            [qb.util :refer (wrap-ack-chan-xf ack-blocking-op*)])
  (:import [org.joda.time DateTimeZone]
           [java.util UUID]))

;; set default time zone that a org.joda.time.DateTime instances
;; will use
(DateTimeZone/setDefault DateTimeZone/UTC)

;; Connection stuff

(defn- ensure-indices! [db coll]
  (mc/ensure-index db coll (array-map :ready -1 :locked -1)))

;; Util

(defn- make-id [] (str (UUID/randomUUID)))

(defn- interval->rand-ms
  "Converts a joda time interval into a random
  number of milliseconds averaging to that interval"
  [interval]
  (let [ms (t/in-millis interval)]
    (+ (/ ms 2) (rand-int ms))))

(def ^:private unlocked-val (t/minus (t/now) (t/hours 1)))
(def ^:private neverending-val nil)

;; Wrap messages

(defn- wrap-deferred-msg [{id :id :as msg} when]
  {:_id (or id (make-id))
   :ready when
   :msg msg
   :locked unlocked-val})

(defn- wrap-recurring-msg [{id :id :as msg} every starting ending]
  {:_id (or id (make-id))
   :ready starting
   :recur (t/in-millis every)
   :msg msg
   :locked unlocked-val
   :expire ending})

;; DB Operations

(defn- update-msg! [{:keys [db coll]} {:keys [_id] :as full-msg}]
  (ack-blocking-op*
    (if (= 0 (-> (mc/update-by-id db coll _id full-msg {:upsert true}) .getN))
        (throw (Exception. "update failed to upsert")))))

(defn- remove-msg! [{:keys [db coll]} id]
  (ack-blocking-op*
    (mc/remove-by-id db coll id)))

(defn- update-ready! [{:keys [db coll]} id new-ready]
  (ack-blocking-op*
    (mc/update db coll {:_id id} {"$set" {:ready new-ready
                                          :locked unlocked-val}})))

(defn- get-and-lock-ready-msgs! [{:keys [db coll lock-time]}]
  (let [locked-by (make-id)
        mres (mc/update db coll
                        {:locked {"$lte" (t/now)}
                         :ready {"$lte" (t/now)}}
                        {"$set" {:locked (t/plus (t/now) lock-time)
                                 :locked-by locked-by}}
                        {:upsert false :multi true})]
    (if (= 0 (.getN mres))
        []
        (mc/find-maps db coll {:locked-by locked-by}))))

;; Higher level stuff

(defn- unexpired-msg-check [inst {:keys [_id expire]}]
  (if (and expire (t/after? (t/now) expire))
      (do (go (remove-msg! inst _id))
          false)
      true))

(defn- on-ack-success [inst {:keys [_id recur ready expire]}]
  (let [new-ready (if recur (t/plus ready (t/millis recur)))]
    (if (and recur (t/after? expire new-ready))
        (update-ready! inst _id new-ready)
        (remove-msg! inst _id))))

(defn- on-ack-error [inst {:keys [_id] :as msg} error]
  (update-msg! inst (assoc msg :last-error error)))

(defn- message-xform [inst]
  (comp (filter (partial unexpired-msg-check inst))
        (wrap-ack-chan-xf (partial on-ack-success inst)
                          (partial on-ack-error inst))
        (map #(assoc (:msg %) :ack (:ack %)))
        (map #(select-keys % [:msg :ack]))))

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
  [{:keys [lock-time loop-time coll] :as cfg}]
  (mconn/wmong cfg
    (ensure-indices! mconn/db coll)
    (MongoStorage. mconn/db coll
                   (or loop-time (t/minutes 1))
                   (or lock-time (t/minutes 15)))))