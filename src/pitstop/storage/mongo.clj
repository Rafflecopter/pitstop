(ns pitstop.storage.mongo
  (:require [pitstop.storage.core :as s]
            [clojure.string :as string]
            [clojure.core.async :refer (go go-loop chan alt! <! close!) :as async]
            [monger (core :as mg)
                    (collection :as mc)
                    joda-time]
            [clj-time.core :as t])
  (:import [org.joda.time DateTimeZone]
           [java.util UUID]))

;; set default time zone that a org.joda.time.DateTime instances
;; will use
(DateTimeZone/setDefault DateTimeZone/UTC)

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

(defn- make-id [] (str (UUID/randomUUID)))

(defn- unlocked-val [lock-time] (t/minus (t/now) lock-time))

(defn- update-ready-time [{:keys [db coll lock-time]} {id :id :as msg} when]
  (let [id (or id (make-id))]
    (mc/update db coll
               {:_id id}
               {:_id id :ready when :msg msg :locked (unlocked-val lock-time)}
               {:upsert true :multi false})))

(defn- remove-msg! [{:keys [db coll]} id]
  (mc/remove-by-id db coll id))

(defn- get-ready-msgs [{:keys [db coll lock-time]}]
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

(defn- interval->rand-ms
  "Converts a joda time interval into a random
  number of milliseconds averaging to that interval"
  [interval]
  (let [ms (t/in-millis interval)]
    (+ (/ ms 2) (rand-int ms))))

(defn- attach-success-chan [inst {id :_id :as msg}]
  (let [c (chan)]
    (go (<! c)
        (remove-msg! inst id))
    (assoc msg :success c)))

(defn- message-xform [inst]
  (comp (map (partial attach-success-chan inst))
        (map #(select-keys % [:msg :success]))))

(defn- listener-loop [{:keys [loop-time] :as inst} stopper]
  (let [c (chan)
        mktimeout #(async/timeout (interval->rand-ms loop-time))]
    (go-loop [tmout (mktimeout)]
      (<! (async/onto-chan c (get-ready-msgs inst) false))
      (alt! stopper ([_] (close! c))
            tmout ([_] (recur (mktimeout)))))
    c))

(defmethod s/init! :mongo
  [{:keys [lock-time loop-time] :as cfg}]
  (assoc (connect cfg)
         :lock-time (or lock-time (t/minutes 15))
         :loop-time (or loop-time (t/minutes 1))))

(defmethod s/listen-for-msgs! :mongo
  [{inst :inst stopper :stopper}]
  (async/pipe (listener-loop inst stopper)
              (chan 1 (message-xform inst))))

(defmethod s/store-msg! :mongo
  [{inst :inst msg :msg when :when}]
  (update-ready-time inst msg when))

(defmethod s/remove-msg! :mongo
  [{inst :inst id :id}]
  (remove-msg! inst id))