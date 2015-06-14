(ns pitstop.pipeline
  (:require [pitstop.storage.core :as s]
            [pitstop.messaging.core :as m]
            [clojure.core.async :refer (go-loop alt! chan close! <!) :as async]))


(defn- make-send-xform
  "Make an xform that will filter messages that should be sent by
  the messaging instance given. Also send the message and handle
  the returned success channel."
  [{type :type :as inst}]
  (filter
    (fn [{msg :msg mtype :type success :success}]
      (if (= type mtype)
        (let [csend (m/send! {:inst inst :msg msg})]
            (async/pipe csend success) ; This closes success on send success
            false)
        true))))

(defn- make-comp-send-xform
  "Make an xform that composes each messaging's xform"
  [ms]
  (reduce #(comp %1 (make-send-xform %2)) identity ms))

(defn- make-type-xform
  "Make a mapping xform to add :type as default or msg type"
  [default-type]
  (map #(assoc % :type (or (get-in % [:msg :type]) default-type))))

;; TODO errors?
(defn- leftover-chan
  "Create & listen on a channel of leftovers for messages that didn't get sent"
  []
  (let [c (chan)]
    (go-loop [v (<! c)]
      (when-not (nil? v)
        (println "Message with unknown messaging type received!" v)
        (recur (<! c))))
    c))

(defn start-pipeline!
  [storages messagings
   & {:keys [default-type parallelism] :or {parallelism 1}}]
  (let [stopper (chan)
        leftover (leftover-chan)
        default-type (or default-type (-> messagings first :type))]

    (->> storages
         ;; Start all the listeners
         (map #(s/listen-for-msgs! {:stopper stopper :inst %}))
         ;; Merge onto single channel
         async/merge
         ;; Add default type in transducer
         (#(async/pipe % (chan 1 (make-type-xform default-type))))
         ;; Send using parallel pipeline
         (async/pipeline parallelism leftover (make-comp-send-xform messagings)))

    #(close! stopper)))