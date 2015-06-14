(ns pitstop.core
  (:require [pitstop.storage (core :as s) mongo]
            [pitstop.messaging (core :as m)]
            [pitstop.pipeline :as pipeline]
            [clojure.core.async :refer (go-loop alt! chan close! <! pipe)]
            [clj-time.core :as t])
  (:import [clojure.lang IFn]
           [java.util Map]
           [org.joda.time DateTime]))


(defn init-storage!
  "Initialize a storage config into a storage instance"
  ^Map [^Map storage-cfg]
  (assoc (s/init! storage-cfg)
         :type (:type storage-cfg)))

(defn init-messaging!
  "Initialize a messaging config into a messaging instance"
  ^Map [^Map messaging-cfg]
  (assoc (m/init! messaging-cfg)
         :type (:type messaging-cfg)))


(defn start-pipeline!
  "Starts a listener to monitor storages
   for ready to send messages. Then send those
   to messaging instances.
   Each message may have a {:type type} determining its
   messaging instance to send with. If a message does not
   have a type, the default type will be set, defaulting to the first
   messaging instance in the list.
   Returns a function of no arguments to stop the pipeline.
   Options: default-type and parallelism"
  ^IFn [storage-insts messaging-insts
        & {:keys [default-type parallelism] :as opts}]
  {:pre [(not (empty? storage-insts))
         (not (empty? messaging-insts))]}
  (apply pipeline/start-pipeline! storage-insts messaging-insts opts))

(defn defer-msg!
  "Defer a message to a time when"
  [^Map storage-inst ^Map msg ^DateTime when]
  {:pre [(t/after? when (t/now))]}
  (s/store-deferred-msg! {:msg msg :when when :inst storage-inst}))