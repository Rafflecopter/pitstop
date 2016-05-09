(ns pitstop.core
  (:require [pitstop.storage.mongo :as pitmong]
            [pitstop.protocols :as proto]))

(defmulti init!
  "Initialize using a config object.
  Return an instance of Storage"
  :type)

(def listen proto/listen)

(def store! proto/store!)

(def remove! proto/remove!)

;; mongo init function
(defmethod init! :mongo
  [{:keys [;; mongodb host i.e. "localhost"
           host

           ;; mongodb port [int] i.e. 27017
           port

           ;; database name i.e. "pitstop"
           dbname

           ;; collection name i.e. "pitstop-jobs"
           coll

           ;; joda time interval how long jobs should be locked for processing
           ;; defaults to 15 minutes
           lock-time

           ;; joda time interval how frequently we should poll mongo for new jobs
           ;; defaults to 1 minute
           ;; note: this is just an average. random noise is applied to prevent
           ;; many processes from polling mongo at the exact same time
           loop-time] :as cfg}]
  (pitmong/init! cfg))
