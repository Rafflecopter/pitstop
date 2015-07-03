(ns pitstop.mongo-test
  (:require [midje.sweet :refer :all]
            [pitstop.core :as p]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [monger.collection :as mc]
            [qb.util :as qbu]
            [clojure.core.async :refer (<!! close!) :as async]
            pitstop.storage.mongo)
  (:import [pitstop.storage.mongo MongoStorage]))

(defonce mongo-cfg {:type :mongo
                :host "localhost"
                :port 27017
                :dbname "pitstop"
                :coll "pitstop"
                :lock-time (t/minutes 1)
                :loop-time (t/seconds 10)})

(defonce mongo-inst (atom nil))

(defn find-one [q]
  (mc/find-one-as-map (:db @mongo-inst) (:coll @mongo-inst) q))

(defn after? [pivot]
  (chatty-checker [testtime] (< (c/to-long pivot) (c/to-long testtime))))
(defn before? [pivot]
  (chatty-checker [testtime] (> (c/to-long pivot) (c/to-long testtime))))

(fact "after? now"
  (t/plus (t/now) (t/hours 1)) => (after? (t/now)))
(fact "after? before"
  (t/now) => (after? (t/minus (t/now) (t/hours 1))))
(fact "before? now"
  (t/now) => (before? (t/plus (t/now) (t/hours 1))))
(fact "before? before"
  (t/minus (t/now) (t/hours 1)) => (before? (t/now)))


(facts "about mongo storage"
  (fact "init! gives an instance"
    (if-not @mongo-inst
      (reset! mongo-inst (p/init! mongo-cfg)))
    (instance? MongoStorage @mongo-inst) => true)
  (mc/drop (:db @mongo-inst) (:coll @mongo-inst))
  (fact "store! deferred with id stores correctly"
    (let [when (t/plus (t/now) (t/minutes 15))]
      (fact "store! completes result chan"
        (<!! (p/store! @mongo-inst {:id "abc123" :message true} when)) => nil)
      (let [msg (find-one {:_id "abc123"})]
        msg => (contains {:_id "abc123" :msg {:id "abc123" :message true} :ready when})
        (fact "lock time is before now"
          (:locked msg) => (before? (t/now))))))
  (fact "store! deferred with same id updates"
    (let [when (t/now)]
      (fact "store! completes result chan"
        (<!! (p/store! @mongo-inst {:id "abc123" :message true} when)) => nil)
      (find-one {:_id "abc123"}) => (contains {:_id "abc123"
                                               :msg {:id "abc123" :message true}
                                               :ready when})))
  (fact "store! without id creates id and stores"
    (let [when (t/plus (t/now) (t/minutes 20))]
      (fact "store! completes result chan"
        (<!! (p/store! @mongo-inst {:noid true} when)) => nil)
      (let [id (:_id (find-one {"msg.noid" true}))]
        (fact "new id is uuid"
          id => #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
        (p/remove! @mongo-inst id))
      (fact "remove removed the new message"
        (find-one {"msg.noid" true}) => nil)))

  (let [start (t/plus (t/now) (t/seconds 10))
        end (t/plus (t/now) (t/days 1))
        every (t/minutes 20)]
    (fact "store! a recurring message"
      (fact "store! completes result chan"
        (<!! (p/store! @mongo-inst {:id "recur1"} start end every)) => nil)
      (let [msg (find-one {:_id "recur1"})]
        msg => (contains {:_id "recur1" :msg {:id "recur1"}
                          :ready start :expire end :recur (t/in-millis every)})))

    (fact "store! update recurring message"
      (let [nstart (t/now)]
        (fact "store! completes result chan"
          (<!! (p/store! @mongo-inst {:id "recur1"} nstart end every)) => nil)
        (let [msg (find-one {:_id "recur1"})]
          msg => (contains {:_id "recur1" :ready nstart})))))

  (facts "about listen"
    (let [{:keys [data stop]} (p/listen @mongo-inst)
          tmout (async/timeout 100)]
      (async/alt!!
        data ([{:keys [msg result]}]
               (fact "deferred message looks right"
                 msg => {:id "abc123" :message true})
               (qbu/success result))
        tmout ([_] (fact "timeout occured" true => false)))
      (async/alt!!
        data ([{:keys [msg result]}]
               (fact "recurring message looks right"
                 msg => {:id "recur1"})
               (qbu/success result))
        tmout ([_] (fact "timeout occured" true => false)))
      (close! stop)
      (fact "data chan is closed on stop"
        (<!! data) => nil)
      (fact "message is gone after close of data chan"
        (find-one {:_id "abc123"}) => nil)
      (fact "recurring message is still there"
        (let [msg (find-one {:_id "recur1"})]
          msg => (contains {:_id "recur1" :msg {:id "recur1"}})
          (:ready msg) => (after? (t/now)))))))

;; TODO recurring
;; TODO attaching errors