(ns pitstop.mongo-test
  (:require [midje.sweet :refer :all]
            [pitstop.core :as p]
            [clj-time.core :as t]
            [monger.collection :as mc]
            [qb.util :as qbu]
            [clojure.core.async :refer (<!! close!) :as async])
  (:import [pitstop.storage.mongo MongoStorage]))

(defonce mongo-cfg {:type :mongo
                :host "localhost"
                :port 27017
                :dbname "pitstop"
                :coll "pitstop"
                :lock-time (t/minutes 1)
                :loop-time (t/seconds 30)})

(defonce mongo-inst (atom nil))

(defn find-one [q]
  (mc/find-one-as-map (:db @mongo-inst) (:coll @mongo-inst) q))

(defn- wait [] (<!! (async/timeout 50)))

(defn after? [t]
  (chatty-checker [t-] (t/after? t t-)))

(facts "about mongo storage"
  (fact "init! gives an instance"
    (if-not @mongo-inst
      (reset! mongo-inst (p/init! mongo-cfg)))
    (instance? MongoStorage @mongo-inst) => true)
  (mc/drop (:db @mongo-inst) (:coll @mongo-inst))
  (fact "store! deferred with id stores correctly"
    (let [when (t/plus (t/now) (t/minutes 15))]
      (p/store! @mongo-inst {:id "abc123" :message true} when)
      (wait)
      (let [msg (find-one {:_id "abc123"})]
        msg => (contains {:_id "abc123" :msg {:id "abc123" :message true} :ready when})
        (fact "lock time is after now"
          (:locked msg) => (after? (t/now))))))
  (fact "store! deferred with same id updates"
    (let [when (t/now)]
      (p/store! @mongo-inst {:id "abc123" :message true} when)
      (wait)
      (find-one {:_id "abc123"}) => (contains {:_id "abc123"
                                               :msg {:id "abc123" :message true}
                                               :ready when})))
  (fact "store! without id creates id and stores"
    (let [when (t/plus (t/now) (t/minutes 20))]
      (p/store! @mongo-inst {:noid true} when)
      (wait)
      (let [id (:_id (find-one {"msg.noid" true}))]
        (fact "new id is uuid"
          id => #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
        (p/remove! @mongo-inst id))
      (wait)
      (fact "remove removed the new message"
        (find-one {"msg.noid" true}) => nil)))

  (facts "about listen"
    (let [{:keys [data stop]} (p/listen @mongo-inst)
          tmout (async/timeout 100)]
      (async/alt!!
        data ([{:keys [msg result]}]
               (fact "message looks right"
                 msg => {:id "abc123" :message true})
               (qbu/success result))
        tmout ([_] (fact "timeout occured" true => false)))
      (close! stop)
      (fact "data chan is closed on stop"
        (<!! data) => nil)
      (fact "message is gone after close of data chan"
        (find-one {:_id "abc123"}) => nil))))