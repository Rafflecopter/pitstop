(ns pitstop.storage.mongo-test
  (:require [clojure.test :refer :all]
            [pitstop.core :as pitstop]
            [pitstop.core-test :refer (calls)]
            [clj-time.core :as t]
            [monger.collection :as mc]
            [clojure.core.async :refer (<!!) :as async]))

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

(deftest mongo-storage-test
  (testing "init!"
    (if-not @mongo-inst
      (reset! mongo-inst (pitstop/init-storage! mongo-cfg)))
    (is (= (select-keys @mongo-inst [:type :lock-time :loop-time])
           {:type :mongo :lock-time (:lock-time mongo-cfg) :loop-time (:loop-time mongo-cfg)})))
  (mc/drop (:db @mongo-inst) (:coll @mongo-inst))
  (testing "defer-msg! new with id"
    (let [when (t/plus (t/now) (t/minutes 15))]
      (pitstop/defer-msg! @mongo-inst {:id "abc123" :message true} when)
      (<!! (async/timeout 50))
      (let [msg (find-one {:_id "abc123"})]
        (is (= {:_id "abc123" :msg {:id "abc123" :message true} :ready when}
               (dissoc msg :locked)))
        (is (t/after? (t/now) (:locked msg))))))
  (testing "defer-msg! update with id"
    (let [when (t/now)]
      (pitstop/defer-msg! @mongo-inst {:id "abc123" :message true} when)
      (<!! (async/timeout 50))
      (is (= {:_id "abc123" :msg {:id "abc123" :message true} :ready when}
             (dissoc (find-one {:_id "abc123"}) :locked)))))
  (testing "defer-msg! without id and remove-msg!"
    (let [when (t/plus (t/now) (t/minutes 20))]
      (pitstop/defer-msg! @mongo-inst {:noid true} when)
      (<!! (async/timeout 50))
      (let [id (:_id (find-one {"msg.noid" true}))]
        (is (re-find #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" id))
        (pitstop/remove-msg! @mongo-inst id))
      (<!! (async/timeout 50))
      (is (nil? (find-one {"msg.noid" true})))))
  (testing "pipeline"
    (let [stop-pipeline! (pitstop/start-pipeline! [@mongo-inst] [{:type :test}])]
      (<!! (async/timeout 1000))
      (stop-pipeline!)
      (<!! (async/timeout 50))
      (is (= {:messaging {:send! `({:inst {:type :test} :msg {:id "abc123" :message true}})}}
             @calls))
      (is (nil? (find-one {:_id "abc123"}))))))