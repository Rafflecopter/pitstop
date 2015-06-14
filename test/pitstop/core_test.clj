(ns pitstop.core-test
  (:require [clojure.test :refer :all]
            [pitstop.core :as pitstop]
            [pitstop.messaging.core :as m]
            [pitstop.storage.core :as s]
            [clj-time.core :as t]
            [clojure.core.async :refer (go chan <! >! <!! >!! close!) :as async])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(def calls (atom nil))
(def test-msg (atom {:type :test :message true}))
(defmethod m/init! :test [obj]
  (swap! calls update-in [:messaging :init!] conj obj)
  {:instance true :messaging true})
(defmethod m/send! :test [obj]
  (swap! calls update-in [:messaging :send!] conj obj)
  (doto (chan) close!))
(defmethod s/init! :test [obj]
  (swap! calls update-in [:storage :init!] conj obj)
  {:instance true :storage true})
(defmethod s/listen-for-msgs! :test [{stopper :stopper :as obj}]
  (swap! calls update-in [:storage :listen-for-msgs!] conj obj)
  (let [c (chan 1)
        succ (chan)]
    (>!! c {:msg @test-msg :success succ})
    (go (<! stopper) (close! c) (swap! calls update-in [:storage :->stopper] conj true))
    (go (<! succ) (swap! calls update-in [:storage :->success] conj true))
    c))
(defmethod s/store-msg! :test [obj]
  (swap! calls update-in [:storage :store-msg!] conj obj))
(defmethod s/remove-msg! :test [obj]
  (swap! calls update-in [:storage :remove-msg!] conj obj))

(deftest test-pipeline-test
  (testing "init-messaging!"
    (is (= {:type :test :instance true :messaging true}
           (pitstop/init-messaging! {:config true :type :test})))
    (is (= {:messaging {:init! '({:config true :type :test})}}
           @calls))
    (reset! calls nil))
  (testing "init-storage!"
    (is (= {:type :test :instance true :storage true}
           (pitstop/init-storage! {:config true :type :test})))
    (is (= {:storage {:init! '({:config true :type :test})}}
           @calls))
    (reset! calls nil))
  (testing "defer-msg!"
    (let [storage-inst {:type :test :instance true :storage true}
          when (t/plus (t/now) (t/hours 1))]
      (pitstop/defer-msg! storage-inst {:message true} when)
      (is (= {:storage {:store-msg! `({:inst ~storage-inst :msg {:message true} :when ~when})}}
             @calls)))
    (reset! calls nil))
  (testing "remove-msg!"
    (let [storage-inst {:type :test :instance true :storage true}
          when (t/plus (t/now) (t/hours 1))]
      (pitstop/remove-msg! storage-inst "yoyo")
      (is (= {:storage {:remove-msg! `({:inst ~storage-inst :id "yoyo"})}}
             @calls)))
    (reset! calls nil))
  (testing "start-pipeline!"
    (let [storage-inst {:type :test :instance true :storage true}
          messaging-inst {:type :test :instance true :messaging true}
          stop-pipeline! (pitstop/start-pipeline! [storage-inst] [messaging-inst])]
      (stop-pipeline!)
      (<!! (async/timeout 50))
      (is (= {:storage {:listen-for-msgs! `({:inst ~storage-inst
                                             :stopper ~(-> @calls :storage :listen-for-msgs! first :stopper)})
                        :->success '(true)
                        :->stopper '(true)}
              :messaging {:send! `({:inst ~messaging-inst :msg ~(deref test-msg)})}}
             @calls)))
    (reset! calls nil)))