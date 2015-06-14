(defproject pitstop "0.1.0-SNAPSHOT"
  :description "Meessage-deferring application"
  :url "http://github.com/Rafflecopter/pitstop"
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.7.0-RC1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [clj-time "0.9.0"]
                 [com.novemberain/monger "2.1.0"]]
  :profiles {
   :repl {:dependencies [[org.clojure/tools.namespace "0.2.10"]]}})
