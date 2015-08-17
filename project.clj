(defproject com.rafflecopter/pitstop "0.2.3-SNAPSHOT"
  :description "Meessage-deferring application"
  :url "http://github.com/Rafflecopter/pitstop"
  :license {:name "MIT"
            :url "http://github.com/Rafflecopter/pitstop/blob/master/LICENSE"}
  :scm {:name "git"
        :url "https://github.com/Rafflecopter/pitstop"}
  :deploy-repositories [["releases" :clojars]]

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.rafflecopter/qb "0.2.1"]
                 [clj-time "0.9.0"]
                 [com.rafflecopter/monger-connect "0.1.1"]
                 [com.novemberain/monger "3.0.0"]

                 ;; Monger time-encoding dependencies. go figure
                 [org.clojure/data.json "0.2.6"]
                 [cheshire "5.1.1"]]

  :profiles {:dev {:dependencies [[midje "1.7.0-beta1"]]
                   :plugins [[lein-midje "3.1.3"]]}})
