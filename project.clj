(defproject com.rafflecopter/pitstop "0.1.0-SNAPSHOT"
  :description "Meessage-deferring application"
  :url "http://github.com/Rafflecopter/pitstop"
  :license {:name "MIT"
            :url "http://github.com/Rafflecopter/pitstop/blob/master/LICENSE"}
  :scm {:name "git"
        :url "https://github.com/Rafflecopter/pitstop"}
  :deploy-repositories [["clojars" {:creds :gpg}]]

  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.rafflecopter/qb "0.1.0"]
                 [clj-time "0.9.0"]
                 [com.novemberain/monger "2.1.0"]]

  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.1.3"]]}})
