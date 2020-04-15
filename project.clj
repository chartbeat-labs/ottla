(defproject com.chartbeat.cljbeat/ottla "1.0-SNAPSHOT"
  :description "An opinionated framework for writing kafka consumers, producers and consumer-producers."
  :dependencies [[org.clojure/tools.cli "1.0.194"]
                 [org.clojure/tools.logging "1.0.0"]
                 [clojurewerkz/propertied "1.3.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.1.587"]
                 [org.apache.kafka/kafka-clients "2.4.1"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "1.0.0"]]
                   :source-paths ["dev" "test"]}}

  :vcs :git)
