(ns user
  (:require
   [cb.cljbeat.ottla :as ottla]
   [cb.cljbeat.ottla.consumer :as consumer]
   [test_ottla :as tsts]
   [clojure.tools.namespace.repl :refer [refresh]]))


(alter-var-root #'consumer/-make-consumer (fn [_] (fn [a b] (tsts/-make-mock-consumer a b))))
(def cnsmr (consumer/consumer {:group.id "foo" :bootstrap.servers "foo01:9092"} "foo" [0]))
(def messages ["foo1" "foo2"])
