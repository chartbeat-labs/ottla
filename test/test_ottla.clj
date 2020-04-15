(ns test_ottla
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [cb.cljbeat.ottla :as ottla]
            [cb.cljbeat.ottla.consumer :as consumer])
  (:import (org.apache.kafka.clients.consumer ConsumerRecord MockConsumer OffsetResetStrategy)))


(defn ^MockConsumer -make-mock-consumer [_ parts]
  (let [beginMap (reduce #(assoc %1 %2 (Long. 0)) {} parts)]
    (doto (MockConsumer. OffsetResetStrategy/EARLIEST)
      (.assign parts)
      (.updateBeginningOffsets beginMap))))

(alter-var-root #'consumer/-make-consumer (fn [_] (fn [a b] (-make-mock-consumer a b))))

(defn add-records [^MockConsumer cnsmr records]
  (doseq [r records] (.addRecord cnsmr r)))

(def record-count (atom -1))

(defn add-messages [cnsmr list-of-messages]
  (let [records (map #(ConsumerRecord. "foo" 0 (swap! record-count inc) "akey" %) list-of-messages)]
    (add-records cnsmr records)
    cnsmr))

(defrecord TestMachine []
  ottla/AutocommittingOttlaMachine
  ottla/OttlaMachine
  (init [this options]
    (assoc this :counter 0))
  (step [this msgs]
    (doseq [{k :key v :value} msgs]
      (println "k/v" k " / " v))
    (update this :counter + (count msgs))))

(deftest test-the-tests []
  (testing "does the machine work"
    (let [machine (.init (TestMachine.) {})
          messages ["foo" "bar"]
          cnsmr (consumer/consumer {:group.id "foo" :bootstrap.servers "foo01:9092"} "foo" [0])
          cnsmr (add-messages cnsmr messages)
          msgs (consumer/poll! cnsmr 1000)
          machine (.step machine msgs)]
      (is (= (:counter machine) 2)))))