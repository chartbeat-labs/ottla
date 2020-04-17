(ns test_ottla
  (:require [clojure.test :refer :all]
            [cb.cljbeat.ottla :as ottla]
            [cb.cljbeat.ottla.consumer :as consumer])
  (:import (org.apache.kafka.clients.consumer ConsumerRecord MockConsumer OffsetResetStrategy)
           (org.apache.kafka.common TopicPartition)))

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
  ottla/OttlaMachine
  (init [this options]
    (assoc this :counter 0))
  (step [this msgs]
    (doseq [{k :key v :value} msgs]
      (println "k/v" k " / " v))
    (update this :counter + (count msgs))))

(defrecord TestManualMachine []
  ottla/OttlaMachine
  (init [this options]
    (assoc this :counter 0))
  (step [this msgs]
    (doseq [{k :key v :value} msgs]
      (println "k/v" k " / " v))
    (update this :counter + (count msgs)))
  ottla/ManualCommittingOttlaMachine
  (commit! [this cnsmr]
    (consumer/commit! cnsmr)
    this))

(deftest test-basic-machine []
  (testing "does the machine work"
    (let [machine (.init (TestMachine.) {})
          messages ["foo" "bar"]
          cnsmr (consumer/consumer {:group.id "foo" :bootstrap.servers "foo01:9092"} "foo" [0])
          cnsmr (add-messages cnsmr messages)
          part (TopicPartition. "foo" (Long. 0))
          machine (ottla/-step-and-commit! machine cnsmr 100)]
      (is (= (:counter machine) 2))
      (is (some? (.committed cnsmr part)))))
  (testing "test with manual commit"
    (let [machine (.init (TestManualMachine.) {})
          messages ["foo" "bar"]
          cnsmr (consumer/consumer {:group.id "foo" :bootstrap.servers "foo01:9092"} "foo" [0])
          cnsmr (add-messages cnsmr messages)
          machine (ottla/-step-and-commit! machine cnsmr 100)
          part (TopicPartition. "foo" (Long. 0))]
      (is (= (:counter machine) 2))
      (is (nil? (.committed cnsmr part)))
      (.commit! machine cnsmr)
      (is (some? (.committed cnsmr part))))))