(ns test_producer
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [cb.cljbeat.ottla.producer :as producer])
  (:import (java.util ArrayList Collection Collections)
           (org.apache.kafka.common Cluster)
           (org.apache.kafka.common Node)
           (org.apache.kafka.common PartitionInfo)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.clients.producer MockProducer)))


; Ottla will internally use the MockProducer
(defn ^MockProducer -make-mock-producer [opts]
    (MockProducer.))

(alter-var-root #'producer/producer (fn [_] (fn [b] (-make-mock-producer b))))

(def TestTopicProducer
    (producer/producer {:bootstrap.servers "foo01:9092" :client.id "test-client"}))

; (def test-cluster
;   (let [
;     nodes (vector (Node. 0 "localhost" 1000) (Node. 1 "localhost" 2000))
;     partitions (vector (PartitionInfo. "test-topic" (int 0) (get 0 nodes) (into-array Node nodes) (into-array Node nodes)))]
        
;     (Cluster. 1 nodes partitions (Collections/emptyList) (Collections/emptyList))))

(defn send-message-batch [producer-dest list-of-messages]
  (do 
    (println "Adding " list-of-messages)
    (producer/send-and-flush-batch! producer-dest list-of-messages)))

(deftest test-send-and-flush []
  (testing "Basic kafka message production"
    (let [test-producer (MockProducer.)
          messages [
            {:topic "test-topic" :partition (int 0) :key "foo" :valus "bar"} 
            {:topic "test-topic" :partition (int 0) :key "bar" :value "foo"}]
          sent-messages (send-message-batch test-producer messages)]

      (is (= 2 (count sent-messages)))
      (is (every? #(= "test-topic" (.topic %)) sent-messages))
      (is (every? #(= 0 (.partition %)) sent-messages))))

  (testing "Multiple Assigned Partitions"
    (let [test-producer (MockProducer.)
          messages [
            {:topic "test-topic" :partition (int 0) :key "foo" :valus "bar"} 
            {:topic "test-topic" :partition (int 1) :key "bar" :value "foo"}]
          sent-messages (send-message-batch test-producer messages)
          history (.history test-producer)
          partition-0-messages (filter #(= 0 (.partition %)) history)
          partition-1-messages (filter #(= 1 (.partition %)) history)]

      (is (= 2 (count sent-messages)))
      (is (= 1 (count partition-0-messages)))
      (is (= 1 (count partition-1-messages)))))
      
 (testing "Different Partitions"
   (let [test-producer (MockProducer.); (Cluster/empty) true nil nil nil)
     messages [
       {:topic "test-topic-1" :partition (int 0) :key "foo" :valus "bar"} 
       {:topic "test-topic-1" :partition (int 1) :key "foo" :valus "bar"} 
       {:topic "test-topic-2" :partition (int 0) :key "bar" :value "foo"}]
     sent-messages (send-message-batch test-producer messages)
     _ (println "Partitions: " (for [m sent-messages] (.topic m)))
     history (.history test-producer)
     topic-2-messages (filter #(= "test-topic-2" (.topic %)) history)
     topic-1-messages (filter #(= "test-topic-1" (.topic %)) history)]

      (is (= 3 (count sent-messages)))
      (is (= 2 (count topic-1-messages)))
      (is (= 1 (count topic-2-messages)))))
      
  (testing "Auto Partitioning"
    (let [test-producer (MockProducer.)
      ;test-producer (MockProducer. test-cluster true nil nil nil)
      messages [
        {:topic "test-topic" :key "foo" :valus "bar"} 
        {:topic "test-topic" :key "foo" :valus "bar"}]
      sent-messages (send-message-batch test-producer messages)
      history (.history test-producer)]
      ;sent-partitions (for [m sent-messages] (.topic m))
      
    (is (= 0 (.partition (first sent-messages)))))))
