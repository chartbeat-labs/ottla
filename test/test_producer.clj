(ns test_producer
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [cb.cljbeat.ottla.producer :as producer])
  (:import (java.util ArrayList Collection Collections)
           (org.apache.kafka.common Cluster)
           (org.apache.kafka.common Node)
           (org.apache.kafka.common PartitionInfo)
           (org.apache.kafka.common.serialization StringSerializer)
           (org.apache.kafka.clients.producer Partitioner)
           (org.apache.kafka.clients.producer MockProducer)))


; Ottla will internally use the MockProducer
(defn ^MockProducer -make-mock-producer [opts]
    (MockProducer.))

(alter-var-root #'producer/producer (fn [_] (fn [b] (-make-mock-producer b))))

(def TestTopicProducer
    (producer/producer {:bootstrap.servers "foo01:9092" :client.id "test-client"}))

(def test-cluster
  (let [
    nodes (vector 
      (Node. 0 "localhost" 1000) 
      (Node. 1 "localhost" 2000) 
      (Node. 2 "localhost" 3000))
    empty-node-array (into-array Node [])
    tt1-partition (PartitionInfo. "test-topic-1" 0 (get nodes 0) empty-node-array empty-node-array)
    tt2-partition (PartitionInfo. "test-topic-2" 1 (get nodes 1) empty-node-array empty-node-array)
    partitions [tt1-partition tt2-partition]]
        
    (Cluster. "test-cluster" nodes partitions (Collections/emptySet) (Collections/emptySet))))

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
   (let [test-producer (MockProducer.)
     messages [
       {:topic "test-topic-1" :partition (int 0) :key "foo" :valus "bar"} 
       {:topic "test-topic-1" :partition (int 1) :key "foo" :valus "bar"} 
       {:topic "test-topic-2" :partition (int 0) :key "bar" :value "foo"}]
     sent-messages (send-message-batch test-producer messages)
     history (.history test-producer)
     topic-2-messages (filter #(= "test-topic-2" (.topic %)) history)
     topic-1-messages (filter #(= "test-topic-1" (.topic %)) history)]

      (is (= 3 (count sent-messages)))
      (is (= 2 (count topic-1-messages)))
      (is (= 1 (count topic-2-messages)))))
      
  (def TestPartitioner
    (reify Partitioner
      (partition [this topic key keyBytes value valueBytes cluster]
        (if (= "test-topic-1" topic) 1 (if (= "test-topic-2" topic) 2 0)))))

  (testing "Auto Partitioning"
    (let [test-producer (MockProducer. test-cluster true TestPartitioner (StringSerializer.) (StringSerializer.))
      messages [
        {:topic "test-topic-1" :key "foo" :valus "bar"} 
        {:topic "test-topic-2" :key "foo" :valus "bar"}]
      sent-messages (send-message-batch test-producer messages)]
      
    (is (= 1 (count (filter #(= "test-topic-1" (.topic %)) sent-messages))))
    (is (= 1 (.partition (first (filter #(= "test-topic-1" (.topic %)) sent-messages)))))
    (is (= 1 (count (filter #(= "test-topic-2" (.topic %)) sent-messages))))
    (is (= 2 (.partition (first (filter #(= "test-topic-2" (.topic %)) sent-messages))))))))
