(ns test_producer
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer :all]
            [cb.cljbeat.ottla.producer :as producer])
  (:import (java.util ArrayList Collection)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.clients.producer MockProducer)))


; Ottla will internally use the MockProducer
(defn ^MockProducer -make-mock-producer [opts]
    (MockProducer.))

(alter-var-root #'producer/producer (fn [_] (fn [b] (-make-mock-producer b))))

(def TestTopicProducer
    (producer/producer {:bootstrap.servers "foo01:9092" :client.id "test-client"}))

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
      
  (testing "Auto Partitioning"
    (let [test-producer (MockProducer.)
      messages [
        {:topic "test-topic" :key "foo" :valus "bar"} 
        {:topic "test-topic" :key "foo" :valus "bar"}]
      sent-messages (send-message-batch test-producer messages)
      history (.history test-producer)
      _ (println "HISTORY: " history)
      _ (println "SENT: " sent-messages)]
      
    (is (= 0 (.partition (first history)))))))
