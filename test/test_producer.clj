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

(def record-count (atom -1))

(defn send-message-batch [producer-dest list-of-messages]
  (do 
    (println "Adding " list-of-messages)
    (producer/send-and-flush-batch! producer-dest list-of-messages)))

(def TestTopicProducer
    (producer/producer {:bootstrap.servers "foo01:9092" :client.id "test-client"}))

(deftest test-send-and-flush []
  (testing "Basic kafka message production"
    (let [test-producer TestTopicProducer
          messages [
            {:topic "test-topic" :partition (int 0) :key "foo" :valus "bar"} 
            {:topic "test-topic" :partition (int 0) :key "bar" :value "foo"}]
          sent-messages (send-message-batch test-producer messages)]

      (is (= 2 (count sent-messages)))
      (is (every? #(= 0 (.partition %)) sent-messages))))

  (testing "Multiple Assigned Partitions"
    (let [test-producer TestTopicProducer
          messages [
            {:topic "test-topic" :partition (int 0) :key "foo" :valus "bar"} 
            {:topic "test-topic" :partition (int 1) :key "bar" :value "foo"}]
          sent-messages (send-message-batch test-producer messages)
          partition-0-messages (filter #(= 0 (.partition %)) sent-messages)
          partition-1-messages (filter #(= 1 (.partition %)) sent-messages)
          _ (println "Partition 0 count: " (count partition-0-messages))]

      (is (= 2 (count sent-messages)))
      (is (= 1 (filter #(= 0 (.partition %)) sent-messages)))
      ;(is (= 1 (filter #(= 1 (.partition %)) sent-messages)))
      
      ))
)
      
;  (testing "Different Partitions"
;    (let [test-producer TestTopicProducer
;      messages [
;        {:topic "test-topic-1" :partition (int 0) :key "foo" :valus "bar"} 
;        {:topic "test-topic-1" :partition (int 1) :key "foo" :valus "bar"} 
;        {:topic "test-topic-2" :partition (int 0) :key "bar" :value "foo"}]
;      sent-messages (send-message-batch test-producer messages)]

  ;     (is (= 3 (count sent-messages)))
  ;     (is (= 2 (filter #(= "test-topic-1" (.topic %)) sent-messages)))
  ;     (is (= 1 (filter #(= "test-topic-2" (.topic %)) sent-messages)))))
      
  ; (testing "Auto Partitioning"
  ;   (let [test-producer TestTopicProducer
  ;     messages [
  ;       {:topic "test-topic" :key "foo" :valus "bar"} 
  ;       {:topic "test-topic" :key "foo" :valus "bar"}]
  ;     sent-messages (send-message-batch test-producer messages)
  ;     _ (println "Auto Partition: " (.partition (first sent-messages)))]
      
  ;   (is (= 0 (.partition (first sent-messages)))))))
