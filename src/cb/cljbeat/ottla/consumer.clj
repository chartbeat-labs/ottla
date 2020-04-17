(ns cb.cljbeat.ottla.consumer
  "An opinionated wrapper around KafkaConsumer."
  (:require [clojure.walk :as walk]
            [clojure.string :as string]
            [clojurewerkz.propertied.properties :as p]
            [cb.cljbeat.ottla.cli :as cli]
            [clojure.tools.logging :as log])
  (:import [java.util ArrayList]
           [java.time Duration]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer Consumer KafkaConsumer ConsumerRecord]))


(def DEFAULT_POLL_TIMEOUT_MS
  "Default timeout in ms for poll."
  1000)


(def CONSUMER_CONFIGURABLES
  "Consumer options defined and mapped into CONSUMER_DEFAULT_PROPS (used by
  consumer function) and CONSUMER_CLI_OPTION_SPECS (automatically used by
  ottla/start).

  Many options were skipped, I've only added ones that seem useful at this time.

  For more detailed descriptions, see:
  https://kafka.apache.org/documentation/#newconsumerconfigs
  "
  ;; options that must be set manually
  [{:prop "group.id"
    :short-opt "g"
    :long-opt "ottla.consumer.group.id"
    :missing "ottla.consumer.group.id must be specified"
    :hint "ID"
    :desc "The unique identifier of the consumer group."}

   {:prop "bootstrap.servers"
    :short-opt "b"
    :long-opt "ottla.bootstrap.servers"
    :missing "ottla.bootstrap.servers must be specified"
    :hint "HOST:9092,HOST:9092"
    :desc "Broker(s) to connect to."}

   ;; offset options
   {:prop "auto.offset.reset"
    :long-opt "ottla.consumer.auto.offset.reset"
    :desc "What to do for initial or a missing offsets (earliest, latest, none)"
    :default "latest"}

   ;; autocommmit options - unlike the official defaults, we default
   ;; enable.auto.commit to false because ottla/step handles this
   {:prop "enable.auto.commit"
    :long-opt "ottla.consumer.enable.auto.commit"
    :desc "If true, offset will be periodically committed in the background."
    :default "false"}

   ;; deserializer options - these are different from Kafka's defaults to more
   ;; closely fit how we're writing consumers.
   {:prop "key.deserializer"
    :long-opt "ottla.consumer.key.deserializer"
    :desc "Deserializer class for key that implements Deserializer interface."
    :default "org.apache.kafka.common.serialization.StringDeserializer"}

   {:prop "value.deserializer"
    :long-opt "ottla.consumer.value.deserializer"
    :desc "Deserializer class for value that implements Deserializer interface."
    :default "org.apache.kafka.common.serialization.ByteArrayDeserializer"}

   ;; fetch and session options
   {:prop "fetch.min.bytes"
    :long-opt "ottla.consumer.fetch.min.bytes"
    :desc "The minimum amount of data the server should return for a fetch."
    :long-opts "ottla.consumer.fetch.min.bytes"
    :default "1"}

   {:prop "fetch.max.bytes"
    :long-opt "ottla.consumer.fetch.max.bytes"
    :desc "The maximum amount of data the server should return for a fetch."
    :default "52428800"}

   {:prop "fetch.max.wait.ms"
    :long-opt "ottla.consumer.fetch.max.wait.ms"
    :desc "Max time server blocks if it hasn't satisfied fetch.min.bytes."
    :default "500"}

   {:prop "max.poll.records"
    :long-opt "ottla.consumer.max.poll.records"
    :desc "The maximum number of records returned in a single call to poll()."
    :default "500"}

   {:prop "max.partition.fetch.bytes"
    :long-opt "ottla.consumer.max.partition.fetch.bytes"
    :desc "The maximum amount of data per-partition the server will return."
    :default "1048576"}

   {:prop "receive.buffer.bytes"
    :long-opt "ottla.consumer.receive.buffer.bytes"
    :desc "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data."
    :default "1048576"}

   {:prop "heartbeat.interval.ms"
    :long-opt "ottla.consumer.heartbeat.interval.ms"
    :desc "The expected time between heartbeats to the consumer coordinator."
    :default "3000"}

   {:prop "session.timeout.ms"
    :long-opt "ottla.consumer.session.timeout.ms"
    :desc  "The timeout used to detect consumer failures."
    :default "10000"}

   {:prop "request.timeout.ms"
    :long-opt "ottla.consumer.request.timeout.ms"
    :desc  "The timeout configures the maximum amount of time a consumer will wait for a broker response"
    :default "30000"}])



(def CONSUMER_DEFAULT_PROPS
  "The map of default properties created from CONSUMER_CONFIGURABLES"
  (into {} (map (juxt :prop :default) CONSUMER_CONFIGURABLES)))


(def CONSUMER_CLI_OPTIONS
  "A list of cli-option-specs to be used in conjunction with cli/parse-opts and
  with consumer/extract-props-from-options."
  (map cli/configurable-map->cli-opt-vect CONSUMER_CONFIGURABLES))


(def extract-props-from-options
  "Converts a map of options created by parsing CONSUMER_CLI_OPTIONS into a map
  of properties for the consumer."
  (partial cli/extract-props-from-options CONSUMER_CONFIGURABLES))

(defn -make-consumer [props parts]
  (doto (KafkaConsumer. props)
    (.assign parts)))

(defn consumer
  "Returns a consumer with properities, props, and assigns it to partitions,
  part, for broker topic, topic."
  [props topic parts]
  (let [;; apply defaults, java-fy props, allow keyword keys.
        props (walk/stringify-keys props)
        props (into CONSUMER_DEFAULT_PROPS props)
        _ (println props)
        props (p/load-from props)

        ;; java-fy the partitions
        parts (map #(TopicPartition. topic %) parts)
        parts (ArrayList. parts)

        _ (log/infof "Connecting to topic %s w/ group.id %s and partitions %s"
                     topic (get props "group.id") (string/join "," parts))

        ;; create the consumer and assign it to the partitions
        cnsmr (-make-consumer props parts)]
    cnsmr))


(defn consumers
  "Like consumer, but returns a vector of n consumers with partitions, parts,
  split evenly among them. See consumer."
  [n props topic parts]
  (log/info n " " props " - " topic " " parts)
  (let [parts-per-cnsmr (/ (count parts) n)]
    ;; we return a vector here instead of a lazy-seq (result of map) so that
    ;; any misconfiguration is raised right away.
    ;; interestingly, partiton-all accepts floats and
    ;; ceils them which is what we want
    (mapv #(consumer props topic %)
          (partition-all parts-per-cnsmr parts)))) 


(defn- ConsumerRecord->hash-map
  "this is the representation of a record as a map.
   A lazy seq of these is returned to consumers from a call to poll!"
  [^ConsumerRecord rec]
  {:topic     (.topic rec)
   :partition (.partition rec)
   :offset    (.offset rec)
   :key       (.key rec)
   :value     (.value rec)})


(defn poll!
  "Light wrapper arround KafkaConsumer.poll. 
   Returns a lazy seq of maps"
  ([^Consumer cnsmr]
   (poll! cnsmr DEFAULT_POLL_TIMEOUT_MS))
  ([^Consumer cnsmr ^long timeout-ms]
   (->> (.poll cnsmr (Duration/ofMillis timeout-ms))
        (map ConsumerRecord->hash-map))))


(defn commit!
  "Light wrapper around KafkaConsumer.commitSync. Commits the youngest offset
  of the most recently 'poll'ed messages for each partition."
  [^Consumer cnsmr]
  (.commitSync cnsmr)
  cnsmr)
