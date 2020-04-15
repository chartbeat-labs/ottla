# Ottla

A small, but opinionated framework and library for writing kafka 
consumers and producers in clojure.

Ottla is designed to build kafka pipelines where each consumer is assigned a
static set of partitions to read from for each topic. 

## Usage

```clojure
(ns foo
  (:require [cb.cljbeat.ottla :as ottla]
            [cb.cljbeat.ottla.consumer :as consumer]
            [clojurewerkz.spyglass.client :as mc]))


;; define application specific cli-options
(def cli-options
  [["-m" "--memcached-cluster HOST:PORT" "..."]])

;; Create a record which impliments the AutocommittingOttlaMachine protocol.
(defrecord FooMachine []
  ottla/OttlaMachine
  ottla/AutocommittingOttlaMachine
  ;; Define an init function that runs once with the FooMachine (this) and the
  ;; parsed command-line options (options) and returns the newly initalized
  ;; FooMachine (a record which can be treated like an immutable hash-map).
  (init [this options]
    (let [memcached-client (mc/text-connection (get options :memcache-cluster))
          this (assoc this :memcached-client memcached-client)
          this (assoc this :batch-count 0)]
      this))
  
  ;; Define a step function which takes the FooMachine (this) and a list of
  ;; maps each representing a message from the consumer (msgs) and returns a
  ;; newly updated FooMachine.
  (step [this msgs]

    (doseq [{k :key v :value} msgs]
      (memecached/set (:memcached-client this) k v))

    (let [batch-count (+ (:batch-count this) 1)
          _ (logging/info "I have processed %d batches" batch-count)
          this (assoc :batch-count batch-count)]

      this)))

(defn -main
 [& args] 
 ;; Use ottla/start to parse both the user-defined and the ottla-defined
 ;; cli-options and start the FooMachine processing the topic.
 (ottla/start (FooMachine.) agrs cli-options))
```

## Ottla Protocols

Ottla defines a main protocol, `OttlaMachine` and additional protocols which
define specific behavior. 

Currently, this is used to define offset commit behavior. 
Ottla exposes two protocols you can implement, `AutocommitingOttlaMachine` and
`ManualCommittingOttlaMachine`.

If you implement the former, then the `step` function only takes `this` and
`msgs`. At the end of the `step` function, ottla will commit the messages.

If you instead implement `ManualCommittingOttlaMachine`, `step` takes `this`,
`msgs`, and `cnsmr` (the `KafkaConsumer` object) that you can manually call
`cb.cljbeat.ottla.consumer/commmit!` on.

A `ManualCommittingOttlaMachine` would be used if you'd rather
"write-no-more-than-once" behavior than "write-at-least-once" behavior in your
`step` function.

## Development In Repl

Most development will take place in the `step` method of the machine. The
following is an example of how to get a list of messages, test the step
function, reload the code, and test again.

1. Start a repl in the project.
2. Create a consumer reading from dev and get a batch of messages to work with.
	```clojure
	cb.hound=> (require '[cb.cljbeat.ottla.consumer :as consumer])
	nil
	cb.hound=> (def cnsmr (consumer/consumer {:group.id "foo" :bootstrap.servers "localhost:9092"} "my_topic" [1]))
	#'cb.hound/cnsmr
    cb.hound=> (def msgs (consumer/poll! cnsmr 5000)) ; you may need a longer than normal timeout in the repl

    #'cb.hound/msgs
	```
2. Create an instance of the machine and initialize it
	```clojure
	cb.hound=> (def hound-machine (.init (HoundMachine.) {:value-unpacker "count"})) ; I can pass a map to my init method
	#'cb.hound/hound-machine
	```
3. Call `.step` on the machine with the messages.
	```clojure
	cb.hound=> (.step hound-machine msgs)
	...
	```
4. Change some code.
5. Reload the namespace.
	```clojure
	cb.hound=> (use '[cb.hound] :reload-all)
	nil
	```
8. Repeate from step 2.


The reloading and recreating of the machine can be consolidated into single-line
commands such as...
```clojure
(do (use '[cb.hound] :reload-all) (.step (.init (HoundMachine.) {:value-unpacker "count"}) msgs))
```


## Producers

One nice way to use producers is to just interact with an async channel. Really you should only have one instance of the kafka producer
(unless you're sending messages to more than one fleet of brokers). Using the async-producer this is really easy.

Configure as with the consumer except the option ```ottla.producer.bootstrap.servers``` allows you to specify the producer
servers separately so you can write data to a different set of kafka brokers.

The below example is all it takes to read data off of one topic and duplicate it to a topic on another server.

In this case the ottla.producer.bootstrap.servers are set to a different fleet of brokers than the ones being read from. 
Obviously you could do the same to filter a subset of data from one topic to another topic on the same broker.

Below is an example of a simple consumer-producer that filters a kafka topic named "pings" for messages that match a list of
hosts and publish them back to a different set of kafka brokers. We use this currently to send a subset of data to our development
cluster.

```clojure
(ns cb.doppelganger.core
            (:require [cb.cljbeat.ottla :as ottla]
                      [cb.cljbeat.ottla.consumer :as consumer]
                      [msgpack.core :as msgpack]
                      [clojure.core.async :refer [put!]]
                      [clojure.tools.cli :as cli]
                      [cb.cljbeat.ottla.producer :as producer])
            (:gen-class))
          
          (def unpack-fn msgpack/unpack)
          
          (def ping-channel (atom nil))
          
          (defn match-host? [hosts msg]
            (some #(= (get-in msg ["keys" "h"]) %) hosts))
          
          (defn ping->kafkamsg [ping]
            (select-keys ping [:topic :partition :key :value]))
          
          (defrecord DoppelgangerMachine []
            ottla/AutocommittingOttlaMachine
          
            (init [this options]
              (assoc this
                :hosts (:hosts options)))
          
            (step [this msgs]
              "We just filter the hosts we want and put them on the producer channel"
              (let [pings-to-send
                    (filterv #(match-host? (:hosts this) (unpack-fn (:value %))) msgs)]
                (when-not (empty? pings-to-send)
                  (put! @ping-channel pings-to-send)))
              this))
          
          (def cli-options
            [[nil "--hosts HOSTS" "Comma separated list of hosts" :parse-fn #(clojure.string/split % #",")]])
          
          (defn -main
            [& args]
            (let [{:keys [options arguments errors summary]}
                  (cli/parse-opts args
                                  (into #{} (concat ottla/OTTLA_CLI_OPTIONS
                                                    consumer/CONSUMER_CLI_OPTIONS
                                                    producer/PRODUCER_CLI_OPTIONS
                                                    cli-options)))
                  producer-channel (ottla/start-async-producer options ping->kafkamsg)]
              (reset! ping-channel producer-channel)
              (ottla/start (DoppelgangerMachine.) options)))

```

## AUTHORS
## TODO

