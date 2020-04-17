# Ottla
A small, but opinionated framework and library for writing kafka 
consumers and producers in clojure.

> “Knowledge must seep into your blood, into your self, not just into your head, you must live it.”
― Franz Kafka, Letters to Ottla and the Family

Ottla is designed to build kafka pipelines where a consumer is assigned a
static set of partitions for a topic. 

## Motivation

Ottla was initially built at Chartbeat for kafka 9 to give us a consistent way of building high performance single purpose kafka consumers. One of the first issues we tried to solve was the inconsistent performance of consumer group re-balancing. Automatic distribution of load across a consumer group is a great feature of kafka in many use-cases but it can lead to a cascade of lag in others. When traffic is consistent across partitions and throughput is the most important feature of a system, it can become the worst kind of bottle-neck -- a black box. Frequent stalling in the consumers became known as the "kafka offset dance". A lot of that has been improved in more recent versions of kafka, but we also found that manually assigning partitions had many advantages and we have stuck with it.

Ottla (being opinionated) makes a few important design decisions for you. It's entirely possible that it's not a good fit for your use-case and that's ok! There are a lot of great frameworks for kafka, even in clojure. 

1. There is a single consumer bound to a static list of partitions in a single topic (no streaming joins)
    - by default, ottla runs from the command line and expects to be starting a single blocking consumer. You don't *need* to do this but that's the intention. 
2. The messages from kafka are consumed in micro-batches
    - ottla loops continuously over a step function that grabs a batch of messages and processes them. You can do whatever you want with each batch, but there are no semantics for getting one message at a time from the broker.
3. The kafka client is a clojure record (or another object that implements the ottla protocol)
    - this fits in nicely with component or other similar libraries
4. Exactly-once or guaranteed delivery semantics is not as important as throughput and simplicity
    - ottla was designed for high throughput. You can commit offsets manually whenever you want but by default it takes a batch and commits when it's done. If the consumer crashes for some reason mid-processing, messages will be reprocessed from the last committed offset.

In the world of streaming data pipelines there are many design patterns that have been adopted. One main differentiator is the concept of the system topology as a single deployable entity (as I first saw with Apache Storm http://storm.apache.org/) or as a set of individual components (as in Apache Samza http://samza.apache.org/). Note: I haven't used either of these in a number of years so it's possible that they've changed. We have adopted the individual component model for a number of reasons, most importantly it makes it much easier to iterate on the pipeline and to develop new components for sub-topologies. This is why we consider each Ottla component to be single purpose and tied to a single kafka topic. A component does not care who else reads from a topic, who put the data into the topic or why. Each consumer is a cog in the machine. Consumers either read from a topic and write to another one (or more than one) after doing some sort of transformation or they read from a topic and write to a data sink of some sort.

The biggest disadvantage that someoone might find with our design is the lack of support for joining between two kafka topics. This is actually something that we rarely find a need for. You could use ottla to do this but there are better ways. Generally our pattern for solving those sorts of use-cases is to have a consumer write data into a database with a time index and another consumer to join against that. If that doesn't work for you, I suggest kafka streams!


### What Ottla is Not

Ottla is not a wrapper around the kafka consumer api in clojure. If you want to build your own special purpose framework for working with kafka in clojure there are several mature libraries that provide a nice idiomatic api over all of kafka, this is not it. I could see ottla integrating with one of those libraries in the future, but for now we're just calling the java objects directly.

## Usage

Ottla consumers are simple. 

1. Define a record implementing the `OttlaMachine` protocol (the "machine")
2. Implement the init funciton to set up your consumer
3. Implement the step function to do something with each batch of data
4. Implement a main method that passes the configuration to your ottla machine
5. If you don't to auto-commit after every step, implement a commit function (and extend `ManualCommittingOttlaMachine` )

```clojure
(ns foo
  (:require [cb.cljbeat.ottla :as ottla]
            [cb.cljbeat.ottla.consumer :as consumer]
            [clojurewerkz.spyglass.client :as mc]))


;; define application specific cli-options
(def cli-options
  [["-m" "--memcached-cluster HOST:PORT" "..."]])

;; Create a record which impliments the OttlaMachine protocol.
(defrecord FooMachine []
  ottla/OttlaMachine
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

Ottla defines a main protocol, `OttlaMachine` and additionally an optional `ManualCommittingOttlaMachine` protocol
for cases where you don't want auto-commits after every step.

Then the `step` function takes `this` and
`msgs`. At the end of the `step` function, ottla commits the messages.

If you dont want auto-commits, implement `ManualCommittingOttlaMachine` as well which adds a `commit!` funciton which takes `this` and `cnsmr` (the `KafkaConsumer` object) that you can manually call
`cb.cljbeat.ottla.consumer/commmit!` on.

A `ManualCommittingOttlaMachine` would be used if you'd rather
"write-no-more-than-once" behavior than "write-at-least-once" behavior in your
`step` function.

IMPORTANT
---------

All OttlaMachine functions expect `this` (the machine itself) to be returned.

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
            ottla/OttlaMachine
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

## Testing

## Authors

Ottla was spun out of the Chartbeat repo and owes its thanks to the entire company.

Maintainer: 
- Rick Mangi https://github.com/rmangi

Original Author
- Devon Peticolas https://github.com/x 

Contributors
- Nathan Butler https://github.com/butlern
- David Labarbera https://github.com/foodneutrino

And countless others who have used the framework and provided feedback

## TODO

