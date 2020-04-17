(ns cb.cljbeat.ottla
  "Top-level namespace where all public API functions live. See README for
  example usage."
  (:require [cb.cljbeat.ottla.consumer :as consumer]
            [cb.cljbeat.ottla.cli :as cli]
            [cb.cljbeat.ottla.producer :as producer]
            [cb.cljbeat.ottla.sys :as sys]
            [clojure.core.async :refer [<! chan go]]))

(def OTTLA_CLI_OPTIONS
  "The options that are automagically included when you use defconsumer"
  [["-h" "--help"]
   ["-r" "--ottla.manual.mode" "Don't start consuming, step must be called manually." :default false]
   ["-n" "--ottla.parallelism N" "Consume with this many threads and consumers."
    :parse-fn cli/str->long :default 1]
   ["-p" "--ottla.partitions PART1,PART2" "Consume from these partitions."
    :parse-fn cli/comma-str-list->long-list
    :validate [not-empty "Must not be empty"]]
   ["-m" "--ottla.poll.timeout MILLISECS" "Timeout to pass to consumer/poll!"
    :parse-fn cli/str->long :default consumer/DEFAULT_POLL_TIMEOUT_MS]
   ["-t" "--ottla.topic KAFKATOPIC" "Topic to consume from."
    :validate [not-empty "Must not be empty"]]])

(defprotocol OttlaMachine
  (init [this cli-options]
    "Given a map of cli-options, inits the machine.")
  (step [this msgs] "Given a list of msgs, updates the machine."))

(defprotocol ManualCommittingOttlaMachine
  (commit! [this cnsmr] "commit the current offsets, by default this happens after every step"))

; this allows us to have a default implementation of commit behavior
; without having to add an un-used commit! method to the base protocol
(defmulti -step-and-commit! (fn [this cnsmr timeout] (satisfies? ManualCommittingOttlaMachine this)))

; manual implementation doesn't commit
(defmethod -step-and-commit! true [machine cnsmr timeout]
  (let [msgs (consumer/poll! cnsmr timeout)]
    (step machine msgs)))

; default implementation steps and then commits
(defmethod -step-and-commit! false
  [machine cnsmr timeout]
  (let [msgs (consumer/poll! cnsmr timeout)
        machine (step machine msgs)]
    (consumer/commit! cnsmr)
    machine))

(defn start
  "Parses args and starts an OttlaMachine."
  ([machine args cli-options]
   (start machine (cli/parse-opts args (into #{} (concat OTTLA_CLI_OPTIONS
                                                         consumer/CONSUMER_CLI_OPTIONS
                                                         cli-options)))))
  ([machine opts]

    ;; set uncaught thread exception handling
    ;; @TODO allow this to be configured
   (sys/set-default-uncaught-exception-handler!)

   (let [;; create the state that the user wants from the init fn
         machine (.init machine opts)

         ;; create --ottla.parallelism number of consumers, default is 1
         cnsmrs (consumer/consumers (opts :ottla.parallelism)
                                    (consumer/extract-props-from-options opts)
                                    (opts :ottla.topic)
                                    (opts :ottla.partitions))]

     ;; for each cnsmr, start a thread that runs forever updating the machine and
     ;; that cnsmr. Since the records are immutable, each thread will have it's
     ;; own copy of the machine. Mutable objects assigned to the machine (such as
     ;; database connections) will be shared among all threads.
     (if (not (opts :ottla.manual.mode))
       (doseq [cnsmr cnsmrs]
         (.. (Thread. #(loop [machine machine]
                         (recur (-step-and-commit! machine cnsmr (opts :ottla.poll.timeout)))))
             (start)))
       machine))))

;; producer functions
(defn start-producer
  "parses args and returns a new producer"
  ([args cli-options]
   (start-producer (cli/parse-opts args (into #{} (concat OTTLA_CLI_OPTIONS
                                                          producer/PRODUCER_CLI_OPTIONS
                                                          cli-options)))))
  ([opts]
   (producer/producer (producer/extract-props-from-options opts))))

(defn -async-producer-loop
  [prdcr handler]
  (let [producer-channel (chan 1000)]
    (go (loop []
          (let [val (<! producer-channel)
                kafka-msgs (map handler val)]
            (producer/send-and-flush-batch! prdcr kafka-msgs))
          (recur)))
    producer-channel))

(defn start-async-producer
  "Handler is a function that converts a message from the channel into a kafka message. 
  This function must return a map containing {:topic :key :value} or {:topic :partition :key :value}
  as per producer/send-and-flush-batch"
  ([args cli-options handler]
   (-async-producer-loop (start-producer args cli-options) handler))
  ([opts handler]
   (-async-producer-loop (start-producer opts) handler)))
