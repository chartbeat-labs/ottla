(ns cb.cljbeat.ottla.cli
  "Top-level namespace where all public API functions live. See README for
  example usage."
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :as log]))


(defn str->long [s]
  (Long/parseLong s))


(defn comma-str-list->long-list [s]
  (map str->long (clojure.string/split s #",")))


(defn parse-opts
  "Parses cli-options."
  [args cli-options]
  (let [{:keys [options errors summary]}
        (cli/parse-opts args cli-options)]
    (when (:help options)
      (log/info summary)
      (System/exit 0))
    (when errors
      ;; note - we don't want to exit here until we can be smarter about allowing args to be parsed
      ;; once for both the producer and the consumer otherwise there are conflicts if both are present
      (log/warn "There were errors parsing command-line arguments:")
      (log/warn (clojure.string/join "\n" errors)))
    options))


(defn extract-props-from-options
  "Given the list of options parsed from the command-line using the long-opt
  names described in CONSUMER_CONFIGURABLES or PRODUCER_CONFIGURABLES, parses
  out the consumer or producer properties."
  [configurables options]
  (let [lookup (into {} (map (juxt :long-opt :prop) configurables))]
    (reduce
      (fn [props [opt-k opt-v]]
        (if-let [prop-k (lookup (name opt-k))]
          (assoc props prop-k opt-v)
          props))
      {}
      options)))


(defn configurable-map->cli-opt-vect
  "Given a map from CONSUMER_CONFIGURABLES or PRODUCER_CONFIGURABLES returns a
  cli-option spec vector."
  [m]
  (let [v [(if (m :short-opt) (format "-%s" (m :short-opt)) nil)]
        v (conj v (format "--%s %s" (m :long-opt) (m :hint "ARG")))
        v (conj v (get m :desc ""))
        v (if (m :missing) (conj v :missing (m :missing)) v)
        v (if (m :default) (conj v :default (m :default)) v)]
    v))
