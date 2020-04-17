(ns cb.cljbeat.ottla.sys
  (:require [clojure.tools.logging :as log]))

;; @TODO - make this configurable!
;; 
(defn set-default-uncaught-exception-handler!
  "Sets global-wide exception handler. With no ex-handler passed, by default
  when we raise an exception in a thread, we log it and exit the application.

  See: https://stuartsierra.com/2015/05/27/clojure-uncaught-exceptions"
  ([]
   (set-default-uncaught-exception-handler!
    (fn [thread ex]
      (log/error ex "Uncaught exception in thread: " (.getName thread))
      (System/exit 1))))
  ([ex-handler]
   (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException [_ thread ex] (ex-handler thread ex))))))
