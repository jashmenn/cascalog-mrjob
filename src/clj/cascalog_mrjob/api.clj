(ns cascalog-mrjob.api
  (:use [cascalog api testing util])
  (:import [cascading.flow Flow FlowConnector])
  (:require [cascalog [workflow :as w] [vars :as v] [ops :as c]])
  (:import  [cascading.pipe Pipe])
  )

(defn ?|
  "Builds a flow based on the sinks binded to the rules. 
  Bindings are of form: sink rule"
  [& bindings]
  (let [[sinks gens]    (unweave bindings)
        sourcemap       (apply merge (map :sourcemap gens))
        tails           (map cascalog.rules/connect-to-sink gens sinks)
        sinkmap         (w/taps-map tails sinks)
        flow            (.connect (FlowConnector. (merge {"cascading.flow.job.pollinginterval" 100} cascalog.rules/*JOB-CONF*))
                          sourcemap sinkmap (into-array Pipe tails))]
        flow))


(defn ?>
  [& flows]
  )
