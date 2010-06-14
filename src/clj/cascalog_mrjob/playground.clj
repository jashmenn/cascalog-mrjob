; Usage:
;
; REPL: 
;   $ env JAVA_OPTS=-Xmx768m LEIN_CLASSPATH=src/clj ~/lib/lein repl
;   # OR
;   $ lein uberjar && hadoop jar cascalog-mrjob-standalone.jar clojure.lang.Repl
;   # then 
;   (use 'cascalog-mrjob.playground) (bootstrap)
;
; Running a job: 
;   $ lein uberjar

(ns cascalog-mrjob.playground
  (:use [cascalog api testing])
  (:use [cascalog-mrjob api])
  (:require [cascalog [workflow :as w] [vars :as v] [ops :as c]])
  (:require [clojure.contrib [str-utils :as s]])
  (:import  [cascading.cascade CascadeConnector Cascade])
  (:import  [cascalog.jobs SampleMRJob SampleMRJob$Map])
  (:import  [cascading.flow MapReduceFlowTapped])
  (:import  [org.apache.hadoop.mapred JobConf TextInputFormat TextOutputFormat 
             FileInputFormat FileOutputFormat])
  (:import  [org.apache.hadoop.io Text LongWritable IntWritable])
  (:import  [org.apache.hadoop.fs Path])
  (:import  [org.apache.hadoop.mapred.lib IdentityReducer])
  (:gen-class)
  )

(defmacro bootstrap []
  '(do
    (use (quote cascalog.api))
    (require (quote [cascalog [workflow :as w] [vars :as v] [ops :as c]]))
    (ns cascalog-mrjob.playground) ; tmp?
  ))

(def words (memory-source-tap [
  ["My dog has fleas. Good dog"]
  ]))

(def word-split (memory-source-tap ["My" "dog" "has" "fleas." "Good" "dog" ]))

(w/defmapcatop [re-split-op [pattern]] [str]
  (s/re-split pattern (.toString str)))

(defn to-lower-case [token-string]
  (.toLowerCase token-string))

(comment 
(?<- (stdout) [?word] (words ?line) 
                       (re-split-op [#"\s+"] ?line :> ?word) (:distinct false))

(let [q (<- [?word] (words ?line) 
                    (re-split-op [#"\s+"] ?line :> ?word) (:distinct false))]
  (?- (stdout) q))
  )

(defn cascade [& flows]
  (let [c (new CascadeConnector)]
    (do 
      (.connect c (into-array flows)))))

(comment
(let [tmp1 (hfs-textline "tmp/tmp1")
      q1 (<- [?word] (words ?line) 
                    (re-split-op [#"\s+"] ?line :> ?word) (:distinct false))
      flow1 (?| tmp1 q1)
      q2 (<- [?w] (tmp1 ?line) 
                    (to-lower-case ?line :> ?w) (:distinct false))
      flow2 (?| (stdout) q2)
      c (cascade flow1 flow2)]
  (.complete c))
)


(defn build-jobconf [args]
  (let [jobconf (new JobConf)]
    (doto jobconf 
      (.setJobName "wordcount1")
      (.setOutputKeyClass Text)
      (.setOutputValueClass IntWritable)
      (.setMapperClass SampleMRJob$Map)
      (.setReducerClass IdentityReducer)
      (.setInputFormat TextInputFormat)
      (.setOutputFormat TextOutputFormat)
      (FileInputFormat/addInputPath (first args)) ; todo: setInputPaths
      (FileOutputFormat/setOutputPath (second args))
      )))

(defn build-mr-flow [in-tap out-tap]
  (new MapReduceFlowTapped "name" (build-jobconf [(.getPath in-tap) (.getPath out-tap)])
       in-tap out-tap false true))

(comment 

(let [tmp1 (hfs-textline "tmp/tmp1")
      tmp2 (hfs-textline "tmp/tmp2")
      flow1 (build-mr-flow tmp1 tmp2)
      c (cascade flow1)]
  (.complete c))

  )

; wordsplit cascalog
; downcase mr job
; count cascadlog


; tmpdir, guid, intermediate taps type
; building cascade , calling cascade, building flows
; new mapreduceflow with better taps
; a java job to run
; ability to cleanly put regular cascalog on either end

; (?<- (stdout) [?w] ((hfs-textline "tmp/tmp1") ?line) (to-lower-case ?line :> ?w) (:distinct false))

; (let [q (<- [?w] (word-split ?word) 
;                     (to-lower-case ?word :> ?w) (:distinct false))]
;   (?- (stdout) q))




(comment 

  (use 'cascalog-mrjob.playground) (bootstrap)

  (.printStackTrace *e)

)
