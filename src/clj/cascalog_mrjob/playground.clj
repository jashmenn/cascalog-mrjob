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
  (:import  [cascading.flow Flow MapReduceFlowTapped])
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
  ["My dog has fleas. Good Dog"]
  ]))

(def word-split (memory-source-tap ["My" "dog" "has" "fleas." "Good" "Dog" ]))

(w/defmapcatop [re-split-op [pattern]] [str]
  (s/re-split pattern (.toString str)))

(w/defmapop [re-line-split [pattern]] [str]
  (s/re-split pattern (.toString str)))

(defn parse-int [number] (Integer. number))

(defn lowercase-jobconf []
  (let [jobconf (new JobConf)]
    (doto jobconf 
      (.setJobName "lowercase")
      (.setOutputKeyClass Text)
      (.setOutputValueClass IntWritable)
      (.setMapperClass SampleMRJob$Map)
      (.setReducerClass IdentityReducer)
      (.setInputFormat TextInputFormat)
      (.setOutputFormat TextOutputFormat)
      )))

(defn mr-flow [jobconf in-tap out-tap]
  (do
  (doto jobconf
      (FileInputFormat/addInputPath (.getPath in-tap)) ; todo: setInputPaths
      (FileOutputFormat/setOutputPath (.getPath out-tap)))
  (new MapReduceFlowTapped (.getJobName jobconf) jobconf 
       in-tap out-tap false true)))

(defn cascade [& flows]
  (let [c (new CascadeConnector)]
    (do 
      (.connect c (into-array Flow flows)))))

(defn goal 
  "ideal api"
  []
    (?<- (stdout)  
         [?word ?sum] 
         (words ?line) 
         (re-split-op [#"\s+"] ?line :> ?word) (:distinct false)
         (job [jobconf] ?word :> ?word ?one-str)
         (parse-int ?one-str :> ?one) ; seqfile would solve this but complicate things elsewhere
         (c/sum ?one :> ?sum)
    )
  )

(defn run-test-job []
  (let [tmp1 (hfs-textline "tmp/tmp1")
        tmp2 (hfs-textline "tmp/tmp2")
        q1 (<- [?word] (words ?line) 
                      (re-split-op [#"\s+"] ?line :> ?word) (:distinct false))
        flow1 (?| tmp1 q1)
        flow2 (mr-flow (lowercase-jobconf) tmp1 tmp2) 
        q3 (<- [?word ?sum] 
               (tmp2 ?line) 
               (re-line-split [#"\t"] ?line :> ?word ?count) (:distinct false)
               (parse-int ?count :> ?count-i)
               (c/sum ?count-i :> ?sum))
        flow3 (?| (stdout) q3)
        c (cascade flow1 flow2 flow3)]
    (.complete c))
  )

(comment 

  (use 'cascalog-mrjob.playground) (bootstrap)

  (run-test-job)

  (.printStackTrace *e)

)
