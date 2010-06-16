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

(defn to-lower-case [token-string]
  (.toLowerCase token-string))

(defn parse-int [number] (Integer. number))

(comment 
(?<- (stdout) [?word] (words ?line) 
                       (re-split-op [#"\s+"] ?line :> ?word) (:distinct false))

(let [q (<- [?word] (words ?line) 
                    (re-split-op [#"\s+"] ?line :> ?word) (:distinct false))]
  (?- (stdout) q))
  )

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

(defn run-test-job []
  (let [tmp1 (hfs-textline "tmp/tmp1")
        tmp2 (hfs-textline "tmp/tmp2")
        q1 (<- [?word] ((hfs-textline "test/data/dog.txt") ?line) 
                      (re-split-op [#"\s+"] ?line :> ?word) (:distinct false))
        flow1 (?| tmp1 q1)
        ;
        ; q2 (<- [?w] (tmp1 ?line) 
        ;               (to-lower-case ?line :> ?w) (:distinct false))
        ; flow2 (?| tmp2 q2)
        flow2 (mr-flow (lowercase-jobconf) tmp1 tmp2) 
        ;
        q3 (<- [?word ?sum] 
               (tmp2 ?line) 
               (re-line-split [#"\t"] ?line :> ?word ?count) (:distinct false)
               (parse-int ?count :> ?count-i)
               (c/sum ?count-i :> ?sum))
        flow3 (?| (stdout) q3)
        ;
        c (cascade flow1 flow2 flow3)]
    (.complete c))
  )

(comment 

)

(def word-prelim-counts 
  (memory-source-tap [["my" "1"] 
                      ["dog" "1"]
                      ["has" "1"]
                      ["fleas." "1"]
                      ["good" "1"]
                      ["dog" "1"]]))


(comment

(?<- (stdout) [?word ?sum] 
     (word-prelim-counts ?word ?count)
     (parse-int ?count :> ?count-i)
     (c/sum ?count-i :> ?sum) 
     )

  )

(comment 

(let [tmp1 (hfs-textline "tmp/tmp1")
      tmp2 (hfs-textline "tmp/tmp2")
      jobconf (build-jobconf)
      flow1 (mr-flow jobconf tmp1 tmp2)
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

  (run-test-job)

  (.printStackTrace *e)

)
