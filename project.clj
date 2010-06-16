(defproject cascalog-mrjob "1.0.0-SNAPSHOT"
  :description "Use a plain MapReduce job in cascalog"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 [cascading1.1rc3 "1.1.3-SNAPSHOT"]
                 [cascalog "1.0.1-SNAPSHOT"]]
  :dev-dependencies [[lein-javac "0.0.2-SNAPSHOT"]
                     ; [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [hadoop-core "0.20.3-dev-mine"]
                     [org.clojars.brandonw/lein-nailgun "1.0.0"]]
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :java-fork "true"
  :javac-debug "true"
  :repositories [["clojars" "http://clojars.org/repo"]]
  :namespaces [bonobo-geo.playground])