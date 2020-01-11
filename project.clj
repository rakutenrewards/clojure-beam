(def ^:private apache-beam-version "2.16.0")

(defproject com.curbside/curbside-clojure-beam "0.1.5-SNAPSHOT"
  :description "Clojure wrapper for Apache Beam"
  :url "https://github.com/RakutenReady/curbside-clojure-beam"

  :repositories
  [["redshift"
    {:url "https://s3.amazonaws.com/redshift-maven-repository/release"}]
   ["confluent"
    {:url "https://packages.confluent.io/maven/"}]
   ["curbside-snapshot-repo"
    {:url "https://curbside.jfrog.io/curbside/libs-snapshot-local/"
     :username "curbside-api-build"
     :password :env/jfrog_password}]
   ["curbside-release-repo"
    {:url "https://curbside.jfrog.io/curbside/libs-release-local/"
     :username "curbside-api-build"
     :password :env/jfrog_password}]]

  :deploy-repositories
  [["releases"
    {:url "https://curbside.jfrog.io/curbside/libs-release-local/"
     :username :env/artifactory_user
     :password :env/artifactory_pass}]]

  :dependencies
  [[org.clojure/clojure "1.10.0"]
   [nrepl "0.6.0"] ; Network REPL server
   [medley "1.2.0"]
   [clj-time "0.15.2"]
   [org.clojure/core.match "0.3.0"]

   [com.taoensso/nippy "2.14.0"]

   ;; Logging
   [com.fzakaria/slf4j-timbre "0.3.14"] ; Forward SLF4J to Timbre
   [com.taoensso/timbre "4.10.0"] ; Pure Clojure logging
   [org.slf4j/log4j-over-slf4j "1.7.27"] ; https://www.slf4j.org/legacy.html
   [org.slf4j/jul-to-slf4j "1.7.27"] ;
   [org.slf4j/jcl-over-slf4j "1.7.27"] ;
   [timbre-ns-pattern-level "0.1.2"] ; Middleware to filter logs by namespace
   [org.apache.commons/commons-lang3 "3.9"]
   [org.clojure/tools.logging "0.5.0"]

   ;; beam
   [org.apache.beam/beam-sdks-java-io-kafka ~apache-beam-version]
   [org.apache.beam/beam-sdks-java-core ~apache-beam-version]
   [org.apache.beam/beam-runners-direct-java ~apache-beam-version]
   [org.apache.beam/beam-runners-google-cloud-dataflow-java ~apache-beam-version
    :exclusions [io.netty/netty-codec-http2 io.grpc/grpc-netty-shaded io.grpc/grpc-core]]

   ;; kafka
   [org.apache.kafka/kafka-clients "2.0.0"]
   [io.confluent/kafka-avro-serializer "5.0.0"
    :exclusions [org.apache.kafka/kafka-clients]]
   [curbside/abracad "0.4.21"] ; AVRO for Clojure
   [org.apache.avro/avro "1.9.0"]
   [curbside-avro-schemas "0.0.33"]

   ;; redis
   [com.taoensso/carmine "2.19.1"]

   ;; Serialization
   [metosin/jsonista "0.2.5"]
   [camel-snake-kebab "0.4.0"]

   ;; 3rd-Party Services
   [com.indeed/java-dogstatsd-client "2.0.16"]

   ;; geography utilities
   [factual/geo "2.1.1"]]

  :how-to-ns {:require-docstring? false
              :sort-clauses? true
              :allow-refer-all? false
              :allow-extra-clauses? false
              :align-clauses? false
              :import-square-brackets? false}
  :source-paths ["src"]
  :java-source-paths ["src"]
  :javac-options ["-target" "1.8" "-source" "1.8"]

  :test-paths ["test" "test_integration"]
  :test-selectors {:unit #(not (:integration %))
                   :integration :integration}
  :test2junit-output-dir "test-reports"

  :jvm-opts ["-Duser.timezone=UTC"
             "-XX:-OmitStackTraceInFastThrow"]
  :min-lein-version "2.8.0"

  :plugins [[jonase/eastwood "0.3.6"]
            [lein-ancient "0.6.15"]
            [lein-bikeshed "0.5.1"
             :exclusions [org.clojure/tools.cli
                          org.clojure/tools.namespace]]
            [com.gfredericks/lein-how-to-ns "0.2.3"
             :exclusions [com.googlecode.java-diff-utils/diffutils]]
            [lein-cljfmt "0.5.6"
             :exclusions [org.clojure/clojure]]
            [lein-cloverage "1.0.13"
             :exclusions [org.clojure/clojure]]
            [lein-kibit "0.1.6"]]

  :profiles
  {:test {:global-vars {*assert* true}}
   :ci-medium {:plugins [[test2junit "1.3.3"]]
               :jvm-opts ["-Xms3G" "-Xmx3G"]}})
