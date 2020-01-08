(ns curbside.beam.runtime
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.stacktrace]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [curbside.beam.api :as beam]
   [curbside.beam.dataflow :as dataflow]
   [curbside.beam.examples.redis-key-inc.pipeline :as redis-key-inc]
   [curbside.beam.examples.stateful-count.pipeline :as stateful-count]
   [curbside.beam.examples.word-count.pipeline :as word-count]
   [curbside.beam.jobs.dwell.pipeline :as dwell]
   [curbside.beam.jobs.ml-kafka-to-bq.pipeline :as ml-kafka-bq]
   [curbside.env :as env])
  (:import
   (java.io File)
   (java.nio.file FileSystems))
  (:gen-class))

;; -- beam-specific env/cfg --
(def datadog-cfg
  {:host "localhost"
   :port 8125
   :prefix "curbside.flow"})

(def kafka-cfg
  {:with-bootstrap-servers env/-kafka-bootstrap-servers-env
   :with-topic env/ad-topic
   :with-consumer-config-updates {"auto.offset.reset" (str env/kafka-offset-reset)}})

(def redis-cfg
  {:uri env/redis})

;; -- launch --

(defn- deploy-pipeline!
  "Deploys and executes a Dataflow pipeline.

   `runner`      \"direct\" or \"dataflow\"
   `simple-name` Simple name for pipeline for helpful messaging.
   `custom-spec` A spec validating the pipeline-specific config
   `pipeline-fn` A fn creating a Pipeline given args as required by dataflow
                `PipelineOptionsFactory/fromArgs`
   `config`      A map containing the full config for the pipeline; this
                 must include dataflow core execution parameters in addition
                 to pipeline-specific config. (These will be converted to
                 parsed args and passed to the `pipeline-fn`.)"
  [runner simple-name custom-spec pipeline-fn config]
  {:pre [(#{"direct" "dataflow"} runner)]}
  ;; -- some validation --
  (let [full-spec (case runner
                    "direct" custom-spec
                    "dataflow" (s/merge :dataflow-exec/core-pipeline-config custom-spec))]
    (when-not (s/valid? full-spec config)
      (throw (RuntimeException. ^String (s/explain-str full-spec config)))))
  ;; -- deploy! --
  (let [pipeline-args (beam/->pipeline-args config)
        pipeline-result (-> pipeline-args
                            pipeline-fn
                            beam/run-pipeline)]
    (log/infof "%s pipeline '%s' using runner '%s' with args %s"
               (if (true? (:update config)) "UPDATING" "DEPLOYING") simple-name runner pipeline-args)
    (if (= "development" env/environment)
      (beam/wait-pipeline-result pipeline-result)
      (beam/wait-pipeline-result pipeline-result 30000))))

(defn- ^File require-uberjar* [project-dir]
  (if-let [configured-uberjar env/dataflow-uberjar]
    (let [as-file (io/file configured-uberjar)]
      (assert (.exists as-file)) as-file)
    (let [matcher (.getPathMatcher (FileSystems/getDefault) "glob:curbside-flow-*-standalone.jar")
          found (->> (file-seq (io/as-file project-dir))
                     (filter #(.matches matcher (.getFileName (.toPath %))))
                     first)]
      (assert (.exists ^File found)) found)))

(defn- dataflow-config->job-update-config
  "Given a core dataflow config, convert it to a pipeline-updating configuration.

   As an extra safety measure, we fail fast if the name of the job to update does
   not match an expected prefix."
  [config expected-job-name-prefix job-name-to-update]
  (when (not (str/starts-with? job-name-to-update expected-job-name-prefix))
    (throw (RuntimeException.
            (format "expected job name prefix to start with '%s' but was '%s'"
                    expected-job-name-prefix job-name-to-update))))
  (assoc config :update true :job-name job-name-to-update))

(defn- make-dataflow-core-config [prefix-job-name app-name]
  {:post [(s/valid? :dataflow-exec/core-pipeline-config %)]}
  (let [job-name-prefix (format "%s-%s" prefix-job-name app-name)
        config
        (cond-> {:app-name app-name
                 :job-name (format "%s-%d" job-name-prefix (System/currentTimeMillis))
                 :runner "DataflowRunner"
                 :gcp-temp-location (some-> env/dataflow-gs-bucket (dataflow/gs-url "tmp-dataflow-staging"))
                 :project env/dataflow-project
                 :region env/dataflow-region
                 :network env/dataflow-network
                 :subnetwork env/dataflow-sub-network
                 :files-to-stage [(-> "user.dir" (System/getProperty) require-uberjar* (.getAbsolutePath))]}
          (some? env/dataflow-profiling-enabled) (assoc :profiling-agent-configuration {:APICurated (Boolean/parseBoolean env/dataflow-profiling-enabled)})
          (some? env/dataflow-num-workers) (assoc :num-workers (Integer/parseInt env/dataflow-num-workers))
          (some? env/dataflow-max-num-workers) (assoc :max-num-workers (Integer/parseInt env/dataflow-max-num-workers))
      ;; When update-job-name is provided, we configure dataflow pipeline to updating an existing job. One use
      ;;  case for an update is resizing the cluster after it is caught up to head of topic.
          (some? env/update-job-name) (dataflow-config->job-update-config job-name-prefix env/update-job-name))]
    (when-not (s/valid? :dataflow-exec/core-pipeline-config config)
      (throw (Exception. (s/explain-str :dataflow-exec/core-pipeline-config config))))
    config))

(defn- make-core-config [runner prefix-job-name simple-name]
  (case runner
    "direct" {:app-name simple-name :runner "DirectRunner"}
    "dataflow" (make-dataflow-core-config prefix-job-name simple-name)))

;; --

(defn- deploy-stateful-count! [runner prefix-job-name simple-name]
  (deploy-pipeline! runner
                    simple-name
                    ::stateful-count/custom-pipeline-config
                    stateful-count/create-pipeline
                    (make-core-config runner prefix-job-name simple-name)))

(defn- deploy-word-count! [runner prefix-job-name simple-name]
  (->> (-> (make-core-config runner prefix-job-name simple-name)
           (assoc :input-file "gs://apache-beam-samples/shakespeare/kinglear.txt" :output "counts"))
       (deploy-pipeline! runner simple-name :word-count/custom-pipeline-config word-count/create-pipeline)))

(defn- deploy-ml-kafka-bq! [runner prefix-job-name simple-name]
  (->> (-> (make-core-config runner prefix-job-name simple-name)
           (assoc ::ml-kafka-bq/bootstrap-servers env/-kafka-bootstrap-servers-env
                  ::ml-kafka-bq/ad-topic env/ad-topic
                  ::ml-kafka-bq/ad-site-topic env/ad-site-topic
                  ::ml-kafka-bq/schema-registry-url env/registry-url
                  ::ml-kafka-bq/big-query-project-id env/big-query-project-id))
       (deploy-pipeline! runner simple-name ::ml-kafka-bq/custom-pipeline-config ml-kafka-bq/create-pipeline)))

(defn- deploy-redis-key-inc! [runner prefix-job-name simple-name]
  (->> (-> (make-core-config runner prefix-job-name simple-name)
           (assoc ::redis-key-inc/redis-uri env/redis))
       (deploy-pipeline! runner simple-name ::redis-key-inc/custom-pipeline-config redis-key-inc/create-pipeline)))

(defn- deploy-dwell! [runner prefix-job-name simple-name]
  (->> (-> (make-core-config runner prefix-job-name simple-name)
           (assoc :custom-options {:deploy-namespace env/deploy-namespace
                                   :site-id-filter env/site-id-filter
                                   ;; kafka configuration
                                   :bootstrap-servers env/kafka-bootstrap-servers
                                   :ad-topic env/ad-topic
                                   :schema-registry-url env/registry-url
                                   ;; NOTE: beam expects our pipeline opts/args to go through
                                   ;;   their own CLI parsing system (which supports json) so
                                   ;;   we convert keyword to string here.
                                   :kafka-offset-reset (name env/kafka-offset-reset)
                                   ;; not required/nilable:
                                   :start-read-time env/start-read-time
                                   ;; redis configuration
                                   :redis-uri env/redis}))
       (deploy-pipeline! runner simple-name ::dwell/custom-pipeline-config dwell/create-pipeline)))

;; --

(defn -main [& [cmd runner prefix-job-name & simple-names]]
  (try
    (doseq [simple-name simple-names]
      (case cmd
        "deploy-pipeline" (case simple-name
                            "word-count" (deploy-word-count! runner prefix-job-name simple-name)
                            "stateful-count" (deploy-stateful-count! runner prefix-job-name simple-name)
                            "ml-kafka-bq" (deploy-ml-kafka-bq! runner prefix-job-name simple-name)
                            "redis-key-inc" (deploy-redis-key-inc! runner prefix-job-name simple-name)
                            "dwell" (deploy-dwell! runner prefix-job-name simple-name))))
    (catch Exception e
          ;; Print stacktrace to *out*/stdout; we run these from, eg Jenkins builds, and
          ;;   it'll be nice to see full error in console.
      (clojure.stacktrace/print-cause-trace e)
      (throw e))))
