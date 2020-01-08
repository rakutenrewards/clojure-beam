(ns curbside.beam.examples.redis-key-inc.pipeline
  (:require
   [clojure.spec.alpha :as spec]
   [clojure.tools.logging :as log]
   [curbside.beam.api :as beam]
   [curbside.beam.metrics.api :as metrics]
   [curbside.beam.transform.redis-io :as redis-io])
  (:import
   (curbside.beam.examples.redis_key_inc RedisKeyIncOptions)
   (org.apache.beam.sdk.transforms DoFn$ProcessContext)))

(def redis-cmds [{:op "setnx" :args ["redis-key-inc" 0]}
                 {:op "incr" :args ["redis-key-inc"]}])

(spec/def ::redis-uri string?)
(spec/def ::custom-pipeline-config
  (spec/keys :req [::redis-uri]))

(defn report-metrics
  [{:keys [^DoFn$ProcessContext process-context]}]
  (log/info "Incrementing the redis counter.")
  (.inc (metrics/counter "redis.key.inc" "num.commands"))
  (.output process-context (.element process-context)))

;; This just keeps incrementing a redis key at every run
(defn- build-pipeline! [pipeline pipeline-opts]
  (-> pipeline
      (beam/create-pcoll redis-cmds)
      (beam/pardo #'report-metrics)
      (redis-io/write {:pool {} :spec {:uri (.getRedisUri pipeline-opts)}})))

(defn create-pipeline
  [pipeline-args]
  (let [pipeline (beam/make-pipeline pipeline-args RedisKeyIncOptions)
        pipeline-opts (.getOptions pipeline)]
    (build-pipeline! pipeline pipeline-opts)))
