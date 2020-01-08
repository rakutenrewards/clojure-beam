(ns curbside.beam.pipeline
  (:require
   [curbside.beam.api :as beam]
   [curbside.beam.runtime :as runtime]
   [curbside.beam.transform.current-trips :as current-trips]
   [curbside.beam.transform.kafka-io :as kafka-io]
   [curbside.beam.transform.redis-io :as redis-io]))

(defn current-trips [pipeline-options]
  (let [pipeline (beam/make-pipeline pipeline-options)]
    (-> pipeline
        (kafka-io/read-avro-message {:kafka-cfg runtime/kafka-cfg})
        (current-trips/apply-transform {:datadog-cfg runtime/datadog-cfg})
        (redis-io/write {:pool {} :spec runtime/redis-cfg}))
    (beam/run-pipeline pipeline)))
