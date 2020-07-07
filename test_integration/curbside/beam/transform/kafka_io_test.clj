(ns curbside.beam.transform.kafka-io-test
  (:require
   [abracad.avro :as avro]
   [abracad.helpers.schema :as schema]
   [clojure.test :refer [deftest is testing]]
   [curbside.beam.api :as beam]
   [curbside.beam.kafka :as kafka]
   [curbside.beam.schema-registry-util :as schema-registry-util]
   [curbside.beam.test-env :as env]
   [curbside.beam.testing :as beam.testing]
   [curbside.beam.transform.kafka-io :as kafka-io]
   [curbside.beam.utils.avro :as avro-utils]
   [curbside.beam.utils.kafka-test :as kafka-test]
   [curbside.ad.avro-schemas.ad-notify-estimate :as ad-notify-estimate]
   [curbside.common.avro-schemas.core :as core])
  (:import
   (curbside.beam.java ClojureKafkaSupport$KafkaValueSerializer ClojureKafkaSupport$KafkaKeySerializer)
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient)
   (java.util UUID)))

(def ^:private identity-map-capacity 1000)

(defn kafka-event
  ([]
   (kafka-event "dida"))
  ([dida]
   {:course       120.2
    :dest-id      #uuid "d7b126b6-055a-467e-8a6e-7ff4737fbc8b"
    :dida         dida
    :distance     nil
    :est-in-zone  nil
    :eta          nil
    :lat          39.22505569458008
    :lng          -94.64643096923828
    :received-at  "2018-08-20T00:57:17Z"
    :sandbox      false
    :event-ts     "2018-08-20T00:57:17Z"
    :event-type   "ad-notify-estimate"
    :site-id      "cvs_8577"
    :site-account "cvs"
    :status       "arrived"
    :v-accuracy   0.2
    :h-accuracy   0.3
    :speed        10.12}))

(defn kafka-events
  []
  [(kafka-event "dida-1")
   (kafka-event "dida-2")
   (kafka-event "dida-3")
   (kafka-event "dida-4")
   (kafka-event "dida-5")
   (kafka-event "dida-6")])

(def ^:private retries "5")
(def ^:private acks "1")

;; Need to cast to an int or else...
;; Execution error (ConfigException) at org.apache.kafka.common.config.ConfigDef/parseType (ConfigDef.java:675).
;; Invalid value 30000 for configuration request.timeout.ms: Expected value to be a 32-bit integer, but it was a java.lang.Long
(def ^:private request-timeout-ms (int 30000))

(defn- create-producer []
  (kafka-test/producer
   (kafka-test/producer-config {:bootstrap-servers (first env/kafka-bootstrap-servers)
                                :acks                   acks
                                :retries                retries
                                :request-timeout-ms     request-timeout-ms})))

(defn produce-one-event [topic]
  (let [producer (create-producer)
        schema-registry-client (CachedSchemaRegistryClient. ^String env/registry-url (int identity-map-capacity))]
    (kafka-test/produce-event! producer schema-registry-client
                               topic
                               (avro/parse-schema schema/uuid) (:dest-id (kafka-event))
                               (avro-utils/default-subject->reader-schema "ad.ad_notify_estimate") (kafka-event))))

(deftest ^:integration kafka-avro-serialization-test
  (schema-registry-util/register-schemas-latest)
  (testing "given an event in kafka, when processing the event in Beam, the event is processed."
    (let [topic (str (UUID/randomUUID))
          _ (produce-one-event topic)
          pipeline-result (-> (beam.testing/test-pipeline)
                              (kafka-io/read-bytes
                               {:with-bootstrap-servers env/-kafka-bootstrap-servers-env
                                :with-topic topic
                                ;; for testing only
                                :with-consumer-config-updates {"auto.offset.reset" "earliest"}
                                ;; limited number of records for testing only
                                :with-max-num-records 1
                                :without-metadata true})
                              (beam/values)
                              (kafka-io/read-avro-message {:schema-registry-cfg {:url env/registry-url
                                                                                 :cache-capacity identity-map-capacity}})
                              (beam.testing/get-pcollection-element 10000))]
      (is (= [(kafka-event)]
             pipeline-result)))))

(def serialize-kafka-key (avro-utils/serialize-record core/string-edn-v1-parsed))
(def serialize-kafka-value (avro-utils/serialize-record ad-notify-estimate/edn-schema-v3-parsed))

(deftest ^:integration kafka-write-test
  (schema-registry-util/register-schemas-latest)
  (let [test-id (str (UUID/randomUUID))
        events (kafka-events)
        pipeline-write (-> (beam.testing/test-pipeline)
                           (beam/create-pcoll events)
                           (kafka-io/write {:with-bootstrap-servers       (first env/kafka-bootstrap-servers)
                                            :with-producer-config-updates (kafka/generate-kafka-producer-config {:schema-registry-url env/registry-url}
                                                                                                                #'serialize-kafka-key
                                                                                                                #'serialize-kafka-value)
                                            :find-key-fn                  :event-type
                                            :with-key-serializer          ClojureKafkaSupport$KafkaKeySerializer
                                            :with-value-serializer        ClojureKafkaSupport$KafkaValueSerializer
                                            :with-topic                   test-id
                                            :step-name                    "write-kafka-test"}))
        pipeline-read (-> (beam.testing/test-pipeline)
                          (kafka-io/read-bytes
                           {:with-bootstrap-servers env/-kafka-bootstrap-servers-env
                            :with-topic test-id
              ;; for testing only
                            :with-consumer-config-updates {"auto.offset.reset" "earliest"}
              ;; limited number of records for testing only
                            :with-max-num-records 6
                            :without-metadata true})
                          (beam/values)
                          (kafka-io/read-avro-message {:schema-registry-cfg {:url env/registry-url
                                                                             :cache-capacity identity-map-capacity}}))]
    (beam/run-pipeline pipeline-write)
    (is (= (set events)
           (set (beam.testing/get-pcollection-element pipeline-read 5000))))))
