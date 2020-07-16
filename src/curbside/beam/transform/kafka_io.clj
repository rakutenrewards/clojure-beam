(ns curbside.beam.transform.kafka-io
  (:require
   [curbside.beam.api :as beam]
   [curbside.beam.nippy-coder :as nippy-coder]
   [curbside.beam.schema-registry.api :as schema-registry]
   [curbside.beam.utils.avro :as avro])
  (:import
   (org.apache.beam.sdk Pipeline)
   (org.apache.beam.sdk.io.kafka KafkaIO)
   (org.apache.beam.sdk.transforms DoFn$ProcessContext)
   (org.apache.beam.sdk.values PBegin KV)
   (org.joda.time Instant)))

;; Kafka IO

(defn read-bytes
  "PTransform that read data from Kafka topics.

  - with-bootstrap-servers: Sets the bootstrap servers for the Kafka consumer.
  - with-consumer-config-updates: Update consumer configuration with new properties.
  - with-topic: Sets the topic to read from.
  - with-max-num-records: Similar to org.apache.beam.sdk.io.Read.Unbounded#withMaxNumRecords(long). Mainly used for tests and demo applications.
  - with-read-commited: Sets isolation.level to read-commited
  - with-commit-offsets-in-finalize: Sets commitOffsetsInFinalizeEnabled to True
  - without-metadata: Returns a PTransform for PCollection of KV, dropping Kafka metatdata.

  Example
    ```
    (read-bytes pipeline {:with-bootstrap-servers \"127.0.0.1:29092\"
                          :with-consumer-config-updates {\"auto.offset.reset\" \"earliest\"}
                          :with-topic \"some topic\"
                          :with-max-num-records 100
                          :without-metadata true})
    ```

  See https://beam.apache.org/releases/javadoc/2.9.0/org/apache/beam/sdk/io/kafka/KafkaIO.html"
  [p {:keys [with-bootstrap-servers with-consumer-config-updates with-topic with-max-num-records
             ^Instant with-start-read-time with-create-time with-timestamp-policy-factory
             with-value-deserializer-and-coder without-metadata with-commit-offsets-in-finalize with-read-commited step-name]}]
  (let [kafka-transform (cond-> (KafkaIO/readBytes)
                          with-bootstrap-servers (.withBootstrapServers with-bootstrap-servers)
                          with-consumer-config-updates (.withConsumerConfigUpdates with-consumer-config-updates)
                          with-topic (.withTopic with-topic)
                          with-max-num-records (.withMaxNumRecords with-max-num-records)
                          with-create-time (.withCreateTime with-create-time)
                          with-start-read-time (.withStartReadTime with-start-read-time)
                          with-value-deserializer-and-coder (.withValueDeserializerAndCoder (first with-value-deserializer-and-coder)
                                                                                            (second with-value-deserializer-and-coder))
                          with-timestamp-policy-factory (.withTimestampPolicyFactory with-timestamp-policy-factory)
                          with-commit-offsets-in-finalize (.commitOffsetsInFinalize)
                          with-read-commited (.withReadCommitted)
                          without-metadata (.withoutMetadata))]
    (-> p
        (cond-> (instance? Pipeline p) (PBegin/in))
        (.apply (beam/make-step-name step-name #'read-bytes) kafka-transform))))

(defn- to-key-value
  [{:keys [^DoFn$ProcessContext process-context runtime-parameters]}]
  (let [element        (.element process-context)
        kafka-key      ((:find-key-fn runtime-parameters) element)]
    (-> process-context
        (.output (KV/of kafka-key element)))))

(defn write
  "PTransform that write data from Kafka topics.

  - with-bootstrap-servers: Sets the bootstrap servers for the Kafka consumer.
  - with-producer-config-updates: Update producer configuration with new properties.
  - with-topic: Sets the topic to write to.
  - to-key-value-fn: Provide a function that finds the Kafka key in the record

  See https://beam.apache.org/releases/javadoc/2.9.0/org/apache/beam/sdk/io/kafka/KafkaIO.html"
  [pipeline {:keys [with-bootstrap-servers with-producer-config-updates find-key-fn
                    with-key-serializer with-value-serializer with-topic step-name]}]
  (let [kafka-transform (cond-> (KafkaIO/write)
                          with-bootstrap-servers (.withBootstrapServers with-bootstrap-servers)
                          with-producer-config-updates (.withProducerConfigUpdates with-producer-config-updates)
                          with-key-serializer (.withKeySerializer with-key-serializer)
                          with-value-serializer (.withValueSerializer with-value-serializer)
                          with-topic (.withTopic with-topic))]
    (-> pipeline
        (beam/pardo #'to-key-value {:step-name          (format "%s:to-key-value" step-name)
                                    :runtime-parameters {:find-key-fn find-key-fn}
                                    :coder              (nippy-coder/make-kv-coder)})
        (.apply (beam/make-step-name step-name #'write) kafka-transform))))

(def ^:private schema-registry-client (atom nil))

(defn- create-schema-registry-client [{{:keys [schema-registry-cfg]} :runtime-parameters}]
  (swap! schema-registry-client (fn [curr] (or curr (schema-registry/make-client schema-registry-cfg)))))

(defn- deserialize-avro-message [{:keys [^DoFn$ProcessContext process-context runtime-parameters]}]
  (let [data (.element process-context)
        decoded (avro/decode @schema-registry-client
                             data
                             (some-> (:subject->reader-schema runtime-parameters)
                                     deref))]
    (.output process-context decoded)))

(defn read-avro-message
  [^Pipeline pipeline {:keys [schema-registry-cfg
                              subject->reader-schema
                              step-name]}]
  {:pre [(or (nil? subject->reader-schema) (var? subject->reader-schema))]}
  (-> pipeline
      (beam/pardo #'deserialize-avro-message
                  {:do-fn/setup #'create-schema-registry-client
                   :runtime-parameters {:schema-registry-cfg schema-registry-cfg
                                        :subject->reader-schema subject->reader-schema}
                   :step-name step-name})))
