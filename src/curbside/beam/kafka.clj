(ns curbside.beam.kafka
  (:require
   [clj-time.format :as time-format]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [curbside.ad.avro.decode :refer [decode*]]
   [curbside.beam.api :as beam]
   [curbside.beam.nippy-coder :as nippy-coder]
   [curbside.beam.schema-registry.api :as schema-registry]
   [curbside.beam.transform.kafka-io :as kafka]
   [curbside.beam.utils.time :as time-utils])
  (:import
   (curbside.beam.java ClojureKafkaSupport$ClojureKafkaDeserializer ClojureKafkaSupport$ClojureTimestampPolicyFactory)
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient)
   (java.io ByteArrayOutputStream)
   (java.nio ByteBuffer)
   (org.apache.beam.sdk.transforms PTransform DoFn$ProcessContext)
   (org.apache.beam.sdk.values KV)
   (org.apache.kafka.clients CommonClientConfigs)
   (org.apache.kafka.common.config SslConfigs SaslConfigs)
   (org.apache.kafka.common.security.auth SecurityProtocol)
   (org.joda.time Instant)))

(defn- deserialize-kafka-bytes
  "Deserialize Kafka bytes, but before parsing the Avro messsage, we make sure it is
   a schema type that we care about. If it isn't we produce ::ignored-kafka-message
   that will be filtered out downstream.

   (We do this manually here so we can optimize which messages we parse, but this
    can/should be generalized.)"
  [_kafka-topic ^"[B" record-bytes {:keys [runtime-config]}]
  (let [schema-registry (schema-registry/make-client (:schema-registry-cfg runtime-config))
        schema-id (.getInt (ByteBuffer/wrap record-bytes 1 4))
        writer-schema (.getById ^CachedSchemaRegistryClient schema-registry schema-id)
        subject-name (.getFullName writer-schema)]
    (if-let [reader-schema ((:subject->reader-schema runtime-config) subject-name)]
      (let [number-of-bytes-header 5
            payload-length (- (alength record-bytes) number-of-bytes-header)
            avro-raw-data (with-open [out (ByteArrayOutputStream.)]
                            (.write out record-bytes number-of-bytes-header payload-length)
                            (.toByteArray out))]
        (let [decoded (decode* writer-schema reader-schema avro-raw-data)
              event-ts-ms (.getMillis (time-utils/parse (:event-ts decoded)))
              enter-ts-ms (System/currentTimeMillis)]
          ;; parse event ts once and make it avail for downstream use.
          (assoc decoded
                 ::event-ts-ms event-ts-ms
                 ::enter-ts-ms enter-ts-ms)))
      ::ignored-kakfa-message)))

(defn- ^Instant get-timestamp*
  "Our custom event timestamp provider for Kafka. We want to use the actual event
   timestamps for our Kafka messages (ie, :event-ts) instead of the default timestamps
   that KafkaIO offers. Note these are all UTC timestamps!"
  [prev-timestamp entity]
  ;; Note: some messages we "ignore" and never parse from Kafka. The timestamp
  ;;       we use for these ignored messages will simply be the timestamp of the
  ;;       previous un-ignored message. If there is no previous message (i.e., the
  ;;       first message is ignored, then prev-message will be
  ;;       BoundedWindow/TIMESTAMP_MIN_VALUE.)
  (if (= entity ::ignored-kakfa-message)
    prev-timestamp (Instant. ^long (::event-ts-ms entity))))

(defn- get-watermark*
  "Our custom watermark provider for Kafka."
  [^Instant last-timestamp] last-timestamp)

(defn- filter-ignored*
  "Drop ::ignored-kafka-message events."
  [{:keys [^DoFn$ProcessContext process-context]}]
  (let [elem (.getValue ^KV (.element process-context))]
    (when (not= elem ::ignored-kakfa-message)
      (.output process-context elem))))

(defn- schema-registry-config
  [{:keys [schema-registry-url schema-registry-access-key schema-registry-secret-key] :as _options}]
  (let [base-config {:url schema-registry-url :cache-capacity 100}]
    (if (and schema-registry-access-key schema-registry-secret-key)
      (assoc base-config :auth {"basic.auth.credentials.source" "USER_INFO"
                                "basic.auth.user.info" (format "%s:%s" schema-registry-access-key schema-registry-secret-key)})
      base-config)))

(defn- generate-kafka-consumer-config
  [job-name subject->reader-schema
   {:keys [kafka-offset-reset deploy-namespace confluent-api-key confluent-api-secret] :as options}]
  (let [base-config {"auto.offset.reset" kafka-offset-reset
                     "group.id" (format "%s/%s" deploy-namespace job-name)
                     ;; We are going to use our own custom Kafka deserializer but KafkaIO won't
                     ;;   let us instantiate directly. Instead we must use consumer config to
                     ;;   "pass" it the Clojure deserializer function (and config):
                     "clojure.kafka-deserializer.fn" #'deserialize-kafka-bytes
                     "clojure.kafka-deserializer.require-vars" [subject->reader-schema]
                     "clojure.kafka-deserializer.config" {:schema-registry-cfg (schema-registry-config options)
                                                          :subject->reader-schema subject->reader-schema}}]
    (if (and (not (string/blank? confluent-api-secret)) (not (string/blank? confluent-api-secret)))
      (assoc base-config
             CommonClientConfigs/SECURITY_PROTOCOL_CONFIG (.name SecurityProtocol/SASL_SSL)
             SaslConfigs/SASL_MECHANISM "PLAIN"
             SaslConfigs/SASL_JAAS_CONFIG (format "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";" confluent-api-key confluent-api-secret))
      base-config)))

(defn generate-kafka-producer-config
  [schema-registry-config-params serialize-kafka-key-var serialize-kafka-value-var]
  {"clojure.kafka-serializer.config" {:schema-registry-cfg (schema-registry-config schema-registry-config-params)}
   "clojure.kafka-key-serializer.fn" serialize-kafka-key-var
   "clojure.kafka-value-serializer.fn" serialize-kafka-value-var})

(defn ^PTransform make-kafka-input-xf
  "Create a (composite) transform that reads from a Kafka topic and emits Avro-decoded
   events (Clojure maps) downstream.

   The provided `job-name` is used to uniquely identify the consumer to Kafka (i.e., to
   create the Kafka consumer's \"group.id\".)

   A `subject->reader-schema` is provided; the function returns the Avro schema to use
   to decode the given subject/type. This function may return null to ignore messages
   in the topic."
  [job-name subject->reader-schema
   {:keys [deploy-namespace kafka-offset-reset start-read-time bootstrap-servers topic] :as options}]
  {:pre [(string? job-name) (var? subject->reader-schema) deploy-namespace]}
  (beam/make-composite-transform
   "kafka-input"
   (fn [inp]
     (when (and (= "latest" kafka-offset-reset)
                (some? start-read-time))
       (log/warnf (str "opt 'kafka-offset-reset' was configured to 'latest' but it will be"
                       " overridden by configured opt 'start-read-time'; if you want to read"
                       " from latest offset, you should not provide a 'start-read-time' opt")))
     (-> inp
         (kafka/read-bytes
          {:with-bootstrap-servers (string/join "," bootstrap-servers)
           :with-topic topic
           :with-consumer-config-updates
           ;; We have no need for any offset commit strategies on the kafka side;
           ;; Beam's/Dataflow's KafkaIO will keep offset tracked by its own checkpoint
           ;; snapshots.
           ;; Note: if :start-read-time is configured auto.offset.reset property
           ;;    here will be ignored.
           (generate-kafka-consumer-config job-name subject->reader-schema options)
           ;; Configure our custom deserializer by giving the deserializer class to KafkaIO:
           :with-value-deserializer-and-coder [ClojureKafkaSupport$ClojureKafkaDeserializer
                                               (nippy-coder/make-custom-coder)]
           ;; KafkaIO offers some default event timestamp logic (like processing
           ;;   wall clock time, by default) and record create or append time. But
           ;;   for use to properly reason about how our trip events get windowed
           ;;   we would like to use our **own** event-ts directly from our event
           ;;   objects:
           :with-timestamp-policy-factory (ClojureKafkaSupport$ClojureTimestampPolicyFactory.
                                           #'get-timestamp* #'get-watermark*)
           ;; note: we use tf/parse here we do not need to do anything special b/c performance
           ;;    doesn't matter. our time-utils/parse does not handle (nor needs to) yyyy-mm-dd format.
           :with-start-read-time (some-> start-read-time time-format/parse (.toInstant))
           ;; this is useful for direct/local testing:
           ;; :with-max-num-records 175
           :with-commit-offsets-in-finalize true
           :without-metadata true})
         (beam/pardo #'filter-ignored*)))))
