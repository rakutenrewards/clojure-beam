(ns curbside.beam.utils.kafka-test
  "Curbside API outputs data to Kafka. This service is the interface to it."
  (:require
   [abracad.avro :as avro]
   [abracad.helpers.clojure]
   [abracad.helpers.joda-time]
   [clojure.tools.logging :as log])
  (:import
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient SchemaRegistryClient)
   (java.io ByteArrayOutputStream DataOutputStream)
   (java.util Map)
   (org.apache.avro Schema)
   (org.apache.kafka.clients.producer ProducerConfig ProducerRecord KafkaProducer)
   (org.apache.kafka.common.serialization ByteArraySerializer)))

(def ^:const ^:private magic-byte 0)

(defn producer-config
  [{:keys [acks bootstrap-servers request-timeout-ms retries]}]
  {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-servers
   ProducerConfig/ACKS_CONFIG acks
   ProducerConfig/RETRIES_CONFIG retries
   ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG request-timeout-ms
   ;; Must be 1 when retries are used to prevent out of order messages.
   ProducerConfig/MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION "1"
   ProducerConfig/MAX_BLOCK_MS_CONFIG "1000"})

(defn registry-client [^String base-url identity-map-capacity]
  (CachedSchemaRegistryClient. base-url (int identity-map-capacity)))

(defn- encode*
  "Encode is going to create the header of kafka message, put the schema id inside the header and encode the data.
  Byte 0 -> a magic number
  Byte 1-4 -> schema id
  Byte 5-n -> avro object
  https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format"
  [schema-registry-id schema record]
  (with-open [byte-array-output-stream (ByteArrayOutputStream.)
              out (DataOutputStream. byte-array-output-stream)]
    (doto out
      (.writeByte magic-byte)
      (.writeInt schema-registry-id))
    (avro/encode schema out record)
    (.toByteArray byte-array-output-stream)))

(defn- encode [^SchemaRegistryClient registry ^Schema schema record]
  ;; null in Kafka has a special meaning for deletion in a topic with the compact retention policy.
  ;; https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
  (when record
    (let [schema-registry-id (.getId registry (.getFullName schema) schema)]
      (try
        (encode* schema-registry-id schema record)
        (catch Exception e
          (log/error e "Error trying to encode the record : " record)
          (throw e))))))

(defn ^KafkaProducer producer [props]
  (let [byte-array-serializer (ByteArraySerializer.)]
    (KafkaProducer. ^Map props
                    byte-array-serializer
                    byte-array-serializer)))

(defn ^ProducerRecord producer-record
  [^String topic key value]
  (ProducerRecord. topic key value))

(defn- send-event-to-kafka
  [producer topic k v]
  (->> (producer-record topic k v)
       (.send producer)))

(defn produce* [producer topic k v]
  (log/debug producer topic k v)
  (if-not producer
    (log/infof "Attempting to produce (%s,%s,%s) to Kafka without valid configuration."
               topic k v)
    (try
      @(send-event-to-kafka producer topic k v)
      (catch Exception e
        (log/error "Failed to produce Kafka message" e)))))

(defn produce-event!
  "Add circuit breaker for kafka-service to break service call connection. When produce* result is
  nil makes failed-event? evaluates to true"
  [producer registry topic k-schema k v-schema v]
  (let [encoded-k (encode registry k-schema k)
        encoded-v (encode registry v-schema v)]
    (produce* producer topic encoded-k encoded-v))
  ;; make sure we don't leak kafka objects to caller
  nil)
