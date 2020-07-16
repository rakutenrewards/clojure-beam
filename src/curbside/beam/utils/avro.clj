(ns curbside.beam.utils.avro
  (:require
   [abracad.avro :as avro]
   [abracad.helpers.clojure]
   [curbside.ad.avro-schemas.ad-dests-cancel :as ad-dests-cancel]
   [curbside.ad.avro-schemas.ad-dests-serviced :as ad-dests-serviced]
   [curbside.ad.avro-schemas.ad-dests-start :as ad-dests-start]
   [curbside.ad.avro-schemas.ad-dests-stop :as ad-dests-stop]
   [curbside.ad.avro-schemas.ad-notify-estimate :as ad-notify-estimate]
   [curbside.ad.avro-schemas.ad-stories :as ad-stories]
   [curbside.ad.avro.decode :refer [decode*]]
   [curbside.beam.schema-registry.api :as schema-registry-api])
  (:import
   (io.confluent.kafka.schemaregistry.client SchemaRegistryClient)
   (java.io ByteArrayOutputStream DataOutputStream)
   (java.nio ByteBuffer)
   (org.apache.avro Schema)))

(defn schema->subject-name [^Schema schema]
  (.getFullName schema))

(def default-subject->reader-schema
  {"ad.ad_dests_cancel" ad-dests-cancel/edn-schema-v2-parsed
   "ad.ad_dests_start" ad-dests-start/edn-schema-v2-parsed
   "ad.ad_dests_stop" ad-dests-stop/edn-schema-v2-parsed
   "ad.ad_dests_serviced" ad-dests-serviced/edn-schema-v2-parsed
   "ad.ad_notify_estimate" ad-notify-estimate/edn-schema-v3-parsed
   "ad.ad_stories" ad-stories/edn-schema-v1-parsed})

(defn split-message-bytes*
  "Split message-bytes per confluent schema registry wire format,
   and return a vector of [schema-id, avro-raw-data].

     Byte 0 -> a magic number
     Byte 1-4 -> schema id
     Byte 5-n -> avro object

   https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format"
  [message-bytes]
  (let [schema-byte-array (with-open [out (ByteArrayOutputStream.)]
                            (.write out message-bytes 1 4)
                            (.toByteArray out))
        schema-id (.getInt (ByteBuffer/wrap schema-byte-array))
        number-of-bytes-header 5
        payload-length (- (alength message-bytes) number-of-bytes-header)
        avro-raw-data (with-open [out (ByteArrayOutputStream.)]
                        (.write out message-bytes number-of-bytes-header payload-length)
                        (.toByteArray out))]
    [schema-id avro-raw-data]))

(defn decode
  "Given a full Kafka record (a byte array assumed to be in Confluent's Schema
   Registry's documented wire format), return the contained Avro payload decoded
   according to a preferred, explicit reader schema.

   The result is a Clojure value, typically a map, as most/all of our messages
   are Avro record types."
  ([^SchemaRegistryClient registry record]
   (decode registry record default-subject->reader-schema))
  ([^SchemaRegistryClient registry record subject->reader-schema]
   (when record
     (let [[schema-id avro-raw-data] (split-message-bytes* record)
           writer-schema (.getById registry schema-id)
           subject-name (schema->subject-name writer-schema)
           subject->reader-schema (or subject->reader-schema default-subject->reader-schema)
           reader-schema (subject->reader-schema subject-name)]
       (decode* writer-schema (or reader-schema writer-schema) avro-raw-data)))))

(def ^:const ^:private magic-byte 0)

(defn- encode*
  "Encode is going to create the header of kafka message, put the schema id inside the header and encode the data.
  Byte 0 -> a magic number
  Byte 1-4 -> schema id
  Byte 5-n -> avro object
  https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format"
  [schema-registry-id schema record]
  {:pre [schema]}
  (with-open [byte-array-output-stream (ByteArrayOutputStream.)
              out (DataOutputStream. byte-array-output-stream)]
    (doto out
      (.writeByte magic-byte)
      (.writeInt schema-registry-id))
    (avro/encode schema out record)
    (.toByteArray byte-array-output-stream)))

(defn get-schema-id
  [schema-registry schema]
  (.getId schema-registry (.getFullName schema) schema))

(defn encode [registry schema record]
  ;; null in Kafka has a special meaning for deletion in a topic with the compact retention policy.
  ;; https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
  (when record
    (when-let [schema-registry-id (get-schema-id registry schema)]
      (encode* schema-registry-id schema record))))

(defn serialize-record
  "This is an example for a serialize function.
    - topic: Topic name where the bytes will be written
    - record: Data to serialize
    - runtime-config: Config in the producter-config-updates as 'clojure.kafka-serializer.config'
    - schema: Single schema to serialize"
  [schema]
  (fn [_ record {:keys [runtime-config]}]
    (let [schema-registry (schema-registry-api/make-client (:schema-registry-cfg runtime-config))]
      (encode schema-registry schema record))))

