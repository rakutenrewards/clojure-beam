(ns curbside.beam.schema-registry-util
  (:require
   [abracad.avro :as avro]
   [abracad.helpers.schema :as schema]
   [curbside.beam.test-env :as env]
   [curbside.beam.utils.avro :as avro-utils]
   [curbside.beam.utils.kafka-test :as kafka]
   [curbside.common.avro-schemas.core :as core]))

(def ^:private identity-map-capacity 1000)

(def schema-registry-client
  (kafka/registry-client env/registry-url identity-map-capacity))

(defn register-schemas
  "Register provided schemas in schema registry.

   Schemas are no longer auto-registered since all schemas are registered at schema release-time.
   See https://github.com/RakutenReady/curbside-avro-schemas/tree/master#using-schemas-locally"
  [edn-schema-parsed-coll]
  (doseq [schema edn-schema-parsed-coll]
    (try
      ;; First, ensure compat checking by the SR is disabled:
      (.updateCompatibility schema-registry-client nil "NONE")
      (.register schema-registry-client (.getFullName schema) schema)
      (catch Exception e
        (throw (ex-info "failed to register %s/%s with schema registry" {:schema schema} e)))
      ;; Ensure compat checking by the SR is restored to FULL:
      (finally
        (.updateCompatibility schema-registry-client nil "FULL")))))

(defn register-schemas-latest
  "Register latest version of default schemas in schema registry.

   Schemas are no longer auto-registered since all schemas are registered at schema release-time.
   See https://github.com/RakutenReady/curbside-avro-schemas/tree/master#using-schemas-locally"
  []
  (register-schemas (conj (vals avro-utils/default-subject->reader-schema)
                          core/string-edn-v1-parsed
                          (avro/parse-schema schema/uuid))))
