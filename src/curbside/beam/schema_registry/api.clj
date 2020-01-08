(ns curbside.beam.schema-registry.api
  (:import
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient)))

(defn- make-client- [{:keys [url cache-capacity] :as _schema-registry-cfg}]
  (CachedSchemaRegistryClient. ^String url ^int cache-capacity))

(def make-client (memoize make-client-))
