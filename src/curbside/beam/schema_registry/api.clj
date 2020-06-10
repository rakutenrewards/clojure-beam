(ns curbside.beam.schema-registry.api
  (:require
   [clojure.tools.logging :as log])
  (:import
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient)
   (io.confluent.kafka.schemaregistry.client.rest RestService)))

(defn- make-client- [{:keys [url cache-capacity auth] :as _schema-registry-cfg}]
  (let [restClient (RestService. ^String url)]
    (CachedSchemaRegistryClient. ^RestService restClient ^int cache-capacity auth)))

(def make-client (memoize make-client-))
