(ns curbside.beam.test-env
  (:require
   [clojure.string :as string]))

(def registry-url
  (or (System/getenv "CS_FLOW_KAFKA_SCHEMA_REGISTRY_URL")
      "http://localhost:8081"))

(def -kafka-bootstrap-servers-env (or (System/getenv "CS_FLOW_KAFKA_BOOTSTRAP_SERVERS")
                                      "127.0.0.1:29092"))

(def kafka-bootstrap-servers (string/split -kafka-bootstrap-servers-env #","))

(def redis (str "redis://" (or (System/getenv "CS_FLOW_REDIS") "127.0.0.1:6380")))
