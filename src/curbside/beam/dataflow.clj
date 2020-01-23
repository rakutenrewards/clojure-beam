(ns curbside.beam.dataflow
  (:require
   [clojure.spec.alpha :as s]
   [clojure.string :as str]))

(defn gs-url
  [bucket child]
  (format "gs://%s/%s" (-> bucket (str/replace #"^gs:/*" "") (str/replace #"/$" "")) child))

;; -- Dataflow core config spec --
;;  see https://cloud.google.com/dataflow/docs/guides/specifying-exec-params

(s/def :dataflow-exec/app-name string?)
(s/def :dataflow-exec/runner (partial = "DataflowRunner"))
(s/def :dataflow-exec/region string?)
(s/def :dataflow-exec/gcp-temp-location string?)
(s/def :dataflow-exec/network string?)
(s/def :dataflow-exec/subnetwork string?)
(s/def :dataflow-exec/project string?)
(s/def :dataflow-exec/files-to-stage (s/coll-of string?))
(s/def :dataflow-exec/num-workers integer?)
(s/def :dataflow-exec/max-num-workers integer?)
(s/def :dataflow-exec/enable-streaming-engine (s/nilable boolean?))
(s/def :dataflow-exec/update (s/nilable boolean?))

;; -- Dataflow profiling
;; See https://medium.com/google-cloud/profiling-dataflow-pipelines-ddbbef07761d
(s/def :dataflow-profiling/APICurated boolean?)
(s/def :dataflow-exec/profiling-agent-configuration
  (s/keys :req-un [:dataflow-profiling/APICurated]))

(s/def :dataflow-exec/core-pipeline-config
  (s/keys :req-un [:dataflow-exec/app-name :dataflow-exec/runner :dataflow-exec/region
                   :dataflow-exec/subnetwork
                   :dataflow-exec/project :dataflow-exec/files-to-stage]
          :opt-un [:dataflow-exec/update :dataflow-exec/network :dataflow-exec/gcp-temp-location
                   :dataflow-exec/num-workers :dataflow-exec/max-num-workers
                   :dataflow-exec/profiling-agent-configuration :dataflow-exec/enable-streaming-engine]))
