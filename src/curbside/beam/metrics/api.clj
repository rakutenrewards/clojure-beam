(ns curbside.beam.metrics.api
  (:import
   (org.apache.beam.sdk.metrics Metrics Counter Distribution Gauge)))

;; This namespace is a wrapper around the beam metrics API
;; See https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/metrics/Metrics.html

(defn- counter*
  "Returns a counter

  - namespace: This ideally would be the name of the package and/or the name of
    the job where counter is used.
  - name: Custom name of the counter to identify in the dashboards.

  See https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/metrics/Counter.html"
  ([^String namespace ^String name]
   {:pre [(string? name) (string? namespace)]}
   (Metrics/counter namespace name)))

(defn- distribution*
  "Returns a distribution metric - https://docs.datadoghq.com/graphing/metrics/distributions/

  - namespace: This ideally would be the name of the package and/or the name of
  the job where the distribution is used.
  - name: Custom name of the distribution to identify in the dashboards.

  See https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/metrics/Counter.html"
  ([^String namespace ^String name]
   {:pre [(string? name) (string? namespace)]}
   (Metrics/distribution namespace name)))

(defn- gauge*
  "Returns a gauge metric - https://prometheus.io/docs/concepts/metric_types/#gauge

  - namespace: This ideally would be the name of the package and/or the name of
  the job where gauge is used.
  - name: Custom name of the gauge to identify in the dashboards.

  See https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/metrics/Counter.html"
  ([^String namespace ^String name]
   {:pre [(string? name) (string? namespace)]}
   (Metrics/gauge namespace name)))

(def ^Counter counter (memoize counter*))

(def ^Distribution distribution (memoize distribution*))

(def ^Gauge gauge (memoize gauge*))
