(ns curbside.beam.datadog.api
  "https://docs.datadoghq.com/developers/metrics/"
  (:refer-clojure :exclude [count dec inc set])
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log])
  (:import
   (com.timgroup.statsd NonBlockingStatsDClient Event Event$AlertType)
   (org.apache.beam.sdk.transforms DoFn$ProcessContext)))

(def ^:private string-array-class
  "Reference to Java's `String[]` type. Used for type annotations."
  (Class/forName "[Ljava.lang.String;"))

(def ^string-array-class empty-tags
  "Null value for Datadog tags."
  (make-array String 0))

(defn- conj-tag
  "Appends a Datadog tag composed of `k` and `v` to `coll`."
  [coll k v]
  (conj coll (str (name k) \: (name v))))

(defn- ^string-array-class coerce-array
  "Converts its argument into a Java String array."
  [coll]
  {:pre [(or (nil? coll)
             (map? coll)
             (every? string? coll))]}
  (cond (instance? string-array-class coll)
        coll

        (or (nil? coll) (empty? coll))
        empty-tags

        (map? coll)
        (into-array String (reduce-kv conj-tag [] coll))

        :else
        (into-array String coll)))

(defn start-client* [{:keys [host port prefix constant-tags]}]
  (try
    (log/infof "Starting the StatsD client on %s:%s with prefix=%s"
               host port prefix)
    (NonBlockingStatsDClient. prefix host port (coerce-array constant-tags))
    (catch Exception e
      (log/error e "Failed to start the StatsD client!"))))

(def start-client
  (memoize start-client*))

(def log-datadog-disabled
  "We memoize because the setup function is called multiple times in beam
  and we don't want to log the same line multiple times for nothing."
  (memoize #(log/warn "Datadog is disabled")))

(defn stop-client [^NonBlockingStatsDClient client]
  (try
    (log/info "Stopping StatsD client...")
    (.stop client)
    (catch Exception e
      (log/error e "Failed to stop StatsD client!"))))

(defn setup-datadog
  "Use this function as the setup function of pardo functions to add datadog support"
  [{:keys [runtime-parameters]}]
  (let [{:keys [datadog-config]} runtime-parameters
        {:keys [enabled]} datadog-config]
    (if (boolean enabled)
      {:datadog-client (start-client datadog-config)}
      (log-datadog-disabled))))

(defn- ^String dash->underscore
  "Converts `-` to `_` in `x`"
  [x]
  (str/replace (name x) \- \_))

(defn gauge-java [datadog metric ^double value tags]
  (when datadog
    (.gauge datadog metric value tags)))

(defn gauge
  ([datadog metric value]
   (gauge datadog metric value empty-tags))
  ([datadog metric value tags]
   (gauge-java datadog (dash->underscore metric) value (coerce-array tags))))

(defn inc
  ([datadog metric]
   (inc datadog metric empty-tags))
  ([datadog metric tags]
   (when datadog
     (.increment datadog (dash->underscore metric) (coerce-array tags)))))

(defn inc-pardo
  "Use this as a pardo function to count the number of elements processed"
  [{:keys [^DoFn$ProcessContext process-context runtime-parameters custom-context]}]
  (let [{:keys [metric tags]} runtime-parameters
        {:keys [datadog-client]} custom-context]
    (inc datadog-client metric tags)
    (.output process-context (.element process-context))))

(defn dec
  ([datadog metric]
   (dec datadog metric empty-tags))
  ([datadog metric tags]
   (when datadog
     (.decrement datadog (dash->underscore metric) (coerce-array tags)))))

(defn count
  ([datadog metric delta]
   (count datadog metric delta empty-tags))
  ([datadog metric ^long delta tags]
   (when datadog
     (.count datadog (dash->underscore metric) delta (coerce-array tags)))))

(defn set
  ([datadog metric value]
   (set datadog metric value empty-tags))
  ([datadog metric value tags]
   (when datadog
     (.recordSetValue datadog (dash->underscore metric) value (coerce-array tags)))))

(defn histogram
  ([datadog metric value]
   (histogram datadog metric value empty-tags))
  ([datadog metric value tags]
   (when datadog
     (.histogram datadog (dash->underscore metric) (double value) (coerce-array tags)))))

(defn distribution
  ([datadog metric value]
   (distribution datadog metric value empty-tags))
  ([datadog metric value tags]
   (when datadog
     (.recordDistributionValue datadog (dash->underscore metric) (double value) (coerce-array tags)))))

(defn timing
  ([datadog metric ms-timestamp]
   (timing datadog metric ms-timestamp empty-tags))
  ([datadog metric ^long ms-timestamp tags]
   (when datadog
     (.recordExecutionTime datadog (dash->underscore metric) ms-timestamp (coerce-array tags)))))

(defmacro timed
  "Times the execution of `body` and report it to `timing`."
  {:style/indent [2]}
  [client metric tags & body]
  `(let [start-time# (System/currentTimeMillis)
         result# (do ~@body)]
     (timing ~client ~metric (- (System/currentTimeMillis) start-time#) ~tags)
     result#))

(defn event*
  "Constructs a Datadog `Event` object."
  [title text type]
  (-> (Event/builder)
      (.withAlertType (Event$AlertType/valueOf (name type)))
      (.withTitle (dash->underscore title))
      (.withText text)
      (.build)))

(defn event
  "Reports an event to Datadog."
  ([client data]
   (event client data empty-tags))
  ([client {:keys [title text type]} tags]
   (event client title text type tags))
  ([client title text type]
   (event client title text type empty-tags))
  ([client title text type tags]
   (.recordEvent ^NonBlockingStatsDClient client
                 (event* title text type)
                 (coerce-array tags))))
