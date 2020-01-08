(ns curbside.beam.transform.current-trips
  (:require
   [clj-time.format :as time.format]
   [curbside.beam.api :as beam]
   [curbside.beam.datadog.api :as datadog]
   [curbside.reducers.current-trips.current-trips :as current-trips]
   [curbside.reducers.current-trips.current-trips-aggregate :as current-trips-aggregate])
  (:import
   (org.apache.beam.sdk.transforms DoFn$ProcessContext)
   (org.apache.beam.sdk.transforms.windowing Repeatedly AfterPane)))

(def ^:private empty-hash-map (constantly {}))

(defn- latest-current-trips [{acc :accumulator input :input datadog-cfg :runtime-parameters}]
  (current-trips/reducer (datadog/make-client datadog-cfg)
                         acc
                         input))

(defn- trip-status [latest-trip]
  (zipmap (keys latest-trip)
          (->> (vals latest-trip)
               (map :status))))

(defn- emit-current-trips [{:keys [accumulator]}]
  (zipmap (keys accumulator)
          (->> (vals accumulator)
               (map trip-status))))

(defn- aggregate-current-trips [{acc :accumulator input :input}]
  (current-trips-aggregate/reducer acc input))

(defn- emit-redis-command [{:keys [accumulator]}]
  (:commands accumulator))

(defn- expand-coll [{:keys [^DoFn$ProcessContext process-context]}]
  ;; FIXME: Make sure the output the same as Onyx due to special handling of Vector in Onyx
  (doseq [elem (seq (.element process-context))]
    (.output process-context elem)))

(defn- filter-by-event-type [e]
  (contains? #{"ad-dests-start", "ad-notify-estimate" "ad-dests-stop"} (:event-type e)))

(defn- flow-condition [pcoll]
  ;; See curbside.pipeline.tasks.current-trips/flow-condition
  ;; The flow-condition is not used in unit test. See curbside.test.current-trips-pipeline/current-trips-job
  (beam/filter-by #'filter-by-event-type {} pcoll))

(defn- parse-event-ts [{:keys [input]}]
  (update input :event-ts time.format/parse))

(defn- dest-id [{:keys [input]}]
  (:dest-id input))

(defn apply-transform [pcoll {:keys [datadog-cfg] :as _options}]
  (let [windowed-source (beam/global-windows pcoll
                                             {:trigger (Repeatedly/forever (AfterPane/elementCountAtLeast 1))
                                              :with-on-time-behavior :fire-if-not-empty
                                              :accumulation-mode :accumulate})
        current-trips-transform (-> windowed-source
                                    (beam/map-elements #'parse-event-ts)
                                    (beam/with-keys #'dest-id)
                                    (beam/combine-per-key {:combine-fn/add-input #'latest-current-trips
                                                           :combine-fn/create-accumulator #'empty-hash-map
                                                           :combine-fn/extract-output #'emit-current-trips
                                                           :runtime-parameters datadog-cfg})
                                    (beam/values {:step-name "curr-trips-vals"}))
        current-trips-aggregate-task (-> current-trips-transform
                                         (beam/with-keys #'keys)
                                         (beam/combine-per-key {:combine-fn/add-input #'aggregate-current-trips
                                                                :combine-fn/create-accumulator #'empty-hash-map
                                                                :combine-fn/extract-output #'emit-redis-command})
                                         (beam/values {:step-name "curr-trips-aggregate-vals"})
                                         (beam/pardo #'expand-coll))]
    current-trips-aggregate-task))
