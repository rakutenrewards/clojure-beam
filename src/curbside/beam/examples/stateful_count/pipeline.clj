(ns curbside.beam.examples.stateful-count.pipeline
  (:require
   [clj-time.format :as time]
   [clojure.spec.alpha :as spec]
   [clojure.tools.logging :as log]
   [curbside.beam.api :as beam])
  (:import
   (curbside.beam.java ClojureSerializableFunction)
   (org.apache.beam.sdk.state ValueState)
   (org.apache.beam.sdk.transforms WithTimestamps DoFn$ProcessContext)
   (org.apache.beam.sdk.transforms.windowing Repeatedly AfterPane Window FixedWindows)
   (org.apache.beam.sdk.values KV)
   (org.joda.time Duration Instant)))

(def sample-data
  [{:dest-id 1 :event-ts-ms (-> "2020-01-01T01:01:01Z" (time/parse) (.getMillis))}
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:01:01Z" (time/parse) (.getMillis))}
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:01:01Z" (time/parse) (.getMillis))}
   ;; 20mn later
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:21:01Z" (time/parse) (.getMillis))}
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:21:11Z" (time/parse) (.getMillis))}
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:21:21Z" (time/parse) (.getMillis))}
   ;; more 20mn later
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:41:01Z" (time/parse) (.getMillis))}
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:41:01Z" (time/parse) (.getMillis))}
   {:dest-id 1 :event-ts-ms (-> "2020-01-01T01:41:01Z" (time/parse) (.getMillis))}])

(defn- ^Instant event-ts-inst* [{:keys [input]}]
  (Instant. ^long (:event-ts-ms input)))

(defn- select-keys* [{:keys [input runtime-parameters]}]
  (let [{:keys [keyseq]} runtime-parameters]
    (select-keys input keyseq)))

(defn- peek* [{:keys [^DoFn$ProcessContext process-context runtime-parameters]}]
  (let [seg (.element process-context) prefix (or (:prefix runtime-parameters) "log")]
    (log/infof "[%s] seg = %s @ %s ~ %s" prefix seg (.timestamp process-context) (.pane process-context))
    (.output process-context seg)))

(defn- count-element* [{:keys [^DoFn$ProcessContext process-context ^ValueState dofn-state]}]
  (let [element ^KV (.element process-context)
        k (.getKey element)
        state (.read dofn-state)]
    (.write dofn-state (update state k (fnil inc 0)))
    (.output process-context (KV/of k (.read dofn-state)))))

(defn- build-pipeline!
  "Count the number of elements per window by using stateful pardo"
  [pipeline]
  (-> pipeline
      (beam/create-pcoll sample-data)
      (.apply (WithTimestamps/of (ClojureSerializableFunction. #'event-ts-inst* {})))

      ;; Global window is working as expected, the following is output
      ;2019-12-04T10:51:21.876Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:21.879Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 5}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:21.881Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 7}} @ 2020-01-01T01:21:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:21.877Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 4}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:21.876Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 2}} @ 2020-01-01T01:21:11.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:21.880Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 6}} @ 2020-01-01T01:21:21.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:21.876Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 3}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:21.884Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 8}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T10:51:26.883Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 9}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      #_(beam/global-windows {:trigger (Repeatedly/forever (AfterPane/elementCountAtLeast 1))
                              :with-allowed-lateness Duration/ZERO
                              :accumulation-mode :accumulate})

      ;; Fixed window is working as expected, grouping into 2 windows(6 elements and 3 elements window), the following is output
      ;2019-12-04T11:07:36.620Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 6}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:36.613Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:21:11.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:36.615Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 2}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:36.616Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 5}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:36.614Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:36.614Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 3}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:36.614Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 4}} @ 2020-01-01T01:21:21.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:36.614Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 2}} @ 2020-01-01T01:21:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:07:41.627Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 3}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      (.apply "fixed-windows*" (-> (Window/into (FixedWindows/of (Duration/standardMinutes 30)))
                                   (.triggering (Repeatedly/forever (AfterPane/elementCountAtLeast 1)))
                                   (.withAllowedLateness (Duration/ZERO))
                                   (.accumulatingFiredPanes)))

      ;; Sessions window is not working as expected, grouping into one window(9 elements), the following is output
      ;2019-12-04T11:11:40.725Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 3}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:40.717Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 2}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:40.723Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 3}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:40.718Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:21:11.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:40.717Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:40.719Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 2}} @ 2020-01-01T01:01:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:40.717Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:41:01.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:40.720Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:21:21.000Z ~ PaneInfo.NO_FIRING
      ;2019-12-04T11:11:45.727Z [log] seg = KV{{:dest-id 1}, {{:dest-id 1} 1}} @ 2020-01-01T01:21:01.000Z ~ PaneInfo.NO_FIRING
      ;
      ; DataflowRunner does not currently support state or timers with merging windows
      #_(.apply "session-windows*" (-> (Window/into (Sessions/withGapDuration (Duration/standardDays 1)))
                                       (.triggering (Repeatedly/forever (AfterPane/elementCountAtLeast 1)))
                                       (.withAllowedLateness (Duration/ZERO))
                                       (.accumulatingFiredPanes)))

      (beam/with-keys #'select-keys* {:step-name "with-keys-dest-id*" :runtime-parameters {:keyseq [:dest-id]}})
      (beam/stateful-pardo #'count-element*)
      (beam/pardo #'peek*)))

(spec/def ::custom-pipeline-config map?)

(defn create-pipeline [pipeline-args]
  (let [pipeline (beam/make-pipeline pipeline-args)]
    (build-pipeline! pipeline)))
