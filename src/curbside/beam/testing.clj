(ns curbside.beam.testing
  (:require
   [curbside.beam.api :as beam]
   [curbside.beam.nippy-coder :as nippy-coder])
  (:import
   (curbside.beam.java ClojureSerializableFunction)
   (java.util UUID)
   (org.apache.beam.sdk Pipeline)
   (org.apache.beam.sdk.coders IterableCoder)
   (org.apache.beam.sdk.schemas.transforms Group)
   (org.apache.beam.sdk.testing TestStream TestStream$Builder TestPipeline PAssert PAssert$IterableAssert)
   (org.apache.beam.sdk.transforms Flatten)
   (org.apache.beam.sdk.values PCollection)))

(defn test-pipeline []
  (-> (TestPipeline/create)
      (.enableAbandonedNodeEnforcement false)))

(defn test-stream [^Pipeline pipeline coll {:keys [new-watermark-instant-fn]}]
  (let [coder (nippy-coder/make-custom-coder)
        ptransform (-> (reduce (fn [^TestStream$Builder ts-builder elm]
                                 ;; TODO: add more options that TestStream support
                                 (cond-> ts-builder
                                   new-watermark-instant-fn (.advanceWatermarkTo (new-watermark-instant-fn elm))
                                   :always (.addElements elm (to-array []))))
                               (TestStream/create coder)
                               ;; Excluding nils here allows easy (comment)ing out
                               ;; of test data while creating and analyzing test runs:
                               (filter some? coll))
                       ^TestStream (.advanceWatermarkToInfinity))]
    (.apply pipeline ptransform)))

(def ^:private pcollection-element-by-uid (atom {}))

(defn- get-and-clear-pcollection-element [uid]
  (let [result (get @pcollection-element-by-uid uid)]
    ;; clean data in memory
    (swap! pcollection-element-by-uid dissoc uid)
    result))

(defn- set-pcollection-element [{input :input {:keys [uid]} :runtime-parameters}]
  (swap! pcollection-element-by-uid assoc uid input))

(defn run-and-wait-pipeline [timeout-ms ^PCollection pcoll]
  (-> (beam/run-pipeline pcoll)
      (beam/wait-pipeline-result timeout-ms)))

(defn get-pcollection-element
  ([^PCollection pcoll] (get-pcollection-element pcoll 10000))
  ([^PCollection pcoll timeout-ms]
   (let [uid (UUID/randomUUID)]
     (-> (PAssert/that pcoll)
         ;; TODO: provide more options that PAssert support
         (.satisfies (ClojureSerializableFunction. #'set-pcollection-element {:uid uid})))
     (run-and-wait-pipeline timeout-ms pcoll)
     (get-and-clear-pcollection-element uid))))

(defn ^PCollection group-and-flatten*
  "Helpful for testing; globally groups then flattens elements; going through a
  grouping operation like this ensures that each element lands in a fired pane;
  in turn, this allows for test assertions to be made using `.inOnTimePane` etc."
  [^PCollection pcoll]
  (-> pcoll
      ^PCollection (.apply (Group/globally))
      (.setCoder (IterableCoder/of (nippy-coder/make-custom-coder)))
      (.apply (Flatten/iterables))))

(defn ^PAssert$IterableAssert assert-that* [^PCollection pcoll ^String reason]
  (PAssert/that reason pcoll))
