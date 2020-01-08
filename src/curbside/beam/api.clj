(ns curbside.beam.api
  (:require
   [camel-snake-kebab.core :as snake-kebab]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [curbside.beam.nippy-coder :as nippy-coder]
   [jsonista.core :as j])
  (:import
   (curbside.beam.java ClojureCombineFn ClojureSerializableFunction ClojureDoFn ClojureStatefulDoFn)
   (java.util Map)
   (org.apache.beam.sdk Pipeline PipelineResult)
   (org.apache.beam.sdk.coders Coder)
   (org.apache.beam.sdk.options PipelineOptionsFactory PipelineOptions)
   (org.apache.beam.sdk.transforms MapElements Values Filter WithKeys Combine ParDo PTransform Create)
   (org.apache.beam.sdk.transforms.windowing Window GlobalWindows Window$OnTimeBehavior)
   (org.apache.beam.sdk.values PCollection PDone PCollectionView)
   (org.joda.time Duration)))

;; Pipelines

(defn ->pipeline-args
  "Reduces a map config to a vector of command-line-esque args, as required by
   Beam option parsing. Camelizes and stringfies arg names in the process; vals
   are converted to strings (e.g., colls to comma-delimeted str, etc.)

   Values at key `:custom-options` are specially treated (rendered as JSON) with
   the assumption that the pipeline will be using `SimpleOptions` class."
  [conf]
  (->> conf
       (keep (fn [[k v]]
               (format "--%s=%s"
                       (-> k name snake-kebab/->camelCase name)
                       (cond
                         (map? v) (j/write-value-as-string v)
                         (coll? v) (str/join "," (map #(str/escape % {\" "\"\""}) v))
                         (number? v) v
                         (boolean? v) v
                         (string? v) (str/escape v {\" "\"\""})
                         :else (println "Unexpected config value type:" k v)))))
       (into [])))

(defn ^Pipeline make-pipeline
  "Constructs a pipeline from the provided command line arguments string.

  Example GNU style command line arguments that can be parsed:

     --project=MyProject (simple property, will set the 'project' property to 'MyProject')
     --readOnly=true (for boolean properties, will set the 'readOnly' property to \"true\")
     --readOnly (shorthand for boolean properties, will set the 'readOnly' property to \"true\")
     --x=1 --x=2 --x=3 (list style simple property, will set the 'x' property to [1, 2, 3])
     --x=1,2,3 (shorthand list style simple property, will set the 'x' property to [1, 2, 3])
     --complexObject='{\"key1\":\"value1\",...} (JSON format for all other complex types)
  "
  ([args]
   (make-pipeline args PipelineOptions))
  ([args as-class]
   (-> (PipelineOptionsFactory/fromArgs (into-array String args))
       (.withValidation)
       (.as as-class)
       (Pipeline/create))))

(defn run-pipeline
  "Runs the Pipeline according to the PipelineOptions used to create the Pipeline. Returns the PipelineResult.

  - p: a pipeline or a PCollection
  "
  [p]
  (if (instance? Pipeline p)
    (.run p)
    (-> p
        (.getPipeline)
        (.run))))

(defn wait-pipeline-result
  "Waits until the pipeline finishes and returns the final status.

  - timeout-ms: The time to wait for the pipeline to finish. Provide a value of nil or less than 1 ms for an infinite wait.
  "
  ([^PipelineResult pipeline-result]
   (wait-pipeline-result pipeline-result nil))
  ([^PipelineResult pipeline-result timeout-ms]
   (some-> (if-not timeout-ms
             (.waitUntilFinish pipeline-result)
             (.waitUntilFinish pipeline-result (Duration/millis timeout-ms)))
           (.name)
           (str/lower-case)
           (keyword))))

;; Transforms

(defn make-step-name [step-name f]
  (name (or step-name (str (symbol f)))))

(defn apply-transform
  [^PCollection pcoll step-name ptransform coder]
  (-> ^PCollection (.apply pcoll step-name ptransform)
      ^PCollection (.setCoder coder)))

(defn create-pcoll
  "Creates a PCollection from a Clojure collection.

  - pipeline: ^Pipiline object which will be used to apply the transformation.
  - coll: A Clojure collection - list, vector, map etc.

  See https://beam.apache.org/releases/javadoc/2.4.0/org/apache/beam/sdk/transforms/Create.html"
  [^Pipeline pipeline coll]
  (cond
    (instance? Map coll)
    (-> pipeline (.apply (Create/of ^Map coll)) (.setCoder (nippy-coder/make-kv-coder)))
    (seqable? coll)
    (-> pipeline (.apply (Create/of ^Iterable (seq coll))) (.setCoder (nippy-coder/make-custom-coder)))))

(def ^:private always-nil (constantly nil))

(def ^:private accumulator :accumulator)

(defn- var-or-nil? [v]
  (or (var? v) (nil? v)))

(defn combine-per-key
  "PTransform that first groups its input PCollection of KVs by keys and windows, then invokes the given function on
  each of the values lists to produce a combined value, and then returns a PCollection of KVs mapping each distinct key
  to its combined value for each window.

  - step-name: Name to identify this specific application of the transform. This name is used in various places,
    including the monitoring UI, logging, and to stably identify this application node in the job graph.
  - runtime-parameters: A serializable map. It specifies values that are only available during pipeline execution.
  - add-input: Adds the given input value to the given accumulator. returning the new accumulator value.
    Example
      ```
      (defn add-input [{:keys [accumulator input runtime-parameters]}]
        ...)
      ```

  - create-accumulator: Returns a new, mutable accumulator value, representing the accumulation of zero input values.
    Example
      ```
      (defn create-accumulator [{:keys [runtime-parameters]}]
        ...)
      ```

  - extract-output: Returns the output value that is the result of combining all the input values represented by the given accumulator.
    Default to :accumulator.
    Example
      ```
      (defn extract-output [{:keys [accumulator runtime-parameters]}]
        ...)
      ```

  - merge-accumulators: Returns an accumulator representing the accumulation of all the input values accumulated in the merging accumulators.
    Example
      ```
      (defn merge-accumulators [{:keys [accumulator-coll runtime-parameters]}]
        ...)
      ```

  add-input, extract-output, merge-accumulators, and create-accumulator correspond to methods
  of the org.apache.beam.sdk.transforms.Combine.CombineFn interface.

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/Combine.html
  "
  [^PCollection pcoll {:keys [step-name runtime-parameters] :combine-fn/keys [add-input extract-output merge-accumulators create-accumulator]}]
  {:pre [(var? add-input) (var? create-accumulator) (var-or-nil? merge-accumulators) (var-or-nil? extract-output)]}
  (let [extract-output (or extract-output #'accumulator)]
    (apply-transform pcoll
                     (make-step-name step-name add-input)
                     (Combine/perKey (ClojureCombineFn. {"createAccumulatorFn" create-accumulator
                                                         "addInputFn" add-input
                                                         "mergeAccumulatorsFn" merge-accumulators
                                                         "extractOutputFn" extract-output}
                                                        runtime-parameters))
                     (nippy-coder/make-kv-coder))))

(defn ^PTransform make-composite-transform
  ([xform] (make-composite-transform nil xform))
  ([name xform]
   (proxy [PTransform] [name]
     (expand [pcoll-or-pipeline] (xform pcoll-or-pipeline)))))

(defn apply-composite-transform
  "Applies a composite transform given a transformation function
   `xform` (PCollection -> PCollection).

  See https://beam.apache.org/documentation/programming-guide/#composite-transforms
  "
  [pipeline-or-pcoll name xform]
  (.apply pipeline-or-pcoll (make-composite-transform name xform)))

(defn filter-by
  "PTransform that takes an input PCollection<T> and returns a PCollection<T> with elements that satisfy the given predicate.

  - step-name: Name to identify this specific application of the transform. This name is used in various places,
    including the monitoring UI, logging, and to stably identify this application node in the job graph.
  - runtime-parameters: A serializable map. It specifies values that are only available during pipeline execution.
  - predicate: Takes a map with keys [:input :runtime-parameters] and returns a boolean.
               Caution: you should explicitly wrap your returned value in a `boolean` to
               avoid returning nil from Clojure logic functions.
    Example
      ```
      (defn predicate [{:keys [input runtime-parameters]}]
        ...)
      ```

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/Filter.html
  "
  [^PCollection pcoll predicate {:keys [step-name runtime-parameters]}]
  {:pre [(var? predicate)]}
  (apply-transform pcoll
                   (make-step-name step-name predicate)
                   (Filter/by (ClojureSerializableFunction. predicate runtime-parameters))
                   (nippy-coder/make-custom-coder)))

(defn map-elements
  "PTransforms for mapping a simple function over the elements of a PCollection.

  - step-name: Name to identify this specific application of the transform. This name is used in various places,
    including the monitoring UI, logging, and to stably identify this application node in the job graph.
  - runtime-parameters: A serializable map. It specifies values that are only available during pipeline execution.
  - f: Function to be applied for every element of the PCollection.
    Example
      ```
      (defn f [{:keys [input runtime-parameters]}]
        ...)
      ```

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/MapElements.html
  "
  ([^PCollection pcoll f]
   (map-elements pcoll f {}))
  ([^PCollection pcoll f {:keys [step-name runtime-parameters coder]}]
   {:pre [(var? f)]}
   (let [^Coder coder (or coder (nippy-coder/make-custom-coder))
         map-elements-transform (-> (MapElements/into (.getEncodedTypeDescriptor coder))
                                    (.via (ClojureSerializableFunction. f runtime-parameters)))]
     (apply-transform pcoll (make-step-name step-name f) map-elements-transform coder))))

(defn pardo
  "PTransform that will invoke the given DoFn function.

  - step-name: Name to identify this specific application of the transform. This name is used in various places,
    including the monitoring UI, logging, and to stably identify this application node in the job graph.
  - runtime-parameters: A serializable map. It specifies values that are only available during pipeline execution.
  - process-element: Function to be applied for every element of the PCollection.
    Example
      ```
      (defn process-element [{:keys [process-context runtime-parameters]}]
        ...)
      ```

  - setup: Function to prepare an instance for processing bundles of elements. This is a good place to initialize
    transient in-memory resources, such as network connections. The resources can then be disposed in teardown.
    Example
      ```
      (defn setup [{:keys [runtime-parameters]}]
        ...)
      ```

  - teardown: Function to clean up an instance before it is discarded. No other method will be called after a call to this function.
    Example
      ```
      (defn teardown [{:keys [runtime-parameters]}]
        ...)
      ```

  - start-bundle: Function to prepare an instance for processing a batch of elements.
    Example
      ```
      (defn start-bundle [{:keys [runtime-parameters]}]
        ...)
      ```

  - finish-bundle: Function to finish processing a batch of elements.
    Example
      ```
      (defn finish-bundle [{:keys [runtime-parameters]}]
        ...)
      ```

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/ParDo.html
  "
  ([^PCollection pcoll process-element]
   (pardo pcoll process-element {}))
  ([^PCollection pcoll process-element {:keys [step-name runtime-parameters side-inputs coder]
                                        :do-fn/keys [setup teardown start-bundle finish-bundle]}]
   {:pre [(var? process-element) (var-or-nil? setup) (var-or-nil? teardown) (var-or-nil? start-bundle) (var-or-nil? finish-bundle)]}
   ;; TODO: support side-input, tags, multiple output
   (let [setup (or setup #'always-nil)
         teardown (or teardown #'always-nil)
         start-bundle (or start-bundle #'always-nil)
         finish-bundle (or finish-bundle #'always-nil)]
     (apply-transform pcoll
                      (make-step-name step-name process-element)
                      (cond-> (ParDo/of (ClojureDoFn. {"processElementFn" process-element
                                                       "setupFn" setup
                                                       "teardownFn" teardown
                                                       "startBundleFn" start-bundle
                                                       "finishBundleFn" finish-bundle}
                                                      runtime-parameters
                                                      side-inputs))
                        (not-empty side-inputs) (.withSideInputs (into-array PCollectionView (vals side-inputs))))
                      (cond
                        (= coder ::inherit) (.getCoder pcoll)
                        (some? coder) coder
                        :else (nippy-coder/make-custom-coder))))))

(defn stateful-pardo
  "PTransform that will invoke the given DoFn function.

  - step-name: Name to identify this specific application of the transform. This name is used in various places,
    including the monitoring UI, logging, and to stably identify this application node in the job graph.
  - runtime-parameters: A serializable map. It specifies values that are only available during pipeline execution.
  - dofn-state: An object of type ValueState<IPersistentMap>. The state of the current pardo.
  - process-element: Function to be applied for every element of the PCollection.
    Example
      ```
      (defn process-element [{:keys [process-context runtime-parameters dofn-state]}]
        ...)
      ```

  - setup: Function to prepare an instance for processing bundles of elements. This is a good place to initialize
    transient in-memory resources, such as network connections. The resources can then be disposed in teardown.
    Example
      ```
      (defn setup [{:keys [runtime-parameters]}]
        ...)
      ```

  - teardown: Function to clean up an instance before it is discarded. No other method will be called after a call to this function.
    Example
      ```
      (defn teardown [{:keys [runtime-parameters]}]
        ...)
      ```

  - start-bundle: Function to prepare an instance for processing a batch of elements.
    Example
      ```
      (defn start-bundle [{:keys [runtime-parameters]}]
        ...)
      ```

  - finish-bundle: Function to finish processing a batch of elements.
    Example
      ```
      (defn finish-bundle [{:keys [runtime-parameters]}]
        ...)
      ```

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/ParDo.html
  "
  ([^PCollection pcoll process-element]
   (stateful-pardo pcoll process-element {}))
  ([^PCollection pcoll process-element {:keys [step-name runtime-parameters side-inputs coder]
                                        :do-fn/keys [setup teardown start-bundle finish-bundle]}]
   {:pre [(var? process-element) (var-or-nil? setup) (var-or-nil? teardown) (var-or-nil? start-bundle) (var-or-nil? finish-bundle)]}
   ;; TODO: support side-input, tags, multiple output
   (let [setup (or setup #'always-nil)
         teardown (or teardown #'always-nil)
         start-bundle (or start-bundle #'always-nil)
         finish-bundle (or finish-bundle #'always-nil)]
     (apply-transform pcoll
                      (make-step-name step-name process-element)
                      (cond-> (ParDo/of (ClojureStatefulDoFn. {"processElementFn" process-element
                                                               "setupFn" setup
                                                               "teardownFn" teardown
                                                               "startBundleFn" start-bundle
                                                               "finishBundleFn" finish-bundle}
                                                              runtime-parameters
                                                              side-inputs))
                        (not-empty side-inputs) (.withSideInputs (into-array PCollectionView (vals side-inputs))))
                      (cond
                        (= coder ::inherit) (.getCoder pcoll)
                        (some? coder) coder
                        :else (nippy-coder/make-kv-coder))))))

(defn pdone
  "PDone is the output of a PTransform that has a trivial result, such as a WriteFiles.

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/values/PDone.html
  "
  [^PCollection pcoll]
  (PDone/in (.getPipeline pcoll)))

(defn values
  "Values<V> takes a PCollection of KV<K, V>s and returns a PCollection<V> of the values.

  - step-name: Name to identify this specific application of the transform. This name is used in various places,
  including the monitoring UI, logging, and to stably identify this application node in the job graph.

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/Values.html
  "
  ([^PCollection pcoll]
   (values pcoll {}))
  ([^PCollection pcoll {:keys [step-name]}]
   (apply-transform pcoll
                    (make-step-name step-name #'values)
                    (Values/create)
                    (nippy-coder/make-custom-coder))))

(defn with-keys
  "PTransform that takes a PCollection<V> and returns a PCollection<KV<K, V>>, where each of the values in
  the input PCollection has been paired with a key computed from the value by invoking the given SerializableFunction.

  - step-name: Name to identify this specific application of the transform. This name is used in various places,
    including the monitoring UI, logging, and to stably identify this application node in the job graph.
  - runtime-parameters: A serializable map. It specifies values that are only available during pipeline execution.
  - f: Function to be applied.
    Example
      ```
      (defn f [{:keys [input runtime-parameters]}]
        ...)
      ```

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/WithKeys.html
  "
  ([^PCollection pcoll f]
   (with-keys pcoll f {}))
  ([^PCollection pcoll f {:keys [step-name runtime-parameters]}]
   {:pre [(var? f)]}
   (apply-transform pcoll
                    (make-step-name step-name f)
                    (WithKeys/of (ClojureSerializableFunction. f runtime-parameters))
                    (nippy-coder/make-kv-coder))))

;; Windows

(defn- set-window-accumulation-mode [^Window window-transform mode]
  (case mode
    :accumulate (.accumulatingFiredPanes window-transform)
    :discard (.discardingFiredPanes window-transform)))

(defn- set-window-on-time-behavior [^Window window-transform mode]
  (case mode
    :fire-if-not-empty (.withOnTimeBehavior window-transform Window$OnTimeBehavior/FIRE_IF_NON_EMPTY)
    :fire-always (.withOnTimeBehavior window-transform Window$OnTimeBehavior/FIRE_IF_NON_EMPTY)))

(defn global-windows
  "Window PTransform that assigns all data to the same window.

  - trigger: Adds a Trigger to the Window.
  - with-allowed-lateness: Allow late data. Mandatory for custom trigger.
  - accumulation-mode: Accumulate mode when a Trigger is fired (:accumulate or :discard).
  - with-on-time-behavior: Control whether to output an empty on-time pane when a window is closed (:fire-always or :fire-if-not-empty).

  Example
    ```
    (global-windows pcoll {:trigger (Repeatedly/forever (AfterPane/elementCountAtLeast 1))
                           :with-allowed-lateness Duration/ZERO
                           :accumulation-mode :accumulate
                           :with-on-time-behavior :fire-if-not-empty})
    ```

  See https://beam.apache.org/releases/javadoc/2.9.0/index.html?org/apache/beam/sdk/transforms/windowing/GlobalWindows.html"
  [^PCollection pcoll {:keys [trigger with-allowed-lateness accumulation-mode with-on-time-behavior]}]
  (let [window-transform (cond-> (Window/into (GlobalWindows.))
                           accumulation-mode ^Window (set-window-accumulation-mode accumulation-mode)
                           with-allowed-lateness ^Window (.withAllowedLateness with-allowed-lateness)
                           with-on-time-behavior ^Window (set-window-on-time-behavior with-on-time-behavior)
                           trigger ^Window (.triggering trigger))]
    (.apply pcoll window-transform)))
