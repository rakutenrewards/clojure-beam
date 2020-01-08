(ns curbside.beam.examples.word-count.pipeline
  (:require
   [clojure.spec.alpha :as spec]
   [clojure.string :as str]
   [curbside.beam.api :as beam])
  (:import
   (curbside.beam.examples.word_count WordCountOptions)
   (org.apache.beam.sdk.io TextIO)
   (org.apache.beam.sdk.transforms DoFn$ProcessContext)
   (org.apache.beam.sdk.values KV)))

(defn- extract-words [{:keys [^DoFn$ProcessContext process-context]}]
  (let [words (-> process-context
                  (.element)
                  (str/split #"[^\p{L}]+"))]
    (doseq [word words :when (not-empty word)]
      (.output process-context word))))

(defn- format-as-text [{:keys [^KV input]}]
  (str (.getKey input) ": " (.getValue input)))

;; -- count-words-combiner --

(defn- count-words-key-fn [{:keys [input]}] input)

(defn- count-words-accumulator [& _] 0)
(defn- count-words-add-input [{acc :accumulator}] (inc acc))
(defn- count-words-extract-output [{acc :accumulator}] acc)
(defn- count-words-merge-accumulators [{accs :accumulator-coll}] (reduce + accs))

(def ^:private count-words-combiner
  {:combine-fn/create-accumulator #'count-words-accumulator
   :combine-fn/add-input #'count-words-add-input
   :combine-fn/extract-output #'count-words-extract-output
   :combine-fn/merge-accumulators #'count-words-merge-accumulators})

(def count-words-transform
  (fn [pcoll]
    (-> pcoll
        (beam/pardo #'extract-words)
        (beam/with-keys #'count-words-key-fn)
        (beam/combine-per-key count-words-combiner))))

(defn- build-pipeline! [pipeline pipeline-opts]
  (-> pipeline
      (.apply "read-lines"
              (-> (TextIO/read)
                  (.from (.getInputFile pipeline-opts))))
      (beam/apply-composite-transform "count-words" count-words-transform)
      (beam/map-elements #'format-as-text)
      (.apply "write-counts"
              (-> (TextIO/write)
                  (.to (.getOutput pipeline-opts))))))

;; -- public --

(spec/def :word-count/input-file string?)
(spec/def :word-count/output string?)
(spec/def :word-count/custom-pipeline-config
  (spec/keys :req-un [:word-count/input-file :word-count/output]))

(defn create-pipeline
  [pipeline-args]
  (let [pipeline (beam/make-pipeline pipeline-args WordCountOptions)
        pipeline-opts (.getOptions pipeline)]
    (doto pipeline
      (build-pipeline! pipeline-opts))))
