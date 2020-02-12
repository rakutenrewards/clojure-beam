(ns curbside.beam.java.window.calendar-window-test
  (:require
   [clj-time.core :as t]
   [clj-time.format :as f]
   [clojure.test :refer [deftest is testing]]
   [curbside.beam.api :as beam]
   [curbside.beam.testing :as beam.testing])
  (:import
   (curbside.beam.java.window CalendarDayWindowFn CalendarDaySlidingWindowFn)
   (org.apache.beam.sdk.testing PAssert PAssert$IterableAssert)
   (org.apache.beam.sdk.transforms DoFn$ProcessContext)
   (org.apache.beam.sdk.transforms.windowing Window IntervalWindow)
   (org.apache.beam.sdk.values PCollection)
   (org.joda.time Duration DateTime)))

(defn- ->tz [elem] (t/time-zone-for-id (:tz elem)))

(defn- inc-interval-window
  "Increment/slide the window by `n` days."
  [^IntervalWindow window n]
  (IntervalWindow. (.plus (.start window) (Duration/standardDays n))
                   (.plus (.end window) (Duration/standardDays n))))

(defn- set-interval-window-duration
  [^IntervalWindow window n]
  (IntervalWindow. (.start window) (Duration/standardDays n)))

(defn- calendar-window-fn
  [^PCollection pcoll]
  (.apply pcoll (Window/into (CalendarDayWindowFn/forTimezoneFn #'->tz))))

(defn- calendar-sliding-window-fn
  [^PCollection pcoll day-span]
  (.apply pcoll (Window/into (CalendarDaySlidingWindowFn/forSizeInDaysAndTimezoneFn day-span #'->tz))))

(defn- calendar-sliding-window-with-visibility-date-fn
  [^PCollection pcoll day-span ^DateTime visibility-date]
  (.apply
   pcoll
   (Window/into
    (CalendarDaySlidingWindowFn/forSizeInDaysAndTimezoneFnAndVisibilityStart day-span #'->tz visibility-date))))

(defn- ->color [{:keys [^DoFn$ProcessContext process-context]}]
  (.output process-context (:color (.element process-context))))

(def ^:private test-data
  "Our test data includes two timezones (ie, those for New York and Cambodia) and
  some timestamps that reside right on the boundaries between days in these zones."
  [[{:color :red :tz "Asia/Phnom_Penh"} #inst "2019-12-31T17:00:00"] ; 12am Cambodia
   [{:color :orange :tz "America/New_York"} #inst "2020-01-01T05:00:00"] ; 12am NY
   [{:color :yellow :tz "Asia/Phnom_Penh"} #inst "2020-01-01T05:00:00"] ; 12pm Cambodia
   [{:color :green :tz "America/New_York"} #inst "2020-01-01T07:00:00"] ; 12pm NY
   [{:color :blue :tz "Asia/Phnom_Penh"} #inst "2020-01-01T16:59:59"] ; 11:59pm Cambodia
   [{:color :indigo :tz "Asia/Phnom_Penh"} #inst "2020-01-01T17:00:00"] ; 12am Cambodia (next day)
   [{:color :violet :tz "America/New_York"} #inst "2020-01-02T04:59:59"] ; 11:59:59pm NY
   [{:color :pink :tz "America/New_York"} #inst "2020-01-02T05:00:00"] ; 12am NY (next day)
])

(def ^:private new-years-new-york (IntervalWindow. (.toInstant ^DateTime (f/parse "2020-01-01T00:00:00-05:00"))
                                                   (.toInstant ^DateTime (f/parse "2020-01-02T00:00:00-05:00"))))
(def ^:private new-years-cambodia (IntervalWindow. (.toInstant ^DateTime (f/parse "2020-01-01T00:00:00+07:00"))
                                                   (.toInstant ^DateTime (f/parse "2020-01-02T00:00:00+07:00"))))

(deftest test-calendar-window
  (testing "calendar-window"
    (let [pipeline (beam.testing/test-pipeline)
          result (-> pipeline
                     (beam/create-timestamped-pcoll test-data)
                     calendar-window-fn
                     (beam/pardo #'->color)
                     beam.testing/group-and-flatten*)]
      ;; New Year's day in New York:
      (-> result
          (PAssert/that)
          (.inOnTimePane new-years-new-york)
          (.containsInAnyOrder [:orange :violet :green]))
      ;; Day *after* New Year's day in New York:
      (-> result
          (PAssert/that)
          (.inOnTimePane (inc-interval-window new-years-new-york 1))
          (.containsInAnyOrder [:pink]))
      ;; Day *before* New Year's day in New York should be EMPTY:
      (-> result
          (PAssert/that)
          (.inOnTimePane (inc-interval-window new-years-new-york -1))
          (.empty))
      ;; New Year's day in Cambodia:
      (-> result
          (PAssert/that)
          (.inOnTimePane new-years-cambodia)
          (.containsInAnyOrder [:red :yellow :blue]))
      ;; Day *after* New Year's day in Cambodia:
      (-> result
          (PAssert/that)
          (.inOnTimePane (inc-interval-window new-years-cambodia 1))
          (.containsInAnyOrder [:indigo]))
      ;; Day *before* New Year's day in Cambodia should be EMPTY:
      (-> result
          (PAssert/that)
          (.inOnTimePane (inc-interval-window new-years-cambodia -1))
          (.empty))
      (beam/run-pipeline
       pipeline (Duration/standardSeconds 30)))))

(defn- contribute-sliding-window-assertions*
  "Contribute common sliding window assertions; these assertions are very similar
  for some tests so we consolidate it here for ease of reading tests."
  ([^PCollection result] (contribute-sliding-window-assertions* result nil))
  ([^PCollection result ^DateTime visibility-date?]
   (letfn [(add-contains! [^PAssert$IterableAssert assertion
                           ^IntervalWindow win
                           ^Iterable asserted-coll]
             (if (or (nil? visibility-date?) (-> win .end (.isAfter visibility-date?)))
               (-> assertion (.containsInAnyOrder asserted-coll))
               (-> assertion .empty)))]
     ;; For New York, these windows should have the same elements:
     ;;      * the three sequential days starting on New Year's (in New York)
     ;;      * the three sequential days starting on New Year's eve (New York)
     (doseq [win (map #(-> new-years-new-york
                           (inc-interval-window (- %))
                           (set-interval-window-duration 3))
                      (range 2))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:orange :violet :green :pink])))
     ;; New York: three sequential days *ending* on New Year's (in New York)
     (let [win (-> new-years-new-york
                   (inc-interval-window -2)
                   (set-interval-window-duration 3))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:orange :violet :green])))
     ;; New York: three sequential days starting *after* New Year's (in New York)
     (let [win (-> new-years-new-york
                   (inc-interval-window +1)
                   (set-interval-window-duration 3))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:pink])))
     ;; New York: three sequential days starting *two days after* New Year's (in New York)
     ;;  should be EMPTY:
     (let [win (-> new-years-new-york
                   (inc-interval-window +2)
                   (set-interval-window-duration 3))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (.empty)))
     ;; For Cambodia, these windows should have the same elements:
     ;;      * the three sequential days starting on New Year's (in Cambodia)
     ;;      * the three sequential days starting on New Year's eve (Cambodia)
     (doseq [win (map #(-> new-years-cambodia
                           (inc-interval-window (- %))
                           (set-interval-window-duration 3))
                      (range 2))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:red :yellow :blue :indigo])))
     ;; Cambodia: three sequential days *ending* on New Year's (in Cambodia)
     (let [win (-> new-years-cambodia
                   (inc-interval-window -2)
                   (set-interval-window-duration 3))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:red :yellow :blue])))
     ;; Cambodia: three sequential days starting *after* New Year's (in Cambodia)
     (let [win (-> new-years-cambodia
                   (inc-interval-window +1)
                   (set-interval-window-duration 3))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:indigo])))
     ;; Cambodia: three sequential days starting *two days after* New Year's (in Cambodia)
     ;;  should be EMPTY:
     (let [win (-> new-years-cambodia
                   (inc-interval-window +2)
                   (set-interval-window-duration 3))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (.empty))))))

(deftest test-calendar-sliding-window
  (testing "calendar-sliding-window"
    (let [pipeline (beam.testing/test-pipeline)
          result (-> pipeline
                     (beam/create-timestamped-pcoll test-data)
                     (calendar-sliding-window-fn 3)
                     (beam/pardo #'->color)
                     beam.testing/group-and-flatten*)]
      (contribute-sliding-window-assertions* result)
      (beam/run-pipeline pipeline (Duration/standardSeconds 30)))))

(deftest test-calendar-sliding-window-with-visibility-date
  (testing "calendar-sliding-window-with-visibility-date"
    (let [pipeline (beam.testing/test-pipeline)
          visibility-date (-> new-years-cambodia .end .toDateTime)
          result (-> pipeline
                     (beam/create-timestamped-pcoll test-data)
                     (calendar-sliding-window-with-visibility-date-fn 3 visibility-date)
                     (beam/pardo #'->color)
                     beam.testing/group-and-flatten*)]
      (contribute-sliding-window-assertions* result visibility-date)
      (beam/run-pipeline pipeline (Duration/standardSeconds 30)))))
