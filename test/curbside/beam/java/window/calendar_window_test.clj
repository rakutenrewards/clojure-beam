(ns curbside.beam.java.window.calendar-window-test
  (:require
   [clj-time.core :as t]
   [clj-time.format :as f]
   [clojure.test :refer [deftest is testing]]
   [curbside.beam.api :as beam]
   [curbside.beam.testing :as beam.testing]
   [clojure.tools.logging :as log])
  (:import
   (curbside.beam.java.window CalendarDayWindowFn CalendarDaySlidingWindowFn)
   (org.apache.beam.sdk.testing PAssert PAssert$IterableAssert)
   (org.apache.beam.sdk.transforms DoFn$ProcessContext)
   (org.apache.beam.sdk.transforms.windowing Window IntervalWindow BoundedWindow)
   (org.apache.beam.sdk.values PCollection)
   (org.joda.time Duration DateTime DateTimeZone)))

(defn- ->tz [elem] (t/time-zone-for-id (:tz elem)))

(defn- inc-interval-window
  "Increment/slide the window by `n` days."
  [^IntervalWindow window n ^DateTimeZone tz]
  (IntervalWindow.
    (.toInstant (.plusDays (.toDateTime (.start window) tz) n))
    (.toInstant (.plusDays (.toDateTime (.end window) tz) n))))

(defn- set-interval-window-duration
  [^IntervalWindow window n ^DateTimeZone tz]
  (IntervalWindow. (.start window)
    (.toInstant (.plusDays (.toDateTime (.start window) tz) n))))

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

(def ^:private new-york-time-zone (DateTimeZone/forID "America/New_York"))
(def ^:private cambodia-time-zone (DateTimeZone/forID "Asia/Phnom_Penh"))

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
          (.inOnTimePane (inc-interval-window new-years-new-york 1 new-york-time-zone))
          (.containsInAnyOrder [:pink]))
      ;; Day *before* New Year's day in New York should be EMPTY:
      (-> result
          (PAssert/that)
          (.inOnTimePane (inc-interval-window new-years-new-york -1 new-york-time-zone))
          (.empty))
      ;; New Year's day in Cambodia:
      (-> result
          (PAssert/that)
          (.inOnTimePane new-years-cambodia)
          (.containsInAnyOrder [:red :yellow :blue]))
      ;; Day *after* New Year's day in Cambodia:
      (-> result
          (PAssert/that)
          (.inOnTimePane (inc-interval-window new-years-cambodia 1 cambodia-time-zone))
          (.containsInAnyOrder [:indigo]))
      ;; Day *before* New Year's day in Cambodia should be EMPTY:
      (-> result
          (PAssert/that)
          (.inOnTimePane (inc-interval-window new-years-cambodia -1 cambodia-time-zone))
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
                           (inc-interval-window (- %) new-york-time-zone)
                           (set-interval-window-duration 3 new-york-time-zone))
                      (range 2))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:orange :violet :green :pink])))
     ;; New York: three sequential days *ending* on New Year's (in New York)
     (let [win (-> new-years-new-york
                   (inc-interval-window -2 new-york-time-zone)
                   (set-interval-window-duration 3 new-york-time-zone))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:orange :violet :green])))
     ;; New York: three sequential days starting *after* New Year's (in New York)
     (let [win (-> new-years-new-york
                   (inc-interval-window +1 new-york-time-zone)
                   (set-interval-window-duration 3 new-york-time-zone))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:pink])))
     ;; New York: three sequential days starting *two days after* New Year's (in New York)
     ;;  should be EMPTY:
     (let [win (-> new-years-new-york
                   (inc-interval-window +2 new-york-time-zone)
                   (set-interval-window-duration 3 new-york-time-zone))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (.empty)))
     ;; For Cambodia, these windows should have the same elements:
     ;;      * the three sequential days starting on New Year's (in Cambodia)
     ;;      * the three sequential days starting on New Year's eve (Cambodia)
     (doseq [win (map #(-> new-years-cambodia
                           (inc-interval-window (- %) cambodia-time-zone)
                           (set-interval-window-duration 3 cambodia-time-zone))
                      (range 2))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:red :yellow :blue :indigo])))
     ;; Cambodia: three sequential days *ending* on New Year's (in Cambodia)
     (let [win (-> new-years-cambodia
                   (inc-interval-window -2 cambodia-time-zone)
                   (set-interval-window-duration 3 cambodia-time-zone))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:red :yellow :blue])))
     ;; Cambodia: three sequential days starting *after* New Year's (in Cambodia)
     (let [win (-> new-years-cambodia
                   (inc-interval-window +1 cambodia-time-zone)
                   (set-interval-window-duration 3 cambodia-time-zone))]
       (-> result
           (beam.testing/assert-that* (format "for: %s" win))
           (.inOnTimePane win)
           (add-contains! win [:indigo])))
     ;; Cambodia: three sequential days starting *two days after* New Year's (in Cambodia)
     ;;  should be EMPTY:
     (let [win (-> new-years-cambodia
                   (inc-interval-window +2 cambodia-time-zone)
                   (set-interval-window-duration 3 cambodia-time-zone))]
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

;; --

(defn- ^DateTime clock-in-timezone->dt
  "What is the precise Instant corresponding to what you would see on a clock
   in a given timezone? For example, consider a wall clock in New York that
   read these times:

    (A) March 10, 2019 1:59:59    = 2019-03-10T01:59:59.000-05:00
    (B) March 10, 2019 2:00:00    = 2019-03-10T03:00:00.000-04:00 = 2019-03-10T02:00:00.000-05:00
    (C) March 10, 2019 3:00:00    = 2019-03-10T03:00:00.000-04:00 = 2019-03-10T02:00:00.000-05:00

    Note that (B) and (C) represent identical times. But technically (B) is not a
    valid time to see on the clock as the hour-hand should have been set forward
    due to Daylight Savings Time in 2019."
  [year month day hour minute second tz]
  (t/from-time-zone (t/date-time year month day hour minute second)
    (if (string? tz) (DateTimeZone/forID tz) tz)))

(def ^:private test-data-daylight-savings
  [[{:color :alpha :tz "America/New_York"} (clock-in-timezone->dt 2019 3 9 0 30 0 "America/New_York")]
   [{:color :beta :tz "America/New_York"} (clock-in-timezone->dt 2019 3 10 0 30 0 "America/New_York")]
   ;; After DST "spring-forward" on 11/03/2019 in New York:
   [{:color :gamma :tz "America/New_York"} (clock-in-timezone->dt 2019 3 10 2 30 0 "America/New_York")]
   [{:color :delta :tz "America/New_York"} (clock-in-timezone->dt 2019 3 11 0 30 0 "America/New_York")]
   [{:color :epsilon :tz "America/New_York"} (clock-in-timezone->dt 2019 4 2 0 30 0 "America/New_York")]
   ;; ... months pass ...
   [{:color :zeta :tz "America/New_York"} (clock-in-timezone->dt 2019 11 3 0 30 0 "America/New_York")]
   ;; After DST "fall-back" on 11/03/2019 in New York:
   [{:color :eta :tz "America/New_York"} (clock-in-timezone->dt 2019 11 3 2 30 0 "America/New_York")]
   [{:color :theta :tz "America/New_York"} (clock-in-timezone->dt 2019 12 1 2 30 0 "America/New_York")]
   ])

(deftest test-calendar-sliding-window-daylight-savings-scenarios
  (testing "test-calendar-sliding-window-daylight-savings-scenarios"
    (let [pipeline (beam.testing/test-pipeline)
          result (-> pipeline
                     (beam/create-timestamped-pcoll test-data-daylight-savings)
                     (calendar-sliding-window-fn 30)
                     (beam/pardo #'->color))]

      ;; -- scenarios that cover "spring-forward" in 2019 in New York --

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 2 8 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 3 10 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:alpha]))

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 2 9 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 3 11 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:alpha :beta :gamma]))

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 2 10 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 3 12 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:alpha :beta :gamma :delta]))

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 3 10 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 4 9 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:beta :gamma :delta :epsilon]))

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 3 11 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 4 10 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:delta :epsilon]))

      ;; -- scenarios that cover "fall-back" in 2019 in New York --

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 10 4 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 11 03 0 0 0 "America/New_York"))))
          (.empty))

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 10 5 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 11 04 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:eta :zeta]))

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 10 5 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 11 4 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:eta :zeta]))

      (-> (PAssert/that result)
          (.inWindow (IntervalWindow.
                       (.toInstant (clock-in-timezone->dt 2019 11 3 0 0 0 "America/New_York"))
                       (.toInstant (clock-in-timezone->dt 2019 12 3 0 0 0 "America/New_York"))))
          (.containsInAnyOrder [:eta :zeta :theta]))

      (beam/run-pipeline pipeline (Duration/standardSeconds 30)))))
