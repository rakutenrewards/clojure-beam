(ns curbside.beam.transform.bigquery-io-test
  (:require
   [clojure.test :refer [are deftest is testing]]
   [curbside.beam.transform.bigquery-io :as bq-io]))

(deftest format-timestamps-for-bigquery
  (testing "Given various formats of ISO8601 timestamp strings
            when formatting them for BigQuery,
            then they are all converted to the following format: YYYY-MM-DDTHH:MM:SS.DDDZ"
    (are [input output] (= (#'bq-io/format-timestamp input) output)
      "2010-01-01T01:02:03.456Z" "2010-01-01T01:02:03.456Z"
      "2010-01-01T01:02:03Z" "2010-01-01T01:02:03.000Z"
      "2010-01-01T12:00:00.000-05" "2010-01-01T17:00:00.000Z"
      "2010-01-01T12:00:00.000-05:00" "2010-01-01T17:00:00.000Z"
      "2010-01-01T12:00:00.000-0500" "2010-01-01T17:00:00.000Z"
      "2010-01-01T12:00:00.000-05:30" "2010-01-01T17:30:00.000Z"
      "2010-01-01T12:00:00-05" "2010-01-01T17:00:00.000Z"
      "2010-01-01T12:00:00-0500" "2010-01-01T17:00:00.000Z")))

(deftest format-invalid-timestamps-for-bigquery
  (testing "Given an invalid timestamp string
            when formatting it for BigQuery,
            then nil is returned"
    (are [input output] (= (#'bq-io/format-timestamp input) output)
      nil nil
      "" nil)))
