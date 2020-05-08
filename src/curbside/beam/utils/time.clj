(ns curbside.beam.utils.time
  (:require
   [clj-time.format :as format])
  (:import
   (org.joda.time DateTime DateTimeZone)))

(def basic-formatter (format/formatters :date-time))

(def formatter (format/formatter DateTimeZone/UTC :date-time :date-time-no-ms))

(defn ^DateTime parse [string] (format/parse formatter string))

(defn unparse [timestamp] (format/unparse basic-formatter timestamp))
