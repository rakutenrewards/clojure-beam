(ns curbside.beam.transform.bigquery-io
  (:require
   [clojure.core.match :refer [match]]
   [clojure.string :as str]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [clojure.walk :as walk]
   [curbside.beam.api :as beam]
   [geo.io :as geo-io]
   [geo.jts :as geo-jts])
  (:import
   (com.google.api.services.bigquery.model TableRow)
   (curbside.beam.java ClojureSerializableFunction)
   (org.apache.beam.sdk.io.gcp.bigquery BigQueryIO TableRowJsonCoder)))

(defn- ->WriteDisposition
  [disp]
  (case disp
    :append org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO$Write$WriteDisposition/WRITE_APPEND
    :empty org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO$Write$WriteDisposition/WRITE_EMPTY
    :truncate org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO$Write$WriteDisposition/WRITE_TRUNCATE))

(defn- ->CreateDisposition
  [disp]
  (case disp
    :never org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO$Write$CreateDisposition/CREATE_NEVER
    :if-needed org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO$Write$CreateDisposition/CREATE_IF_NEEDED))

(defn- geojson-coord->jts-point
  "Given a coord [lng lat], return a JTS Point. Note that the geo lib expects
   points in the format lat,lng and coordinates in lng,lat."
  [[lng lat]]
  (geo-jts/point lat lng))

(defn- geojson-coord->jts-coord
  "Given a coord [lng lat], return a JTS Coordinate. Note that the geo lib expects
   points in the format lat,lng and coordinates in lng,lat."
  [[lng lat]]
  (geo-jts/coordinate lng lat))

(defn- geojson-ring->LinearRing
  "Given a list of coords of the form
   [[lng-1 lat-1] ... [lng-n lat-n] [lng-1 lat-1]], return a JTS LinearRing.
   NOTE: BigQuery assumes that the inside of the polygon is to the left as
   you move from point to point on the given coordinates. The data coming from
   the API is the opposite of that, so we reverse the coordinates."
  [coords]
  (let [jts-coords (map geojson-coord->jts-coord (reverse coords))]
    (geo-jts/linear-ring jts-coords)))

(defn- geojson-poly->Polygon
  "Given GeoJSON polygon coordinates of the form
   [[coord1 coord2 coord3] ... [coord1 coord2 coord3]], return a JTS Polygon."
  [poly]
  (if (= 1 (count poly))
    (geo-jts/polygon (geojson-ring->LinearRing (first poly)))
    (geo-jts/polygon (geojson-ring->LinearRing (first poly))
                     (map geojson-ring->LinearRing (rest poly)))))

(defn- ->well-known-text
  "Converts GeoJSON-like format in our events to well-known text.
   https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry
   This is the format used by BigQuery for GEOGRAPHY columns."
  [{:keys [type coordinates] :as _geojson}]
  (case type
    "Point" (geo-io/to-wkt (geojson-coord->jts-point coordinates))
    "Polygon" (geo-io/to-wkt (geojson-poly->Polygon coordinates))
    nil))

;; See https://beam.apache.org/documentation/io/built-in/google-bigquery/#data-types
;; for how to format data for BQ requests. Add more cases to this as we need
;; them.
;; The above link doesn't document nested structs and arrays, but it seems
;; that we are supposed to use a nested TableRow for STRUCT types, and
;; an Array for ARRAY types.
(declare convert-types)
(defn- convert-by-type
  "Convert a value into the type expected by BigQuery, using the given
   type. type-mapping is also passed in, so that the types of nested fields
   in structs can be looked up, too."
  [type-mapping k type v]
  (try
    (match [type]
      [:int64] v
      [:timestamp] (when (not-empty v) (str v))
      [:string] v
      [:bool] v
      [:float64] v
      [:geography] (try (->well-known-text v)
                        (catch Exception e
                          (log/warn "Received bad geo data for " k ":" v ". Overwriting with null.")
                          nil))
      [[:struct sub-mapping]] (let [substruct (TableRow.)]
                                (doseq [[k v] (convert-types sub-mapping v)]
                                  (.set substruct (name k) v))
                                substruct)
      [[:array [:struct sub-mapping]]]
      (->> v
           (map (fn [submap]
                  (let [substruct (TableRow.)]
                    (doseq [[k v] (convert-types sub-mapping submap)]
                      (.set substruct (name k) v))
                    substruct)))
           (into-array TableRow))
      [[:array t]] (into-array (map #(convert-by-type type-mapping k t %) v)))
    (catch Exception e
      (log/error "Got an unexpected exception while converting to BigQuery type. Re-throwing." type k v)
      (throw e))))

(defn- convert-types
  "Converts all values in m into the types expected by BigQuery, using the
   given type-mapping."
  [type-mapping m]
  (keep
   (fn [[k v]]
     (when (type-mapping k)
       [k (convert-by-type type-mapping k (type-mapping k) v)]))
   m))

(defn- underscorify-keys
  "Recursively transform all the dashes in map keys into underscores. Affects
  both string and keyword keys, maintaining their original type.
  BigQuery only allows underscores as separators in column names."
  [obj]
  (let [f (fn [[k v]]
            (let [replaced (-> (name k) (str/replace #"-" "_"))
                  replaced (if (keyword? k) (keyword replaced) replaced)]
              [replaced v]))]
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) obj)))

(defn ->TableRow
  "Given a Clojure map representing a row in a table, and a map from keys
   to types, create a TableRow object that can be sent to BQ. See
   convert-type for valid types.

   Any keys not present in the type-mapping will not be included in the result."
  [type-mapping {:keys [input]}]
  {:post [(= (type %) TableRow)]}
  (let [table-row (TableRow.)
        input (underscorify-keys input)]
    (doseq [[k v] (convert-types type-mapping input)]
      (.set table-row (name k) v))
    table-row))

(defn type-mapping->TableRow
  "Returns a function that converts a Clojure map into a BigQuery TableRow
   using the given mapping from keys to BigQuery types. See convert-by-type in
   this namespace for a list of possible types. Any keys containing dashes will
   be converted to use underscores before the conversion, so the type mapping
   should use underscores."
  [type-mapping]
  #(->TableRow type-mapping %))

(defn write-bigquery
  "Writes to a BigQuery table
   - pcoll: a PCollection of Clojure maps
   - ->table-row: A var referring to a function created with
                  type-mapping->TableRow.
   - dataset: a string specifying the BigQuery dataset containing the table.
   - project-id: project to write data to
   - table: a string specifying the BigQuery table to write to.
   - write-disposition: one of :append, :empty, :truncate
   - create-disposition: one of :never, :if-needed"
  [pcoll ->table-row project-id dataset table write-disposition create-disposition {:keys [step-name]}]
  {:pre [(var? ->table-row)]}
  (let [project-id (when-not (string/blank? project-id) (str project-id ":"))
        write (-> (BigQueryIO/writeTableRows)
                  (.to (str project-id dataset "." table))
                  (.withWriteDisposition (->WriteDisposition write-disposition))
                  (.withCreateDisposition (->CreateDisposition create-disposition))
                  (.withFormatFunction (ClojureSerializableFunction. ->table-row {})))]
    (.apply pcoll (beam/make-step-name step-name ->table-row) write)))
