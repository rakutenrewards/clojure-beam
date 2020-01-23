(ns curbside.beam.transform.redis-io
  (:require
   [curbside.beam.api :as beam]
   [curbside.beam.metrics.api :as m]
   [taoensso.carmine :as car])
  (:import
   (org.apache.beam.sdk.values PCollection)))

(defn- execute-redis-command-or-commands [{input :input redis-cfg :runtime-parameters}]
  (let [as-vec (if (vector? input) input [input])]
    (.inc (m/counter "curbside.beam.redis_io" "execute_redis_bulk_call"))
    (.inc (m/counter "curbside.beam.redis_io" "execute_redis_command") (count as-vec))
    (car/wcar
     redis-cfg
     (doseq [{:keys [op args]} as-vec
             :let [op-fn (resolve (symbol "taoensso.carmine" (name op)))]]
       (when-not op-fn
         (throw (RuntimeException.
                 (format "The Redis operation taoensso.carmine/%s is not supported by carmine" (name op)))))
       (apply op-fn args)))))

(defn- write-transform [^PCollection pcoll redis-cfg]
  (-> pcoll
      (beam/map-elements #'execute-redis-command-or-commands {:runtime-parameters redis-cfg})))

(defn write
  "Execute the Redis operation against Redis servers.
   Note: This transform does not support exactly-once processing.

   - redis-cfg: See \"conn-opts\" described at http://ptaoussanis.github.io/carmine/taoensso.carmine.html#var-wcar for details.

   Example of Redis operation:
   {:op :hmset :args [\"ad:current-trips-aggregate:kroger\" \"approaching\" 1 \"open\" 1]}
   "
  [^PCollection pcoll redis-cfg]
  ;; TODO: optimize performance with batch processing
  (beam/apply-composite-transform pcoll "execute-redis-command"
                                  (fn [pcoll]
                                    (write-transform pcoll redis-cfg))))
