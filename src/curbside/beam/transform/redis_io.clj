(ns curbside.beam.transform.redis-io
  (:require
   [curbside.beam.api :as beam]
   [curbside.beam.metrics.api :as m]
   [taoensso.carmine :as car])
  (:import
   (org.apache.beam.sdk.values PCollection)))

(defn- execute-redis-command [{input :input redis-cfg :runtime-parameters}]
  (let [{:keys [op args]} input
        op-fn (resolve (symbol "taoensso.carmine" (name op)))]
    (.inc (m/counter "curbside.beam.redis_io" "execute_redis_command"))
    (assert op-fn (str "The Redis operation taoensso.carmine/" (name op) " is not supported by carmine"))
    (car/wcar redis-cfg (apply op-fn args))))

(defn- write-transform [^PCollection pcoll redis-cfg]
  (-> pcoll
      (beam/map-elements #'execute-redis-command {:runtime-parameters redis-cfg})
      (beam/pdone)))

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
