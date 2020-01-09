(ns curbside.beam.test.beam.transform.redis-io-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [curbside.beam.test.env :as env]
   [curbside.beam.test.testing :as beam.testing]
   [curbside.beam.transform.redis-io :as redis-io]
   [taoensso.carmine :as car]))

(def conn-opts {:pool {} :spec {:uri env/redis}})

(deftest ^:integration redis-test
  (testing "Given hmset method is used, when field is number, it is stored as string"
    (let [set-key "test-redis-hmset"
          set-vals ["open" 2 "approaching" 1 "in-transit" 1 "arrived" 1]]
      (car/wcar conn-opts (apply car/hmset (cons set-key set-vals)))
      (is (= (map str set-vals)
             (car/wcar conn-opts (car/hgetall set-key)))))))

(defn- run-write-redis-pipeline [cmds]
  (let [test-pipeline (beam.testing/test-pipeline)]
    (-> test-pipeline
        (beam.testing/test-stream cmds {})
        (redis-io/write conn-opts))
    (beam.testing/run-and-wait-pipeline 10000 test-pipeline)))

(deftest ^:integration redis-io-test
  (testing "Given a pipeline,"
    (testing "When write with hmset method, the data is saved"
      (let [redis-key "test-write-hmset"
            set-vals ["open" 1 "approaching" 1 "in-transit" 1 "arrived" 1]]
        (run-write-redis-pipeline [{:op :hmset :args (cons redis-key set-vals)}])
        (is (= (into #{} (map str set-vals))
               (into #{} (car/wcar conn-opts (car/hgetall redis-key)))))))

    (testing "When write with hdel method, the data is removed"
      (let [redis-key "test-write-hdel"
            set-vals ["open" 1 "approaching" 2 "in-transit" 3 "arrived" 4]]
        (run-write-redis-pipeline [{:op :hmset :args (cons redis-key set-vals)}])
        (run-write-redis-pipeline [{:op :hdel :args (cons redis-key set-vals)}])
        (is (empty? (car/wcar conn-opts (car/hgetall redis-key))))))))
