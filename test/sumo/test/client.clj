(ns sumo.test.client
  (:require [sumo.client :as client])
  (:use [clojure.test]))

(def c (client/connect))
(def bucket "test-bucket")

(defn cleanup-bucket
  [^String bucket-name]
  (doseq [key (client/list-keys c bucket-name)]
    (client/delete c bucket-name key)))

(defn cleanup-test-bucket
  [f]
  (cleanup-bucket bucket)
  (f)
  (cleanup-bucket bucket))

(use-fixtures :each cleanup-test-bucket)

(deftest connect-and-ping
  (is (client/ping c)))

(deftest get-missing-test
  (is (empty? (client/get c "does-not-exist" "does-not-exist"))))

(deftest get-head
  (let [bucket bucket
        key    "get-head"
        value  "get-head test"]
    (is (= (:value (first (client/get c bucket key :head true)))
           nil))))

(deftest put-get-json
  (is (let [obj {:content-type "application/json"
                 :value [1 "2" '(3)]}]
        (do
          (client/put c bucket "test-key" obj)
          (= (:value obj)
             (:value (first (client/get c bucket "test-key"))))))))
