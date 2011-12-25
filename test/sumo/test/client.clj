(ns sumo.test.client
  (:require [sumo.client :as client])
  (:use [clojure.test]))

(def c (client/connect))

(deftest connect-and-ping
  (is (client/ping c)))

(deftest get-missing
  (is (nil? (seq (client/get c "does-not-exist" "does-not-exist")))))

(deftest get-head
  (let [bucket "test-bucket"
        key    "get-head"
        value  "get-head test"]
    (client/put c bucket key {:content-type "text/plain"
                              :value value})
    (is (= (:value (first (client/get c bucket key :head true)))
           value))))

(deftest put-get-json
  (is (let [obj {:content-type "application/json"
                 :value [1 "2" '(3)]}]
        (do
          (client/put c "test-bucket" "test-key" obj)
          (= (:value obj)
             (:value (first (client/get c "test-bucket" "test-key"))))))))
