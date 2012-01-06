(ns sumo.test.client
  (:require [sumo.client :as client])
  (:use [clojure.test]))

(def c (client/connect))

(deftest connect-and-ping
  (is (client/ping c)))

(deftest get-missing
  (is (nil? (seq (client/get c "does-not-exist" "does-not-exist")))))

(deftest get-head
  (let [put-ret (client/put c "test-bucket" "get-head"
                            {:content-type "text/plain"
                             :value "get-head test"})
        get-ret (client/get c "test-bucket" "get-head" :head true)]
    (is (= ""
           (:value (first get-ret))))))

(deftest put-get-json
  (let [obj {:content-type "application/json"
             :value [1 "2" '(3)]}
        put-ret (client/put c "test-bucket" "test-key" obj)
        get-ret (client/get c "test-bucket" "test-key")]
    (is (= (:value obj)
           (:value (first get-ret))))))

(deftest put-get-json-default
  (let [obj {:value [1 "2" '(3)]}
        put-ret (client/put c "test-bucket" "test-key" obj)
        get-ret (client/get c "test-bucket" "test-key")]
    (is (= (:value obj)
           (:value (first get-ret))))))

(deftest put-get-indexes
  (let [indexes {:a #{1 "binary"}
                 :b #{2}}
        obj {:content-type "application/json"
             :value "Hello"
             :indexes indexes}
        put-ret (client/put c "test-bucket" "test-indexes-key" obj)
        get-ret (client/get c "test-bucket" "test-indexes-key")]
    (is (= indexes
           (:indexes (first get-ret))))))
