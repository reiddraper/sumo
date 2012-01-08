(ns sumo.test.client
  (:require [sumo.client :as client])
  (:use midje.sweet ))

(def c (client/connect))

(fact "can ping the client"
  (client/ping c) => true)

(fact "get of non-existant key returns empty result" 
  (client/get c "does-not-exist" "does-not-exist") => [])

(defn- put-then-get [obj]
  (client/put c "test-bucket" "test-key" obj)
  (client/get c "test-bucket" "test-key"))

(against-background [(before :facts (client/put c "test-bucket" "get-head" 
                                        { :content-type "text/plain" 
                                          :value "get-head test"}))]
  (fact "get-head" 
    (client/get c "test-bucket" "get-head" {:head true}) => (one-of (contains {:value ""})) ))

(fact "put-get-json"
  (put-then-get {:content-type "application/json"
                 :value [1 "2" '(3)]})            =>  (one-of (contains {:value [1 "2" '(3)]})))

(fact "can save and retrieve, with JSON as the default"
  (put-then-get {:value [1 "2" '(3)]}) => (one-of (contains {:value [1 "2" '(3)]})))

(fact "put-get-indexes"
  (let [indexes {:a #{1 "binary"}
                 :b #{2}}]
    (put-then-get {:content-type "application/json"
                   :value "Hello"
                   :indexes indexes}) => (one-of (contains {:indexes indexes}))))