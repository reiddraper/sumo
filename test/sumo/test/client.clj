(ns sumo.test.client
  (:require [sumo.client :as client]
            [sumo.mr-helpers :as mr-helpers])
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

(fact "put-get-links"
  (let [links [{:bucket "b" :key "k0" :tag "t"}
         {:bucket "b" :key "k1" :tag "t"}]]
    (put-then-get {:content-type "application/json"
                   :value "Hello"
                   :links links}) => (one-of (contains {:links links}))))


(fact "summing the keys in an empty bucket through
      map-reduce results in zero keys being summed"
      (let [query {"inputs" "non-existent-bucket"
                   "query" [(mr-helpers/map-js "function(v) {return [1]}")
                            (mr-helpers/reduce-erlang "riak_kv_mapreduce" "reduce_sum")]}]
        (client/map-reduce c query) => [0]))
