(ns sumo.test.serializers
  (:require [sumo.serializers :as s])
  (:use midje.sweet))

(fact "serialize-to-json"
  (s/serialize {:content-type "application/json"
                :value [1, [1,2], "Foo", {:a 5}]})  =>  "[1,[1,2],\"Foo\",{\"a\":5}]")

(fact "deserialize-from-json"
  (s/deserialize {:content-type "application/json"
                  :value (.getBytes "[1,[1,2],\"Foo\",{\"a\":5}]")})  =>  [1, [1,2], "Foo", {"a" 5}])

(fact "serialize-to-reader"
  (s/serialize {:content-type "application/clojure"
                :value [1, [1,2], "Foo", {:a 5}]})  
  
  =>  "[1 [1 2] \"Foo\" #=(clojure.lang.PersistentArrayMap/create {:a 5})]")

(fact "deserialize-from-reader"
  (s/deserialize {:content-type "application/clojure"
                  :value (.getBytes "[1 [1 2] \"Foo\" #=(clojure.lang.PersistentArrayMap/create {:a 5})]")})  
  
  =>  [1, [1,2], "Foo", {:a 5}])
