(ns sumo.test.serializers
  (:require [sumo.serializers :as s])
  (:use [clojure.test]))

(deftest serialize-to-json
  (is (= (s/serialize {:content-type "application/json"
                       :value [1, [1,2], "Foo", {:a 5}]})
         "[1,[1,2],\"Foo\",{\"a\":5}]")))

(deftest deserialize-from-json
  (is (= (s/deserialize {:content-type "application/json"
                         :value "[1,[1,2],\"Foo\",{\"a\":5}]"})
         [1, [1,2], "Foo", {"a" 5}])))
