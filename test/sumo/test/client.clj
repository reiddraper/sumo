(ns sumo.test.client
  (:use [sumo.client])
  (:use [clojure.test]))

(def c (connect))

(deftest connect-and-ping
  (ping c))

(deftest get-missing
  (is (nil? (get c "does-not-exist" "does-not-exist"))))
