;; -------------------------------------------------------------------
;; Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.
;;
;; This file is provided to you under the Apache License,
;; Version 2.0 (the "License"); you may not use this file
;; except in compliance with the License.  You may obtain
;; a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing,
;; software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
;; KIND, either express or implied.  See the License for the
;; specific language governing permissions and limitations
;; under the License.
;;
;; -------------------------------------------------------------------

(ns sumo.serializers
  (:require [cheshire.core :as json]))

(defmulti serialize :content-type)

(defmethod serialize nil
  [obj]
  (serialize (assoc obj :content-type "application/json")))

(defmethod serialize :default
  [{content-type :content-type value :value}]
  (throw (Exception. (str "No serializer for content-type: " content-type))))

(defmethod serialize "application/octet-stream"
  [{value :value}]
  value)

(defmethod serialize "application/json"
  [{value :value}]
  (json/generate-string value))

(defmethod serialize "text/plain"
  [{value :value}]
  value)

(defmethod serialize "application/clojure"
  [{value :value}]
  (binding [*print-dup* true]
    (pr-str value)))


(defmulti deserialize :content-type)

(defmethod deserialize :default
  [{content-type :content-type}]
  (throw (Exception. (str "No deserializer for content-type: " content-type))))

(defmethod deserialize "application/octet-stream"
  [{value :value}]
  value)

(defmethod deserialize "application/json"
  [{value :value}]
  ; TODO: Having to turn the byte
  ; array into a String seems wrong?
  (json/parse-string (String. ^bytes value)))

(defmethod deserialize "text/plain"
  [{value :value}]
  (String. ^bytes value))

(defmethod deserialize "application/clojure"
  [{value :value}]
  (binding [*print-dup* true]
    (read-string (String. ^bytes value))))
