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

(defmethod serialize :default
  [{content-type :content-type value :value}]
  (println "the content type is" content-type)
  (if (= content-type "application/octet-stream")
    value
    (throw (Exception.
             (str "could not serialize content-type " content-type)))))

(defmethod serialize "application/json"
  [{value :value}]
  (json/generate-string value))

(defmethod serialize "text/plain"
  [{value :value}]
  value)

(defmulti deserialize :content-type)

(defmethod deserialize :default
  [{content-type :content-type value :value}]
  (if (= content-type "application/octet-stream")
    value
    (throw (Exception.
             (str "could not deserialize content-type " content-type)))))

(defmethod deserialize "application/json"
  [{value :value}]
  (json/parse-string (String. value)))

(defmethod deserialize "text/plain"
  [{value :value}]
  (String. value))
