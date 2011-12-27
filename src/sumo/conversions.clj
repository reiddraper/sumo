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

(ns sumo.conversions
  (:import [clojure.lang IPersistentMap]
           [com.basho.riak.client.builders RiakObjectBuilder]
           [com.basho.riak.client IRiakObject]))

(defn ^IPersistentMap riak-object-to-map
  "Turn an IRiakObject implementation into
  a Clojure map"
  [^IRiakObject riak-object]
  ;; TODO
  ;; add support for 2i
  (-> {}
    (assoc :vector-clock (.. riak-object getVClock getBytes))
    (assoc :content-type (.getContentType riak-object))
    (assoc :vtag (.getVtag riak-object))
    (assoc :last-modified (.getLastModified riak-object))
    (assoc :metadata (into {} (.getMeta riak-object)))
    (assoc :value (.getValue riak-object))))

(defn ^IRiakObject map-to-riak-object
  "Construct a DefaultRiakObject from
  a `bucket` `key` and `obj` map"
  [bucket key obj]
  (let [vclock (:vector-clock obj)
        ^RiakObjectBuilder riak-object (-> ^RiakObjectBuilder (RiakObjectBuilder/newBuilder bucket key)
                      (.withValue (:value obj))
                      (.withContentType (:content-type obj))
                      (.withUsermeta (:metadata obj {})))]
    (if vclock
      (.build (.withValue riak-object vclock))
      (.build riak-object))))
