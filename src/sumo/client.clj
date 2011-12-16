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

(ns sumo.client
  (:refer-clojure :exclude [get])
  (:import (com.basho.riak.client.builders RiakObjectBuilder))
  (:import (com.basho.riak.pbc RiakClient))
  (:import (com.basho.riak.client.raw.pbc PBClientAdapter)))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn connect
  "Return a connection. With no arguments,
  this returns a connection to localhost
  at the default protocol buffers port"
  ([] (connect default-host
               default-port))
  ([host port]
    (PBClientAdapter.
      (RiakClient. host port))))

(defn ping [client]
  (.ping client))

(defn get [client bucketname keyname]
  (let [results (.fetch client bucketname keyname)
       first-result  (first (seq results))]
    (.getValueAsString first-result)))

(defn put [client bucketname keyname value]
  "Currently value is expected to be a utf-8 string"
  (let [base-object (RiakObjectBuilder/newBuilder
                      bucketname keyname)
        riak-object (-> base-object
                     (.withValue value) (.build))]
    (.store client riak-object)))
