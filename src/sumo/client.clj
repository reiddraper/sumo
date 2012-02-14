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
  (:refer-clojure :exclude [get key pr])
  (:require [sumo.internal :as i])
  (:require [cheshire.core :as json])
  (:use [sumo.serializers :only [serialize deserialize]]
        [clojure.set :only [union]])
  (:import [com.basho.riak.client.builders RiakObjectBuilder]
           [com.basho.riak.client.raw FetchMeta StoreMeta DeleteMeta RawClient]
           [com.basho.riak.client.raw.pbc PBClientAdapter]
           [com.basho.riak.client IRiakObject]
           [com.basho.riak.client.query.indexes BinIndex IntIndex]
           [com.basho.riak.client.raw.query.indexes BinValueQuery BinRangeQuery
            IntValueQuery IntRangeQuery]
           [com.basho.riak.client.raw.query MapReduceSpec]
           ))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn connect-pb
  "Return a connection. With no arguments,
  this returns a connection to localhost
  at the default protocol buffers port"
  ([] (connect default-host
               default-port))
  ([^String host ^long port]
   (PBClientAdapter.
     (com.basho.riak.pbc.RiakClient. host port))))

(defn connect [& args] (connect-pb args))

(defn ping
  "Returns true or raises ConnectException"
  [^RawClient client]
  (or (.ping client) true))

(defn get-raw [^RawClient client bucket key & options]
  (let [fetch-meta (i/fetch-options (i/typed-options (or (first options) {})))
        results (.fetch client ^String bucket ^String key ^FetchMeta fetch-meta)]
    (map i/riak-object-to-map results)))

(defn get [^RawClient client bucket key & options]
  "Retrieve a lazy-seq of objects at `bucket` and `key`
  Usage looks like:
  (def results (sumo.client/get client \"bucket\" \"key\"))
  (println (:value (first (results))))"
  (let [results (get-raw client bucket key (or (first options) {}))]
    (for [r results]
      (assoc r :value (deserialize r)))))

(defn put-raw [^RawClient client bucket key obj & options]
  (let [riak-object (i/map-to-riak-object bucket key obj)
        store-meta (i/store-options (i/typed-options (or (first options) {})))
        results (.store client ^IRiakObject riak-object ^StoreMeta store-meta)]
    (map i/riak-object-to-map results)))

(defn put [^RawClient client bucket key obj & options]
  "Store an object into Riak.
  Usage looks like:
  (sumo.client/put client \"bucket\" \"key\" {:content-type \"text/plain\" :value \"hello!\"})"
  (let [new-obj (assoc obj :value (serialize obj))
        results (put-raw client bucket key new-obj (or (first options) {}))]
    (for [r results]
      (assoc r :value (deserialize r)))))

(defn delete [^RawClient client bucket key & options]
  (let [delete-meta (i/delete-options (i/typed-options (or (first options) {})))]
    (.delete client ^String bucket ^String key ^DeleteMeta delete-meta))
  true)

(defmulti create-index-query (fn [_ _ val-or-range] 
                               (if (vector? val-or-range) :vector :single)))

(defmethod create-index-query :vector [bucket index-name range]
  (let [start (clojure.core/get range 0)
        end (clojure.core/get range 1)]
    (cond
      (string? start)
      (BinRangeQuery.
        (i/create-index index-name start) bucket start end)
      (number? start)
      (IntRangeQuery.
        (i/create-index index-name start) bucket (Integer. ^Long start) (Integer. ^Long end)))))

(defmethod create-index-query :single [bucket index-name value]
  (cond
    (string? value)
    (BinValueQuery.
      (i/create-index index-name value) bucket value)
    (number? value)
    (IntValueQuery.
      (i/create-index index-name value) bucket (Integer. ^Long value))))

(defn index-query [^RawClient client bucket index-name value-or-range]
  (let [query (create-index-query bucket index-name value-or-range)]
    (seq (.fetchIndex client query))))

(defn map-reduce [^RawClient client query]
  (let [serialized-query (json/generate-string query)
        spec (MapReduceSpec. serialized-query)
        res (. client mapReduce spec)
        raw-result (.getResultRaw res)]
    (json/parse-string raw-result)))
