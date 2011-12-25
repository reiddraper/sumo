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
  (:use [sumo.serializers :only [serialize deserialize]]
        [sumo.utils]
        [sumo.conversions])
  (:import [com.basho.riak.pbc RiakClient]
           [com.basho.riak.client.raw FetchMeta StoreMeta DeleteMeta RawClient]
           [com.basho.riak.client.raw.pbc PBClientAdapter]
           [com.basho.riak.client IRiakObject]))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn connect
  "Return a connection. With no arguments,
  this returns a connection to localhost
  at the default protocol buffers port"
  ([] (connect default-host
               default-port))
  ([^String host ^long port]
   (PBClientAdapter.
     (RiakClient. host port))))

(defn ping
  "Returns true or raises ConnectException"
  [^RawClient client]
  (let [result (.ping client)]
    (if (nil? result) true result)))

(defn get-raw
  [^RawClient client ^String bucket ^String key options]
  (let [options (or (first options) {})
        fetch-meta (fetch-options options)
        results (.fetch client ^String bucket ^String key ^FetchMeta fetch-meta)]
    (map riak-object-to-map results)))

(defn get
  "Retrieve a lazy-seq of objects at `bucket` and `key`

  Usage looks like:

      (def results (sumo.client/get client \"bucket\" \"key\"))
      (println (:value (first (results))))"
  [^RawClient client ^String bucket ^String key &{:keys [r pr notfound-ok basic-quorum head value] :as options}]
  (let [options (or (first options) {})
        results (get-raw client bucket key options)]
    (map #(assoc % :value (deserialize %)) results)))

(defn put-raw
  [^RawClient client ^String bucket ^String key obj options]
  (let [options (or (first options) {})
        riak-object (map-to-riak-object bucket key obj)
        store-meta (store-options options)
        results (.store client ^IRiakObject riak-object ^StoreMeta store-meta)]
    (map riak-object-to-map results)))

(defn put [^RawClient client ^String bucket ^String key obj &{:keys [r pr notfound-ok basic-quorum head value] :as options}]
  "Store an object into Riak.

  Usage looks like:

      (sumo.client/put client \"bucket\" \"key\" {:content-type \"text/plain\" :value \"hello!\"})"
  (let [options (or (first options) {})
        new-obj (assoc obj :value (serialize obj))
        results (put-raw client bucket key new-obj options)]
    (map #(assoc % :value (deserialize %)) results)))

(defn delete [^RawClient client bucket key & options]
  (let [options (or (first options) {})
        delete-meta (delete-options options)]
    (.delete client ^String bucket ^String key ^DeleteMeta delete-meta))
  true)

