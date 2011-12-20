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
  (:use [sumo.serializers :only [serialize deserialize]])
  (:import (com.basho.riak.client.builders RiakObjectBuilder))
  (:import (com.basho.riak.pbc RiakClient))
  (:import (com.basho.riak.client.raw FetchMeta))
  (:import (com.basho.riak.client.raw.pbc PBClientAdapter)))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn get-as-integer
  [key container]
    (if-let [i (key container)]
      (Integer. i)))

(defn- fetch-options
  [options]
  ;TODO
  ; assert there are no extra
  ; keys in options, as this helps
  ; prevent things like misspelling
  ; :basic-quorom ;)
  (let [r (get-as-integer :r options)
        pr (get-as-integer :pr options)
        notfound-ok (:notfound-ok options)
        basic-quorum (:basic-quorum options)
        head (:head options)]
        ; %TODO
        ; add:
        ; * return-deleted-vlock
        ; * ifmodifiedvclock
        ; * ifmodifiedsince
  (FetchMeta. r, pr, notfound-ok,
              basic-quorum, head,
              nil, nil, nil)))


(defn- riak-object-to-map
  "Turn an IRiakObject implementation into
  a Clojure map"
  [riak-object]
  ;; TODO
  ;; add support for 2i
  (-> {}
    (assoc :vector-clock (.getVClock riak-object))
    (assoc :content-type (.getContentType riak-object))
    (assoc :vtag (.getVtag riak-object))
    (assoc :last-modified (.getLastModified riak-object))
    (assoc :metadata (into {} (.getMeta riak-object)))
    (assoc :value (.getValue riak-object))))

(defn- map-to-riak-object
  "Construct a DefaultRiakObject from
  a `bucket` `key` and `obj` map"
  [bucket key obj]
  (-> (RiakObjectBuilder/newBuilder bucket key)
    (.withValue (:value obj))
    (.withContentType (:content-type obj))
    (.withVClock (:vector-clock obj))
    (.withUsermeta (:metadata obj {}))
    (.build)))

(defn connect
  "Return a connection. With no arguments,
  this returns a connection to localhost
  at the default protocol buffers port"
  ([] (connect default-host
               default-port))
  ([host port]
     (PBClientAdapter.
      (RiakClient. host port))))

(defn ping
  "Returns true or raises ConnectException"
  [client]
  (let [result (.ping client)]
    (if (nil? result) true result)))

(defn get-raw [client bucket key & options]
  (let [options (or (first options) {})
        fetch-meta (fetch-options options)
        results (.fetch client bucket key fetch-meta)]
    (map riak-object-to-map results)))

(defn get [client bucket key & options]
  "Retrieve a lazy-seq of objects at `bucket` and `key`
  Usage looks like:
      (def results (sumo.client/get client \"bucket\" \"key\"))
      (println (:value (first (results))))"
  (let [options (or (first options) {})
        results (get-raw client bucket key options)]
    (map #(assoc % :value (deserialize %)) results)))

(defn put-raw [client bucket key obj]
  (let [riak-object (map-to-riak-object bucket key obj)]
    (.store client riak-object)))

(defn put [client bucket key obj]
  "Store an object into Riak.
  Usage looks like:
      (sumo.client/put client \"bucket\" \"key\" {:content-type \"text/plain\" :value \"hello!\"})"
  (let [new-obj (assoc obj :value (serialize obj))]
    (put-raw client bucket key new-obj)))
