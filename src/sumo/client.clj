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
  (:import [com.basho.riak.client.builders RiakObjectBuilder]
           [com.basho.riak.pbc RiakClient]
           [com.basho.riak.client.raw FetchMeta StoreMeta DeleteMeta RawClient]
           [com.basho.riak.client.raw.pbc PBClientAdapter]
           [com.basho.riak.client IRiakObject]))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn get-as-integer
  [key container]
  (if-let [i (key container)]
    (Integer. i)))

(defn- fetch-options
  [options]
  (let [r (get-as-integer :r options)
        pr (get-as-integer :pr options)
        notfound-ok (:notfound-ok options)
        basic-quorum (:basic-quorum options)
        head (:head options)]
    (FetchMeta. r, pr, notfound-ok,
                basic-quorum, head,
                nil, nil, nil)))

(defn- store-options
  [options]
  (let [w (get-as-integer :w options)
        dw (get-as-integer :dw options)
        pw (get-as-integer :pw options)
        return-body (:return-body options)]
    (StoreMeta. w, dw, pw, return-body,
                nil, nil)))

(defn- delete-options
  [options]
  (let [r (get-as-integer :r options)
        pr (get-as-integer :pr options)
        w (get-as-integer :w options)
        dw (get-as-integer :dw options)
        pw (get-as-integer :pw options)
        rw (get-as-integer :rw options)
        vclock (:vclock options)]
    (DeleteMeta. r, pr, w, dw, pw, rw, vclock)))


(defn- riak-object-to-map
  "Turn an IRiakObject implementation into
  a Clojure map"
  [^IRiakObject riak-object]
  ;; TODO
  ;; add support for 2i
  (-> {}
    (assoc :vector-clock (.getBytes (.getVClock riak-object)))
    (assoc :content-type (.getContentType riak-object))
    (assoc :vtag (.getVtag riak-object))
    (assoc :last-modified (.getLastModified riak-object))
    (assoc :metadata (into {} (.getMeta riak-object)))
    (assoc :value (.getValue riak-object))))

(defn- ^IRiakObject map-to-riak-object
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

(defn get-raw [^RawClient client bucket key & options]
  (let [options (or (first options) {})
        fetch-meta (fetch-options options)
        results (.fetch client ^String bucket ^String key ^FetchMeta fetch-meta)]
    (map riak-object-to-map results)))

(defn get [^RawClient client bucket key & options]
  "Retrieve a lazy-seq of objects at `bucket` and `key`
  Usage looks like:
  (def results (sumo.client/get client \"bucket\" \"key\"))
  (println (:value (first (results))))"
  (let [options (or (first options) {})
        results (get-raw client bucket key options)]
    (map #(assoc % :value (deserialize %)) results)))

(defn put-raw [^RawClient client bucket key obj & options]
  (let [options (or (first options) {})
        riak-object (map-to-riak-object bucket key obj)
        store-meta (store-options options)
        results (.store client ^IRiakObject riak-object ^StoreMeta store-meta)]
    (map riak-object-to-map results)))

(defn put [^RawClient client bucket key obj & options]
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

