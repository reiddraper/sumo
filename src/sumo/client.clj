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
        [clojure.set :only [union]])
  (:import [com.basho.riak.client.builders RiakObjectBuilder]
           [com.basho.riak.pbc RiakClient]
           [com.basho.riak.client.raw FetchMeta StoreMeta DeleteMeta RawClient]
           [com.basho.riak.client.raw.pbc PBClientAdapter]
           [com.basho.riak.client IRiakObject]
           [com.basho.riak.client.query.indexes BinIndex IntIndex]
           [com.basho.riak.client.raw.query.indexes BinValueQuery BinRangeQuery
            IntValueQuery IntRangeQuery]))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn- typed-options [options]
  (reduce (fn [in-process opt-key]
            (update-in in-process [opt-key] #(if % (Integer. %) %))) 
    options
    [:r :pr :w :dw :pw :rw]))

(defn- fetch-options
  [{:keys [r pr notfound-ok basic-quorum head]}]
  (FetchMeta. r, pr, notfound-ok, basic-quorum, head, nil, nil, nil))

(defn- store-options
  [{:keys [w dw pw return-body]}]
  (StoreMeta. w, dw, pw, return-body, nil, nil))

(defn- delete-options
  [{:keys [r pr w dw pw rw vclock]}]
  (DeleteMeta. r, pr, w, dw, pw, rw, vclock))

(defn- riak-indexes-to-map
  "Converts a seq of Riak Indexes into a hash of sets. The seq could
  define two indexes named 'foo', one binary and one integer, so we
  must be careful to add duplicates to the set rather than overriding
  the first values."
  [indexes]
  (letfn [(add-or-new-set [prev val]
            (if prev (union prev val) val))
         
          (step [index-map index]
            (update-in index-map
                       [ (keyword (.getName (.getKey index))) ]
                       add-or-new-set 
                       (set (.getValue index))))]

          (reduce step {} indexes)))

(defn- riak-object-to-map
  "Turn an IRiakObject implementation into
  a Clojure map"
  [^IRiakObject riak-object]
  { :vector-clock (.getBytes (.getVClock riak-object))
    :content-type (.getContentType riak-object)
    :vtag (.getVtag riak-object)
    :last-modified (.getLastModified riak-object)
    :metadata (into {} (.getMeta riak-object))
    :value (.getValue riak-object)
    :indexes (riak-indexes-to-map
                      (concat (seq (.allBinIndexes riak-object))
                              (seq (.allIntIndexes riak-object)))) })

(defn- ^IRiakObject map-to-riak-object
  "Construct a DefaultRiakObject from
  a `bucket` `key` and `obj` map"
  [bucket key obj]
  (let [^RiakObjectBuilder riak-object (-> ^RiakObjectBuilder (RiakObjectBuilder/newBuilder bucket key)
                                         (.withValue (:value obj))
                                         (.withContentType (or (:content-type obj)
                                                               "application/json"))
                                         (.withUsermeta (:metadata obj {})))]
    (doseq [[index-name index-seq] (:indexes obj)
             index-value index-seq]
        (.addIndex riak-object (name index-name) index-value))
    (if-let [vclock (:vector-clock obj)]
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
  (or (.ping client) true))

(defn get-raw [^RawClient client bucket key & options]
  (let [fetch-meta (fetch-options (typed-options (or (first options) {})))
        results (.fetch client ^String bucket ^String key ^FetchMeta fetch-meta)]
    (map riak-object-to-map results)))

(defn get [^RawClient client bucket key & options]
  "Retrieve a lazy-seq of objects at `bucket` and `key`
  Usage looks like:
  (def results (sumo.client/get client \"bucket\" \"key\"))
  (println (:value (first (results))))"
  (let [results (get-raw client bucket key (or (first options) {}))]
    (for [r results]
      (assoc r :value (deserialize r)))))

(defn put-raw [^RawClient client bucket key obj & options]
  (let [riak-object (map-to-riak-object bucket key obj)
        store-meta (store-options (typed-options (or (first options) {})))
        results (.store client ^IRiakObject riak-object ^StoreMeta store-meta)]
    (map riak-object-to-map results)))

(defn put [^RawClient client bucket key obj & options]
  "Store an object into Riak.
  Usage looks like:
  (sumo.client/put client \"bucket\" \"key\" {:content-type \"text/plain\" :value \"hello!\"})"
  (let [new-obj (assoc obj :value (serialize obj))
        results (put-raw client bucket key new-obj (or (first options) {}))]
    (for [r results]
      (assoc r :value (deserialize r)))))

(defn delete [^RawClient client bucket key & options]
  (let [delete-meta (delete-options (typed-options (or (first options) {})))]
    (.delete client ^String bucket ^String key ^DeleteMeta delete-meta))
  true)

(defn- create-index [index-name start]
  (let [str-name (name index-name)]
    (cond
      (string? start) (BinIndex/named str-name)
      (number? start) (IntIndex/named str-name))))

(defmulti create-index-query (fn [_ _ val-or-range] 
                               (if (vector? val-or-range) :vector :single)))

(defmethod create-index-query :vector [bucket index-name range]
  (let [start (clojure.core/get range 0)
        end (clojure.core/get range 1)]
    (cond
      (string? start)
      (BinRangeQuery.
        (create-index index-name start) bucket start end)
      (number? start)
      (IntRangeQuery.
        (create-index index-name start) bucket (Integer. start) (Integer. end)))))

(defmethod create-index-query :single [bucket index-name value]
  (cond
    (string? value)
    (BinValueQuery.
      (create-index index-name value) bucket value)
    (number? value)
    (IntValueQuery.
      (create-index index-name value) bucket (Integer. value))))

(defn index-query [^RawClient client bucket index-name value-or-range]
  (let [query (create-index-query bucket index-name value-or-range)]
    (seq (.fetchIndex client query))))
