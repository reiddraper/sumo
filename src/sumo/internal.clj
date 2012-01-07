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

(ns sumo.internal
  (:refer-clojure :exclude [key pr])
  (:use [clojure.set :only [union]])
  (:import [com.basho.riak.client.builders RiakObjectBuilder]
           [com.basho.riak.client.raw FetchMeta StoreMeta DeleteMeta]
           [com.basho.riak.client IRiakObject]
           [com.basho.riak.client.query.indexes RiakIndex BinIndex IntIndex]
           [com.basho.riak.client.raw.query.indexes BinValueQuery BinRangeQuery
            IntValueQuery IntRangeQuery]))

(defn typed-options [options]
  (reduce (fn [in-process opt-key]
            (update-in in-process [opt-key] #(if % (Integer. ^Long %) %)))
    options
    [:r :pr :w :dw :pw :rw]))

(defn fetch-options
  [{:keys [r pr notfound-ok basic-quorum head]}]
  (FetchMeta. r, pr, notfound-ok, basic-quorum, head, nil, nil, nil))

(defn store-options
  [{:keys [w dw pw return-body]}]
  (StoreMeta. w, dw, pw, return-body, nil, nil))

(defn delete-options
  [{:keys [r pr w dw pw rw vclock]}]
  (DeleteMeta. r, pr, w, dw, pw, rw, vclock))

(defn riak-indexes-to-map
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

(defn riak-object-to-map
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

(defn ^IRiakObject map-to-riak-object
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


(defn create-index [index-name start]
  (let [str-name (name index-name)]
    (cond
      (string? start) (BinIndex/named str-name)
      (number? start) (IntIndex/named str-name))))
