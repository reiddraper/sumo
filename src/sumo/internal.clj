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
           [com.basho.riak.client.query.indexes BinIndex IntIndex]
           [com.basho.riak.client.raw.query.indexes BinValueQuery BinRangeQuery
            IntValueQuery IntRangeQuery]))

(defn get-as-integer
  [key container]
  (if-let [i (key container)]
    (Integer. i)))

(defn fetch-options
  [options]
  (let [r  (get-as-integer :r options)
        pr (get-as-integer :pr options)
        notfound-ok (:notfound-ok options)
        basic-quorum (:basic-quorum options)
        head (:head options)]
    (FetchMeta. r, pr, notfound-ok,
                basic-quorum, head,
                nil, nil, nil)))

(defn store-options
  [options]
  (let [w  (get-as-integer :w options)
        dw (get-as-integer :dw options)
        pw (get-as-integer :pw options)
        returnbody (:returnbody options)]
    (StoreMeta. w, dw, pw, returnbody,
                nil, nil)))

(defn delete-options
  [options]
  (let [r  (get-as-integer :r options)
        pr (get-as-integer :pr options)
        w  (get-as-integer :w options)
        dw (get-as-integer :dw options)
        pw (get-as-integer :pw options)
        rw (get-as-integer :rw options)
        vclock (:vclock options)]
    (DeleteMeta. r, pr, w, dw, pw, rw, vclock)))

(defn riak-indexes-to-map
  "Converts a seq of Riak Indexes into a hash of sets. The seq could
  define two indexes named 'foo', one binary and one integer, so we
  must be careful to add duplicates to the set rather than overriding
  the first values."
  [indexes]
  (let [add-or-new-set (fn [prev val]
                         (if (nil? prev) val (union prev val)))
        add-index (fn [map name val]
                    (update-in map [name] add-or-new-set val))]
    (loop [index-seq indexes index-map {}]
      (if (seq index-seq)
        (let [index (first index-seq)
              index-map (add-index index-map
                                   (keyword (.getName (.getKey index)))
                                   (set (.getValue index)))]
          (recur (rest index-seq) index-map))
        index-map))))

(defn riak-object-to-map
  "Turn an IRiakObject implementation into
  a Clojure map"
  [^IRiakObject riak-object]
  (-> {}
    (assoc :vector-clock (.getBytes (.getVClock riak-object)))
    (assoc :content-type (.getContentType riak-object))
    (assoc :vtag (.getVtag riak-object))
    (assoc :last-modified (.getLastModified riak-object))
    (assoc :metadata (into {} (.getMeta riak-object)))
    (assoc :value (.getValue riak-object))
    (assoc :indexes (riak-indexes-to-map
                      (concat (seq (.allBinIndexes riak-object))
                              (seq (.allIntIndexes riak-object)))))))

(defn ^IRiakObject map-to-riak-object
  "Construct a DefaultRiakObject from
  a `bucket` `key` and `obj` map"
  [bucket key obj]
  (let [vclock (:vector-clock obj)
        ^RiakObjectBuilder riak-object (-> ^RiakObjectBuilder (RiakObjectBuilder/newBuilder bucket key)
                                         (.withValue (:value obj))
                                         (.withContentType (or (:content-type obj)
                                                               "application/json"))
                                         (.withUsermeta (:metadata obj {})))]
    (doseq [[index-name index-seq] (:indexes obj)]
      (doseq [index-value index-seq]
        (.addIndex riak-object (name index-name) index-value)))
    (if vclock
      (.build (.withValue riak-object vclock))
      (.build riak-object))))

(defn create-index [index-name start]
  (let [str-name (name index-name)]
    (cond
      (string? start) (BinIndex/named str-name)
      (number? start) (IntIndex/named str-name))))

(defn create-index-query
  [bucket index-name value-or-range]
  (if (vector? value-or-range)
    (let [start (clojure.core/get value-or-range 0)
          end   (clojure.core/get value-or-range 1)]
      (cond
        (string? start)
        (BinRangeQuery.
          (create-index index-name start) bucket start end)
        (number? start)
        (IntRangeQuery.
          (create-index index-name start) bucket (Integer. start) (Integer. end))))
    ; single value
    (let [value value-or-range]
      (cond
        (string? value)
        (BinValueQuery.
          (create-index index-name value) bucket value)
        (number? value)
        (IntValueQuery.
          (create-index index-name value) bucket (Integer. value))))))
