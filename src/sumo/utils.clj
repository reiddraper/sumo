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

(ns sumo.utils
  (:import [com.basho.riak.client.raw FetchMeta StoreMeta DeleteMeta RawClient]))

(defn get-as-integer
  [key container]
  (if-let [i (key container)]
    (Integer. i)))

(defn fetch-options
  [options]
  (let [r (get-as-integer :r options)
        pr (get-as-integer :pr options)
        notfound-ok (:notfound-ok options)
        basic-quorum (:basic-quorum options)
        head (:head options)]
    (FetchMeta. r, pr, notfound-ok,
                basic-quorum, head,
                nil, nil, nil)))

(defn store-options
  [options]
  (let [w (get-as-integer :w options)
        dw (get-as-integer :dw options)
        pw (get-as-integer :pw options)
        return-body (:return-body options)]
    (StoreMeta. w, dw, pw, return-body,
                nil, nil)))

(defn delete-options
  [options]
  (let [r (get-as-integer :r options)
        pr (get-as-integer :pr options)
        w (get-as-integer :w options)
        dw (get-as-integer :dw options)
        pw (get-as-integer :pw options)
        rw (get-as-integer :rw options)
        vclock (:vclock options)]
    (DeleteMeta. r, pr, w, dw, pw, rw, vclock)))