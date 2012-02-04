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

(ns sumo.mr-helpers)

;; MapReduce function descriptions are simply
;; hashes. They describe the phase, language,
;; etc. for a particular MapReduce phase.

(def ^:private js     {:language "javascript"})
(def ^:private erlang {:language "erlang"})

(defn wrap-map [input]
  {:map input})

(defn wrap-reduce [input]
  {:reduce input})

;; Javascript functions from source ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn map-js [source & {:keys [keep] :or {keep false}}]
  (wrap-map
    (merge {:source source :keep keep}
           js)))

(defn reduce-js [source & {:keys [keep] :or {keep false}}]
  (wrap-reduce
    (merge {:source source :keep keep}
           js)))


;; Erlang functions from source ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn map-erlang [module function & {:keys [keep] :or {keep false}}]
  (wrap-map
    (merge {:module module :function function :keep keep}
           erlang)))

(defn reduce-erlang [module function & {:keys [keep] :or {keep false}}]
  (wrap-reduce
    (merge {:module module :function function :keep keep}
           erlang)))
