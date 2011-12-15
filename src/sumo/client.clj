(ns sumo.client
  (:refer-clojure :exclude [get])
  (:import (com.basho.riak.client IRiakClient RiakFactory)))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn connect
  "Return a connection. With no arguments,
  this returns a connection to localhost
  at the default protocol buffers port"
  ([] (connect default-host
               default-port))
  ([host port]
    (RiakFactory/pbcClient host port)))

(defn ping [client]
  (.ping client))

(defn get [client bucketname keyname]
  (let [bucket (.createBucket client bucketname)]
    (.execute (.fetch (.execute bucket) keyname))))

(defn put [client bucketname keyname obj]
  (let [bucket (.createBucket client bucketname)]
    (.execute (.store (.execute bucket) keyname obj))))

(comment
  ;; usage currently looks like
  (require 'sumo.client :reload)

  (def c (sumo.client/connect))

  (sumo.client/ping c)

  (def value (.getBytes "hello, sumo!" "Utf-8"))

  (sumo.client/put c "bucket" "key" value) 

  (java.lang.String. (.getValue (sumo.client/get c "bucket" "key"))))
