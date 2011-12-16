(ns sumo.client
  (:refer-clojure :exclude [get])
  (:import (com.basho.riak.client.builders RiakObjectBuilder))
  (:import (com.basho.riak.pbc RiakClient))
  (:import (com.basho.riak.client.raw.pbc PBClientAdapter)))

(def ^{:private true} default-host "127.0.0.1")
(def ^{:private true} default-port 8087)

(defn connect
  "Return a connection. With no arguments,
  this returns a connection to localhost
  at the default protocol buffers port"
  ([] (connect default-host
               default-port))
  ([host port]
    (PBClientAdapter.
      (RiakClient. host port))))

(defn ping [client]
  (.ping client))

(defn get [client bucketname keyname]
  (let [results (.fetch c bucketname keyname)
       first-result  (first (seq results))]
    (.getValueAsString first-result)))

(defn put [client bucketname keyname value]
  "Currently value is expected to be a utf-8 string"
  (let [base-object (RiakObjectBuilder/newBuilder
                      bucketname keyname)
        riak-object (-> base-object
                     (.withValue value) (.build))]
    (.store c riak-object)))


(comment
  ;; usage currently looks like
  (require 'sumo.client :reload)

  (def c (sumo.client/connect))

  (sumo.client/ping c)

  (sumo.client/put c "bucket" "key" "hello, sumo!\n")

  (print (sumo.client/get c "bucket" "key"))
)
