(ns sumo.client
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

