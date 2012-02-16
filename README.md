# sumo

## Build status

[![Build Status](https://secure.travis-ci.org/reiddraper/sumo.png)](http://travis-ci.org/reiddraper/sumo)

## Dependencies

```
lein deps
```

To use sumo with another project, add `[sumo "0.1.0"]` to your `projects.clj`.

## Usage
```clojure
(require '[sumo.client :as sumo]
         '[clojure.pprint :as pp])

;; connect with the protocol buffers
;; client
(def client (sumo/connect-pb))

;; three examples of how to connect
;; with HTTP
(sumo/connect-http)
(sumo/connect-http "http://prod0.example.com:8098/riak")
(sumo/connect-http "127.0.0.1" 8098)
;; the last param is the HTTP path that all riak
;; requests start with. You might have your proxy
;; change this, for example.
(sumo/connect-http "127.0.0.1" 8098 "riak")

(sumo/ping client)
;; => true

(sumo/put client "bucket" "key" {:value "hello, sumo!"})
;; => nil

;; sumo will default to "application/json" content-type and serialization
;; you can override this by setting it in the object hash-map
(sumo/put client "bucket" "key" {:content-type "text/plain"
                                 :value "hello, sumo!"})
;; => nil


;; returns a lazy-seq of hash-maps
(pp/pprint (sumo/get client "bucket" "key"))
;; => ({:value "hello, sumo!",
;;      :metadata {},
;;      :last-modified #<Date Sun Dec 18 20:40:03 CST 2011>,
;;      :vtag "6YWpgJW8WpnKlNTOeOxKZf",
;;      :content-type "application/json",
;;      :vector-clock
;;      #<BasicVClock com.basho.riak.client.cap.BasicVClock@50aec4>})

;; you can also pass an options hash-map into requests
;; for example, to use an R value of 2 and only return the object metadata
(sumo/get client "bucket" "key" {:r 2 :head true}))

;; or to use a W value of 3 and return a lazy-seq of hash-maps just like sumo/get
(sumo/put client "bucket" "key" {:value "hey there"}
                                {:w 3 :return-body true})

;; you can store secondary indexes by adding an :indexes hash-map containing
;; keywords as keys and sets as values to your object
(sumo/put client "bucket" "key" {:value {:name "John" :email "john@example.com"}
                                 :indexes {:email #{"john@example.com"}})

;; querying secondary indexes is supported as well
(sumo/index-query client "bucket" :email "john@example.com")

; search for a range of strings
(sumo/index-query client "bucket" :email ["i" "k"])

; or of integers
(sumo/index-query client "bucket" :age [21 80])

;; Here is an example of sumo's map-reduce support,
;; it follows the data from this example from the
;; Riak wiki, http://wiki.basho.com/MapReduce.html#HTTP-Example

(require '[sumo.mr-helpers :as mr-helpers])

(def query {"inputs" [["alice" "p1"] ["alice" "p2"] ["alice" "p5"]]
            "query" [(mr-helpers/map-js "function(v) {return [1]}")
                     (mr-helpers/reduce-erlang "riak_kv_mapreduce" "reduce_sum")]})

;; the query parameter to the map-reduce
;; call is a data structure that will be
;; json-encoded and used for the query.
;; Read through http://wiki.basho.com/MapReduce.html
;; for more examples of how to use map-reduce with Riak
(sumo/map-reduce client query)
```

## Roadmap

sumo currently supports key/value access, secondary indexes
and map-reduce. The following is a TODO list of sorts
along with some ideas for a higher level interface.

### TODO

* multi-client connections
* http connections (sumo is currently just protocol buffers)
* links
* search

### Higher-level interface ideas

These are just some ideas we've been playing
around with for how the high-level API might look:

Apply a series of forms to whatever
value is currently stored at `bucket`, `key`:

```clojure
;; Apply a form to the
;; current value at `key`,
;; and resave. Similar to Mutations
;; in the Java client
(send-riak "bucket" "key"
  ;; your form will be called
  ;; threading in the obj
  ;; as the second value,
  ;; much like (-> obj (..) (..))
  (assoc-in [:value] inc))
```

The next idea is an ODM of sorts that supports
automatic conflict-resolution of your domain objects
but letting you construct them from [knockbox](http://github.com/reiddraper/knockbox)
data types.

```clojure
(defbox Account
  ;; the second item in the
  ;; list is a type or protocol
  ;; to restrict the value to
  (:name       LWWRegister :required true)
  (:address    LWWRegister :required true)
  ;; specify a default value for this field,
  ;; in this case, the result of calling the
  ;; function `lww-set`
  (:followers  ObservedRemoveSet :default (lww-set)))

(def person (Account.))

(-> person
  ;; add :bar to the followers list
  (update-in [:followers] conj :bar)
  ;; set the name
  (assoc-in  [:name]      "Reid")
  ;; set the address
  (assoc-in  [:address]   "27 Lexington Ave"))
```

## License
Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.

Sumo is released under the Apache 2 License. See LICENSE file for more information.
