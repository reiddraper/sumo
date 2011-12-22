# sumo

## Dependencies

sumo currently relies on a development version of the
Riak Java client. To get this version:

```
git clone git@github.com:basho/riak-java-client.git
cd riak-java-client
mvn install
```

After this, run:

```
lein deps
```

To use sumo with another project, add `[sumo "0.0.1-SNAPSHOT"]` to your `projects.clj`.

## Usage
```clojure
(require '[sumo.client :as sumo]
         '[clojure.pprint :as pp])

(def client (sumo/connect))

(sumo/ping client)
;; => true

(sumo/put client "bucket" "key" {:content-type "application/json"
                                 :value "hello, sumo!"})
;; => nil

;; returns a lazy-seq of hashes
(pp/pprint (sumo/get client "bucket" "key"))
;; => ({:value "hello, sumo!",
;;      :metadata {},
;;      :last-modified #<Date Sun Dec 18 20:40:03 CST 2011>,
;;      :vtag "6YWpgJW8WpnKlNTOeOxKZf",
;;      :content-type "application/json",
;;      :vector-clock
;;      #<BasicVClock com.basho.riak.client.cap.BasicVClock@50aec4>})

;; you can also pass in an options
;; hash for a get request
(sumo/get client "bucket" "key" {:r 2 :head true}))

;; you can also pass in an options
;; hash for put operations
(sc/put client "bucket" "key" {:content-type "text/plain" :value "hey there"} {:w 3 :return-body true})
;; which returns a list of values just
;; like sc/get does
```
#
## License
Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.

Sumo is released under the Apache 2 License. See LICENSE file for more information.
