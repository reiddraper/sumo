# sumo

## Usage
```clojure
(require '[sumo.client :as sumo])

(def client (sumo/connect))

(sumo/ping client)
true

(sumo/put client "bucket" "key" {:content-type "application/json"
                                 :value "hello, sumo!"})
nil

;; returns a lazy-seq of hashes
(pprint (sumo/get client "bucket" "key"))
({:value "hello, sumo!",
  :metadata {},
  :last-modified #<Date Sun Dec 18 20:40:03 CST 2011>,
  :vtag "6YWpgJW8WpnKlNTOeOxKZf",
  :content-type "application/json",
  :vector-clock
  #<BasicVClock com.basho.riak.client.cap.BasicVClock@50aec4>})
```
#
## License
Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.

Sumo is released under the Apache 2 License. See LICENSE file for more information.
