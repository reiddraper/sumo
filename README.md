# sumo

## Usage
```clojure
(require '[sumo.client :as sc])

(def c (sc/connect))

(sc/ping c)

(sc/put c "bucket" "key" "hello, sumo!\n")

(print (sumo.client/get c "bucket" "key"))
```
#
## License
Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.

Sumo is released under the Apache 2 License. See LICENSE file for more information.
