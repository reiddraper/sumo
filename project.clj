(defproject sumo "0.2.0"
  :description "Riak driver"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [com.basho.riak/riak-client "1.0.5"]
                 [cheshire "2.0.4"]]
  :dev-dependencies [[midje "1.3.1"  :exclusions [org.clojure/clojure]]
                     [lein-midje "1.0.7"]]
  :repositories {"sonatype"
                  {:url "http://oss.sonatype.org/content/repositories/releases"
                   :snapshots false
                   :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots"
                  {:url "http://oss.sonatype.org/content/repositories/snapshots"
                   :snapshots true
                   :releases {:checksum :fail :update :always}}}
  :warn-on-reflection true)
