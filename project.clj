(defproject sumo "0.0.1-SNAPSHOT"
  :description "Riak driver"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [com.basho.riak/riak-client "1.0.3-SNAPSHOT"]
                 [cheshire "2.0.4"]]
  :dev-dependencies [[midje "1.3.1"]
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
