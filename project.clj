(defproject sumo "0.0.1-SNAPSHOT"
  :description "Riak driver"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [com.basho.riak/riak-client "1.0.2"]]
  :repositories { "sonatype"
                 {:url "http://oss.sonatype.org/content/repositories/releases"
                  :snapshots false
                  :releases {:checksum :fail :update :always}
                 }}
  :warn-on-reflection true)
