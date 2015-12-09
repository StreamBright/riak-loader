(defproject riak-loader "0.1.0"
  :description "Loading data into Riak"
  :url "https://github.com/StreamBright/riak-loader"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [
    [org.clojure/clojure        "1.7.0" ]
    [org.clojure/data.json      "0.2.6" ]
    [com.basho.riak/riak-client "2.0.2" ]
  ]
:profiles {
    :uberjar {
      :aot :all
    }
  }
  :jvm-opts [
    "-Xms128m" "-Xmx256m" 
    "-server"  "-XX:+UseConcMarkSweepGC" 
    "-XX:+TieredCompilation" "-XX:+AggressiveOpts"
    ;"-Dcom.sun.management.jmxremote"
    ;"-Dcom.sun.management.jmxremote.port=8888"
    ;"-Dcom.sun.management.jmxremote.local.only=false"
    ;"-Dcom.sun.management.jmxremote.authenticate=false"
    ;"-Dcom.sun.management.jmxremote.ssl=false"
    ;"-XX:+UnlockCommercialFeatures" "-XX:+FlightRecorder"
    ;"-XX:StartFlightRecording=duration=60s,filename=myrecording.jfr"
    ;"-Xprof" "-Xrunhprof"
  ]
  :repl-options {:init-ns riak-loader.core}
  :main riak-loader.core)
