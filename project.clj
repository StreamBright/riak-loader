(defproject riak-loader "0.1.0"
  :description "Loading data into Riak"
  :url "https://github.com/StreamBright/riak-loader"
  :license {:name "Apache License 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [
    [org.clojure/clojure        "1.7.0"   ]
    [com.damballa/abracad       "0.4.13"  ]
    [cheshire                   "5.5.0"   ]
    [org.clojure/tools.cli      "0.3.3"   ]
    [com.basho.riak/riak-client "2.0.2"   ]
    [org.clojure/tools.logging  "0.3.1"   ]
    [org.slf4j/slf4j-log4j12    "1.7.12"  ]
    [log4j/log4j                "1.2.17"  ]
    [org.clojure/core.async     "0.2.374" ]
  ]
  :exclusions [
    javax.mail/mail
    javax.jms/jms
    com.sun.jdmk/jmxtools
    com.sun.jmx/jmxri
    jline/jline
  ]
  :profiles {
    :uberjar {
      :aot :all
    }
  }
  :jvm-opts [
    "-Xms128m" "-Xmx4000m" 
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
