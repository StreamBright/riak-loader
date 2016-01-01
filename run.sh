java \
  -server -Xms512m \
  -Xmx4096m \
  -XX:+UseConcMarkSweepGC -XX:+TieredCompilation -XX:+AggressiveOpts \
  -XX:+UnlockCommercialFeatures -XX:+FlightRecorder \
  -XX:StartFlightRecording=defaultrecording=true,dumponexit=true,settings=riak_loader_profiling.jfc \
  -jar target/riak-loader-0.1.0-standalone.jar -c conf/app.edn -f data/20000.ipcs.json -e dev -t ipcs
