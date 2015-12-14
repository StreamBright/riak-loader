java \
  -server -Xms512m \
  -Xmx4096m \
  -XX:+UseConcMarkSweepGC -XX:+TieredCompilation -XX:+AggressiveOpts \
  -jar target/riak-loader-0.1.0-standalone.jar -c conf/app.edn -f resources/test.json -e dev -t patents
