# riak-loader

Riak-loader is loading huge amount of JSON entries into Riak. The source of data can be JSON files on disk (1 entry per line, see example below). Each JSON file has its on key in the data and the key is essembled at insertion time.

## Usage

### Building a JAR

I like to run the loder as a standalone jar.

````bash
$ lein uberjar
Compiling riak-loader.core
Created ...riak-loader/target/riak-loader-0.1.0.jar
Created ...riak-loader/target/riak-loader-0.1.0-standalone.jar
```

### Running it

#### Dev

Configure the dev envinroment in the app.edn. 

```bash
$lein run -c conf/app.edn -f sample.json -e dev -t patents
```

#### Prod

In prod the following script can be used: run.sh 

```bash
java \
  -server -Xms512m \
  -Xmx4096m \
  -XX:+UseConcMarkSweepGC -XX:+TieredCompilation -XX:+AggressiveOpts \
  -XX:+UnlockCommercialFeatures -XX:+FlightRecorder \
  -XX:StartFlightRecording=defaultrecording=true,dumponexit=true,settings=riak_loader_profiling.jfc \
  -jar target/riak-loader-0.1.0-standalone.jar -c app.edn -f huge.json -e prod -t type_of_data
```

#### Todo

* checking command line parameters 

## License

See LICENSE file in the repo.


