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
##### Using REPL

```bash
$lein repl
```


```Clojure
riak-loader.core=> (def config (read-config "conf/app.edn"))
#'riak-loader.core/config
riak-loader.core=> config
{:ok {:json-keys {:cpcs ("doc_number" "section" "class" "subclass" "main-group" "subgroup"), :entities ("entity_id"), :entities_patents ("entity_id" "doc_number"), :ipcs ("doc_number" "section" "class" "subclass" "main-group" "subgroup"), :patents ("doc_number")}, :key-extension ".json", :env {:dev {:min-conn 10, :max-conn 15, :conn-string ("10.10.10.11:10017" "10.10.10.12:10017" "10.10.10.13:10017"), :thread-count 3, :thread-wait 1000, :channel-timeout 5000}, :prod {:min-conn 16, :max-conn 64, :conn-string ("172.31.21.56:10017" "172.31.21.55:10017" "172.31.21.54:10017"), :thread-count 32, :thread-wait 5000, :channel-timeout 10000}}}}
riak-loader.core=> (def avro-entries  (lazy-avro "data/ipcs.json.avro"))
#'riak-loader.core/avro-entries
riak-loader.core=> (first avro-entries)
{:section "F", :class "16", :subclass "L", :main-group "15", :subgroup "04", :doc-number "ES2349166T3"}
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
* split functionality into separate files

## License

See LICENSE file in the repo.


