;; Copyright 2015 StreamBright LLC and contributors

;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at

;;     http://www.apache.org/licenses/LICENSE-2.0

;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns ^{  :doc "Loading data into Riak"
        :author "Istvan Szukacs"  }
  riak-loader.core
  (:require 
    [clojure.java.io        :as   io    ]
    [clojure.string         :as   cstr  ]
    [clojure.tools.cli      :as   cli   ]
    [clojure.tools.logging  :as   log   ]
    [clojure.edn            :as   edn   ]
    [cheshire.core          :as   ches  ]
    [clojure.core.async     :refer 
      [alts! chan go thread timeout 
       >! >!! <! <!! go-loop]           ]
  )
  (:import 
    [com.basho.riak.client.api              RiakClient                            ]
    [com.basho.riak.client.core             RiakNode$Builder RiakCluster$Builder  ]
    [com.basho.riak.client.core.query       Namespace Location RiakObject         ]
    [com.basho.riak.client.api.commands.kv  StoreValue StoreValue$Builder 
                                            StoreValue$Option                     ]
    [com.basho.riak.client.api.cap          Quorum                                ]
    [com.basho.riak.client.core.util        BinaryValue                           ]
    [java.net                               InetSocketAddress                     ]
    [java.io                                File                                  ]
  )
  (:gen-class))

(defn read-file
  "Returns {:ok string } or {:error...}"
  [^File file]
  (try
    (cond
      (.isFile file)
        {:ok (slurp file) }
      :else
        (throw (Exception. "Input is not a file")))
  (catch Exception e
    {:error "Exception" :fn "read-file" :exception (.getMessage e) })))

(defn parse-edn-string
  [s]
  (try
    {:ok (edn/read-string s)}
  (catch Exception e
    {:error "Exception" :fn "parse-config" :exception (.getMessage e)})))

(defn read-config
  [path]
  (let
    [ file-string (read-file (File. path)) ]
    (cond
      (contains? file-string :ok)
        ;this return the {:ok} or {:error} from parse-edn-string
        (parse-edn-string (file-string :ok))
      :else
        file-string)))


(defn exit [n] 
    (log/info "init :: stop")
    (System/exit n))

(defn lazy-helper
    "Processes a java.io.Reader lazily"
    [reader]
    (lazy-seq
          (if-let [line (.readLine reader)]
                  (cons line (lazy-helper reader))
                  (do (.close reader) nil))))
(defn lazy-lines
    "Return a lazy sequence with the lines of the file"
    [^String file]
    (lazy-helper (io/reader file)))

(defn riak-connect!
  "Connecting a Riak cluster"
  [host-port-list]
  (RiakClient/newClient 
    (into-array 
      (map #(InetSocketAddress. (first %) (Integer. (second %))) 
        (map #(cstr/split % #":") host-port-list)))))

(defn build-node-template
  "Budilgin Riak node template"
  [node-builder min-conn max-conn]
  (.withMaxConnections
    (.withMinConnections
      node-builder min-conn) max-conn))

(defn build-node
  "Building Riak node"
  [node-template host port]
  (.build 
    (.withRemotePort 
      (.withRemoteAddress node-template host) port)))

(defn riak-connect2!
  "Connecting to a Riak cluster"
  [host-port-list]
  (let [  node-template (build-node-template (RiakNode$Builder.) 16 128) 
          nodes         (map #(build-node node-template (first %) (Integer. (second %)))
                          (map #(cstr/split % #":") host-port-list)) 
          cluster       (.build (RiakCluster$Builder. nodes))
        ]
    ;; return
    cluster))

(defn riak-store!
  "Storing a string in Riak"
  [riak-client riak-bucket riak-key riak-value]
  (let [
        riak-object     (RiakObject.)
        _               (.setValue (.setContentType riak-object "application/json") riak-value)
        store           (.build 
                          (.withOption 
                            (.withLocation 
                              (StoreValue$Builder. riak-object) riak-key) 
                                StoreValue$Option/N_VAL (Integer. 1)))
        exx             (.execute riak-client store)
      ]
    {:ok :ok}))

(def blocking-producer >!!)
(def blocking-consumer <!!)

(def non-blocking-producer >!)
(def non-blocking-consumer <!)

(def cli-options
  ;; An option with a required argument
  [
  ["-c" "--config CONFIG" "Config file name"
    :default "conf/app.edn"]
  ["-f" "--file FILE" "File to process"
    :default "/dev/null"]
  ["-t" "--type TYPE" "Upload type (patents, cpcs, entities..)"
    :default "patents"]
  ["-e" "--env ENV" "Environment (dev or prod)"
    :default "dev"]
  ["-h" "--help"]
   ])

(defn update-stats 
  "Todo: add p50, p90, p99 with set atom" 
  [counter result start-time]
  ;might not need do here, return {:ok :ok}
  (do 
    (swap! counter inc)
    (cond (= (mod @counter 10000) 0)
      (do
        (let [ exec-time (with-precision 3
                         (/ (- (. System (nanoTime)) @start-time) 1000000000.0))
              _  (reset! start-time (. System (nanoTime))) ]
        ;log stats
        (log/info (str " res: " result " count: " @counter 
                       " perf: " (int (/ 10000 exec-time)) 
                       " req/s" )))))))

(defn get-doc-key 
  "Returns a Riak doc key string, example: xyz.json" 
  [json-key doc]
  (let [json-keys (map #(get-in doc [ % ]) json-key)]
    (str (clojure.string/join "_" json-keys) ".json")))

(defn process-line 
  "Takes a line and inserts it to Riak as JSON"
  [line json-key riak-client riak-bucket stat-chan]
  (let [    doc-clj     (ches/parse-string line)
            doc-key     (get-doc-key json-key doc-clj) 
            riak-key    (Location. riak-bucket doc-key)
            json-byte   (.getBytes line)
            riak-value  (BinaryValue/unsafeCreate json-byte)
            _           (log/debug (str riak-client riak-bucket riak-key "riak-value"))
            ;returns {:ok ...} || {:err ...} could be checked
            _           (riak-store! riak-client riak-bucket riak-key riak-value)]
    {:ok :ok}))

(defn -main 
  [& args]
  (let [
        {:keys [options arguments errors summary]} (cli/parse-opts args cli-options)
        config          (read-config (:config options))
        _               (log/debug (str "config: " config))
        env             (keyword (:env options))
        bucket-type     (:type options) ;same as bucket-name
        _               (log/debug (str "bucket-type: " bucket-type))
        env             (keyword (:env options))
        bucket-name     bucket-type
        json-key        (get-in config [:ok :json-keys (keyword bucket-type)])
        _               (log/debug (str "json-key: " json-key))
        lines           (lazy-lines (:file options))
        riak-cluster    (riak-connect2! 
                          (get-in config [:ok :env env :conn-string]))
        _               (.start riak-cluster)
        riak-client     (RiakClient. riak-cluster)
        riak-bucket     (Namespace. bucket-type bucket-name)
        stat-chan       (chan)
        work-chan       (chan)
        thread-count    (get-in config [:ok :env env :thread-count])
        thread-wait     (get-in config [:ok :env env :thread-wait])
        channel-timeout (get-in config [:ok :env env :channel-timeout])
	      counter         (atom 0)
        start-time      (atom (. System (nanoTime)))
        ]

      ;; TEST
      ;; (do (println "something") (exit 0))
      ;; END TEST

      ;; creating N threads to insert data into Riak

      (dotimes [i thread-count]
        (thread
          (Thread/sleep thread-wait)
            (while true
              ;; this should be moves to a function and only the time measurement should be here
              ;; start
              ;; call into the function
              ;; stop
              (let [  line        (blocking-consumer work-chan)
                      start       (. System (nanoTime))
                      _           (process-line line json-key riak-client riak-bucket)
                      exec-time   (with-precision 3 
                                    (/ (- (. System (nanoTime)) start) 1000000.0)) ]
                (blocking-producer 
                  stat-chan 
                  {:thread-name (.getName (Thread/currentThread)) :time exec-time})))))
                

        ;; end of creating worker threads
        
        ;; start a thread that sends in json entries to the work-channel

        (thread
          (Thread/sleep 100)
          (doseq [line lines]
            (blocking-producer work-chan line)))

        ;; end of sending thread

        ;; main thread, blocks until timeout or all of the files are uploaded
        (while true 
          (blocking-consumer
            (go
              (let [ [result source] (alts! [stat-chan (timeout channel-timeout)]) ]
                (if (= source stat-chan)
		              (do 
                    ;(log/debug result)
		                (update-stats counter result start-time))
                  ;else - timeout 
                    (do 
                      (log/info "Channel timed out. Stopping...") 
                    ;;shutdown riak
                      (.shutdown riak-cluster)
                      (exit 0)))))))
    ;;END
    ))


