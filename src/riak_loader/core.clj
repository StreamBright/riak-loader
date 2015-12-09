;; Copyright 2015 StreamBright LLC
;; Copyright 2015 Istvan Szukacs <istvan@streambrightdata.com>

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
        :author "Istvan Szukacs"}
  riak-loader.core
  (:require 
    [clojure.java.io        :as   io    ]
    [clojure.data.json      :as   json  ]
    [clojure.string         :as   cstr  ]
    [clojure.tools.logging  :as   log   ]
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
  )
  (:gen-class))

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
  (let [  node-template (build-node-template (RiakNode$Builder.) 16 64) 
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
        _               (.setValue riak-object riak-value)
        store           (.build 
                          (.withOption 
                            (.withLocation 
                              (StoreValue$Builder. riak-object) riak-key) 
                                StoreValue$Option/W  (Quorum. 2)))
        exx             (.execute riak-client store)
      ]))

(def blocking-producer >!!)
(def blocking-consumer <!!)

(def non-blocking-producer >!)
(def non-blocking-consumer <!)

(defn -main 
  [& args]
  (let [
        lines           (lazy-lines "resources/test.json")
        jsons           (map json/read-str lines)
        riak-cluster    (riak-connect2! 
                          (list "127.0.0.1:10017" "127.0.0.1:10027" "127.0.0.1:10037"))
        _               (.start riak-cluster)
        riak-client     (RiakClient. riak-cluster)
        riak-bucket     (Namespace. "test-patents" "test-patents")
        stat-chan       (chan 32)
        work-chan       (chan 32)
        thread-count    128
        thread-wait     1000
        channel-timeout 5000

        ]

      ;; creating N threads to insert data into Riak

      (dotimes [i thread-count]
        (thread
          (Thread/sleep thread-wait)
            (go-loop []
              (let [    doc         (blocking-consumer work-chan) 
                        start       (. System (nanoTime))
                        riak-key    (Location. riak-bucket (get-in doc ["doc_number"]))
                        riak-value  (BinaryValue/create 
                                      (json/write-str doc :escape-unicode false))
                        _           (riak-store! riak-client riak-bucket riak-key riak-value)
                        exec-time   (with-precision 3 
                                      (/ (- (. System (nanoTime)) start) 1000000.0))
                   ]
                  ;; send results to stat-chan
                  (blocking-producer 
                    stat-chan 
                    {:thread-name (.getName (Thread/currentThread)) :time exec-time})
                  (recur)))))

        ;; end of creating worker threads
        
        ;; start a thread that sends in json entries to the work-channel

        (thread
          (Thread/sleep 100)
          (doseq [json-doc jsons]
            (blocking-producer work-chan json-doc)))

        ;; end of sending thread

        ;; main thread, blocks until timeout or all of the files are uploaded
        (while true 
          (blocking-consumer
            (go
              (let [[result source] (alts! [stat-chan (timeout channel-timeout)])]
                (if (= source stat-chan)
                  (do (println "szopki")
                  (log/info result))
                  ;else - timeout 
                  (do 
                    (log/info "Channel timed out. Stopping...") 
                    ;;shutdown riak
                    (exit 0)))))))
    ;;END
    ))


