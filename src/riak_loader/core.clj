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
    [clojure.java.io    :as   io        ]
    [clojure.data.json  :as   json      ]
    [clojure.string     :as   cstr      ]

  )
  (:import 
    [com.basho.riak.client.api              RiakClient                    ]
    [com.basho.riak.client.core.query       Namespace Location RiakObject ]
    [com.basho.riak.client.api.commands.kv  StoreValue StoreValue$Builder 
                                            StoreValue$Option             ]
    [com.basho.riak.client.api.cap          Quorum                        ]
    [com.basho.riak.client.core.util        BinaryValue                   ]
    [java.net                               InetSocketAddress             ]
  )
  (:gen-class))

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

(defn riak-store!
  "Storing a string in Riak"
  [riak-client riak-namespace riak-location riak-value]
  (let [
        riak-object     (RiakObject.)
        _               (.setValue riak-object riak-value)
        store           (.build 
                          (.withOption 
                            (.withLocation 
                              (StoreValue$Builder. riak-object) riak-location) 
                                StoreValue$Option/W  (Quorum. 2)))
        exx             (.execute riak-client store)
      ]
  ))

(defn -main 
  [& args]
  (let [
        lines           (lazy-lines "resources/patents1064.json")
        jsons           (map json/read-str lines)
        riak-client     (riak-connect! 
                          (list "127.0.0.1:10017" "127.0.0.1:10027" "127.0.0.1:10037"))
        riak-namespace  (Namespace. "default" "my_bucket")
        riak-location   (Location. riak-namespace "key")
        riak-value      (BinaryValue/create "my_value")
        ]

        (riak-store! riak-client riak-namespace riak-location riak-value)

    ))

