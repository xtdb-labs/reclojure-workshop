(ns myproject.core
  (:require [crux.api :as c]
            [clojure.java.io :as io])
  (:import java.util.Date)
  (:gen-class))

(defn create-tmdb-rocks-node []
  (let [node-opts {:crux/tx-log {:kv-store {:crux/module 'crux.rocksdb/->kv-store,
                                            :db-dir "data/tx-log"}}
                   :crux/document-store {:kv-store {:crux/module 'crux.rocksdb/->kv-store,
                                                    :db-dir "data/doc-store"}}
                   :crux/index-store {:kv-store {:crux/module 'crux.rocksdb/->kv-store,
                                                 :db-dir "data/indices"}}
                   :crux.http-server/server {}}
        node (doto (c/start-node node-opts) (c/sync))
        submit-data? (nil? (c/entity (c/db node) :bar))]
    (when (not submit-data?) (prn "Resuming node"))
    (when submit-data?
      (prn "Downloading..." (Date.))
      (with-open [dataset-rdr (io/reader "https://crux-data.s3.eu-west-2.amazonaws.com/kaggle-tmdb-movies.edn")]
        (let [last-tx (->> (line-seq dataset-rdr)
                            (partition-all 1000)
                            (reduce (fn [_ docs-chunk]
                                      (c/submit-tx node (mapv read-string docs-chunk)))
                                    nil))]
          (prn "Loading Sample Data..." (Date.))
        ;; Await data-set data
          (c/await-tx node last-tx)
        ;; Submit :bar multiple times - entity to showcase entity searching/history.
          (c/submit-tx node [[:crux.tx/put {:crux.db/id :bar} #inst "2018-06-01"]])
          (c/submit-tx node [[:crux.tx/put {:crux.db/id :bar :map {:a 1 :b 2}} #inst "2019-04-04"]])
          (c/submit-tx node [[:crux.tx/put {:crux.db/id :bar :vector [:a :b]} #inst "2020-01-02"]])
          (c/await-tx node (c/submit-tx node [[:crux.tx/put {:crux.db/id :bar :hello "world"}]]))

          (prn "Sample Data Loaded!" (Date.)))))
    node))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

;; confirm setup
(comment
  ;; setup tmdb

  (def rocks-node (create-tmdb-rocks-node))

  (c/status rocks-node)

  (.close rocks-node) ;; make sure to close the node before closing your repl, otherwise you won't be able to resume without deleting the LOCK files in each data/* directory
  )

;; tmdb data model reference
(comment
  ;; movie entities
  {:crux.db/id :tmdb/movie-XXXXX}

  ;; cast entities
  {:crux.db/id :tmdb/cast-XXXXX}

  ;; credit entities
  {:crux.db/id :tmdb/credit-XXXXX
   :tmdb.movie/id :tmdb/movie-XXXXX
   :tmdb.cast/id :tmdb/cast-XXXXX}

  ;; e.g.

  {:tmdb.movie/genres #{"Comedy" "Thriller" "Crime"},
   :tmdb.movie/keywords
   #{"garbage" "hiding place" "church choir" "duringcreditsstinger"},
   :tmdb.movie/title "The Ladykillers",
   :tmdb.movie/revenue 0,
   :tmdb.movie/release_date "2004-03-25",
   :tmdb.movie/budget 0,
   :tmdb/type :movie,
   :crux.db/id :tmdb/movie-5516,
   :tmdb.movie/id 5516}

  {:crux.db/id :tmdb/cast-17305,
   :tmdb/type :cast,
   :tmdb.cast/id 17305,
   :tmdb.cast/name "Greg Grunberg"}

  {:crux.db/id :tmdb/credit-55f44ee09251413a960026d5,
   :tmdb/type :credit,
   :tmdb.movie/id {:eid :tmdb/movie-5516},
   :tmdb.cast/id {:eid :tmdb/cast-17305},
   :tmdb.cast/character "TV Commercial Director"}
  )

;; workshop
(comment
  ;; new node, db & entity

  (def mem-node (c/start-node {}))

  ;; transactions


  ;; await-tx & sync


  ;; entity-history


  ;; open-tx-log


  ;; tmdb
  (def mynode rocks-node)

  ;; helpers
  (defn q [m & [vt tt]]
    (c/q (c/db mynode vt tt) (assoc m :limit 10)))

  (defn e [eid & [vt tt]]
    (c/entity (c/db mynode vt tt) eid))

  ;; simple query - triple clauses


  ;; boolean predicate (filter)


  ;; predicates with return lvars


  ;; not

  ;; order-by
  ;; Q: What is the movie with the highest revenue?
  ;; Q: What is the movie that made the biggest loss?


  ;; join
  ;; Q: Which characters are in the movie "Alien"?


  ;; or
  ;; Q: Which actors star in the movies "Alien" or "Aliens"?


  ;; and
  ;; Q: Which actors star in the movies "Alien" and "Aliens"?


  ;; complex-join
  ;; Q: Which movies did both Brad Pitt and Cate Blanchett star in?


  ;; :rules


  ;; eql/project
  ;; Q: Retrieve all cast documents for all movies


  ;; aggregations
  ;; Q: What is the total cast size for the movie "Alien"?


  ;; args / in


  ;; open-q, limit & offset


  ;; temporal db query


  ;; transaction functions


  ;; eviction


  ;; or-join, not-join


  ;; persistence & index rebuild


  )

;; solutions
(comment
  )
