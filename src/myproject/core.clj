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
   :tmdb.movie/id :tmdb/movie-5516,
   :tmdb.cast/id :tmdb/cast-17305,
   :tmdb.cast/character "TV Commercial Director"}
  )

;; workshop
(comment
  ;; new node, db & entity

  (def mem-node (c/start-node {}))

  (c/status mem-node)

  ;; transactions

  (c/submit-tx mem-node [[:crux.tx/put {:crux.db/id :jeremy
                                        :location :uk}]
                         [:crux.tx/put {:crux.db/id :jeremy
                                        :success :true
                                        :location :uk}]
                         [:crux.tx/put {:crux.db/id :jeremy
                                        :location :uk}]
                         [:crux.tx/delete :jeremy]])


  (c/submit-tx mem-node [[:crux.tx/match {:crux.db/id :jeremy
                                          :counter 1}]
                         [:crux.tx/put {:crux.db/id :jeremy
                                        :counter 2}]
                         [:crux.tx/put {:crux.db/id :jeremy
                                        :success :true
                                        :location :uk}]
                         [:crux.tx/put {:crux.db/id :jeremy
                                        :location :uk}]
                         [:crux.tx/delete :jeremy]])

  (c/entity (c/db mem-node) :jeremy)
  (c/entity-history (c/db mem-node) :jeremy :asc)

(time
 (do
   (->>  (c/submit-tx mem-node [[:crux.tx/put {:crux.db/id :jeremy
                                               :location :uk}]
                                [:crux.tx/put {:crux.db/id :jon
                                               :location :scotland}]])

         (c/await-tx mem-node))
   (c/entity (c/db mem-node) :jon)))

  ;; await-tx & sync

(time
 (do
   (c/submit-tx mem-node [[:crux.tx/put {:crux.db/id :jeremy
                                         :location :uk}]
                          [:crux.tx/put {:crux.db/id :jeremy
                                         :location :portugal}
                           #inst "2020-11-02"
                           #inst "2020-11-07"]

                          [:crux.tx/put {:crux.db/id :jon
                                         :location :portugal}
                           #inst "2020-11-04"
                           #inst "2020-11-06"]
                          [:crux.tx/put {:crux.db/id :jon
                                         :location :spain}
                           #inst "2020-12-04"
                           #inst "2020-12-06"]
                          [:crux.tx/put {:crux.db/id :jon
                                         :location :uk}
                           ]
                          ])
   (c/sync mem-node)
   (c/entity (c/db mem-node) :jon))
)
(def mynode mem-node)

(q '{:find [(eql/project ?e [*]) (eql/project ?e2 [*])]
     :where [[?e :location ?l]
             [(not= ?e ?e2)]
             [?e2 :location ?l]]}
   #inst "2020-11-05"
)


  ;; entity-history



  (c/entity-history (c/db mem-node #inst "2020-12-11") :jon :asc {:with-docs? true})
 
  ;; open-tx-log


  ;; tmdb
  (def mynode rocks-node)

  ;; helpers
  (defn q [m & [vt tt]]
    (c/q (c/db mynode vt tt) (merge {:limit 10} m)))

  (defn e [eid & [vt tt]]
    (c/entity (c/db mynode vt tt) eid))

  ;; simple query - triple clauses

  (c/q (c/db mynode)
       '{:find [?e]
         :where [[?e :crux.db/id ?e]]
         :limit 10})

  (q '{:find [?e ?t]
      :where [[?e :tmdb.movie/title ?t]]
      })

  (e :tmdb/movie-10027)

  (q '{:find [?e ?title ?date]
       :where [[?e :tmdb.movie/title ?title]
               [?e :tmdb.movie/release_date ?date]]
       })


  ;; boolean predicate (filter)

  (q '{:find [?e ?title ?date]
       :where [[?e :tmdb.movie/title ?title]
               [?e :tmdb.movie/release_date ?date]
               [(> ?date "2006")]]
       })



  ;; predicates with return lvars


  (q '{:find [?e ?title ?date ?profit ?budget ?revenue]
       :where [[?e :tmdb.movie/title ?title]
               [?e :tmdb.movie/release_date ?date]
               [(> ?date "2006")]
               [?e :tmdb.movie/budget ?budget]
               [(> ?budget 0)]
               [?e :tmdb.movie/revenue ?revenue]
               [(- ?revenue ?budget) ?profit]]
       })

  (defn minus [a b]
    (if (string? b)
      0
      (- a b))
    )

  (q '{:find [?e ?title ?date ?profit ?budget ?revenue]
       :where [[?e :tmdb.movie/title ?title]
               [?e :tmdb.movie/release_date ?date]
               [(> ?date "2006")]
               [?e :tmdb.movie/budget ?budget]
               [(> ?budget 0)]
               [?e :tmdb.movie/revenue ?revenue]
               [(myproject.core/minus ?revenue ?title) ?profit]]
       })




  ;; not

  (q '{:find [?e ?title ?date]
       :where [[?e :tmdb.movie/title ?title]
               [?e :tmdb.movie/release_date ?date]
               (not [?e :foo :bar])
               (not [(> ?date "2006")])
               ]
       })

  ;; order-by

  (q '{:find [(max ?budget) ?title]
       :where [[?e :tmdb.movie/title ?title]
               [?e :tmdb.movie/release_date ?date]
               [(> ?date "2006")]
               [?e :tmdb.movie/budget ?budget]
               ]
       :limit 1
       })


  ;; Q: What is the movie with the highest revenue?
  ;; Q: What is the movie that made the biggest loss?


  ;; join
  ;; Q: Which characters are in the movie "Alien"?

  (q '{:find [?e ?credit ?character]
       :where [[?e :tmdb.movie/title "Alien"]
               [?credit :tmdb.movie/id ?e]
               [?credit :tmdb.cast/character ?character]
               [?character :tmdb.cast/name ?name] 
               ]
       })


  (q
   '{:find [?e ?credit ?character ?name]
     :where [[?e :tmdb.movie/title "Alien"]
             [?credit :tmdb.movie/id ?e]
             [?credit :tmdb.cast/character ?character]
             [?credit :tmdb.cast/id ?cast]
             [?cast :tmdb.cast/name ?name]
             ]}) 

  ;; What are the movies where a cast member plays multiple characters



  ;; or
  ;; Q: Which actors star in the movies "Alien" or "Aliens"?

  (q '{:find [?actor-name]
       :where [[?credit :tmdb.cast/id ?actor]
               [?credit :tmdb.movie/id ?movie]
               (or [?movie :tmdb.movie/title "Alien"]
                   [?movie :tmdb.movie/title "Aliens"])
               [?actor :tmdb.cast/name ?actor-name]]}) 


  (q '{:find [?actor-name]
       :where [[?credit :tmdb.cast/id ?actor]
               [?credit :tmdb.movie/id ?movie]
               #_[?movie :tmdb.movie/title #{"Aliens" "Alien"}]
               (or-join [?movie]
                        [?movie :tmdb.movie/title "Alien"]
                        [(text-seach :tmdb.movie/title  )]
                        [?movie :tmdb.movie/title "Aliens"])
               [?actor :tmdb.cast/name ?actor-name]]}) 


  ;; and
  ;; Q: Which actors star in the movies "Alien" and "Aliens"?


  ;; complex-join
  ;; Q: Which movies did both Brad Pitt and Cate Blanchett star in?


  ;; :rules


  ;; eql/project
  ;; Q: Retrieve all cast documents for all movies

  (q {:find '[(eql/project e [{:tmdb.movie/_id [{:tmdb.cast/id [*]}]}])]
      :where '[[e :tmdb/type :movie]]})

  (q '{:find [(eql/project ?cast [*])]
       :where [[?cast :tmdb.cast/id]]})

  ;; aggregations
  ;; Q: What is the total cast size for the movie "Alien"?


  ;; :args / :in

  (q '{:find [?e ?credit ?character]
       :where [[?e :tmdb.movie/title ?movie]
               [?credit :tmdb.movie/id ?e]
               [?credit :tmdb.cast/character ?character]]
       :args [{?movie "Alien"} {?movie #{"Aliens"}}]
       })



  ;; open-q, limit & offset

  (time
   (doall
    (take 40  (->  (c/open-q (c/db mynode) '{:find [?e ?credit ?character]
                                               :where [[?e :tmdb.movie/title ?movie]
                                                       [?credit :tmdb.movie/id ?e]
                                                       [?credit :tmdb.cast/character ?character]]
                                               })
                     iterator-seq
                     ))))


  ;; temporal db query


  ;; transaction functions


  ;; eviction


  ;; or-join, not-join


  ;; persistence & index rebuild


  (c/latest-completed-tx rocks-node)

  (c/q db valid-time transaction-time)

  )



;; solutions
(comment
  )
