(ns datomic-talk.relational
  (:require [datomic.api :as d]
            [clojure.pprint :refer [pprint pp]]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [gadget.core]))

;; This is a walk through of the SQL tutorial from Dataquest here:
;; https://www.dataquest.io/blog/sql-basics/

;; install sqlite and download their database here for importing:
;; https://dataquest.io/blog/large_files/hubway.db

(def sqlite-cmd "sqlite3")
(def db-file "./hubway.db")

(def uri "datomic:free://localhost:4334/mbrainz-1968-1973")

(comment
  (d/delete-database uri)
  (d/create-database uri)
  )

(def conn (d/connect uri))

(def schema
  [;; stations
   {:db/ident :station/id
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :station/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :station/municipality
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :station/lat
    :db/valueType :db.type/float
    :db/cardinality :db.cardinality/one}
   {:db/ident :station/lng
    :db/valueType :db.type/float
    :db/cardinality :db.cardinality/one}

   ;; trips
   {:db/ident :trip/duration
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/start-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/start-station
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/end-date
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/end-station
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/bike-number
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/sub-type
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/zip-code
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/birth-date
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :trip/gender
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   ])

(d/transact conn schema)

(defonce raw-stations (:out (sh/sh sqlite-cmd db-file "SELECT * FROM stations")))
(defonce raw-trips (:out (sh/sh sqlite-cmd db-file "SELECT * FROM trips")))

(defn station-record->entity
  [[id name municipality lat lng]]
  {:station/id (Integer. id)
   :station/name name
   :station/municipality municipality
   :station/lat (Float. lat)
   :station/lng (Float. lng)})

(defn parse-time
  [s]
  (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd' 'HH:mm:ss") s))

(defn trip-record->entity
  [[_ duration start-date start-station end-date end-station
    bike-number sub-type zip-code birth-date gender]]
  (->> {:trip/duration (Long. duration)
        :trip/start-date (parse-time start-date)
        :trip/start-station (when-not (empty? start-station)
                              [:station/id (Integer. start-station)])
        :trip/end-date (parse-time end-date)
        :trip/end-station (when-not (empty? end-station)
                            [:station/id (Integer. end-station)])
        :trip/bike-number bike-number
        :trip/sub-type sub-type
        :trip/zip-code zip-code
        :trip/birth-date (when-not (empty? birth-date)
                           (long (Float. birth-date)))
        :trip/gender gender}
       (remove #(nil? (val %)))
       (into {})))

(def station-entities
  (->> (str/split raw-stations #"\n")
       (map #(str/split % #"\|"))
       (map station-record->entity)))

(def trip-entities
  (->> (str/split raw-trips #"\n")
       (map #(str/split % #"\|"))
       (map trip-record->entity)))

(comment
  (count station-entities)
  (pprint (first station-entities))

  (count trip-entities)
  (pprint (first trip-entities))

  )

(d/transact conn station-entities)

(comment
  ;; only load a small subset
  (do (d/transact conn (take 20000 trip-entities))
      ;; so we don't try to print 50k :db/ids
      :done)

  ;; load everything!
  (doall
   (map (fn [n entities]
          (d/transact conn entities)
          (println (str "~" (* 50 n) "k transacted.")))
        (rest (range))
        (partition-all 50000 trip-entities)))

  )

(into {} (d/entity (d/db conn) [:station/id 23]))
(d/q '[:find ?e .
       :where
       [?e :trip/start-station]]
     (d/db conn))



(comment

  ;; see the start_date and bike_number for the first five trips in the database

  ;; SELECT * FROM trips LIMIT 5;
  (take 5 (d/q '[:find [?e ...]
                 :where
                 [?e :trip/bike-number]]
               (d/db conn)))

  ;; or
  (d/q '[:find (sample 5 ?e) .
         :where
         [?e :trip/bike-number]]
       (d/db conn))



  ;; SELECT duration, start_date FROM trips LIMIT 5
  (pprint
   (take 5 (d/q '[:find [(pull ?e [:trip/duration :trip/start-date]) ...]
                  :where
                  [?e :trip/duration]
                  [?e :trip/start-date]]
                (d/db conn))))









  ;; find out how long the longest 10 trips lasted

  ;; SELECT duration
  ;; FROM trips
  ;; ORDER BY duration DESC
  ;; LIMIT 10
  (pprint
   (take 10
         (reverse
          (sort (d/q '[:find [?d ...]
                       :where
                       [?e :trip/duration ?d]]
                     (d/db conn))))))






  ;; registered trips longer than 9990

  ;; SELECT *
  ;; FROM trips
  ;; WHERE (duration >= 9990) AND (sub_type = "Registered")
  ;; ORDER BY duration DESC;
  (pprint
   (take 20
         (d/q '[:find [(pull ?e [*]) ...]
                :where
                [?e :trip/duration ?d]
                [(>= ?d 9900)]
                [?e :trip/sub-type "Registered"]]
              (d/db conn))))






  ;; How many trips were taken by 'registered' users?

  ;; SELECT COUNT(*)
  ;; FROM trips
  ;; WHERE sub_type = "Registered";
  (d/q '[:find (count ?e) .
         :where
         [?e :trip/sub-type "Registered"]]
       (d/db conn))






  ;; What was the average trip duration?

  ;; SELECT AVG(duration) AS "Average Duration"
  ;; FROM trips;
  (d/q '[:find (avg ?d) .
         :where
         [_ :trip/duration ?d]]
       (d/db conn))








  ;; do registered or casual users take longer trips?

  ;; SELECT sub_type, AVG(duration) AS "Average Duration"
  ;; FROM trips
  ;; GROUP BY sub_type;
  (d/q '[:find ?st (avg ?d)
         :where
         [?e :trip/sub-type ?st]
         [?e :trip/duration ?d]]
       (d/db conn))







  ;; which bike was used for the most trips

  ;; SELECT bike_number as "Bike Number", COUNT(*) AS "Number of Trips"
  ;; FROM trips
  ;; GROUP BY bike_number
  ;; ORDER BY COUNT(*) DESC
  ;; LIMIT 1;
  (take 10
        (reverse
         (sort-by second (d/q '[:find ?bn (count ?e)
                                :where
                                [?e :trip/bike-number ?bn]
                                #_[(not-empty ?bn)]]
                              (d/db conn)))))







  ;; average duration of trips by registered members who were over the age of 30 in 2017

  ;; SELECT AVG(duration)
  ;; FROM trips
  ;; WHERE (2017 - birth_date) > 30;

  ;; expected: 923.014685
  (d/q '[:find (avg ?d) .
         :where
         [?e :trip/birth-date ?bd]
         [(- 2017 ?bd) ?age-in-2017]
         [(> ?age-in-2017 30)]
         [?e :trip/duration ?d]]
       (d/db conn))





  ;; which station is the most frequent starting point?

  ;; SELECT stations.station AS "Station", COUNT(*) AS "Count"
  ;; FROM trips
  ;; INNER JOIN stations
  ;; ON trips.start_station = stations.id
  ;; GROUP BY stations.station
  ;; ORDER BY COUNT(*) DESC
  ;; LIMIT 5;
  (take 5 (d/q '[:find ?s (count ?t)
                 :where
                 [?t :trip/start-station ?s]]
               (d/db conn)))







  ;; which stations are most frequently used for round trips?

  ;; SELECT stations.station AS "Station", COUNT(*) AS "Count"
  ;; FROM trips
  ;; INNER JOIN stations
  ;; ON trips.start_station = stations.id
  ;; WHERE trips.start_station = trips.end_station
  ;; GROUP BY stations.station
  ;; ORDER BY COUNT(*) DESC
  ;; LIMIT 5;
  (take 5 (d/q '[:find ?s (count ?t)
                 :where
                 [?t :trip/start-station ?s]
                 [?t :trip/end-station ?s]]
               (d/db conn)))







  ;; how many trips start and end in different municipalities?

  ;; SELECT COUNT(trips.id) AS "Count"
  ;; FROM trips
  ;; INNER JOIN stations AS start
  ;; ON trips.start_station = start.id
  ;; INNER JOIN stations AS end
  ;; ON trips.end_station = end.id
  ;; WHERE start.municipality <> end.municipality;
  (d/q '[:find (count ?t) .
         :where
         [?t :trip/start-station ?start]
         [?t :trip/end-station ?end]
         [(not= ?start ?end)]]
       (d/db conn))





  ;; When were each of the attributes for this station last modified and who made the change?

  ;; the entity we'll modify
  (into {} (d/entity (d/db conn) [:station/id 5]))

  ;; add an attribute to track who made changes
  (d/transact conn [{:db/ident :audit/author
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}
                    {:db/ident :audit/source
                     :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])

  ;; make a change and include audit info
  (d/transact conn [{:station/id 5
                     :station/name "NorthEast University"}
                    ;; use :db/id "datomic.tx" to add data to the transaction
                    {:db/id (d/tempid :db.part/tx)
                     :audit/author "Brian"
                     :audit/source "updates.2018-07-11.xml"}])

  ;; the times each attribute changed
  (pprint
   (d/q '[:find ?attr-name ?v ?t
          :where
          [?e :station/id 5]
          [?e ?a ?v ?tx]
          [?a :db/ident ?attr-name]
          [?tx :db/txInstant ?t]]
        (d/db conn)))


  ;; what is every name this station ever had?
  (let [db (d/db conn)]
    (map (fn [[name tx]]
           [name (d/pull db '[:audit/author :audit/source] tx)])
         (d/q '[:find ?name ?tx
                :in $ ?station-id
                :where
                [?e :station/id ?station-id]
                [?e :station/name ?name ?tx true]]
              (d/history (d/db conn)) 5)))


  ;; query the db as of a certain point in time
  (into {} (d/entity (d/as-of (d/db conn) #inst "2018-07-11T15:53:29")
                     [:station/id 5]))



  )
