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
;; only load a small subset by default
(do (d/transact conn (take 20000 trip-entities))
    ;; so we don't try to print 50k :db/ids
    :done)

(comment
  ;; load everything!
  (doall
   (map (fn [n entities]
          (d/transact conn entities)
          (println (str "~" (* 50 n) "k transacted.")))
        (rest (range))
        (partition-all 50000 trip-entities)))

  )

(gadget.core/inspect-entity-tree (d/entity (d/db conn) [:station/id 23]))
(d/q '[:find ?e .
       :where
       [?e :trip/start-station]]
     (d/db conn))
