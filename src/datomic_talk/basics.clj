(ns datomic-talk.basics
  (:require [datomic.api :as d]
            [clojure.pprint :refer [pprint pp]]
            [gadget.core]))

(def uri "datomic:free://localhost:4334/mbrainz-1968-1973")

(comment
  (d/delete-database uri)
  (d/create-database uri)
  )

(def conn (d/connect uri))

(def si-schema
  [;; SI units
   {:db/ident :si/units
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :si.unit/yottagrams
    :db/doc "A unit of mass equal to 1,000,000,000,000,000,000,000,000 grams"}
   {:db/ident :si.unit/meters
    :db/doc "The fundamental unit of length in the metric system"}
])

(def schema
  [;; Note: This isn't how most people name their attributes, but I'm showing
   ;; this to discourage tabular thinking and show how you could merge disparate
   ;; datasets. There will be a more idiomatic example in `relational.clj`.

   ;; https://www.w3.org/TR/2014/REC-rdf-schema-20140225/#ch_label
   {:db/ident :rdfs/label
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   ;; https://schema.org/Mass
   {:db/ident :org.schema/Mass
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   ;; https://www.wikidata.org/wiki/Special:EntityData/P610
   {:db/ident :wdata/highest-point
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   ;; http://www.wikidata.org/entity/
   {:db/ident :wd/entity
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    ;; only one entity can have a given value for this attribute and "upsert" is enabled
    :db/unique :db.unique/identity}

   ;; my own attributes
   {:db/ident :datomic-talk/mountains
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   ;; for planets
   {:db/ident :datomic-talk/mass
    :db/valueType :db.type/float
    :db/cardinality :db.cardinality/one
    :si/units :si.unit/yottagrams}
   ;; for ships
   {:db/ident :datomic-talk.ship/length
    :db/valueType :db.type/float
    :db/cardinality :db.cardinality/one
    :si/units :si.unit/meters}
   ;; for movies
   {:db/ident :datomic-talk.movie/length
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   ])

(def data
  [{:db/id "earth"
    :wd/entity "Q2"
    :rdfs/label "Earth"
    :datomic-talk/mass 5972.37
    :wdata/highest-point "everest"}
   {:rdfs/label "Mars"
    :wd/entity "Q111"}
   {:db/id "asia"
    :wd/entity "Q48"
    :rdfs/label "Asia"
    :wdata/highest-point "everest"
    :datomic-talk/mountains ["everest" "K2"]}
   {:db/id "everest"
    :wd/entity "Q513"
    :rdfs/label "Mt. Everest"}
   {:db/id "K2"
    :wd/entity "Q43512"
    :rdfs/label "K2"}])

(d/transact conn si-schema)
(d/transact conn schema)
(def result (d/transact conn data))
(def earth-id (-> @result :tempids (get "earth")))
(def asia-id (-> @result :tempids (get "asia")))

(comment
  (pprint (into {} (d/entity (d/db conn) asia-id)))

  )






(comment
  ;; it's a graph, not tabular



  ;; want a format to describe absolutely anything
  ;; * Every entity has attributes and values...it's a map!

  ;; Planet Earth
  {:name "Earth"
   :mass 5972.37 ;; yottagrams
   :highest-point {:name "Mt. Everest"}
   }









  ;; but the world has multiple entities

  {11 {:name "Earth"
       :mass 5972.37 ;; yottagrams
       :highest-point {:name "Mt. Everest"}}
   12 {:name "Asia"
       :highest-point ???
       :mountains ???}}





  ;; entities in this world are graphs, not trees
  ;; * flatten and use references

  {11 {:name "Earth"
       :mass 5972.37 ;; yottagrams
       :highest-point 13}
   12 {:name "Asia"
       :highest-point 13
       :mountains [13 ...]}
   13 {:name "Mt. Everest"}}










  ;; another way to write multiple entities

  #{[11 :name          "Earth"]
    [11 :mass          5,972.37]
    [11 :highest-point 13]
    [12 :name          "Asia"]
    [12 :highest-point 13]
    [12 :mountains     13]
    [12 :mountains     19] ;; collections
    [13 :name          "Mt. Everest"]}







  ;; what about conflicting attribute names?
  ;; * namespaces to the rescue!

  #{[14 :name  "Ship of Theseus"]
    [14 :ship/length 37] ;; meters
    [15 :name "Inception"]
    [15 :movie/length 148]} ;; minutes

  ;; it's common to use the type as the namespace (eg. :person/name), but not required









  ;; Let's talk about RDF (datomic is not RDF)
  ;; * https://www.wikidata.org/wiki/Q2







  ;; how this could look in Datomic

  ;; [entity attribute      value         transaction added?]
  #{[11     :name          "Earth"       6           true]
    [11     :mass          5,972.37      6           true]
    [11     :highest-point 13            6           true]
    [12     :name          "Asia"        6           true]
    [12     :highest-point 13            6           true]
    [12     :mountains     13            6           true]
    [12     :mountains     19            6           true]
    [13     :name          "Mt. Everest" 6           true]}

  ;; Q: What is an entity without any attributes?







  ;; But wait, attributes are entities too!

  ;; [entity attribute      value          transaction added?]
  #{[1      :db/ident      :name          5           true]
    [2      :db/ident      :mass          5           true]
    [3      :db/ident      :highest-point 5           true]
    [4      :db/ident      :mountains     5           true]
    [11     1              "Earth"        6           true]
    [11     2              5,972.37       6           true]
    [11     3              13             6           true]
    [12     1              "Asia"         6           true]
    [12     3              13             6           true]
    [12     4              13             6           true]
    [12     4              19             6           true]
    [13     1              "Mt. Everest"  6           true]
    [13     5              :mountain      6           true]}

  ;; Note: tx is db time, not wall clock time








  ;; Querying methods








  ;; Datum queries
  ;; Everything is indexed
  ;; EAVT - entity, attribute, value, tx
  ;; AEVT - attribute, entity, value, tx
  ;; AVET - attribute, value, entity, tx
  ;; VAET - value, attribute, entity, tx

  ;; get all data associated with an entity
  (pprint (take 10 (d/datoms (d/db conn) :eavt earth-id)))
  ;; get all the info about this attribute
  (pprint (take 10 (d/datoms (d/db conn) :eavt 64)))
  ;; :db/ident has :db/unique set, so we can use a lookup ref
  (pprint (take 10 (d/datoms (d/db conn) :eavt [:db/ident :rdfs/label])))
  ;; also, :db/ident is just a programmatic name for a :db/id
  (pprint (take 10 (d/datoms (d/db conn) :eavt :rdfs/label)))

  ;; an indexed (and unique) attribute
  (pprint (take 10 (d/datoms (d/db conn) :eavt :wd/entity)))
  ;; all data associated with this attribute
  (pprint (take 10 (d/datoms (d/db conn) :avet :wd/entity)))
  ;; all data with this attribute and value (would see more if it wasn't unique)
  (pprint (take 10 (d/datoms (d/db conn) :avet :wd/entity "Q2")))







  ;; Entity queries

  ;; get all info about this entity as a map (actually an EntityMap)
  (pprint (into {} (d/entity (d/db conn) earth-id)))
  ;; this attribute is unique
  (pprint (into {} (d/entity (d/db conn) :wd/entity)))
  ;; use lookup refs with unique attributes
  (pprint (into {} (d/entity (d/db conn) [:wd/entity "Q2"])))









  ;; reverse relationships: _attr-name
  ;; * relationships can be reversed if you prefix the name with an underscore

  (def asia (d/entity (d/db conn) asia-id))

  ;; the first mountain
  (->> asia
       :datomic-talk/mountains
       first
       (into {})
       pprint)

  ;; has a reverse reference to the continent
  (->> asia
       :datomic-talk/mountains
       first
       :datomic-talk/_mountains
       (into {})
       pprint)





  ;; Pull expressions

  (d/pull (d/db conn) [:db/id :rdfs/label :does.not/exist] earth-id)

  ;; starting at a node in the graph, return a tree of data...
  (pprint
   (d/pull (d/db conn)
           [:db/id
            :rdfs/label
            {:wdata/highest-point [:rdfs/label {:datomic-talk/_mountains [:db/id :rdfs/label]}]}]
           earth-id))

  )
