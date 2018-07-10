(ns datomic-talk.basics
  (:require [datomic.api :as d]
            [clojure.pprint :refer [pprint pp]]
            [gadget.core]))

(def uri "datomic:free://localhost:4334/mbrainz-1968-1973")

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
  [;; planets
   {:db/ident :planet/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    ;; only one entity can have a given value for this attribute and "upsert" is enabled
    :db/unique :db.unique/identity}
   {:db/ident :planet/mass
    :db/valueType :db.type/float
    :db/cardinality :db.cardinality/one
    :si/units :si.unit/yottagrams}
   {:db/ident :planet/highest-point
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}

   ;; mountains
   {:db/ident :mountain/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}

   ;; continents
   {:db/ident :continent/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :continent/highest-point
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :continent/mountains
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}

   ;; ships
   {:db/ident :ship/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :ship/length
    :db/valueType :db.type/float
    :db/cardinality :db.cardinality/one
    :si/units :si.unit/meters}

   ;; movies
   {:db/ident :movie/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :movie/length
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   ])

(def data
  [{:planet/name "Earth"
    :planet/mass 5972.37
    :planet/highest-point "everest"}
   {:planet/name "Mars"}
   {:continent/name "Asia"
    :continent/highest-point "everest"
    :continent/mountains ["everest" "K2"]}
   {:db/id "everest"
    :mountain/name "Mt. Everest"}
   {:db/id "K2"
    :mountain/name "K2"}])

(d/transact conn si-schema)
(d/transact conn schema)
(d/transact conn data)

(def earth-ids (d/q '[:find [?e ...]
                     :where
                     [?e :planet/name "Earth"]]
                   (d/db conn)))

(gadget.core/inspect-entity-tree (d/entity (d/db conn) [:planet/name "Earth"]))

(comment
  (d/delete-database uri)
  (d/create-database uri)

  )
** it's a graph, not tabular
*** want a format to describe absolutely anything
   : Every entity has attributes and values...it's a map!
#+BEGIN_SRC: clojure
;; Planet Earth
{:name "Earth"
 :mass 5,972.37 ;; yottagrams
 :highest-point {:name "Mt. Everest"}
}
#+END_SRC
*** but the world has multiple entities
#+BEGIN_SRC: clojure
{11 {:name "Earth"
     :mass 5,972.37 ;; yottagrams
     :highest-point {:name "Mt. Everest"}}
 12 {:name "Asia"
     :highest-point ???
     :mountains ???}}
#+END_SRC
*** entities in this world are graphs, not trees
   : flatten and use references
#+BEGIN_SRC: clojure
{11 {:name "Earth"
     :mass 5,972.37 ;; yottagrams
     :highest-point 13}
 12 {:name "Asia"
     :highest-point 13
     :mountains [13 ...]}
 13 {:name "Mt. Everest"}}
#+END_SRC
*** another way to write multiple entities
#+BEGIN_SRC: clojure
#{[11 :name          "Earth"]
  [11 :mass          5,972.37]
  [11 :highest-point 13]
  [12 :name          "Asia"]
  [12 :highest-point 13]
  [12 :mountains     13]
  [12 :mountains     19] ;; collections
  [13 :name          "Mt. Everest"]}
#+END_SRC
*** what about conflicting attribute names?
   : namespaces to the rescue!
#+BEGIN_SRC: clojure
#{[14 :name  "Ship of Theseus"]
  [14 :ship/length 37] ;; meters
  [15 :name "Inception"
  [15 :movie/length 148]} ;; minutes
#+END_SRC
   : it's common to use the type as the namespace (eg. :person/name), but not required
*** Let's talk about RDF (datomic is not RDF)
   : https://www.wikidata.org/wiki/Q2
*** how this could look in Datomic
#+BEGIN_SRC: clojure
;; [entity attribute      value         transaction added?]
#{ [11     :name          "Earth"       6           true]
   [11     :mass          5,972.37      6           true]
   [11     :highest-point 13            6           true]
   [12     :name          "Asia"        6           true]
   [12     :highest-point 13            6           true]
   [12     :mountains     13            6           true]
   [12     :mountains     19            6           true]
   [13     :name          "Mt. Everest" 6           true]}
#+END_SRC
   : Q: What is an entity without any attributes?
*** But wait, attributes are entities too!
#+BEGIN_SRC: clojure
;; [entity attribute      value          transaction added?]
#{ [1      :db/ident      :name          5           true]
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
#+END_SRC
   : Note: tx is db time, not wall clock time
** Querying methods
*** Datum queries
**** Everything is indexed
    : EAVT - entity, attribute, value, tx
    : AEVT - attribute, entity, value, tx
    : AVET - attribute, value, entity, tx
    : VAET - value, attribute, entity, tx
#+BEGIN_SRC: clojure
;; get all data associated with an entity
(pprint (take 10 (d/datoms (d/db conn) :eavt [:planet/name "Earth"])))
;; get all the info about this attribute
(pprint (take 10 (d/datoms (d/db conn) :eavt 64)))
;; :db/ident has :db/unique set, so we can use a lookup ref
(pprint (take 10 (d/datoms (d/db conn) :eavt [:db/ident :planet/name])))
;; also, :db/ident is just a programmatic name for a :db/id
(pprint (take 10 (d/datoms (d/db conn) :eavt :planet/name)))
;; all data associated with this attribute
(pprint (take 10 (d/datoms (d/db conn) :avet :planet/name)))
;; all data with this attribute and value
(pprint (take 10 (d/datoms (d/db conn) :avet :planet/name "Earth")))
#+END_SRC
*** Entity queries
#+BEGIN_SRC: clojure
;; get all info about this entity as a map (actually an EntityMap)
(pprint (into {} (d/entity (d/db conn) [:planet/name "Earth"])))
;; this attribute is unique
(pprint (into {} (d/entity (d/db conn) [:db/ident :planet/name])))
(pprint (seq (d/datoms (d/db conn) :eavt :planet/name)))
;; lookup refs work here also
(pprint (into {} (d/entity (d/db conn) [:planet/name "Earth"])))
#+END_SRC
*** reverse relationships: _attr-name
   : relationships can be reversed if you prefix the name with an underscore
#+BEGIN_SRC: clojure
(def asia (d/entity (d/db conn) [:continent/name "Asia"]))
;; the first mountain
(->> asia
     :continent/mountains
     first
     (into {})
     pprint)
;; has a reverse reference to the continent
(->> asia
     :continent/mountains
     first
     :continent/_mountains
     (into {})
     pprint)
#+END_SRC
*** Pull expressions
#+BEGIN_SRC: clojure
(d/pull (d/db conn) [:db/id :planet/name] [:planet/name "Earth"])
;; starting at a node in the graph, return a tree of data...
(pprint
 (d/pull (d/db conn)
         [:db/id
          :planet/name
          {:planet/highest-point [:name {:continent/_mountains [:db/id :continent/name]}]}]
         [:planet/name "Earth"]))
#+END_SRC
