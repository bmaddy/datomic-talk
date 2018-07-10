(ns datomic-talk.wikidata
  (:import [org.wikidata.wdtk.wikibaseapi WikibaseDataFetcher]))

(def wbdf (WikibaseDataFetcher/getWikidataDataFetcher))

(def schema
  [{:db/ident :wikidata/id
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :label
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(doto (.getFilter wbdf)
  (.setSiteLinkFilter #{"enwiki"})
  (.setLanguageFilter #{"en"}))

(def q42 (.getEntityDocument wbdf "Q42"))

(defn property-id
  [statement]
  (.. statement getClaim getMainSnak getPropertyId getId))

(defn statement-value
  [statement]
  (.. statement getClaim getValue getId))

(-> q42
    (.findLabel "en")
    )

(->> q42
     .getAllStatements
     iterator-seq
     (filter #(= "P21" (property-id %)))
     first
     .getClaim
     .getValue
     .getId)

(def male (.getEntityDocument wbdf "Q6581097"))

(.findLabel male "en")

(def sex-or-gender (.getEntityDocument wbdf "P21"))
(-> sex-or-gender
     (.findLabel "en")
     )

(def solar-system-id "Q544")

(def solar-system (.getEntityDocument wbdf solar-system-id))

(def references-to-load
  #{"P397" "P398"})

(defn related-entity-ids
  [e]
  (->> e
       .getAllStatements
       iterator-seq
       (filter #(references-to-load (property-id %)))
       ;; (map property-id)
       (map statement-value)))

(defn fetch-entities
  ([es] (fetch-entities {} es))
  ([cache es]
   (->> es
        (remove (set (keys cache)))
        (.getEntityDocuments wbdf)
        (into {}))))

(defn lazy-entities
  [cache parent-ids]
  (if-not parent-ids
    cache
    (let [entities (fetch-entities parent-ids)]
      (lazy-seq (cons entities
                      (lazy-entities (merge cache entities)
                                     (->> entities
                                          vals
                                          (mapcat related-entity-ids)
                                          (remove (set (keys cache))))
                                     #_(mapcat (fn [[_ e]]
                                               (related-entity-ids e))
                                             entities)))))))

(comment
  (clojure.pprint/pprint
   (->> [solar-system-id]
        (lazy-entities {})
        (map count)
        (take 6)))

  (let [cache (fetch-entities [solar-system-id])
        related-ids (mapcat (fn [[id e]] (related-entity-ids e)) cache)

        related (fetch-entities related-ids)

        cache (merge cache related)
        related-ids (mapcat (fn [[id e]] (related-entity-ids e)) related)

        related (fetch-entities related-ids)
        ]
    (print related-ids)
    (count related))

  (.getEntityDocuments wbdf #{solar-system-id})

  )

(->> (.searchEntities wbdf "solar system" "en")
     (map (fn [r] [(.getEntityId r) (.getUrl r)]))
     (into {}))

;; PLAN:
;; get entities from recursing parent/child astronomical bodies
;; filter to allowed-attributes
;; fetch attribute labels
;; for all values that are refs, fetch those entities and their label
;; generate schema (alternatively, manually write the schema so you can decide on the cardinality)
;; transact everything

;; expected format:
[{:wikidata/id "P2"
  :wikidata/label "Earth"}
 {:wikidata/id "P42"
  :wikidata/label "Douglas Adams"
  :wikidata/sex-or-gender [[:wikidata/id "Q6581097"]]}
 {:wikidata/id "Q6581097"
  :wikidata/label "male"}]
