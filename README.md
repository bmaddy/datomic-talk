## Datomic Talk

## basic stuff about Datomic
See `datomic-talk.basics` 

```bash
# get datomic free
wget https://my.datomic.com/downloads/free/0.9.5697 -O datomic-free-0.9.5697.zip
unzip datomic-free-0.9.5697.zip

# start datomic (after downloading Datomic free)
cd ~/bin/datomic-free-0.9.5697
bin/transactor config/samples/free-transactor-template.properties
```

## relational queries and comparisons to sql
See `datomic-talk.relational`

```bash
# load into datomic
pushd ~/bin/datomic-free-0.9.5697
bin/datomic restore-db file:///Users/bmaddy/src/bmaddy/datomic-talk/mbrainz-1968-1973 datomic:free://localhost:4334/mbrainz-1968-1973
```


## Not finished:
* datomic-talk.codd: Codd's complaints about sql and how they stack up in Datomic
* datomic-talk.wikidata: import entities from wikidata
