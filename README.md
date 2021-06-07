# authority-data-scripts
Code for dealing with external authority data and datasets


## wikidata

### index_dump

Create a Sqlite database with a reduced form of the wikidata dataset, from the gzip JSON dump. The resulting db is 220 Gb, from a 100 Gb gzip file.
It does not include any qualifiers, references or sitelinks. It also collapses all of the repeated internal self-documentation metadata like property datatypes into a `_p_meta` value. On a 2020 macbook pro, it takes 12 hours to build.  Thereafter it takes ~ 7 milliseconds to retrieve and parse the JSON for an entry.

