# Diarium Index

Repo to process so called "Gestrich Index"

* legacy_data: contains a csv dump of original MS-Access-DB
* data: contains processed data better suited for further processing


## run

* run `python simplify.py` to create a simplified version and save it as `./data/data.csv`
* run `python build_index.py` to populate a typesense index based on `./data/data.csv`

## ToDos

* ~~ add `skos:broader` meaning, G1 -> Europa, G11 -> West~~
* ~~process other categories; A,~~ X
* ~~use generated files to populate a dedicated Typesense Index~~
* build web app to query and visualize data fetched from Typesense Index