# Diarium Index (Typesense and NoSke)

Repo to process so called "Gestrich Index"

* legacy_data: contains a csv dump of original MS-Access-DB
* data: contains processed data better suited for further processing


## Typsense Index

* run `python simplify.py` to create a simplified version and save it as `./data/data.csv`
* run `python build_index.py` to populate a typesense index based on `./data/data.csv`
* run `python index_digitarium.py` to add digitarium-issues

## NoSke Index

### Verticals

run `pipenv run python noske/mk_verticals.py` to create vertical files based on `./data/editions`

### Docker Image for NoSke

* build the image with `docker build -t diarium/noske .`
* run the image with `docker run --rm -it -p 8080:8080 diarium/noske`

Note: The Docker Build file is located in `./noske/Dockerfile`. To build to image locally, move the Dockerfile to the root of the project and run `docker build -t diarium/noske .`