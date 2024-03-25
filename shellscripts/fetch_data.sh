# bin/bash

echo "fetching transkriptions from data_repo"
rm -rf data/editions && mkdir data/editions
curl -LO https://github.com/wiener-diarium/wr-data/archive/refs/heads/main.zip
unzip main

mkdir ./data/editions
mkdir ./data/editions/legacy
mkdir ./data/editions/present
mv ./wr-data-main/data/wrd-legacy/*.xml ./data/editions/legacy
mv ./wr-data-main/data/wrd-present/*.xml ./data/editions/present

rm -rf wr-data-main
rm main.zip
rm -rf ./data-main