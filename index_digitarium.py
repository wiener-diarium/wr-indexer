import requests
import tarfile
import glob
import os
import shutil
import json
from tqdm import tqdm

from acdh_tei_pyutils.tei import TeiReader
from acdh_cfts_pyutils import TYPESENSE_CLIENT as client
from utils import ts_index_name


url = "https://api.github.com/repos/acdh-oeaw/digitarium-data/tarball"
target_file = "tmp.tar.gz"
tmp_dir = "tmp"

# print(f"fetching data from {url}")
# response = requests.get(url, stream=True)
# if response.status_code == 200:
#     with open(target_file, "wb") as f:
#         f.write(response.raw.read())

# print(f"extracting {target_file} into {tmp_dir}")
# file = tarfile.open(target_file)
# file.extractall(tmp_dir)
# file.close()
# os.remove(target_file)

files = sorted(glob.glob(f"./{tmp_dir}/**/17*.xml", recursive=True))

records = []

print("building json for index")
counter = 0
for x in tqdm(files, total=len(files)):
    date = os.path.split(x)[1].replace(".xml", "")
    day = int(date.replace("-", ""))
    year = int(date.split("-")[0])
    page = 0
    doc = TeiReader(x)
    for p in doc.any_xpath(".//tei:div[@type='page']"):
        counter += 1
        page += 1
        rec_id = f"wr_{date}__{page}"
        record = {
            "id": rec_id,
            "rec_id": rec_id,
            "title": f"{date}, S. {page}",
            "has_fulltext": True,
            "digitarium_issue": True,
            "extra_full_text": "",
            "day": day,
            "page": page,
            "year": year,
            "places": [
                "keine Orte",
            ],
            "places_top": ["keine Orte"],
            "keywords": [
                "keine Schlagworte",
            ],
            "keywords_top": [
                "keine Schlagworte",
            ],
        }
        record["full_text"] = " ".join("".join(p.itertext()).split()).replace(
            " / ", " "
        )
        records.append(record)
make_index = client.collections[ts_index_name].documents.import_(
    records, {"action": "upsert"}
)
print(make_index)
print(f"done with indexing {counter} new documents for {ts_index_name}")

# shutil.rmtree(tmp_dir)

with open("out_diarim.json", "w", encoding="utf-8") as fp:
    json.dump(records, fp, ensure_ascii=False, indent=2)