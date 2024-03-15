import requests
import tarfile
import glob
import os
import shutil
import json
from tqdm import tqdm

from acdh_tei_pyutils.tei import TeiReader
from acdh_cfts_pyutils import TYPESENSE_CLIENT as client
from utils import ts_index_name, indexed_json


url = "https://api.github.com/repos/acdh-oeaw/digitarium-data/tarball"
target_file = "tmp.tar.gz"
tmp_dir = "tmp"
with open(indexed_json, "r", encoding="utf-8") as fp:
    indexed = json.load(fp)

print(f"fetching data from {url}")
response = requests.get(url, stream=True)
if response.status_code == 200:
    with open(target_file, "wb") as f:
        f.write(response.raw.read())

print(f"extracting {target_file} into {tmp_dir}")
file = tarfile.open(target_file)
file.extractall(tmp_dir)
file.close()
os.remove(target_file)

files = sorted(glob.glob(f"./{tmp_dir}/**/17*.xml", recursive=True))

records = []

print("building json for index")
counter = 0
for x in tqdm(files, total=len(files)):
    date = os.path.split(x)[1].replace(".xml", "")
    day = int(date.replace("-", ""))
    year = int(date.split("-")[0])
    doc = TeiReader(x)
    title = doc.any_xpath(".//tei:titleStmt/tei:title[@type='num' or @level='a']/text()")[0]
    corrections = doc.any_xpath(".//tei:revisionDesc/tei:list/tei:item")
    pb = doc.any_xpath(".//tei:pb")
    for page in pb:
        counter += 1
        nr = page.attrib["n"]
        facs = page.attrib["facs"]
        rec_id = f"wr_{date}__{nr:0>2}"
        record = {
            "id": rec_id,
            "rec_id": rec_id,
            "title": title,
            "has_fulltext": True,
            "digitarium_issue": True,
            "gestrich": False,
            "day": day,
            "page": int(nr),
            "year": year,
            "edition": ["Ausgewählte Ausgaben: 18. Jahrhundert"],
            "corrections": len(corrections)
        }
        full_text = doc.any_xpath(f""".//tei:body/tei:div[@type='page'][@n='{nr}']|
                                  .//tei:body/tei:div[@type='article'][tei:*[contains(@facs, '{facs}')]]""")
        record["full_text"] = (
            " ".join(" ".join("".join(p.itertext()).split()) for p in full_text)
            .replace("\n", " ")
            .replace("¬ ", "")
            .replace(" / ", " ")
            .replace("= ", "")
            .replace("=", "")
        )
        if rec_id in indexed:
            record["gestrich"] = True
        else:
            record["extra_full_text"] = ""
            record["places"] = []
            record["places_top"] = []
            record["keywords"] = []
            record["keywords_top"] = []
        records.append(record)


make_index = client.collections[ts_index_name].documents.import_(
    records, {"action": "upsert"}
)
print(make_index)
print(f"done with indexing {counter} new documents for {ts_index_name}")

shutil.rmtree(tmp_dir)

with open("out_diarim.json", "w", encoding="utf-8") as fp:
    json.dump(records, fp, ensure_ascii=False, indent=2)
