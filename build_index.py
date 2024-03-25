import pandas as pd
import json
import os
from tqdm import tqdm
from typesense.api_call import ObjectNotFound
from acdh_cfts_pyutils import TYPESENSE_CLIENT as client
from utils import set_default, ts_index_name, indexed_json


tmp_file = os.path.join("data", "data.jsonl")
indexed = []

ft_df = pd.read_json(path_or_buf=tmp_file, lines=True).set_index("id")
ft_dict = ft_df.to_dict("index")

data = os.path.join("data", "data.csv")
extra_ft_source = os.path.join("legacy_data", "_Wien_X_.csv")
df = pd.read_csv(data)

print(f"fetching extra full text from {extra_ft_source}")
extra_df = pd.read_csv(extra_ft_source)
extra_text = {}
for gr, ndf in tqdm(extra_df.groupby("ID_X")):
    full_text = []
    for i, row in ndf.iterrows():
        if isinstance(row["Stichwort_X"], str):
            full_text.append(row["Stichwort_X"])
    extra_text[f"{gr}"] = " ".join(full_text)

try:
    client.collections[ts_index_name].delete()
except ObjectNotFound:
    pass

current_schema = {
    "name": ts_index_name,
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "rec_id", "type": "string"},
        {"name": "title", "type": "string"},
        {"name": "full_text", "type": "string"},
        {"name": "has_fulltext", "type": "bool", "facet": True},
        {"name": "digitarium_issue", "type": "bool", "facet": True},
        {"name": "gestrich", "type": "bool", "facet": True},
        {"name": "extra_full_text", "type": "string"},
        {"name": "day", "type": "int32", "sort": True},
        {"name": "page", "type": "int32", "sort": True},
        {
            "name": "year",
            "type": "int32",
            "optional": True,
            "facet": True,
            "sort": True,
        },
        {
            "name": "issue_nr",
            "type": "int32",
            "optional": True,
            "facet": True,
            "sort": True,
        },
        {
            'name': 'edition',
            'type': 'string[]',
            'optional': True,
            'facet': True,
        },
        {
            "name": "article_count",
            "type": "int32",
            "optional": True,
            "facet": True,
            "sort": True,
        },
        {"name": "places", "type": "string[]", "facet": True, "optional": True},
        {"name": "places_top", "type": "string[]", "facet": True, "optional": True},
        {"name": "keywords", "type": "string[]", "facet": True, "optional": True},
        {"name": "keywords_top", "type": "string[]", "facet": True, "optional": True},
        {"name": "corrections", "type": "int32", "facet": True, "optional": True}
    ],
}
print("building index")
records = []
counter = 0
for gr, ndf in tqdm(df.groupby("wr_id")):
    counter += 1
    # if counter > 200:
    #     break
    item = {}
    cfts_record = {}
    x = ndf.iloc[0]
    wr_id = f'{x["wr_id"]}'
    indexed.append(wr_id)
    item["id"] = f'{x["wr_id"]}'
    item["rec_id"] = f'{x["wr_id"]}'
    item["title"] = ", ".join(x["full_title"].split(", ")[:-1])
    item["year"] = int(x["year"])
    item["issue_nr"] = int(x["issue_number"])
    item["ids"] = list(set(ndf["ID"].tolist()))
    item["article_count"] = len(item["ids"])
    item["has_fulltext"] = False
    item["digitarium_issue"] = False
    item["gestrich"] = True
    item["day"] = int(x["day"].replace("-", ""))
    item["page"] = int(x["page"])
    full_text = set()
    try:
        index_matched = ft_dict[wr_id]
    except KeyError:
        index_matched = False
    if index_matched:
        item["full_text"] = ft_dict[wr_id]["text"]
        if len(item["full_text"]) > 0:
            item["has_fulltext"] = True
        else:
            item["full_text"] = "kein Volltext vorhanden"
        item["edition"] = ["Alle Ausgaben: Siebenjähriger Krieg"]
        try:
            item["confidence"] = ft_dict[wr_id]["confidence"]
        except KeyError:
            print("no confidence for", wr_id)
    else:
        item["edition"] = ["Gestrich"]
        print("no index match for", wr_id)
    item["places"] = set()
    item["places_top"] = set()
    item["keywords"] = set()
    item["keywords_top"] = set()
    item["corrections"] = 0
    for i, row in ndf.iterrows():
        for term in ["Beschreibung", "top_concept", "skos_broader"]:
            if isinstance(row[term], str):
                full_text.add(row[term])
                if row["category"] == "G":
                    if term == "Beschreibung":
                        item["places"].add(row[term])
                    if term == "top_concept":
                        item["places_top"].add(row[term])
                else:
                    if term == "Beschreibung":
                        item["keywords"].add(row[term])
                    if term == "top_concept":
                        item["keywords_top"].add(row[term])

        extra_full_text_set = set()
        for eft in item["ids"]:
            try:
                extra_ft = extra_text[str(eft)]
            except KeyError:
                extra_ft = ""
            extra_full_text_set.add(extra_ft)
    item["extra_full_text"] = (
        " ".join(list(extra_full_text_set)).strip().replace("  ", " ")
    )
    records.append(item)

with open("out.json", "w", encoding="utf-8") as fp:
    json.dump(records, fp, ensure_ascii=False, indent=2, default=set_default)

with open("out.json", "r", encoding="utf-8") as fp:
    records = json.load(
        fp,
    )

try:
    client.collections[ts_index_name].delete()
except Exception as e:
    print(e)
client.collections.create(current_schema)
make_index = client.collections[ts_index_name].documents.import_(records)
print(make_index)
print(f"done with indexing of {counter} documents in {ts_index_name}")

indexed = list(set(indexed))
with open(indexed_json, "w", encoding="utf-8") as fp:
    json.dump(indexed, fp, ensure_ascii=False)
