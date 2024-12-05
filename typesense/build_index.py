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

blacklist = ["1756-05-13", "1756-05-29", "1758-01-27", "1760-08-21", "1761-03-24", "1762-01-01"]

current_schema = {
    "name": ts_index_name,
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "rec_id", "type": "string"},
        {"name": "title", "type": "string"},
        {"name": "full_text", "type": "string", "optional": True},
        {"name": "has_fulltext", "type": "bool", "facet": True},
        {"name": "digitarium_issue", "type": "bool", "facet": True},
        {"name": "gestrich", "type": "bool", "facet": True},
        {"name": "extra_full_text", "type": "string", "optional": True},
        {"name": "day", "type": "int32", "sort": True},
        {"name": "page", "type": "int32", "sort": True},
        {
            "name": "weekday",
            "type": "int32",
            "optional": True,
            "facet": True
        },
        {
            "name": "decade",
            "type": "int32",
            "optional": True,
            "facet": True,
            "sort": True,
        },
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
        {
            "name": "page_count",
            "type": "int32",
            "optional": True,
            "facet": True,
        },
        {"name": "places", "type": "string[]", "facet": True, "optional": True},
        {"name": "places_top", "type": "string[]", "facet": True, "optional": True},
        {"name": "keywords", "type": "string[]", "facet": True, "optional": True},
        {"name": "keywords_top", "type": "string[]", "facet": True, "optional": True},
        {"name": "corrections", "type": "int32", "facet": True, "optional": True},
        {"name": "confidence", "type": "float", "facet": True, "optional": True}
    ],
}
print("building index for editions included in gestrich index")
records = []
counter = 0
for gr, ndf in tqdm(df.groupby("day")):
    if gr in blacklist:
        continue
    counter += 1
    # if counter > 200:
    #     break
    item = {}
    cfts_record = {}
    x = ndf.iloc[0]
    wr_id = f'wr_{gr}'
    indexed.append(wr_id)
    item["id"] = wr_id
    item["rec_id"] = wr_id
    item["title"] = ", ".join(x["full_title"].split(", ")[:-1])
    item["year"] = int(x["year"])
    item["issue_nr"] = int(x["issue_number"])
    ids = list(set(ndf["ID"].tolist()))
    item["article_count"] = len(ids)
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
            item["has_fulltext"] = False
        item["edition"] = ["Alle Ausgaben: Siebenjähriger Krieg"]
        try:
            item["confidence"] = ft_dict[wr_id]["confidence"]
        except KeyError:
            print("no confidence for", wr_id)
        try:
            item["weekday"] = ft_dict[wr_id]["weekday"]
        except KeyError:
            print("no weekday for", wr_id)
        try:
            item["decade"] = ft_dict[wr_id]["decade"]
        except KeyError:
            print("no decade for", wr_id)
        try:
            item["page_count"] = ft_dict[wr_id]["page_count"]
        except KeyError:
            print("no page count for", wr_id)
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
        for eft in ids:
            try:
                extra_ft = extra_text[str(eft)]
            except KeyError:
                extra_ft = ""
            extra_full_text_set.add(extra_ft)
    item["extra_full_text"] = (
        " ".join(list(extra_full_text_set)).strip().replace("  ", " ")
    )
    records.append(item)
print("done")

print("building index for for editions not included in gestrich index")
for x in tqdm(ft_dict):
    if x not in indexed:
        item = {}
        item["id"] = x
        item["rec_id"] = x
        item["title"] = ft_dict[x]["title"]
        item["year"] = int(ft_dict[x]["year"])
        item["issue_nr"] = ft_dict[x]["issue_number"]
        item["article_count"] = 0
        item["full_text"] = ft_dict[x]["text"]
        if len(item["full_text"]) > 0:
            item["has_fulltext"] = True
        else:
            item["has_fulltext"] = False
            item["full_text"] = "kein Volltext vorhanden"
        item["digitarium_issue"] = False
        item["gestrich"] = False
        item["day"] = int(ft_dict[x]["day"])
        item["page"] = 1
        item["corrections"] = 0
        item["edition"] = ["Alle Ausgaben: Siebenjähriger Krieg"]
        item["places"] = set()
        item["places_top"] = set()
        item["keywords"] = set()
        item["keywords_top"] = set()
        try:
            item["confidence"] = ft_dict[x]["confidence"]
        except KeyError:
            print("no confidence for", x)
        try:
            item["weekday"] = ft_dict[x]["weekday"]
        except KeyError:
            print("no weekday for", x)
        try:
            item["decade"] = ft_dict[x]["decade"]
        except KeyError:
            print("no decade for", x)
        try:
            item["page_count"] = ft_dict[x]["page_count"]
        except KeyError:
            print("no page count for", x)
        records.append(item)

print("done")

with open("out.json", "w", encoding="utf-8") as fp:
    json.dump(records, fp, ensure_ascii=False, indent=2, default=set_default)

with open("out.json", "r", encoding="utf-8") as fp:
    records = json.load(
        fp,
    )

indexed = list(set(indexed))
with open(indexed_json, "w", encoding="utf-8") as fp:
    json.dump(indexed, fp, ensure_ascii=False)

try:
    client.collections[ts_index_name].delete()
except ObjectNotFound:
    pass

client.collections.create(current_schema)
make_index = client.collections[ts_index_name].documents.import_(records)
print(make_index)
print(f"done with indexing of {counter} documents in {ts_index_name}")
