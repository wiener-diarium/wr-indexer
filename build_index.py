import pandas as pd
from tqdm import tqdm
from typesense.api_call import ObjectNotFound
from acdh_cfts_pyutils import TYPESENSE_CLIENT as client
from acdh_cfts_pyutils import CFTS_COLLECTION


data = "./data/data.csv"
extra_ft_source = "./legacy_data/_Wien_X_.csv"
df = pd.read_csv(data)
ts_index_name = "gestrich_index"

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
    'name': ts_index_name,
    'fields': [
        {
            'name': 'id',
            'type': 'string'
        },
        {
            'name': 'rec_id',
            'type': 'string'
        },
        {
            'name': 'title',
            'type': 'string'
        },
        {
            'name': 'full_text',
            'type': 'string'
        },
        {
            'name': 'year',
            'type': 'int32',
            'optional': True,
            'facet': True,
        },
        {
            'name': 'places',
            'type': 'string[]',
            'facet': True,
            'optional': True
        },
        {
            'name': 'places_top',
            'type': 'string[]',
            'facet': True,
            'optional': True
        },
        {
            'name': 'keywords',
            'type': 'string[]',
            'facet': True,
            'optional': True
        },
        {
            'name': 'keywords_top',
            'type': 'string[]',
            'facet': True,
            'optional': True
        },
    ]
}

client.collections.create(current_schema)
cfts_records = []
cfts_records = []

print("building index")
records = []
for gr, ndf in tqdm(df.groupby('ID')):
    item = {}
    cfts_record = {}
    x = ndf.iloc[0]
    item["id"] = f'{x["ID"]}'
    item["rec_id"] = f'{x["ID"]}'
    item["title"] = x["full_title"]
    item["year"] = int(x['year'])
    cfts_record["id"] = f'{x["ID"]}'
    cfts_record["resolver"] = f'https://dummy-url/{x["ID"]}'
    cfts_record["rec_id"] = f'{x["ID"]}'
    cfts_record["title"] = x["full_title"]
    cfts_record["year"] = int(x['year'])
    cfts_record["project"] = ts_index_name
    full_text = []
    item["places"] = []
    cfts_record["places"] = []
    item["places_top"] = []
    item["keywords"] = []
    cfts_record["keywords"] = []
    item["keywords_top"] = []
    for i, row in ndf.iterrows():
        for term in ["Beschreibung", "top_concept", "skos_broader"]:
            if isinstance(row[term], str):
                full_text.append(row[term])
                if row["category"] == "G":
                    if term == "Beschreibung":
                        item["places"].append(row[term])
                        cfts_record["places"].append(row[term])
                    if term == "top_concept":
                        item["places_top"].append(row[term])
                else:
                    if term == "Beschreibung":
                        item["keywords"].append(row[term])
                        cfts_record["keywords"].append(row[term])
                    if term == "top_concept":
                        item["keywords_top"].append(row[term])
                

    item["full_text"] = " ".join(full_text)
    cfts_record["full_text"] = " ".join(full_text)
    try:
        extra_ft = extra_text[f'{x["ID"]}']
    except KeyError:
        extra_ft = ""
    item["full_text"] = item["full_text"] + extra_ft
    cfts_record["full_text"] = cfts_record["full_text"] + extra_ft
    records.append(item)
    cfts_records.append(cfts_record)

make_index = client.collections[ts_index_name].documents.import_(records)
print(make_index)
print(f"done with indexing {ts_index_name}")

make_index = CFTS_COLLECTION.documents.import_(cfts_records, {'action': 'upsert'})
print(make_index)
print('done with cfts-index emt')