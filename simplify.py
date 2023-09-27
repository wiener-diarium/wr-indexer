import json
import pandas as pd
from utils import make_title, find_broader

buchstaben = ["G", "A"]
codes = "./legacy_data/_tab_code_.csv"

lookup = {}
dfs = []
out = f"./data/data.csv"
for x in buchstaben:
    lookup[x] = {}
    g_file = f"./legacy_data/_Wien_{x}_.csv"
    
    journals = "./legacy_data/_Wien_Meldung_.csv"
    
    df = pd.read_csv(codes)

    print("make unique IDs")
    df["full_code"] = df.apply(
        lambda row: f"{row['code_X']}{row['code_XY']}{row['code_XYY']}{row['code_XYYY']}",
        axis=1
    )
    print("add Category")
    df["category"] = df.apply(
        lambda row: f"{x}",
        axis=1
    )
    print("populate lookup table")
    for i, row in df.iterrows():
        cur_code = row["full_code"]
        description = row["Beschreibung"]
        if cur_code.startswith(f"{x}") and len(cur_code) == 6 and "9999" in cur_code:
            lookup[cur_code[:2]] = description
        elif cur_code.startswith(f"{x}") and len(cur_code) == 5 and cur_code.endswith("99"):
            lookup[cur_code[:3]] = description

    g_df = pd.read_csv(g_file)
    g_df["full_code"] = g_df.apply(
        lambda row: f"{x}{row[f'{x}{x}{x}_kompakt']}",
        axis=1
    )

    print(f"merge {g_file} with {codes} and drop superfluous columns")
    simple = g_df.merge(df, how='left', on="full_code")[[f"ID_{x}", "full_code", "Beschreibung", "category"]]

    print(f"load {journals} and add better titles")
    df = pd.read_csv(journals)
    df["full_title"] = df.apply(
        lambda row: make_title(row),
        axis=1
    )
    df["year"] = df.apply(
        lambda row: f"{row['Ausgabe'][:4]}",
        axis=1
    )
    new_df = simple.merge(df, how="left", left_on=f"ID_{x}", right_on="ID")[["ID", "full_title", "year", "full_code", "Beschreibung", "category"]]
    print("add top concept")
    new_df["top_concept"] = new_df.apply(
        lambda row: find_broader(row, lookup, 2),
        axis=1
    )
    print("add top broader")
    new_df["skos_broader"] = new_df.apply(
        lambda row: find_broader(row, lookup, 3),
        axis=1
    )
    dfs.append(new_df)

print(f"write processed data to dataframe to {out}")
df = pd.concat(dfs)
df.to_csv('./data/data.csv', index=False)