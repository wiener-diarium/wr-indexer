import os
import glob
import json
from acdh_tei_pyutils.tei import TeiReader
from tqdm import tqdm


input_dir = os.path.join("data", "editions", "present")
out_dir = "data"
data_save_path = os.path.join(out_dir, "data.jsonl")
print(f"creating JSONL for each page from upconverted teis and saving it as {data_save_path}")
counter = 0

with open(data_save_path, "w", encoding="utf-8") as f:
    for x in tqdm(glob.glob(f"{input_dir}/*.xml")):
        _, tail = os.path.split(x)
        item = {}
        doc = TeiReader(x)
        try:
            confidence = doc.any_xpath(".//tei:text/@cert")[0]
            item["confidence"] = confidence
        except KeyError:
            print(f"no confidence for {tail}")
        date = doc.any_xpath("@xml:id")[0].split("_")[1].replace(".xml", "")
        pb = doc.any_xpath(".//tei:pb")
        # for page in pb:
        #     facs = page.attrib["facs"]
        #     nr = page.attrib["n"]
        #     dr_id = f"wr_{date}__{nr:0>2}"
        #     full_text = doc.any_xpath(".//tei:body/tei:div[tei:*[contains(@facs, '{}')]]".format(facs))
        #     item["id"] = dr_id
        #     item["full_text"] = (
        #         " ".join(" ".join("".join(p.itertext()).split()) for p in full_text)
        #         .replace("\n", " ")
        #         .replace("¬ ", "")
        #         .replace("= ", "")
        #         .replace("=", "")
        #     )
        #     counter += 1
        #     f.write(json.dumps(item, ensure_ascii=False) + "\n")
        full_text = doc.any_xpath(".//tei:body/tei:div")
        item["full_text"] = (
            " ".join(" ".join("".join(p.itertext()).split()) for p in full_text)
            .replace("\n", " ")
            .replace("¬ ", "")
            .replace("= ", "")
            .replace("=", "")
        )
        item["id"] = f"wr_{date}__01"
        counter += 1
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

print(f"done, created {data_save_path} for {counter} pages. Creating zip archive...")
