import os
import glob
import json
import datetime
from acdh_tei_pyutils.tei import TeiReader
from tqdm import tqdm


def dateToWeekday(year, month, day):
    return datetime.date(int(year), int(month), int(day)).weekday()


def yearToDecade(year):
    year = int(year)
    return year - year % 10


input_dir = os.path.join("data", "editions", "present")
out_dir = "data"
data_save_path = os.path.join(out_dir, "data.jsonl")
print(f"creating JSONL for each page from upconverted teis and saving it as {data_save_path}")
counter = 0
try:
    os.remove("issue_nr_errors.txt")
except FileNotFoundError:
    pass

with open(data_save_path, "w", encoding="utf-8") as f:
    for x in tqdm(glob.glob(f"{input_dir}/*.xml")):
        _, tail = os.path.split(x)
        item = {}
        doc = TeiReader(x)
        try:
            confidence = doc.any_xpath(".//tei:text/@cert")[0]
            item["confidence"] = confidence
        except IndexError:
            print(f"no confidence for {tail}")
        title = doc.any_xpath(".//tei:titleStmt/tei:title[@level='a'][@type='main']/text()")[0]
        edition_title = doc.any_xpath(".//tei:titleStmt/tei:title[@level='s'][@type='main']/text()")[0]
        item["title"] = f"{edition_title}, {title}"
        try:
            issue_nr = int(title.split(" ")[1].replace(".", ""))
        except ValueError:
            issue_nr = 0
            with open("issue_nr_errors.txt", "a") as err:
                err.write(f"error with {tail}\n")
        item["issue_number"] = issue_nr
        date = doc.any_xpath("@xml:id")[0].split("_")[1].replace(".xml", "")
        item["day"] = int(date.replace("-", ""))
        day = int(date.split("-")[2])
        month = int(date.split("-")[1])
        year = int(date.split("-")[0])
        item["year"] = year
        item["decade"] = yearToDecade(year)
        item["weekday"] = dateToWeekday(year, month, day)
        pb = doc.any_xpath(".//tei:pb")
        item["page_count"] = len(pb)
        # for page in pb:
        #     facs = page.attrib["facs"]
        #     nr = page.attrib["n"]
        #     dr_id = f"wr_{date}__{nr:0>2}"
        #     full_text = doc.any_xpath(".//tei:body/tei:div[tei:*[contains(@facs, '{}')]]".format(facs))
        #     item["id"] = dr_id
        #     item["text"] = (
        #         " ".join(" ".join("".join(p.itertext()).split()) for p in full_text)
        #         .replace("\n", " ")
        #         .replace("¬ ", "")
        #         .replace("= ", "")
        #         .replace("=", "")
        #     )
        #     counter += 1
        #     f.write(json.dumps(item, ensure_ascii=False) + "\n")
        full_text = doc.any_xpath(".//tei:body/tei:div")
        item["text"] = (
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
