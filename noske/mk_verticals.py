# creates verticals from xml to import data to NoSketch engine

import os
import glob
import shutil
from tqdm import tqdm
from acdh_tei_pyutils.tei import TeiReader
from acdh_tei_pyutils.utils import extract_fulltext


ns = {
    "tei": "http://www.tei-c.org/ns/1.0",
    "xml": "http://www.w3.org/XML/1998/namespace"
}


def process_xml_files(input_filepath, output_filepath):
    # create dir for output files
    output_dir = os.path.join(output_filepath, "verticals")
    shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    # Use glob to get all XML files
    xml_files = glob.glob(os.path.join(input_filepath, "*.xml"))

    # Iterate through each XML file
    for xml_file_path in tqdm(xml_files, total=len(xml_files)):
        doc = TeiReader(xml_file_path)
        doc_id = doc.any_xpath("@xml:id")[0].replace(".xml", "")
        # legacy and present corpus have different id structures
        # if else to determine the corpus and extract year, month, day
        if "edoc" in doc_id:
            year = doc_id.split("_")[2].split("-")[0]
            month = doc_id.split("_")[2].split("-")[1]
            day = doc_id.split("_")[2].split("-")[2]
            corp = "1"
        else:
            year = doc_id.split("_")[1].split("-")[0]
            month = doc_id.split("_")[1].split("-")[1]
            day = doc_id.split("_")[1].split("-")[2]
            corp = "2"
        # legacy data of corpus 2 has text@cert attribute
        # while corpus 1 does not -> defaults to 1.00
        try:
            cert = float(doc.any_xpath(".//tei:text/@cert")[0])
            cert = "{:.3f}".format(cert)
        except IndexError:
            cert = "1.00"
        # structures with text content
        wrapper = doc.any_xpath(""".//tei:body//tei:p
                                |.//tei:body//tei:list
                                |.//tei:body//tei:head
                                |.//tei:front//tei:docTitle
                                |.//tei:front//tei:imprimatur""")
        # Write the verticals list to a TSV file
        output_file = os.path.join(
            output_dir, os.path.splitext(
                os.path.basename(
                    xml_file_path
                )
            )[0] + ".tsv"
        )
        with open(output_file, "a", encoding="utf-8") as f:
            # doc structure opening tag with attributes
            f.write(f'<doc id="{doc_id}" attrs="word id hi label cert" year="{year}" month="{month}" day="{day}" corpus="{corp}" cert="{cert}">\n')
            for idx, x in enumerate(wrapper):
                # using tag name as structure opening tag
                name = x.tag
                try:
                    w_id = x.xpath("@xml:id", namespaces=ns)[0]
                except IndexError:
                    w_id = f'{name.replace("{http://www.tei-c.org/ns/1.0}", "")}_{idx + 1}'
                f.write(f'<{name.replace("{http://www.tei-c.org/ns/1.0}", "")} id="{w_id}">\n')
                # process "tei:w" and "tei:pc" tags
                teiw_tags = x.xpath(".//tei:w|.//tei:pc", namespaces=ns)
                for idx, teiw_tag in enumerate(teiw_tags):
                    vertical = []
                    # exctract text in any child except tei:orig of tei:choice
                    text = extract_fulltext(teiw_tag, ["{http://www.tei-c.org/ns/1.0}orig"])
                    if teiw_tag.tag == "{http://www.tei-c.org/ns/1.0}pc":
                        # pc tags require a glue tag 'g' to avoid empty whitespace
                        # pc content does not require any other attributes
                        f.write("<g/>\n")
                        vertical.append(text)
                        f.write("\t".join(vertical) + "\n")
                        continue
                    else:
                        vertical.append(text)
                    # extractable attributes id, highlighted, label:
                    try:
                        element_id = teiw_tag.xpath("@xml:id", namespaces=ns)[0]
                        vertical.append(element_id)
                    except IndexError:
                        pass
                    try:
                        teiw_tag.xpath("parent::tei:hi", namespaces=ns)[0]
                        vertical.append("YES")
                    except IndexError:
                        vertical.append("NO")
                    try:
                        teiw_tag.xpath("parent::tei:label", namespaces=ns)[0]
                        vertical.append("YES")
                    except IndexError:
                        vertical.append("NO")
                    try:
                        cert = teiw_tag.xpath("@cert", namespaces=ns)[0]
                        vertical.append(cert)
                    except IndexError:
                        vertical.append("1.00")
                    f.write("\t".join(vertical) + "\n")
                # using tag name as structure closing tag
                f.write(f'</{name.replace("{http://www.tei-c.org/ns/1.0}", "")}>\n')
            # doc structure closing tag
            f.write("</doc>\n")


# def punctuation_normalized(input_glob):
#     for file in glob.glob(input_glob):
#         if "wr_" not in file:
#             continue
#         with open(file, "r", encoding="utf-8") as f:
#             text = f.read()
#             text = text.replace("$.+\\.", "<g/>\n\\.")
#             text = text.replace(",", "<g/>\n,")
#             text = text.replace(";", "<g/>\n;")
#             text = text.replace(":", "<g/>\n:")
#             text = text.replace("!", "<g/>\n!")
#             text = text.replace("?", "<g/>\n?")
#             text = text.replace("(", "<g/>\n(")
#             text = text.replace(")", "<g/>\n)")
#             text = text.replace("[", "<g/>\n[")
#             text = text.replace("]", "<g/>\n]")
#             text = text.replace("=", "")
#         with open(file, "w", encoding="utf-8") as f:
#             f.write(text)


if __name__ == "__main__":
    input_filepath = "./data/editions/*/"
    output_filepath = "./data"
    process_xml_files(input_filepath, output_filepath)
