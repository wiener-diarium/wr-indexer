# creates verticals from xml to import data to NoSketch engine

import os
import glob
import shutil
from tqdm import tqdm
from acdh_tei_pyutils.tei import TeiReader
from acdh_tei_pyutils.utils import extract_fulltext


ns = {
    'tei': 'http://www.tei-c.org/ns/1.0',
    'xml': 'http://www.w3.org/XML/1998/namespace'
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
        doc_id = doc.any_xpath('@xml:id')[0].replace(".xml", "")
        if "edoc" in doc_id:
            year = doc_id.split("_")[2].split("-")[0]
            month = doc_id.split("_")[2].split("-")[1]
            day = doc_id.split("_")[2].split("-")[2]
            col = "1"
        else:
            year = doc_id.split("_")[1].split("-")[0]
            month = doc_id.split("_")[1].split("-")[1]
            day = doc_id.split("_")[1].split("-")[2]
            col = "2"
        # find all "tei:w" tags
        wrapper = doc.any_xpath('.//tei:body//tei:p|.//tei:body//tei:list|.//tei:body//tei:head')
        # Write the verticals list to a TSV file
        output_file = os.path.join(
            output_dir, os.path.splitext(
                os.path.basename(
                    xml_file_path
                )
            )[0] + ".tsv"
        )
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(f'<doc id="{doc_id}" attrs="word" year="{year}" month="{month}" day="{day}" corpus="{col}">\n')
            for x in wrapper:
                name = x.tag
                try:
                    w_id = x.xpath("@xml:id", namespaces=ns)[0]
                except IndexError:
                    w_id = ""
                f.write(f'<{name.replace("{http://www.tei-c.org/ns/1.0}", "")} id="{w_id}">\n')
                teiw_tags = x.xpath('.//tei:w|.//tei:pc', namespaces=ns)
                # process "tei:w" and "tei:pc" tags
                for idx, teiw_tag in enumerate(teiw_tags):
                    verticals = []
                    # extractable attribute:
                    text = extract_fulltext(teiw_tag)
                    try:
                        element_id = teiw_tag.xpath("@xml:id", namespaces=ns)[0]
                    except IndexError:
                        element_id = ""
                    if teiw_tag.tag == '{http://www.tei-c.org/ns/1.0}pc':
                        f.write("<g/>\n")
                        verticals.append(
                            '\t'.join(
                                [
                                    text
                                ]
                            )
                        )
                    else:
                        verticals.append(
                            '\t'.join(
                                [
                                    text,
                                    element_id,
                                ]
                            )
                        )
                    f.write('\t'.join(verticals) + '\n')
                f.write(f"</{name.replace('{http://www.tei-c.org/ns/1.0}', '')}>\n")
            f.write("</doc>\n")


if __name__ == "__main__":
    input_filepath = "./data/editions/*/"
    output_filepath = "./data"
    process_xml_files(input_filepath, output_filepath)
