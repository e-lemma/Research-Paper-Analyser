import xml.etree.ElementTree as ET
import re
import os
import datetime
from dotenv import load_dotenv
import pandas as pd
import botocore.exceptions
from boto3 import client
import spacy
from spacy.language import Doc
import pycountry
from rapidfuzz import process, fuzz
from utils import (
    create_s3_client,
    create_ses_client,
    download_xml,
    upload_csv_to_bucket,
    send_html_email,
)

EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
ZIPCODE_PATTERN = re.compile(
    r"(?i)([A-Z][A-HJ-Y]?\d[A-Z\d]? ?\d[A-Z]{2}|GIR\s*0AA)|\b\d{5}(-\d{4})?\b|([ABCEGHJKLMNPRSTVXY]\d[ABCEGHJKLMNPRSTVWXYZ])\ ?(\d[ABCEGHJKLMNPRSTVWXYZ]\d)|\d{6}"
)

# Loading spacy model
nlp = spacy.load(
    "en_core_web_sm", disable=["parser", "tagger", "attribute_ruler", "lemmatizer"]
)

# Set containing all ISO 3166-1 country names
COUNTRY_NAMES = {country.name for country in pycountry.countries}

COLUMNS = [
    "Article PMID",
    "Article title",
    "Article keywords",
    "Article MESH Identifiers",
    "Article year",
    "Author first name",
    "Author last name",
    "Author initials",
    "Author full name",
    "Author email",
    "Affiliation name (from PubMed dataset)",
    "Affiliation name (from GRID dataset)",
    "Affiliation zipcode",
    "Affiliation country",
    "Affiliation GRID identifier",
]


def insert_article_info(article: ET.Element, target: dict) -> dict:
    """Retrieves information from the article element and inserts it
    into a dictionary, returning it."""

    target["Article PMID"] = article.findtext("./MedlineCitation/PMID")

    target["Article title"] = article.findtext(
        "./MedlineCitation/Article/ArticleTitle")

    keywords = article.findall("./MedlineCitation/KeywordList/Keyword")

    if keywords:
        target["Article keywords"] = ", ".join(
            [keyword.text for keyword in keywords if keyword.text is not None]
        )
    else:
        target["Article keywords"] = None

    mesh_descriptor_names = article.findall(
        "./MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName"
    )
    if mesh_descriptor_names:
        target["Article MESH Identifiers"] = ", ".join(
            descriptor_name.attrib["UI"] for descriptor_name in mesh_descriptor_names
        )
    else:
        target["Article MESH Identifiers"] = None

    year = article.findtext("./MedlineCitation/Article/ArticleDate/Year")
    target["Article year"] = year

    return target


def insert_author_info(author: ET.Element, target: dict) -> dict:
    """Retrieves information from the author element and inserts it
    into a dictionary, returning it."""

    first_name = author.findtext("ForeName")
    last_name = author.findtext("LastName")

    target["Author first name"] = first_name
    target["Author last name"] = last_name
    target["Author initials"] = author.findtext("./Initials")
    target["Author full name"] = f"{first_name} {last_name}"

    return target


def extract_and_insert_affiliation_info(
    affiliation: ET.Element,
    target: dict,
    institution_data: pd.DataFrame,
    affiliation_cache: dict,
) -> dict:
    """Retrieves information from the affiliation element and inserts it
    into a dictionary, returning it."""

    affiliation_text = affiliation.text

    email, affiliation_text = extract_and_remove_email(
        affiliation_text, EMAIL_PATTERN)
    target["Author email"] = email

    zipcode, affiliation_text = extract_and_remove_zipcode(
        affiliation_text, ZIPCODE_PATTERN
    )
    target["Affiliation zipcode"] = zipcode

    doc = nlp(affiliation_text)
    institution_names = institution_data["name"].tolist()

    grid_name, pubmed_name = extract_and_match_affiliation_name(
        doc, institution_names, affiliation_cache
    )
    target["Affiliation name (from PubMed dataset)"] = pubmed_name
    target["Affiliation name (from GRID dataset)"] = grid_name

    target["Affiliation GRID identifier"] = get_grid_identifier(
        grid_name, institution_data
    )

    target["Affiliation country"] = extract_affiliation_country(
        doc, COUNTRY_NAMES)

    return target


def extract_and_remove_zipcode(
    affiliation_text: str, zipcode_regex: re.Pattern
) -> tuple[str, str]:
    """Extracts and removes the zip code / postal code from the affiliation text, returning the
    code and the remaining text."""

    found_zipcode = re.search(zipcode_regex, affiliation_text)

    if not found_zipcode:
        return None, affiliation_text

    zipcode = found_zipcode.group(0)
    affiliation_text = affiliation_text.replace(zipcode, "", 1)

    return zipcode, affiliation_text.strip()


def extract_and_remove_email(
    affiliation_text: str, email_regex: re.Pattern
) -> tuple[str, str]:
    """Extracts and removes the email address from the affiliation text, returning the email
    and the remaining text."""

    found_emails = re.findall(email_regex, affiliation_text)

    if not found_emails:
        return None, affiliation_text

    for email in found_emails:

        # Removes the email and the common format for presenting it
        affiliation_text = affiliation_text.replace(
            f"Electronic address: {email}.", "."
        )
        # Removes email if present outside the common format
        affiliation_text = affiliation_text.replace(email, "")

    return found_emails[0], affiliation_text.strip()


def extract_affiliation_country(affiliation_items: Doc, countries: set[str]) -> str:
    """Returns the country of the affiliation from the spaCy-tokenised affiliation text."""

    for ent in reversed(affiliation_items.ents):

        if ent.label_ == "GPE":

            if ent.text in {"UK", "U.K"}:
                return "United Kingdom"

            if ent.text in {"USA", "U.S.A", "United States of America"}:
                return "United States"

            if ent.text in countries:
                return ent.text

    return None


def extract_and_match_affiliation_name(
    affiliation_items: Doc, affiliation_names: list[str], affiliation_cache: dict
) -> tuple[str, str]:
    """Extracts institute names from the spaCy-tokenised affiliation text, uses rapidfuzz to determine and
    retrieve official name matches from GRID, returning either, both, or none."""

    found_orgs = [
        ent.text for ent in reversed(affiliation_items.ents) if ent.label_ == "ORG"
    ]

    if len(found_orgs) == 0:
        return None, None

    for org in found_orgs:
        if org in affiliation_cache:
            return affiliation_cache.get(org), org

    best_match = None
    best_org = None
    best_score = 0

    for org in found_orgs:

        match = affiliation_cache.get(org)

        if match is None:

            match = process.extractOne(
                org, affiliation_names, scorer=fuzz.ratio, score_cutoff=90
            )

        if match and match[1] > best_score:

            best_match = match
            best_score = match[1]
            best_org = org

    if best_match:
        affiliation_cache[best_org] = best_match[0]
        return best_match[0], best_org

    return None, found_orgs[0]


def get_grid_identifier(
    grid_affiliation_name: str, institute_data: pd.DataFrame
) -> str:
    """Returns the GRID identifier associated with the affiliation name."""

    if not grid_affiliation_name:
        return None

    matched_row = institute_data.loc[institute_data["name"]
                                     == grid_affiliation_name]

    return matched_row["grid_id"].iloc[0]


def process_xml_and_generate_csv(
    xml_path: str, institute_data_path: str, output_csv_filename: str
) -> None:
    """Flattens, enriches, and cleans the XML data, exporting it as a CSV."""

    tree = ET.parse(xml_path)
    root = tree.getroot()
    institute_data = pd.read_csv(institute_data_path)

    affiliation_name_cache = {}

    data = []
    for article in root.iter("PubmedArticle"):
        article_data = insert_article_info(article, {})

        for author in article.findall("./MedlineCitation/Article/AuthorList/Author"):
            # Skips 'collectives'
            if author.findtext("CollectiveName") is not None:
                continue

            article_and_author_data = insert_author_info(
                author, article_data.copy())

            for affiliation in author.findall("./AffiliationInfo/Affiliation"):
                complete_data = extract_and_insert_affiliation_info(
                    affiliation,
                    article_and_author_data.copy(),
                    institute_data,
                    affiliation_name_cache,
                )

                data.append(complete_data)

    research_papers = pd.DataFrame(data, columns=COLUMNS)

    research_papers.to_csv(output_csv_filename, index=False)


def get_output_csv_filename(source_filename: str) -> str:
    """Returns a filename for the generated CSV file, using the source filename
    and the current date."""

    date_str = datetime.date.today().strftime("%Y-%m-%d")
    base_filename = source_filename.removesuffix(".xml")
    return f"{base_filename}-({date_str}).csv"


if __name__ == "__main__":

    load_dotenv()

    aws_access_key_id = os.getenv("ACCESS_KEY")
    aws_secret_access_key = os.getenv("SECRET_ACCESS_KEY")

    s3 = create_s3_client(aws_access_key_id, aws_secret_access_key)
    ses_client = create_ses_client(aws_access_key_id, aws_secret_access_key)

    source_bucket = os.getenv("SOURCE_BUCKET")
    source_key = os.getenv("SOURCE_KEY")
    source_filename = os.path.basename(source_key)

    if not source_bucket or not source_key:
        raise ValueError("Source information is missing!")

    target_bucket = os.getenv("TARGET_BUCKET")
    target_folder = os.getenv("TARGET_FOLDER")

    xml_data_path = f"./{source_filename}"
    institute_data_path = "./institutes.csv"
    output_csv_filename = get_output_csv_filename(source_filename)
    output_csv_path = f"./{output_csv_filename}"
    target_key = f"{target_folder}{
        output_csv_filename}"

    send_html_email(ses_client, source_filename, "start")

    download_xml(s3, source_bucket, source_key, source_filename)
    process_xml_and_generate_csv(
        xml_data_path, institute_data_path, output_csv_path)
    upload_csv_to_bucket(s3, target_bucket, target_key, output_csv_path)

    send_html_email(ses_client, output_csv_filename, "end")
