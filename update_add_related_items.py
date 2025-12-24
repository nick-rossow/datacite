#!/usr/bin/env python3
"""
update_add_related_items.py

Patch existing DOIs in DataCite to add/update a relatedItems block.

Usage:
    python update_add_related_items.py <input_file> --auth repo_id:password [options]

Input file (.csv or .xlsx) should contain at minimum a `doi` column. Optional columns
to customise the related item (case-insensitive):
    related_title, related_relationType, related_publication_year,
    related_item_type, related_url, related_identifier_type

If none of those columns are provided the script will apply a sensible default
related item (the RAID URL example provided).

Options:
    --api-url        DataCite API endpoint (default: https://api.test.datacite.org/dois)
    --dry-run        Print payloads but do not make PATCH requests
    --user-agent     Override User-Agent header
    --fetch-existing Fetch existing DOIs from the DataCite API (paginated) and
                     include them for updating; when used without an input file
                     the script will update all DOIs returned by the API.

"""

import argparse
import json
import os
import logging
import pandas as pd
import requests
from typing import Optional
from urllib.parse import quote

# Logging
LOG_FILE = "update_related_items.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DEFAULT_USER_AGENT = os.environ.get("DATACITE_USER_AGENT", "update_add_related_items/1.0 (mailto:nick.rossow@anu.edu.au)")
DEFAULT_REQUEST_TIMEOUT = 10  # seconds for HTTP requests


# Default related item (same as example provided)
RELATED_ITEM_DEFAULT = {
    "titles": [{"title": "Pipeline Accelerator - Voucher Scheme - 25-26 Round 1"}],
    "relationType": "IsPartOf",
    "publicationYear": "2025",
    "relatedItemType": "Award",
    "relatedItemIdentifier": {
        "relatedItemIdentifier": "https://raid.org/10.82287/f7b08ebc",
        "relatedItemIdentifierType": "URL"
    }
}


def pick_field(metadata: dict, *keys, default=None) -> Optional[str]:
    for k in keys:
        if k in metadata and metadata.get(k) not in (None, "", float('nan')):
            return str(metadata.get(k))
    return default


def build_related_item(metadata: dict) -> dict:
    """Construct relatedItem from row metadata or use defaults."""
    title = pick_field(metadata, "related_title", "Related_Title", default=RELATED_ITEM_DEFAULT["titles"][0]["title"]) or RELATED_ITEM_DEFAULT["titles"][0]["title"]
    relation_type = pick_field(metadata, "related_relationType", "related_relationtype", default=RELATED_ITEM_DEFAULT["relationType"]) or RELATED_ITEM_DEFAULT["relationType"]
    pub_year = pick_field(metadata, "related_publication_year", "Related_publication_year", "related_publicationYear", default=RELATED_ITEM_DEFAULT["publicationYear"]) or RELATED_ITEM_DEFAULT["publicationYear"]
    item_type = pick_field(metadata, "related_item_type", "Related_item_type", "relatedItemType", default=RELATED_ITEM_DEFAULT["relatedItemType"]) or RELATED_ITEM_DEFAULT["relatedItemType"]
    url = pick_field(metadata, "related_url", "Related_URL", "relatedItemIdentifier", default=RELATED_ITEM_DEFAULT["relatedItemIdentifier"]["relatedItemIdentifier"]) or RELATED_ITEM_DEFAULT["relatedItemIdentifier"]["relatedItemIdentifier"]
    id_type = pick_field(metadata, "related_identifier_type", "Related_identifier_type", "relatedItemIdentifierType", default=RELATED_ITEM_DEFAULT["relatedItemIdentifier"]["relatedItemIdentifierType"]) or RELATED_ITEM_DEFAULT["relatedItemIdentifier"]["relatedItemIdentifierType"]

    return {
        "titles": [{"title": title}],
        "relationType": relation_type,
        "publicationYear": str(pub_year),
        "relatedItemType": item_type,
        "relatedItemIdentifier": {
            "relatedItemIdentifier": url,
            "relatedItemIdentifierType": id_type
        }
    }


def normalise_doi(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    # strip doi.org URL if present
    if s.lower().startswith("http://") or s.lower().startswith("https://"):
        # remove common DOI resolver prefixes
        for p in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "http://dx.doi.org/"):
            if s.lower().startswith(p):
                return s[len(p):]
        return s
    return s


def patch_doi(api_url: str, doi: str, related_item: dict, auth: tuple, user_agent: str, dry_run: bool = False) -> bool:
    base = api_url.rstrip("/")
    # ensure DOI is url-quoted but keep slashes
    doi_path = quote(doi, safe="/")
    patch_url = f"{base}/{doi_path}"

    payload = {
        "data": {
            "id": doi,
            "type": "dois",
            "attributes": {
                "relatedItems": [related_item]
            }
        }
    }

    print(f"Prepared PATCH for DOI {doi} -> {patch_url} with payload:\n{json.dumps(payload, indent=2)}")
    logging.info(f"Prepared PATCH for {doi}")

    if dry_run:
        print("--dry-run: not sending request")
        return True

    headers = {"Content-Type": "application/vnd.api+json", "User-Agent": user_agent}
    try:
        resp = requests.patch(patch_url, headers=headers, auth=auth, data=json.dumps(payload), timeout=DEFAULT_REQUEST_TIMEOUT)
    except Exception as e:
        print(f"Error sending PATCH for {doi}: {e}")
        logging.error(f"Error sending PATCH for {doi}: {e}")
        return False
    print(f"PATCH status: {resp.status_code}")
    try:
        print(resp.text)
    except Exception:
        pass
    if resp.status_code in (200, 201):
        logging.info(f"Successfully patched relatedItems for {doi}")
        return True
    else:
        logging.error(f"Failed to patch {doi}: {resp.status_code} {resp.text}")
        return False


def fetch_existing_dois(api_url: str, auth: tuple, user_agent: str) -> list:
    """Fetch existing DOIs from the DataCite API, following pagination."""
    headers = {"Accept": "application/vnd.api+json", "User-Agent": user_agent}
    url = api_url.rstrip("/")
    dois = []
    page = 0
    while url:
        page += 1
        print(f"Fetching DOI list page {page} -> {url}")
        try:
            resp = requests.get(url, headers=headers, auth=auth, timeout=DEFAULT_REQUEST_TIMEOUT)
        except Exception as e:
            print(f"Error fetching DOI list page {page}: {e}")
            logging.error(f"Error fetching DOI list page {page}: {e}")
            break
        if resp.status_code != 200:
            print(f"Failed to fetch DOIs: {resp.status_code} {resp.text}")
            logging.error(f"Failed to fetch DOIs: {resp.status_code} {resp.text}")
            break
        try:
            body = resp.json()
        except Exception as e:
            print(f"Error parsing JSON from DOI list response: {e}")
            logging.error(f"Error parsing JSON from DOI list response: {e}")
            break

        for item in body.get("data", []):
            doi_id = item.get("id")
            if doi_id:
                dois.append(doi_id)

        print(f"Collected {len(dois)} DOIs so far")
        # follow 'next' link if present
        links = body.get("links", {}) or {}
        next_url = links.get("next")
        if next_url:
            url = next_url
        else:
            break

    return dois


def main():
    parser = argparse.ArgumentParser(description="Patch relatedItems into existing DataCite DOIs from a spreadsheet or API")
    parser.add_argument("file", nargs='?', help="Path to input file (.xlsx or .csv) containing at least a 'doi' column (optional when --fetch-existing is used)")
    parser.add_argument("--api-url", default="https://api.test.datacite.org/dois", help="DataCite API endpoint")
    parser.add_argument("--auth", required=True, help="Authentication in format repo_id:password")
    parser.add_argument("--dry-run", action="store_true", help="Show payloads but do not send PATCH requests")
    parser.add_argument("--fetch-existing", action="store_true", help="Fetch existing DOIs from the DataCite API and update them")
    parser.add_argument("--user-agent", help="User-Agent header value (overrides DATACITE_USER_AGENT env var)")

    args = parser.parse_args()

    if ":" not in args.auth:
        print("Error: --auth must be in format repo_id:password")
        return
    username, password = args.auth.split(":", 1)
    auth = (username, password)

    user_agent = args.user_agent or DEFAULT_USER_AGENT

    # Build DOI list from file (if provided)
    doi_map = {}  # doi -> row dict (for custom related item)
    if args.file:
        if not os.path.exists(args.file):
            print(f"Error: File '{args.file}' not found.")
            return
        if args.file.endswith('.xlsx'):
            df = pd.read_excel(args.file)
        elif args.file.endswith('.csv'):
            df = pd.read_csv(args.file)
        else:
            print('Unsupported file type. Use .xlsx or .csv')
            return

        for _, row in df.iterrows():
            rowd = {k: v for k, v in row.to_dict().items()}
            raw_doi = rowd.get('doi') or rowd.get('DOI')
            doi = normalise_doi(raw_doi)
            if doi:
                doi_map[doi] = rowd

    # Optionally fetch existing DOIs from the API
    doi_set = set(doi_map.keys())
    if args.fetch_existing:
        fetched = fetch_existing_dois(args.api_url, auth, user_agent)
        for d in fetched:
            if d not in doi_set:
                doi_set.add(d)

    if not doi_set:
        print('No DOIs to update (provide a file with DOIs or use --fetch-existing).')
        return

    total = len(doi_set)
    success = 0
    fail = 0

    try:
        for doi in sorted(doi_set):
            rowd = doi_map.get(doi, {})
            # If we have per-row data use it to build related item, otherwise use defaults
            related_item = build_related_item(rowd)
            ok = patch_doi(args.api_url, doi, related_item, auth, user_agent, args.dry_run)
            if ok:
                success += 1
            else:
                fail += 1
    except KeyboardInterrupt:
        print('\nOperation cancelled by user (KeyboardInterrupt).')
        logging.info('Operation cancelled by user')

    print('\n=== Summary ===')
    print(f'Total DOIs: {total}')
    print(f'Successful: {success}')
    print(f'Failed: {fail}')


if __name__ == '__main__':
    main()
