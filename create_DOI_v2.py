
"""
===============================================================================
Script: create_DOI_v2.py
Purpose:
    Automate DOI creation or update via the DataCite REST API from a spreadsheet.

Usage:
    python create_DOI_v2.py <input_file> --auth <repo_id:password> [options]

Required:
    file        Path to the input .xlsx or .csv file
    --auth      Repository credentials in format: REPO_ID:REPO_PASSWORD

Options:
    --api-url   DataCite API endpoint (default: https://api.test.datacite.org/dois)
                Use production: https://api.datacite.org/dois
    --dry-run   Simulate run (no API calls)
    --prefix    Repository prefix (required when DOI is blank to auto-generate suffix)
    --append-suffix-to-url
                Append "?wdt_column_filter[5]=" + DOI suffix to the landing page URL.
                If DOI is provided, append pre-publish; if minted, PATCH after creation.
    --event     DOI state: 'draft', 'publish', or 'register' (default: draft)
    --preflight Run a SAFE authentication check (GET /clients/<REPO_ID>) that
                DOES NOT create a DOI (disabled by default).

Spreadsheet Columns:
    title | Creator | Publisher | publication_year | url | doi (optional)
    - If DOI is blank, --prefix is required.
    - publication_year must be an integer.

Notes:
    - Test first in https://api.test.datacite.org/dois with TEST credentials/prefix.
    - Production DOIs are permanent (cannot be deleted, only updated).
    - Change append_suffix_to_url line 65 logic as needed for your use case.
    - Change User-Agent - line 71 - to your contact email.
    Limitations: 
        - Only allows for 1 contributor
        - Doesn't populate the publisher ROR
    To Do:
        - Add linked ROR for supporting organisation (Node)  
===============================================================================
"""

import pandas as pd
import requests
import json
import logging
import argparse
import os
from typing import Optional

# Logging
LOG_FILE = "doi_publish.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Spreadsheet column names expected
REQUIRED_FIELDS = ["title", "Creator", "Publisher", "publication_year", "url"]

# Fixed text to place before DOI suffix when appending to the URL
URL_SUFFIX_PREFIX = "?wdt_column_filter[5]="

# Allowed DOI state transitions
VALID_EVENTS = {"draft", "publish", "register"}

# Default User-Agent (can be overridden via env var DATACITE_USER_AGENT or --user-agent)
DEFAULT_USER_AGENT = os.environ.get("DATACITE_USER_AGENT", "create_DOI_v2/1.0 (mailto:nick.rossow@anu.edu.au)")


def validate_metadata(metadata):
    """Validate required fields and publication_year format."""
    missing_fields = [field for field in REQUIRED_FIELDS if not metadata.get(field)]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    try:
        int(metadata.get("publication_year"))
    except (ValueError, TypeError):
        return False, "Invalid publication_year (must be an integer)"
    return True, None


def extract_doi_suffix(doi: Optional[str]) -> Optional[str]:
    """Return DOI suffix (content after first '/')."""
    if not doi or "/" not in doi:
        return None
    return doi.split("/", 1)[1]


def build_full_suffix(suffix: str) -> str:
    """Always prepend the fixed text before the DOI suffix."""
    return f"{URL_SUFFIX_PREFIX}{suffix}"


def append_suffix_to_url(base_url: str, full_suffix: str) -> str:
    """
    Append full_suffix to base URL.
    If base_url already has a query string, convert leading '?' to '&'.
    """
    if not base_url or not full_suffix:
        return base_url
    if "?" in base_url:
        return base_url + full_suffix.replace("?", "&", 1)
    return base_url + full_suffix


def preflight_auth_check(api_url: str, username: str, password: str, user_agent: Optional[str] = None) -> bool:
    """
    SAFE preflight: authenticate without creating a DOI.
    Calls GET /clients/<REPO_ID> (Member API) with Basic Auth.
    """
    # Derive the API root (strip trailing '/dois' if present)
    base = api_url.rstrip("/")
    if base.endswith("/dois"):
        base = base[:-5]  # remove '/dois'
    # repo_id is the username part of --auth
    repo_id = username
    url = f"{base}/clients/{repo_id}"

    print(f"[Preflight] GET {url}")
    headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT, "Accept": "application/vnd.api+json"}
    resp = requests.get(url, auth=(username, password), headers=headers)
    print(f"[Preflight] Status: {resp.status_code}")
    print(f"[Preflight] Body (first 200 chars): {resp.text[:200]}")
    return resp.status_code == 200


def publish_doi(metadata,
                dry_run: bool,
                api_url: str,
                username: str,
                password: str,
                append_suffix_flag: bool,
                prefix: Optional[str],
                event: str,
                user_agent: Optional[str] = None):
    """Publish/transition DOI or simulate in dry-run, with optional URL suffix appending."""
    # Validate required metadata
    is_valid, error_msg = validate_metadata(metadata)
    if not is_valid:
        msg = f"Skipping row due to validation error: {error_msg}. Row data: {metadata}"
        print(msg)
        logging.error(msg)
        return None

    # DOI field handling (omit if blank/NaN)
    doi_value = metadata.get("doi")
    if pd.isna(doi_value) or str(doi_value).strip() == "":
        doi_value = None
    else:
        doi_value = str(doi_value).strip()

    base_url = str(metadata.get("url")).strip()
    url_for_payload = base_url

    # If DOI provided and we want to append suffix, do it pre-publish
    if append_suffix_flag and doi_value:
        raw_suffix = extract_doi_suffix(doi_value)
        if raw_suffix:
            full_suffix = build_full_suffix(raw_suffix)
            url_for_payload = append_suffix_to_url(base_url, full_suffix)
            print(f"Appended full suffix '{full_suffix}' to URL: '{base_url}' -> '{url_for_payload}'")

    # Build minimal valid DataCite attributes
    attributes = {
        "event": event,  # use CLI-provided event (default: draft)
        "titles": [{"title": metadata.get("title")}],
        "creators": [{
            "name": metadata.get("Creator"),
            "nameType": "Organizational",
            "affiliation": [{
                "affiliationIdentifier": metadata.get("Creator_ROR"),
                "affiliationIdentifierScheme": "ROR"
            }]
         }],
        "publisher": metadata.get("Publisher"),
        "publicationYear": int(metadata.get("publication_year")),
        "types": {"resourceTypeGeneral": "Award"},
        "url": url_for_payload,
        "contributors": [{
            "nameType": "Personal",
            "contributorType": "Researcher",
            "nameIdentifiers": [
                {
                "nameIdentifier": metadata.get("Contrib_ORCID"),
                }]}]
        }

    # If no DOI provided, include prefix so API can auto-generate suffix
    if doi_value:
        attributes["doi"] = doi_value
    else:
        if not prefix:
            msg = ("Skipping row: DOI is empty but no --prefix provided. "
                   "Add --prefix <your-prefix> (e.g., 10.5072) to mint DOIs without specifying a suffix.")
            print(msg)
            logging.error(msg)
            return None
        attributes["prefix"] = prefix

    payload = {
        "data": {
            "type": "dois",
            "attributes": attributes
        }
    }

    # Debug: Show payload
    print(f"Prepared payload for '{metadata.get('title')}':\n{json.dumps(payload, indent=2)}")

    if dry_run:
        msg = f"[DRY RUN] Would send DOI request for: {metadata.get('title')} (event='{event}')"
        print(msg)
        logging.info(msg)
        return True

    print(f"Sending request for: {metadata.get('title')}...")
    headers = {"Content-Type": "application/vnd.api+json", "User-Agent": user_agent or DEFAULT_USER_AGENT}
    response = requests.post(
        api_url,
        headers=headers,
        auth=(username, password),
        data=json.dumps(payload)
    )
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")

    if response.status_code == 201:
        msg = f"DOI request successful for: {metadata.get('title')} (event='{event}')"
        print(msg)
        logging.info(msg)

        # If suffix appending requested but DOI was minted now, PATCH the URL with full suffix
        if append_suffix_flag and not doi_value:
            try:
                resp_json = response.json()
                minted_doi = resp_json.get("data", {}).get("id") or resp_json.get("data", {}).get("attributes", {}).get("doi")
                if minted_doi:
                    raw_suffix = extract_doi_suffix(minted_doi)
                    if raw_suffix:
                        full_suffix = build_full_suffix(raw_suffix)
                        updated_url = append_suffix_to_url(base_url, full_suffix)
                        print(f"Minted DOI: {minted_doi}. Appending full suffix '{full_suffix}' to URL and PATCHing...")
                        patch_payload = {
                            "data": {
                                "id": minted_doi,
                                "type": "dois",
                                "attributes": {
                                    "url": updated_url
                                }
                            }
                        }
                        # PATCH to /dois/{DOI}
                        base = api_url.rstrip("/")
                        patch_url = f"{base}/{minted_doi}"
                        patch_headers = {"Content-Type": "application/vnd.api+json", "User-Agent": user_agent or DEFAULT_USER_AGENT}
                        patch_resp = requests.patch(
                            patch_url,
                            headers=patch_headers,
                            auth=(username, password),
                            data=json.dumps(patch_payload)
                        )
                        print(f"PATCH Status: {patch_resp.status_code}")
                        print(f"PATCH Body: {patch_resp.text}")
                        if patch_resp.status_code in (200, 201):
                            logging.info(f"Updated URL for DOI {minted_doi} to {updated_url}")
                        else:
                            logging.error(f"Failed to update URL for DOI {minted_doi}: {patch_resp.status_code} {patch_resp.text}")
                    else:
                        print("Could not extract suffix from minted DOI.")
                else:
                    print("Could not obtain minted DOI from response JSON.")
            except Exception as e:
                print(f"Error handling minted DOI URL update: {e}")
                logging.error(f"Error handling minted DOI URL update: {e}")
        return True

    else:
        msg = f"Failed DOI request for {metadata.get('title')} (event='{event}'). Status: {response.status_code}, Error: {response.text}"
        print(msg)
        logging.error(msg)
        return False


def main():
    parser = argparse.ArgumentParser(description="Publish DOIs from a spreadsheet using DataCite API.")
    parser.add_argument("file", help="Path to the input file (.xlsx or .csv)")
    parser.add_argument("--dry-run", action="store_true", help="Enable dry-run mode (no API calls)")
    parser.add_argument("--api-url", default="https://api.test.datacite.org/dois", help="DataCite API endpoint")
    parser.add_argument("--auth", required=True, help="Authentication in format repo_id:password")
    parser.add_argument("--prefix", help="Repository prefix (required when DOI is blank to auto-generate suffix)")
    parser.add_argument("--append-suffix-to-url", action="store_true",
                        help="Append '?wdt_column_filter[5]=' + DOI suffix to the landing page URL.")
    parser.add_argument("--event", default="draft",
                        help="DOI event/state to apply: 'draft', 'publish', or 'register' (default: draft)")
    parser.add_argument("--preflight", action="store_true",
                        help="Run a SAFE preflight (GET /clients/<REPO_ID>) that does not create a DOI.")
    parser.add_argument("--user-agent", help="User-Agent header value (overrides DATACITE_USER_AGENT env var)")

    args = parser.parse_args()

    # Validate event value
    event = args.event.strip().lower()
    if event not in VALID_EVENTS:
        print(f"Error: --event must be one of {sorted(VALID_EVENTS)} (got '{args.event}')")
        return

    # Parse auth
    if ":" not in args.auth:
        print("Error: --auth must be in format repo_id:password")
        return
    username, password = args.auth.split(":", 1)

    # File exists?
    if not os.path.exists(args.file):
        print(f"Error: File '{args.file}' not found.")
        return

    # SAFE preflight (only when explicitly requested; never creates a DOI)
    if args.preflight and not args.dry_run:
        ok = preflight_auth_check(args.api_url, username, password, args.user_agent)
        if not ok:
            print("Preflight failed: check repository credentials and endpoint (test vs production).")
            print("Test: https://api.test.datacite.org/dois   Production: https://api.datacite.org/dois")
            return

    # Read input
    if args.file.endswith(".xlsx"):
        df = pd.read_excel(args.file)
    elif args.file.endswith(".csv"):
        df = pd.read_csv(args.file)
    else:
        print("Error: Unsupported file type. Use .xlsx or .csv.")
        return

    success_count, fail_count, skip_count = 0, 0, 0
    for _, row in df.iterrows():
        result = publish_doi(
            row.to_dict(),
            args.dry_run,
            args.api_url,
            username,
            password,
            args.append_suffix_to_url,
            args.prefix,
            event,
            args.user_agent
        )
        if result is True:
            success_count += 1
        elif result is False:
            fail_count += 1
        else:
            skip_count += 1

    print("\n=== Summary ===")
    print(f"Total rows: {len(df)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Skipped: {skip_count}")


if __name__ == "__main__":
    main()
