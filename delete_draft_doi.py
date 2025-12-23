"""delete_draft_doi.py

Safely delete DRAFT DOIs from a DataCite repository.

Usage:
  python delete_draft_doi.py --auth REPO_ID:PASSWORD --dois-file dois.txt [--dry-run] [--api-url URL] [--user-agent UA]

Options:
  --dois-file   Plain text file with one DOI per line (required unless --fetch is used)
  --fetch       Attempt to fetch DOIs from the API (experimental)
  --dry-run     Print actions but do not call DELETE
  --api-url     DataCite API URL (default: https://api.test.datacite.org/dois)
  --user-agent  User-Agent header to send (or set DATACITE_USER_AGENT env var)

Notes:
  - This script will GET each DOI first and only DELETE if the DOI's `attributes.event` == 'draft'.
  - Be careful when running against production API: use dry-run first.
"""

import argparse
import os
import requests
import json
import time
from typing import List, Optional

DEFAULT_API_URL = "https://api.test.datacite.org/dois"
DEFAULT_USER_AGENT = os.environ.get("DATACITE_USER_AGENT", "delete_draft_doi/1.0 (mailto:nick.rossow@anu.edu.au)")


def read_dois_from_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]
    return lines


def fetch_drafts(api_url: str, auth: tuple, user_agent: str, timeout: int = 15, page_size: int = 100) -> List[str]:
    """Attempt to fetch DOIs with event=='draft'. This is experimental and may need adjusting.
    It will walk paginated results if links are provided.
    """
    drafts = []
    params = {"page[size]": page_size}
    headers = {"Accept": "application/vnd.api+json", "User-Agent": user_agent}
    url = api_url
    seen_urls = set()
    max_pages = 50
    page_count = 0

    while url and page_count < max_pages:
        if url in seen_urls:
            print(f"Stopping pagination: already visited {url}")
            break
        seen_urls.add(url)
        page_count += 1

        # retry loop with backoff
        attempts = 0
        resp = None
        while attempts < 3:
            try:
                resp = requests.get(url, auth=auth, headers=headers, params=params, timeout=timeout)
                break
            except requests.RequestException as e:
                attempts += 1
                wait = 2 ** attempts
                print(f"Attempt {attempts} failed fetching {url}: {e}. Retrying in {wait}s...")
                time.sleep(wait)

        if resp is None:
            print(f"Failed to fetch DOIs from {url} after retries")
            break

        if resp.status_code != 200:
            print(f"Failed to fetch DOIs: {resp.status_code} {resp.text[:200]}")
            break

        try:
            data = resp.json()
        except ValueError:
            print(f"Failed to decode JSON from {url}")
            break

        for item in data.get("data", []):
            attrs = item.get("attributes", {})
            if attrs.get("event") == "draft":
                doi_id = item.get("id") or attrs.get("doi")
                if doi_id:
                    drafts.append(doi_id)

        # follow pagination link if present
        links = data.get("links", {})
        next_link = links.get("next")
        if next_link and next_link != url:
            url = next_link
            params = None
            print(f"Following next page: {url}")
            continue
        else:
            break

    if page_count >= max_pages:
        print(f"Stopped after {max_pages} pages to avoid infinite loop")

    return drafts


def get_doi_metadata(api_base: str, doi: str, auth: tuple, user_agent: str) -> Optional[dict]:
    url = f"{api_base.rstrip('/')}/{doi}"
    headers = {"Accept": "application/vnd.api+json", "User-Agent": user_agent}
    resp = requests.get(url, auth=auth, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    print(f"Warning: GET {doi} returned {resp.status_code}: {resp.text[:200]}")
    return None


def delete_doi(api_base: str, doi: str, auth: tuple, user_agent: str) -> requests.Response:
    url = f"{api_base.rstrip('/')}/{doi}"
    headers = {"User-Agent": user_agent}
    return requests.delete(url, auth=auth, headers=headers)


def main():
    parser = argparse.ArgumentParser(description="Delete DRAFT DOIs from DataCite repository.")
    parser.add_argument("--auth", required=True, help="REPO_ID:REPO_PASSWORD")
    parser.add_argument("--dois-file", help="File with one DOI per line")
    parser.add_argument("--fetch", action="store_true", help="Fetch DOIs from API (experimental)")
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="DataCite API URL")
    parser.add_argument("--timeout", type=int, default=15, help="Request timeout in seconds for API calls")
    parser.add_argument("--page-size", type=int, default=100, help="Page size for listing DOIs when fetching")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted")
    parser.add_argument("--user-agent", help="User-Agent header override")

    args = parser.parse_args()

    if ":" not in args.auth:
        print("Error: --auth must be in format REPO_ID:REPO_PASSWORD")
        return
    username, password = args.auth.split(":", 1)
    auth = (username, password)
    user_agent = args.user_agent or DEFAULT_USER_AGENT
    api_url = args.api_url

    dois: List[str] = []
    if args.dois_file:
        if not os.path.exists(args.dois_file):
            print(f"DOI file not found: {args.dois_file}")
            return
        dois = read_dois_from_file(args.dois_file)

    if args.fetch:
        print("Fetching draft DOIs from API (experimental)...")
        fetched = fetch_drafts(api_url, auth, user_agent, timeout=args.timeout, page_size=args.page_size)
        print(f"Fetched {len(fetched)} draft DOIs")
        # merge unique
        for d in fetched:
            if d not in dois:
                dois.append(d)

    if not dois:
        print("No DOIs to delete (provide --dois-file or use --fetch).")
        return

    print(f"Preparing to process {len(dois)} DOIs (dry-run={args.dry_run})")

    for doi in dois:
        print(f"\nChecking DOI: {doi}")
        meta = get_doi_metadata(api_url, doi, auth, user_agent)
        if not meta:
            print(f"Skipping {doi}: cannot retrieve metadata")
            continue
        attrs = meta.get("data", {}).get("attributes", {})
        # DataCite may expose draft state in different fields depending on endpoint/schema/version.
        event = attrs.get("event")
        state = attrs.get("state")
        doi_status = attrs.get("doiStatus") or attrs.get("status")
        published = attrs.get("published")
        registered = attrs.get("registered")

        # Diagnostic print to help debug cases where fields are missing
        print(f"Current event: {event}  state: {state}  doiStatus: {doi_status}  published: {published}  registered: {registered}")

        # Delete only when 'state' equals 'draft'
        is_draft = (state == "draft")

        if not is_draft:
            print(f"Skipping {doi}: state != 'draft' (state={state})")
            continue

        if args.dry_run:
            print(f"[DRY RUN] Would DELETE {doi}")
            continue

        resp = delete_doi(api_url, doi, auth, user_agent)
        if resp.status_code in (200, 204):
            print(f"Deleted {doi}: {resp.status_code}")
        else:
            print(f"Failed to delete {doi}: {resp.status_code} {resp.text[:200]}")


if __name__ == "__main__":
    main()
