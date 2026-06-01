# create_DOI_v4.py

## Overview
`create_DOI_v4.py` automates **DOI creation or update** via the [DataCite REST API](https://support.datacite.org/docs/api) using data from a spreadsheet.

---

## Purpose
- Create or update DOIs from `.xlsx` or `.csv` files.
- Integrate with DataCite API for DOI management.

---
## Changes required
  - Change append_suffix_to_url line 65 logic as needed for your use case. This is what sets the URL of the DOI 
  - Change User-Agent - line 71 - to your contact email.
  - Change ROR for publisher on line 179 - 185

---
## Usage
```bash
python create_DOI_v4.py <input_file> --auth <repo_id:password> [options]
```
## How I used it
``` bash
python3 create_DOI_v4.py metadata.xlsx --auth APN.REPO:<redacted> --api-url https://api.test.datacite.org/dois --prefix 10.83986 --event draft --append-suffix-to-url --publication-year 2026 --related-item-title "Pipeline Accelerator - Voucher Scheme - 25-26 Round 1" --related-item-pub-year "2026" --related-item-identifier "https://raid.org/10.82287/f7b08ebc" 
```
---

## Required Arguments
- **`file`**  
  Path to the input `.xlsx` or `.csv` file.

- **`--auth`**  
  Repository credentials in format:  
  ```
  REPO_ID:REPO_PASSWORD
  ```

---

## Options
| Option | Description |
|--------|-------------|
| `--api-url` | DataCite API endpoint (default: `https://api.test.datacite.org/dois`). Use production: `https://api.datacite.org/dois`. |
| `--dry-run` | Simulate run (no API calls). |
| `--prefix` | Repository prefix (required when DOI is blank to auto-generate suffix). |
| `--append-suffix-to-url` | Append `"?wdt_column_filter[5]=" + DOI suffix` to the landing page URL. If DOI is provided, append pre-publish; if minted, PATCH after creation. |
| `--event` | DOI state: `draft`, `publish`, or `register` (default: `draft`). |
| `--preflight` | Run a safe authentication check (`GET /clients/<REPO_ID>`) that does **not** create a DOI (disabled by default). |

---

## Spreadsheet Columns
```
title | Creator | Publisher | publication_year | url | doi (optional)
```
- If DOI is blank, `--prefix` is required.
- `publication_year` must be an integer.

---

## Notes
- Test in `https://api.test.datacite.org/dois` with **TEST credentials/prefix** first.
- **Production DOIs are permanent.**


