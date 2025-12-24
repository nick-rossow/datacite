# create_DOI_v2.py

## Overview
`create_DOI_v2.py` automates **DOI creation or update** via the [DataCite REST API](https://support.datacite.org/docs/api) using data from a spreadsheet.

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
python create_DOI_v2.py <input_file> --auth <repo_id:password> [options]
```
## How I used it
``` bash
python3 create_DOI_v2.py metadata.xlsx --auth <repo_id:password> --api-url https://api.test.datacite.org/dois --prefix 10.83986 --event draft --append-suffix-to-url
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

---

### Example Command
```bash
python create_DOI_v2.py sample.xlsx --auth repo123:password --prefix 10.1234 --event publish
```
