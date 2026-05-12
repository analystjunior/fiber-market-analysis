"""
Fetch FCC provider_summary_by_geography.csv for all available historical periods.

These are small national summary files (~20MB each) — not the large location CSVs.
Downloads to data/raw/fcc/<period>/provider_summary_by_geography.csv

Usage:
    export FCC_USERNAME=your_email@example.com
    export FCC_API_TOKEN=your_44_char_token
    python3 scripts/fetch_provider_history.py

    # To also extract brand_name->provider_id mapping from a state location file:
    python3 scripts/fetch_provider_history.py --build-mapping --mapping-state MO
"""

import os
import sys
import csv
import json
import time
import zipfile
import argparse
import requests
from pathlib import Path

FCC_USERNAME  = os.getenv("FCC_USERNAME",  "")
FCC_API_TOKEN = os.getenv("FCC_API_TOKEN", "")
FCC_BASE_URL  = "https://broadbandmap.fcc.gov/api/public/map"

RAW_DIR     = Path(__file__).parent.parent / "data" / "raw" / "fcc"
MAPPING_OUT = Path(__file__).parent.parent / "data" / "provider_id_mapping.json"

ALL_STATES = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10",
    "FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20",
    "KY":"21","LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28",
    "MO":"29","MT":"30","NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36",
    "NC":"37","ND":"38","OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45",
    "SD":"46","TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56","DC":"11",
}


def headers():
    return {
        "username":   FCC_USERNAME,
        "hash_value": FCC_API_TOKEN,
        "Accept":     "application/json",
        "User-Agent": "FiberMapUSA/1.0 (fibermapusa.com)",
    }


def list_periods():
    """Return all available availability as-of dates, newest first."""
    resp = requests.get(f"{FCC_BASE_URL}/listAsOfDates", headers=headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status_code") not in (200, None) and data.get("status") != "successful":
        raise ValueError(data.get("message", "FCC API error"))
    dates = sorted(
        [d["as_of_date"] for d in data.get("data", []) if d.get("data_type") == "availability"],
        reverse=True,
    )
    return dates


def find_summary_file(period, category="Provider Summary by Geography Type"):
    """Find the file_id for the national provider summary for a given period."""
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{period}"
    resp = requests.get(url, headers=headers(), params={"category": "Summary"}, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    files = data.get("data", [])
    for f in files:
        if (f.get("subcategory", "") == category
                and f.get("state_fips") is None
                and f.get("technology_type") in ("Fixed Broadband", "")):
            return f
    return None


def find_location_file(period, state_abbr):
    """Find the FTTP (tech 50) location file for a state and period."""
    fips = ALL_STATES.get(state_abbr.upper())
    if not fips:
        return None
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{period}"
    resp = requests.get(url, headers=headers(), params={"category": "State"}, timeout=60)
    resp.raise_for_status()
    files = resp.json().get("data", [])
    for f in files:
        if (f.get("state_fips") == fips
                and str(f.get("technology_code")) == "50"
                and f.get("subcategory") == "Location Coverage"):
            return f
    return None


def download_and_extract(file_info, dest_dir, label=""):
    """Download a file from FCC API, extract CSV, return CSV path."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    file_id   = file_info["file_id"]
    file_name = file_info["file_name"]
    zip_path  = dest_dir / f"{file_name}.zip"

    if not zip_path.exists():
        url = f"{FCC_BASE_URL}/downloads/downloadFile/availability/{file_id}"
        print(f"  Downloading {label or file_name}...", end=" ", flush=True)
        resp = requests.get(url, headers=headers(), timeout=600, stream=True)
        resp.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        size_mb = zip_path.stat().st_size / 1024 / 1024
        print(f"done ({size_mb:.1f} MB)")
    else:
        print(f"  Already downloaded: {zip_path.name}")

    # Extract
    csv_paths = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.endswith(".csv"):
                out = dest_dir / name
                if not out.exists():
                    z.extract(name, dest_dir)
                csv_paths.append(dest_dir / name)

    return csv_paths


def build_mapping_from_location_file(csv_path):
    """Extract brand_name -> provider_id mapping from a location-level CSV."""
    mapping = {}
    print(f"  Extracting brand_name -> provider_id mapping from {Path(csv_path).name}...")
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            bn = row.get("brand_name", "").strip()
            pid = row.get("provider_id", "").strip()
            if bn and pid and bn not in mapping:
                mapping[bn] = pid
    print(f"  Found {len(mapping)} unique brand_name -> provider_id pairs")
    return mapping


def merge_and_save_mapping(new_mapping):
    """Merge new brand_name->provider_id pairs into the persistent mapping file."""
    existing = {}
    if MAPPING_OUT.exists():
        with open(MAPPING_OUT) as f:
            existing = json.load(f)
    merged = {**existing, **new_mapping}
    with open(MAPPING_OUT, "w") as f:
        json.dump(merged, f, indent=2, sort_keys=True)
    added = len(merged) - len(existing)
    print(f"  Mapping saved: {len(merged)} total entries ({added} new) -> {MAPPING_OUT}")
    return merged


def main():
    parser = argparse.ArgumentParser(description="Fetch FCC provider summary history")
    parser.add_argument("--periods", nargs="+", metavar="YYYY-MM-DD",
                        help="Specific periods to fetch. Default: all available.")
    parser.add_argument("--build-mapping", action="store_true",
                        help="Also download a state FTTP location file to build brand_name->provider_id mapping.")
    parser.add_argument("--mapping-state", default="MO", metavar="STATE",
                        help="State to use for mapping extraction (default: MO). "
                             "File is downloaded, mapping extracted, then you can delete the large CSV.")
    parser.add_argument("--mapping-period", metavar="YYYY-MM-DD",
                        help="Period to use for mapping file (default: most recent).")
    args = parser.parse_args()

    if not FCC_USERNAME or not FCC_API_TOKEN:
        print("ERROR: Set FCC_USERNAME and FCC_API_TOKEN environment variables.")
        print("  export FCC_USERNAME=your_email@example.com")
        print("  export FCC_API_TOKEN=your_token")
        sys.exit(1)

    print("=" * 60)
    print("  FiberMapUSA — FCC Provider History Fetcher")
    print("=" * 60)

    # Get available periods
    print("\nFetching available filing periods...")
    all_periods = list_periods()
    print(f"  Found {len(all_periods)} periods: {', '.join(all_periods)}")

    target_periods = args.periods if args.periods else all_periods

    # Download provider_summary_by_geography.csv for each period
    print(f"\nDownloading provider summary by geography for {len(target_periods)} period(s)...")
    downloaded = []
    for period in target_periods:
        period_dir = RAW_DIR / period.replace("-", "").replace("2022", "dec2022")
        # Use human-readable folder names matching existing convention
        label = period
        if period.endswith("-12-31"):
            label = "dec" + period[:4]
        elif period.endswith("-06-30"):
            label = "jun" + period[:4]
        period_dir = RAW_DIR / label

        csv_dest = period_dir / "provider_summary_by_geography.csv"
        if csv_dest.exists():
            print(f"  [{period}] Already exists — skipping")
            downloaded.append((period, csv_dest))
            continue

        print(f"\n[{period}]")
        file_info = find_summary_file(period, "Provider Summary by Geography Type")
        if not file_info:
            print(f"  WARNING: No Provider Summary by Geography file found for {period}")
            continue

        csv_paths = download_and_extract(file_info, period_dir, label=f"provider summary {period}")
        # Find the by-geography file (not national)
        for cp in csv_paths:
            if "geography" in cp.name.lower() and "national" not in cp.name.lower():
                # Rename to consistent name
                target = period_dir / "provider_summary_by_geography.csv"
                if cp != target:
                    cp.rename(target)
                downloaded.append((period, target))
                print(f"  Saved -> {target}")
                break
        time.sleep(0.5)

    print(f"\n  {len(downloaded)} period file(s) ready.")

    # Optionally download a state location file for brand_name mapping
    if args.build_mapping:
        state = args.mapping_state.upper()
        mapping_period = args.mapping_period or all_periods[0]
        label = mapping_period
        if mapping_period.endswith("-12-31"):
            label = "dec" + mapping_period[:4]
        elif mapping_period.endswith("-06-30"):
            label = "jun" + mapping_period[:4]
        period_dir = RAW_DIR / label

        print(f"\n[Mapping] Fetching FTTP location file for {state} ({mapping_period})...")
        file_info = find_location_file(mapping_period, state)
        if not file_info:
            print(f"  WARNING: No FTTP location file found for {state} {mapping_period}")
        else:
            csv_paths = download_and_extract(
                file_info, period_dir,
                label=f"FTTP locations {state} {mapping_period}"
            )
            fttp_csv = next((p for p in csv_paths if p.suffix == ".csv"), None)
            if fttp_csv:
                new_mapping = build_mapping_from_location_file(fttp_csv)
                merge_and_save_mapping(new_mapping)
                print(f"\n  You can now delete the large location file to save space:")
                print(f"    rm '{fttp_csv}'")

    print("\n" + "=" * 60)
    print("  Done. Run build_provider_history.py next to generate the JSON.")
    print("=" * 60)


if __name__ == "__main__":
    main()
