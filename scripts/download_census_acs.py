#!/usr/bin/env python3
"""
Download Census ACS 5-year data for multiple states via Census API.
No API key required (free tier: 500 req/day).

Saves output to:
  data/raw/census/census_acs_<state_lower>.json      (2023)
  data/raw/census/census_acs_<state_lower>_2018.json (2018)

Usage:
  python3 scripts/download_census_acs.py
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(SCRIPT_DIR, '..', 'data', 'raw', 'census')
os.makedirs(OUT_DIR, exist_ok=True)

# States to download: abbrev -> (fips, label)
STATES = {
    'ny': ('36', 'New York'),
    'tx': ('48', 'Texas'),
    'nc': ('37', 'North Carolina'),
    'ga': ('13', 'Georgia'),
    'pa': ('42', 'Pennsylvania'),
}

# ACS variables needed by the pipeline
VARS_2023 = ','.join([
    'NAME',
    'B01003_001E',   # Total population
    'B25001_001E',   # Total housing units
    'B25003_001E',   # Occupied housing units
    'B25003_002E',   # Owner-occupied housing units
    'B08006_001E',   # Total workers (commute universe)
    'B08006_017E',   # Worked from home
    'B19013_001E',   # Median household income
    'B25064_001E',   # Median gross rent
    'B25077_001E',   # Median home value
])

VARS_2018 = ','.join([
    'NAME',
    'B01003_001E',   # Total population (for 5yr growth)
    'B25001_001E',   # Total housing units (for 5yr growth)
])


def fetch(url, label):
    print(f"  Fetching {label} ... ", end='', flush=True)
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        print(f"OK ({len(data)-1} counties)")
        return data
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None


def download_state(abbrev, fips, label):
    print(f"\n{'='*50}")
    print(f"Downloading ACS data for {label} ({abbrev.upper()}, FIPS {fips})")
    print(f"{'='*50}")

    base = 'https://api.census.gov/data'

    url_2023 = (
        f'{base}/2023/acs/acs5'
        f'?get={VARS_2023}'
        f'&for=county:*'
        f'&in=state:{fips}'
    )
    url_2018 = (
        f'{base}/2018/acs/acs5'
        f'?get={VARS_2018}'
        f'&for=county:*'
        f'&in=state:{fips}'
    )

    data_2023 = fetch(url_2023, '2023 ACS 5-year')
    time.sleep(1)  # be polite to Census API
    data_2018 = fetch(url_2018, '2018 ACS 5-year')

    if data_2023:
        path = os.path.join(OUT_DIR, f'census_acs_{abbrev}.json')
        with open(path, 'w') as f:
            json.dump(data_2023, f)
        print(f"  Saved -> {path}")
    else:
        print(f"  WARNING: 2023 data not saved for {abbrev.upper()}")

    if data_2018:
        path = os.path.join(OUT_DIR, f'census_acs_{abbrev}_2018.json')
        with open(path, 'w') as f:
            json.dump(data_2018, f)
        print(f"  Saved -> {path}")
    else:
        print(f"  WARNING: 2018 data not saved for {abbrev.upper()}")

    time.sleep(2)  # pause between states
    return bool(data_2023 and data_2018)


def main():
    print("Census ACS Downloader")
    print(f"Output directory: {OUT_DIR}")

    results = {}
    for abbrev, (fips, label) in STATES.items():
        # Skip if both files already exist (re-run safe)
        path_2023 = os.path.join(OUT_DIR, f'census_acs_{abbrev}.json')
        path_2018 = os.path.join(OUT_DIR, f'census_acs_{abbrev}_2018.json')
        if os.path.exists(path_2023) and os.path.exists(path_2018):
            size = os.path.getsize(path_2023) // 1024
            print(f"\n{abbrev.upper()}: Already exists ({size}KB) — skipping. Delete files to re-download.")
            results[abbrev] = 'skipped'
            continue
        ok = download_state(abbrev, fips, label)
        results[abbrev] = 'ok' if ok else 'failed'

    print(f"\n{'='*50}")
    print("Summary:")
    for abbrev, status in results.items():
        icon = '✓' if status in ('ok', 'skipped') else '✗'
        print(f"  {icon} {abbrev.upper()}: {status}")
    print()
    print("Next steps:")
    print("  1. Run the pipeline for each state:")
    for abbrev, (fips, _) in STATES.items():
        if abbrev != 'ny':
            print(f"     python3 scripts/process_state_data.py --state {abbrev.upper()} --fips-prefix {fips}")
    print("  2. For NY, run: python3 scripts/build_ny_unified.py")
    print("  3. Download FCC FTTP + place summary CSVs for full fiber data:")
    print("     https://broadbandmap.fcc.gov/data-download/bulk-fixed-availability-data")


if __name__ == '__main__':
    main()
