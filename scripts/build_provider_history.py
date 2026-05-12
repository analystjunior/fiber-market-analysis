"""
FiberMapUSA — BDC Provider Passings History Pipeline
=====================================================
For each available FCC BDC filing period:
  1. Downloads the FTTP (tech-50) location coverage CSV for each target state
  2. Counts distinct residential location_ids per county per brand_name
  3. Upserts results into Supabase table: provider_passings_history

This gives exact passings counts (not estimates) from the real BDC location data.

Prerequisites:
    pip install requests supabase python-dotenv

Environment variables (.env or shell):
    FCC_USERNAME        your_fcc_email@example.com
    FCC_API_TOKEN       your_44_char_token
    SUPABASE_URL        https://xxxx.supabase.co
    SUPABASE_SERVICE_KEY  sb_secret_...

Usage:
    # All available periods, all target states
    python3 scripts/build_provider_history.py

    # Specific states only
    python3 scripts/build_provider_history.py --states MO NY

    # Specific periods only (YYYY-MM-DD as returned by FCC API)
    python3 scripts/build_provider_history.py --periods 2024-12-31 2025-06-30

    # Dry run — process but don't upsert to Supabase
    python3 scripts/build_provider_history.py --dry-run

    # Skip download if CSV already exists locally (resume after interruption)
    python3 scripts/build_provider_history.py --keep-local
"""

import os
import sys
import csv
import time
import zipfile
import argparse
import tempfile
import requests
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

FCC_USERNAME  = os.getenv("FCC_USERNAME",  "")
FCC_API_TOKEN = os.getenv("FCC_API_TOKEN", "")
SUPABASE_URL  = os.getenv("SUPABASE_URL",  "")
SUPABASE_KEY  = os.getenv("SUPABASE_SERVICE_KEY", "")

FCC_BASE_URL = "https://broadbandmap.fcc.gov/api/public/map"

# Target states to process. Expand as you add more states to the app.
DEFAULT_STATES = ["MO", "NY", "NC", "GA", "TX", "FL", "SC"]

ALL_STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10",
    "FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20",
    "KY":"21","LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28",
    "MO":"29","MT":"30","NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36",
    "NC":"37","ND":"38","OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45",
    "SD":"46","TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56","DC":"11",
}

TEMP_DIR = Path(tempfile.gettempdir()) / "fibermapusa_bdc"


# ── FCC API helpers ────────────────────────────────────────────────────────────

def fcc_headers():
    return {
        "username":   FCC_USERNAME,
        "hash_value": FCC_API_TOKEN,
        "Accept":     "application/json",
        "User-Agent": "FiberMapUSA/1.0 (fibermapusa.com)",
    }


def list_filing_periods():
    """Return all available BDC availability as-of dates, oldest first."""
    resp = requests.get(f"{FCC_BASE_URL}/listAsOfDates", headers=fcc_headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    dates = sorted(
        d["as_of_date"]
        for d in data.get("data", [])
        if d.get("data_type") == "availability"
    )
    return dates


def find_fttp_location_file(period, state_fips):
    """Return file metadata for tech-50 (FTTP) location coverage for a state+period."""
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{period}"
    resp = requests.get(url, headers=fcc_headers(), params={"category": "State"}, timeout=60)
    resp.raise_for_status()
    for f in resp.json().get("data", []):
        if (f.get("state_fips") == state_fips
                and str(f.get("technology_code")) == "50"
                and f.get("subcategory") == "Location Coverage"):
            return f
    return None


def download_csv(file_info, dest_dir):
    """Download a zipped BDC file, extract CSV, return the CSV path."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    file_id   = file_info["file_id"]
    file_name = file_info["file_name"]
    zip_path  = dest_dir / f"{file_name}.zip"

    if not zip_path.exists():
        url = f"{FCC_BASE_URL}/downloads/downloadFile/availability/{file_id}"
        print(f"    Downloading {file_name}.zip ...", end=" ", flush=True)
        resp = requests.get(url, headers=fcc_headers(), timeout=1800, stream=True)
        resp.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=131072):
                f.write(chunk)
        print(f"done ({zip_path.stat().st_size / 1024 / 1024:.0f} MB)")
    else:
        print(f"    Already downloaded: {zip_path.name}")

    with zipfile.ZipFile(zip_path, "r") as z:
        csv_names = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV found in {zip_path.name}")
        z.extract(csv_names[0], dest_dir)
        return dest_dir / csv_names[0]


# ── Aggregation ────────────────────────────────────────────────────────────────

def aggregate_passings(csv_path, state_fips_prefix):
    """
    Stream through a location CSV and count distinct residential location_ids
    per (county_fips, provider_id, brand_name).

    Returns dict keyed by (county_fips, provider_id, brand_name) -> set of location_ids
    then converted to counts.
    """
    print(f"    Aggregating {Path(csv_path).name} ...", end=" ", flush=True)

    # county_fips -> provider_id -> brand_name -> set(location_id)
    counts = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    row_count = 0

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1

            # Residential and mixed (R or X)
            brc = row.get("business_residential_code", "")
            if brc not in ("R", "X"):
                continue

            block_geoid = row.get("block_geoid", "")
            county_fips = block_geoid[:5]
            if not county_fips.startswith(state_fips_prefix):
                continue

            pid        = row.get("provider_id", "").strip()
            brand      = row.get("brand_name",  "").strip().strip('"')
            location   = row.get("location_id", "").strip()

            if pid and brand and location:
                counts[county_fips][pid][brand].add(location)

    # Flatten to (county_fips, provider_id, brand_name) -> int
    result = {}
    for county, providers in counts.items():
        for pid, brands in providers.items():
            for brand, locs in brands.items():
                result[(county, pid, brand)] = len(locs)

    total_passings = sum(result.values())
    print(f"{row_count:,} rows → {len(result):,} county/provider combos "
          f"({total_passings:,} total passings)")
    return result


# ── Supabase upsert ────────────────────────────────────────────────────────────

def upsert_to_supabase(records, dry_run=False):
    """Upsert a list of dicts into provider_passings_history."""
    if dry_run:
        print(f"    [DRY RUN] Would upsert {len(records):,} records")
        return

    from supabase import create_client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    batch_size = 500
    upserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        result = sb.table("provider_passings_history").upsert(
            batch,
            on_conflict="geoid,provider_id,filing_date"
        ).execute()
        if hasattr(result, "error") and result.error:
            print(f"    ERROR upserting batch: {result.error}")
            return
        upserted += len(batch)

    print(f"    Upserted {upserted:,} records to Supabase")


# ── Main ───────────────────────────────────────────────────────────────────────

def process_state_period(state_abbr, period, keep_local=False, dry_run=False):
    """Download, aggregate, and upsert data for one state + one filing period."""
    fips = ALL_STATE_FIPS.get(state_abbr.upper())
    if not fips:
        print(f"  Unknown state: {state_abbr}")
        return

    fips_prefix = fips  # county FIPS starts with state FIPS (2 digits)

    period_dir = TEMP_DIR / period / state_abbr.upper()

    # Check if already processed by looking for a marker file
    marker = period_dir / ".done"
    if marker.exists():
        print(f"  [{state_abbr} {period}] Already processed — skipping (delete .done to reprocess)")
        return

    print(f"\n  [{state_abbr} {period}] Finding FTTP location file...")
    file_info = find_fttp_location_file(period, fips)
    if not file_info:
        print(f"  [{state_abbr} {period}] No FTTP file found — skipping")
        return

    print(f"  [{state_abbr} {period}] {file_info['record_count']:,} records reported by FCC")

    try:
        csv_path = download_csv(file_info, period_dir)
        aggregated = aggregate_passings(csv_path, fips_prefix)

        if not aggregated:
            print(f"  [{state_abbr} {period}] No residential FTTP data found")
            return

        # Build upsert records
        records = [
            {
                "geoid":       county_fips,
                "provider_id": pid,
                "brand_name":  brand,
                "filing_date": period,
                "passings":    count,
            }
            for (county_fips, pid, brand), count in aggregated.items()
        ]

        upsert_to_supabase(records, dry_run=dry_run)

        # Write marker so we skip on re-runs
        if not dry_run:
            marker.write_text("done")

        # Clean up large CSV unless asked to keep
        if not keep_local:
            csv_path.unlink(missing_ok=True)
            zip_files = list(period_dir.glob("*.zip"))
            for zf in zip_files:
                zf.unlink(missing_ok=True)
            print(f"    Local files removed (use --keep-local to retain)")

    except Exception as e:
        print(f"  [{state_abbr} {period}] ERROR: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Build FCC BDC provider passings history in Supabase"
    )
    parser.add_argument("--states", nargs="+", metavar="STATE",
                        default=DEFAULT_STATES,
                        help=f"States to process (default: {' '.join(DEFAULT_STATES)})")
    parser.add_argument("--periods", nargs="+", metavar="YYYY-MM-DD",
                        help="Filing periods to process (default: all available)")
    parser.add_argument("--keep-local", action="store_true",
                        help="Keep downloaded CSVs after processing")
    parser.add_argument("--dry-run", action="store_true",
                        help="Aggregate but don't write to Supabase")
    args = parser.parse_args()

    print("=" * 60)
    print("  FiberMapUSA — BDC Provider Passings History Pipeline")
    print("=" * 60)

    # Validate credentials
    missing = []
    if not FCC_USERNAME or not FCC_API_TOKEN:
        missing.append("FCC_USERNAME / FCC_API_TOKEN")
    if not args.dry_run and (not SUPABASE_URL or not SUPABASE_KEY):
        missing.append("SUPABASE_URL / SUPABASE_SERVICE_KEY")
    if missing:
        print(f"\nERROR: Missing credentials: {', '.join(missing)}")
        print("Set them in .env or as environment variables.")
        sys.exit(1)

    # Get periods
    print("\nFetching available filing periods from FCC API...")
    all_periods = list_filing_periods()
    print(f"  Available: {', '.join(all_periods)}")

    target_periods = args.periods or all_periods
    target_states  = [s.upper() for s in args.states]

    print(f"\nStates:  {', '.join(target_states)}")
    print(f"Periods: {', '.join(target_periods)}")
    if args.dry_run:
        print("Mode:    DRY RUN (no Supabase writes)")

    total = len(target_states) * len(target_periods)
    done  = 0

    for period in target_periods:
        for state in target_states:
            done += 1
            print(f"\n[{done}/{total}]", end="")
            try:
                process_state_period(state, period, keep_local=args.keep_local, dry_run=args.dry_run)
            except Exception as e:
                print(f"  FAILED ({state} {period}): {e} — continuing")
            time.sleep(0.3)

    print("\n" + "=" * 60)
    print("  Pipeline complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
