"""
FiberMapUSA — BDC Provider Passings History Pipeline
=====================================================
For each available FCC BDC filing period:
  1. Downloads the location coverage CSV for a technology + state
  2. Counts distinct residential location_ids per county per brand_name
  3. Upserts results into Supabase table: provider_passings_history

Supported technologies (--tech):
    fiber   tech-50  FTTP (default)
    cable   tech-40  Coaxial / HFC
    dsl     tech-10  Copper / DSL

Prerequisites:
    pip install requests supabase python-dotenv

Environment variables (.env or shell):
    FCC_USERNAME        your_fcc_email@example.com
    FCC_API_TOKEN       your_44_char_token
    SUPABASE_URL        https://xxxx.supabase.co
    SUPABASE_SERVICE_KEY  sb_secret_...

Usage:
    # Fiber (default) — all periods, all target states
    python3 scripts/build_provider_history.py

    # Cable passings
    python3 scripts/build_provider_history.py --tech cable

    # DSL passings
    python3 scripts/build_provider_history.py --tech dsl

    # Specific states / periods
    python3 scripts/build_provider_history.py --tech cable --states MO NY
    python3 scripts/build_provider_history.py --periods 2024-12-31 2025-06-30

    # Dry run — process but don't upsert to Supabase
    python3 scripts/build_provider_history.py --tech cable --dry-run

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

ALL_STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10",
    "FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20",
    "KY":"21","LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28",
    "MO":"29","MT":"30","NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36",
    "NC":"37","ND":"38","OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45",
    "SD":"46","TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56","DC":"11",
}

# All US states + DC
DEFAULT_STATES = list(ALL_STATE_FIPS.keys())

TEMP_DIR = Path(tempfile.gettempdir()) / "fibermapusa_bdc"

# Technology configs: name → (FCC tech code, display label)
TECH_CONFIGS = {
    "fiber": ("50", "FTTP"),
    "cable": ("40", "Cable / HFC"),
    "dsl":   ("10", "Copper / DSL"),
}


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


def find_location_file(period, state_fips, tech_code):
    """Return file metadata for a given tech-code location coverage for a state+period."""
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{period}"
    resp = requests.get(url, headers=fcc_headers(), params={"category": "State"}, timeout=60)
    resp.raise_for_status()
    for f in resp.json().get("data", []):
        if (f.get("state_fips") == state_fips
                and str(f.get("technology_code")) == str(tech_code)
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
    per (county_fips, provider_id), using the brand name with the most passings.

    Returns dict keyed by (county_fips, provider_id) -> (brand_name, count).
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

            pid      = row.get("provider_id", "").strip()
            brand    = row.get("brand_name",  "").strip().strip('"')
            location = row.get("location_id", "").strip()

            if pid and brand and location:
                counts[county_fips][pid][brand].add(location)

    # Collapse to one row per (county, provider): sum all brands, keep dominant brand name
    result = {}
    for county, providers in counts.items():
        for pid, brands in providers.items():
            brand_counts = {b: len(locs) for b, locs in brands.items()}
            total = sum(brand_counts.values())
            dominant_brand = max(brand_counts, key=brand_counts.__getitem__)
            result[(county, pid)] = (dominant_brand, total)

    total_passings = sum(v[1] for v in result.values())
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
            on_conflict="geoid,provider_id,filing_date,technology"
        ).execute()
        if hasattr(result, "error") and result.error:
            print(f"    ERROR upserting batch: {result.error}")
            return
        upserted += len(batch)

    print(f"    Upserted {upserted:,} records to Supabase")


# ── Main ───────────────────────────────────────────────────────────────────────

def process_state_period(state_abbr, period, technology="fiber", keep_local=False, dry_run=False):
    """Download, aggregate, and upsert data for one state + one filing period + one technology."""
    fips = ALL_STATE_FIPS.get(state_abbr.upper())
    if not fips:
        print(f"  Unknown state: {state_abbr}")
        return

    tech_code, tech_label = TECH_CONFIGS[technology]
    fips_prefix = fips

    period_dir = TEMP_DIR / period / state_abbr.upper() / technology

    marker = period_dir / ".done"
    if marker.exists():
        print(f"  [{state_abbr} {period} {technology}] Already processed — skipping (delete .done to reprocess)")
        return

    print(f"\n  [{state_abbr} {period} {technology}] Finding {tech_label} location file...")
    file_info = find_location_file(period, fips, tech_code)
    if not file_info:
        print(f"  [{state_abbr} {period} {technology}] No {tech_label} file found — skipping")
        return

    print(f"  [{state_abbr} {period} {technology}] {int(file_info['record_count']):,} records reported by FCC")

    try:
        csv_path = download_csv(file_info, period_dir)
        aggregated = aggregate_passings(csv_path, fips_prefix)

        if not aggregated:
            print(f"  [{state_abbr} {period} {technology}] No residential {tech_label} data found")
            return

        records = [
            {
                "geoid":       county_fips,
                "provider_id": pid,
                "brand_name":  brand,
                "filing_date": period,
                "passings":    count,
                "technology":  technology,
            }
            for (county_fips, pid), (brand, count) in aggregated.items()
        ]

        upsert_to_supabase(records, dry_run=dry_run)

        if not dry_run:
            marker.write_text("done")

        if not keep_local:
            csv_path.unlink(missing_ok=True)
            zip_files = list(period_dir.glob("*.zip"))
            for zf in zip_files:
                zf.unlink(missing_ok=True)
            print(f"    Local files removed (use --keep-local to retain)")

    except Exception as e:
        print(f"  [{state_abbr} {period} {technology}] ERROR: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Build FCC BDC provider passings history in Supabase"
    )
    parser.add_argument("--tech", choices=list(TECH_CONFIGS.keys()), default="fiber",
                        help="Technology to process: fiber (tech-50), cable (tech-40), dsl (tech-10). Default: fiber")
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

    tech_code, tech_label = TECH_CONFIGS[args.tech]

    print("=" * 60)
    print(f"  FiberMapUSA — BDC Provider Passings History Pipeline")
    print(f"  Technology: {args.tech.upper()} ({tech_label}, tech-{tech_code})")
    print("=" * 60)

    missing = []
    if not FCC_USERNAME or not FCC_API_TOKEN:
        missing.append("FCC_USERNAME / FCC_API_TOKEN")
    if not args.dry_run and (not SUPABASE_URL or not SUPABASE_KEY):
        missing.append("SUPABASE_URL / SUPABASE_SERVICE_KEY")
    if missing:
        print(f"\nERROR: Missing credentials: {', '.join(missing)}")
        print("Set them in .env or as environment variables.")
        sys.exit(1)

    print("\nFetching available filing periods from FCC API...")
    all_periods = list_filing_periods()
    print(f"  Available: {', '.join(all_periods)}")

    target_periods = args.periods or all_periods
    target_states  = [s.upper() for s in args.states]

    print(f"\nTechnology: {args.tech}")
    print(f"States:     {', '.join(target_states)}")
    print(f"Periods:    {', '.join(target_periods)}")
    if args.dry_run:
        print("Mode:       DRY RUN (no Supabase writes)")

    total = len(target_states) * len(target_periods)
    done  = 0

    for period in target_periods:
        for state in target_states:
            done += 1
            print(f"\n[{done}/{total}]", end="")
            try:
                process_state_period(
                    state, period,
                    technology=args.tech,
                    keep_local=args.keep_local,
                    dry_run=args.dry_run,
                )
            except Exception as e:
                print(f"  FAILED ({state} {period}): {e} — continuing")
            time.sleep(0.3)

    print("\n" + "=" * 60)
    print("  Pipeline complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
