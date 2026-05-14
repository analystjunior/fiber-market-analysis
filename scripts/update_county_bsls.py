"""
FiberMapUSA — Update county total_bsls from FCC BDC Jun 2025
=============================================================
Uses the NGSO Satellite (tech-61, Starlink) location coverage file per state.
Starlink files coverage for essentially every fabric location, so counting
distinct residential location_ids per county gives the full FCC fabric BSL count —
the same denominator the FCC broadband map (broadbandmap.fcc.gov) shows.

Also recomputes fiber_unserved and fiber_penetration to stay consistent.

Usage:
    python3 scripts/update_county_bsls.py
    python3 scripts/update_county_bsls.py --dry-run
    python3 scripts/update_county_bsls.py --states NJ FL CA
"""

import os, sys, csv, time, zipfile, argparse, tempfile, requests
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

FCC_USERNAME  = os.getenv("FCC_USERNAME", "")
FCC_API_TOKEN = os.getenv("FCC_API_TOKEN", "")
SUPABASE_URL  = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY  = os.getenv("SUPABASE_SERVICE_KEY", "")

FCC_BASE_URL = "https://broadbandmap.fcc.gov/api/public/map"
PERIOD       = "2025-06-30"
TECH_CODE    = "61"   # NGSO Satellite (Starlink) — near-universal fabric coverage

ALL_STATE_FIPS = {
    "AL":"01","AK":"02","AZ":"04","AR":"05","CA":"06","CO":"08","CT":"09","DE":"10",
    "FL":"12","GA":"13","HI":"15","ID":"16","IL":"17","IN":"18","IA":"19","KS":"20",
    "KY":"21","LA":"22","ME":"23","MD":"24","MA":"25","MI":"26","MN":"27","MS":"28",
    "MO":"29","MT":"30","NE":"31","NV":"32","NH":"33","NJ":"34","NM":"35","NY":"36",
    "NC":"37","ND":"38","OH":"39","OK":"40","OR":"41","PA":"42","RI":"44","SC":"45",
    "SD":"46","TN":"47","TX":"48","UT":"49","VT":"50","VA":"51","WA":"53","WV":"54",
    "WI":"55","WY":"56","DC":"11",
}

TEMP_DIR = Path(tempfile.gettempdir()) / "fibermapusa_bsls_ngso"


def fcc_headers():
    return {
        "username":   FCC_USERNAME,
        "hash_value": FCC_API_TOKEN,
        "Accept":     "application/json",
        "User-Agent": "FiberMapUSA/1.0",
    }


# ── FCC file discovery + download ──────────────────────────────────────────────

def find_ngso_file(state_fips):
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{PERIOD}"
    resp = requests.get(url, headers=fcc_headers(),
                        params={"category": "State"}, timeout=60)
    resp.raise_for_status()
    for f in resp.json().get("data", []):
        if (f.get("state_fips") == state_fips
                and str(f.get("technology_code")) == TECH_CODE
                and f.get("subcategory") == "Location Coverage"):
            return f
    return None


def download_csv(file_info, state_abbr):
    dest = TEMP_DIR / state_abbr
    dest.mkdir(parents=True, exist_ok=True)

    file_id   = file_info["file_id"]
    file_name = file_info["file_name"]
    zip_path  = dest / f"{file_name}.zip"

    if not zip_path.exists():
        url = f"{FCC_BASE_URL}/downloads/downloadFile/availability/{file_id}"
        print(f"    Downloading {file_name}.zip ...", end=" ", flush=True)
        r = requests.get(url, headers=fcc_headers(), timeout=1800, stream=True)
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=131072):
                f.write(chunk)
        print(f"done ({zip_path.stat().st_size / 1024 / 1024:.0f} MB)")
    else:
        print(f"    Already downloaded: {zip_path.name}")

    with zipfile.ZipFile(zip_path) as z:
        csv_names = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV in {zip_path.name}")
        z.extract(csv_names[0], dest)
        return dest / csv_names[0]


# ── Count distinct BSLs per county ────────────────────────────────────────────

def count_bsls(csv_path, state_fips):
    """
    Count distinct residential location_ids per county from NGSO coverage file.
    Returns dict: county_fips (5-char) → int.
    """
    county_locations = defaultdict(set)
    rows = 0

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows += 1
            brc = row.get("business_residential_code", "")
            if brc not in ("R", "X"):
                continue
            block_geoid = row.get("block_geoid", "")
            county_fips = block_geoid[:5]
            if not county_fips.startswith(state_fips):
                continue
            location_id = row.get("location_id", "").strip()
            if location_id:
                county_locations[county_fips].add(location_id)

    result = {fips: len(locs) for fips, locs in county_locations.items()}
    total  = sum(result.values())
    print(f"    {rows:,} rows → {len(result):,} counties · {total:,} total BSLs")
    return result


# ── Supabase update ────────────────────────────────────────────────────────────

def update_supabase(county_bsls, dry_run=False):
    if dry_run:
        print(f"  [DRY RUN] Would update {len(county_bsls):,} counties")
        for fips, bsls in list(county_bsls.items())[:5]:
            print(f"    {fips} → {bsls:,}")
        return

    from supabase import create_client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Fetch existing geoids and fiber_served in one pass
    print("  Fetching existing counties from Supabase...")
    existing = {}
    page_size, offset = 1000, 0
    while True:
        rows = sb.table("counties").select("geoid,fiber_served").range(offset, offset + page_size - 1).execute().data or []
        for r in rows:
            existing[r["geoid"]] = r.get("fiber_served") or 0
        if len(rows) < page_size:
            break
        offset += page_size
    print(f"  Found {len(existing):,} existing counties")

    updated = errors = skipped = 0
    total = len(county_bsls)

    for i, (fips, total_bsls) in enumerate(county_bsls.items()):
        if fips not in existing:
            skipped += 1
            continue

        fs          = existing[fips]
        unserved    = max(0, total_bsls - fs)
        penetration = round(min(1.0, fs / total_bsls), 4) if total_bsls > 0 else 0.0

        result = (sb.table("counties")
                    .update({"total_bsls": total_bsls,
                             "fiber_unserved": unserved,
                             "fiber_penetration": penetration})
                    .eq("geoid", fips)
                    .execute())
        if hasattr(result, "error") and result.error:
            errors += 1
        else:
            updated += 1

        if (i + 1) % 200 == 0:
            print(f"  ... {i+1}/{total}")

    print(f"  Updated {updated:,} counties ({skipped} skipped, {errors} errors)")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Update county total_bsls from FCC BDC NGSO satellite coverage (Jun 2025)"
    )
    parser.add_argument("--states", nargs="+", metavar="STATE",
                        default=list(ALL_STATE_FIPS.keys()))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-local", action="store_true",
                        help="Keep downloaded CSVs after processing")
    args = parser.parse_args()

    missing = []
    if not FCC_USERNAME or not FCC_API_TOKEN:
        missing.append("FCC_USERNAME / FCC_API_TOKEN")
    if not args.dry_run and (not SUPABASE_URL or not SUPABASE_KEY):
        missing.append("SUPABASE_URL / SUPABASE_SERVICE_KEY")
    if missing:
        print(f"ERROR: Missing credentials: {', '.join(missing)}")
        sys.exit(1)

    print("=" * 60)
    print("  FiberMapUSA — County BSL Update (NGSO Satellite, Jun 2025)")
    print("=" * 60)

    target_states = [s.upper() for s in args.states]
    all_county_bsls = {}

    for i, state_abbr in enumerate(target_states, 1):
        state_fips = ALL_STATE_FIPS.get(state_abbr)
        if not state_fips:
            print(f"\n[{i}/{len(target_states)}] Unknown state: {state_abbr}")
            continue

        print(f"\n[{i}/{len(target_states)}] {state_abbr}")

        try:
            file_info = find_ngso_file(state_fips)
            if not file_info:
                print(f"    No NGSO file found — skipping")
                continue

            print(f"    {int(file_info.get('record_count', 0)):,} records")
            csv_path = download_csv(file_info, state_abbr)
            county_bsls = count_bsls(csv_path, state_fips)
            all_county_bsls.update(county_bsls)

            if not args.keep_local:
                csv_path.unlink(missing_ok=True)
                for zf in (TEMP_DIR / state_abbr).glob("*.zip"):
                    zf.unlink(missing_ok=True)

            time.sleep(0.2)

        except Exception as e:
            print(f"    ERROR: {e}")
            continue

    print(f"\n{'=' * 60}")
    print(f"  Total: {len(all_county_bsls):,} counties across {len(target_states)} states")
    update_supabase(all_county_bsls, dry_run=args.dry_run)
    print("=" * 60)
    print("  Done.")
    print("=" * 60)


if __name__ == "__main__":
    main()
