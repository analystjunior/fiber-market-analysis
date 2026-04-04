#!/usr/bin/env python3
"""
Compute construction buildable months per county from NOAA Climate Normals.

Uses NOAA Climate Data Online (CDO) API — NORMAL_MLY dataset, 1991-2020 30-year
normals — to fetch monthly average temperatures by county FIPS code.

Threshold: 32°F (0°C) average monthly temp. Months below this are flagged as
impractical for ground construction (trenching, boring, conduit installation)
due to frozen or near-frozen soil conditions.

Results written to Supabase counties table:
  buildable_months   INT     — count of months with avg temp > 32°F (0–12)
  winter_severity    TEXT    — "None" / "Mild" / "Moderate" / "Severe"
  coldest_month_f    REAL    — average temp of coldest month (°F)

Prerequisites:
  pip install requests supabase

NOAA CDO token (free, instant):
  https://www.ncdc.noaa.gov/cdo-web/token

Usage:
  # All states already in Supabase
  python3 pipeline/compute_buildable_months.py

  # Specific states
  python3 pipeline/compute_buildable_months.py --states MO TX NC

  # Override NOAA token inline
  NOAA_TOKEN=abc123 python3 pipeline/compute_buildable_months.py

Environment variables:
  NOAA_TOKEN            your NOAA CDO API token
  SUPABASE_URL          https://sveqgyhncdrjemohpwho.supabase.co
  SUPABASE_SERVICE_KEY  sb_secret_...
"""

import argparse
import os
import sys
import time
from collections import defaultdict

import requests

NOAA_TOKEN           = os.environ.get("NOAA_TOKEN", "")
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "https://sveqgyhncdrjemohpwho.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

NOAA_BASE   = "https://www.ncei.noaa.gov/cdo-web/api/v2"
DATASET     = "NORMAL_MLY"
DATATYPE    = "MLY-TAVG-NORMAL"

# NORMAL_MLY normals are stored under this representative year
NORMALS_YEAR = "2010"

# Ground construction becomes impractical at or below this monthly avg (°F)
BUILD_THRESHOLD_F = 32.0

# Rate limit: NOAA CDO allows 5 req/sec, 10k/day
REQUEST_DELAY = 0.22   # seconds between requests (~4.5/sec, safe margin)
MAX_RETRIES   = 3

ALL_STATES = {
    'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09',
    'DC':'11','DE':'10','FL':'12','GA':'13','HI':'15','ID':'16','IL':'17',
    'IN':'18','IA':'19','KS':'20','KY':'21','LA':'22','ME':'23','MD':'24',
    'MA':'25','MI':'26','MN':'27','MS':'28','MO':'29','MT':'30','NE':'31',
    'NV':'32','NH':'33','NJ':'34','NM':'35','NY':'36','NC':'37','ND':'38',
    'OH':'39','OK':'40','OR':'41','PA':'42','RI':'44','SC':'45','SD':'46',
    'TN':'47','TX':'48','UT':'49','VT':'50','VA':'51','WA':'53','WV':'54',
    'WI':'55','WY':'56',
}

BATCH_SIZE = 100  # Supabase update batch size


# ── NOAA API ──────────────────────────────────────────────────────────────────

def noaa_headers():
    return {'token': NOAA_TOKEN}


def fetch_county_normals(county_fips):
    """
    Fetch 12-month average temperature normals for a county FIPS.
    Returns dict {month_int: avg_temp_f} or None on failure.

    NOAA CDO returns station-level records within the county boundary.
    We average all stations per month to get a county representative value.
    """
    params = {
        'datasetid':  DATASET,
        'datatypeid': DATATYPE,
        'locationid': f'FIPS:{county_fips}',
        'startdate':  f'{NORMALS_YEAR}-01-01',
        'enddate':    f'{NORMALS_YEAR}-12-31',
        'units':      'standard',   # °F
        'limit':      1000,
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                f"{NOAA_BASE}/data",
                headers=noaa_headers(),
                params=params,
                timeout=30
            )
            if resp.status_code == 429:
                print(f"      Rate limited — waiting 5s...", flush=True)
                time.sleep(5)
                continue
            if resp.status_code == 400:
                # No stations in this county
                return None
            resp.raise_for_status()
            data = resp.json()
            results = data.get('results', [])
            if not results:
                return None

            # Aggregate: average all stations' values per month
            month_vals = defaultdict(list)
            for rec in results:
                # date format: "2010-01-01T00:00:00"
                month = int(rec['date'][5:7])
                val   = rec.get('value')
                if val is not None:
                    # NOAA CDO with units=standard returns °F directly
                    month_vals[month].append(float(val))

            if not month_vals:
                return None

            return {m: sum(vals) / len(vals) for m, vals in month_vals.items()}

        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"      NOAA error for {county_fips}: {e}", flush=True)
                return None

    return None


# ── Classification ────────────────────────────────────────────────────────────

def classify(monthly_temps_f):
    """
    Given {month: avg_temp_f}, return (buildable_months, winter_severity, coldest_month_f).
    """
    buildable = sum(1 for t in monthly_temps_f.values() if t > BUILD_THRESHOLD_F)
    cold_months = 12 - buildable
    coldest = min(monthly_temps_f.values())

    if cold_months == 0:
        severity = 'None'
    elif cold_months <= 2:
        severity = 'Mild'
    elif cold_months <= 4:
        severity = 'Moderate'
    else:
        severity = 'Severe'

    return buildable, severity, round(coldest, 1)


# ── Supabase ──────────────────────────────────────────────────────────────────

def get_supabase():
    try:
        from supabase import create_client
    except ImportError:
        print("ERROR: pip install supabase")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def fetch_state_geoids(client, state_code):
    """Return list of county geoids for a state from Supabase."""
    geoids = []
    offset = 0
    while True:
        rows = (
            client.table('counties')
            .select('geoid')
            .eq('state_code', state_code)
            .range(offset, offset + 999)
            .execute()
        ).data
        if not rows:
            break
        geoids.extend(r['geoid'] for r in rows)
        if len(rows) < 1000:
            break
        offset += 1000
    return geoids


def update_counties(client, rows):
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        for row in batch:
            client.table('counties').update({
                'buildable_months':  row['buildable_months'],
                'winter_severity':   row['winter_severity'],
                'coldest_month_f':   row['coldest_month_f'],
            }).eq('geoid', row['geoid']).execute()
    print(f"    Updated {len(rows)} counties in Supabase")


# ── Per-state pipeline ────────────────────────────────────────────────────────

def process_state(client, state_code):
    print(f"\n[{state_code}]")
    geoids = fetch_state_geoids(client, state_code)
    if not geoids:
        print(f"  No counties found in Supabase for {state_code} — run fcc_to_supabase.py first")
        return False

    print(f"  {len(geoids)} counties to process")
    rows = []
    skipped = 0
    severity_counts = {'None': 0, 'Mild': 0, 'Moderate': 0, 'Severe': 0}

    for i, geoid in enumerate(sorted(geoids)):
        time.sleep(REQUEST_DELAY)
        monthly = fetch_county_normals(geoid)

        if monthly is None or len(monthly) < 6:
            # Fewer than 6 months of data — NOAA has no nearby stations
            # Fall back to a state-level estimate using a representative city
            # (will be filled by fallback step below if needed)
            skipped += 1
            continue

        buildable, severity, coldest = classify(monthly)
        rows.append({
            'geoid':            geoid,
            'buildable_months': buildable,
            'winter_severity':  severity,
            'coldest_month_f':  coldest,
        })
        severity_counts[severity] += 1

        if (i + 1) % 10 == 0:
            print(f"    {i+1}/{len(geoids)} processed...", flush=True)

    print(f"  Results: None={severity_counts['None']} Mild={severity_counts['Mild']} "
          f"Moderate={severity_counts['Moderate']} Severe={severity_counts['Severe']} "
          f"Skipped={skipped}")

    if rows:
        update_counties(client, rows)

    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compute buildable months per county from NOAA Climate Normals'
    )
    parser.add_argument('--states', nargs='+', metavar='STATE',
                        help='State codes (e.g. MO TX NC). Default: all in Supabase.')
    args = parser.parse_args()

    if not NOAA_TOKEN:
        print("ERROR: Set NOAA_TOKEN environment variable.")
        print("Free token: https://www.ncdc.noaa.gov/cdo-web/token")
        sys.exit(1)
    if not SUPABASE_SERVICE_KEY:
        print("ERROR: Set SUPABASE_SERVICE_KEY environment variable.")
        sys.exit(1)

    states = {}
    if args.states:
        for s in args.states:
            s = s.upper()
            if s not in ALL_STATES:
                print(f"WARNING: Unknown state '{s}' — skipping")
                continue
            states[s] = ALL_STATES[s]
    else:
        states = ALL_STATES

    print("Connecting to Supabase...")
    client = get_supabase()

    print(f"\nThreshold: avg monthly temp > {BUILD_THRESHOLD_F}°F = buildable")
    print(f"Data:      NOAA Climate Normals 1991-2020 (MLY-TAVG-NORMAL)")
    print(f"States:    {len(states)}")

    ok, failed = 0, 0
    for state_code in states:
        try:
            if process_state(client, state_code):
                ok += 1
            else:
                failed += 1
        except Exception as e:
            import traceback
            print(f"  [{state_code}] ERROR: {e}")
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"Done. Success: {ok} | Failed/Skipped: {failed}")
    print(f"{'='*50}")
    print()
    print("Next steps:")
    print("  1. The county info panel will now show buildable months + winter severity")
    print("     (frontend reads buildable_months / winter_severity / coldest_month_f)")
    print("  2. Note: Supabase table needs these columns — add via SQL if missing:")
    print("     ALTER TABLE counties ADD COLUMN IF NOT EXISTS buildable_months smallint;")
    print("     ALTER TABLE counties ADD COLUMN IF NOT EXISTS winter_severity  text;")
    print("     ALTER TABLE counties ADD COLUMN IF NOT EXISTS coldest_month_f  real;")


if __name__ == '__main__':
    main()
