#!/usr/bin/env python3
"""
Compute Build Momentum from FCC BDC delta: Dec 2024 vs Jun 2025.

For each county:
  1. Pull fiber (tech 50) residential location counts from the Dec 2024 FCC filing
  2. Compare with fiber_served already in Supabase (Jun 2025)
  3. Compute fiber_growth_pct = (jun2025 - dec2024) / dec2024 * 100
  4. Derive momentum_class:
       < 0%          → Stalled
       0% – 5%       → Steady
       5% – 15%      → Growing
       15%+          → Surging
  5. Upsert fiber_growth_pct + momentum_class into the counties table

Run one state for testing:
    python3 pipeline/compute_momentum.py --states MO

Run all states loaded in Supabase:
    python3 pipeline/compute_momentum.py

Environment variables:
    FCC_USERNAME          your FCC account email
    FCC_API_TOKEN         your 44-char FCC API token
    SUPABASE_URL          https://sveqgyhncdrjemohpwho.supabase.co
    SUPABASE_SERVICE_KEY  sb_secret_... (service role key)
"""

import argparse
import io
import os
import sys
import zipfile

import requests
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

FCC_USERNAME         = os.environ.get("FCC_USERNAME", "")
FCC_API_TOKEN        = os.environ.get("FCC_API_TOKEN", "")
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "https://sveqgyhncdrjemohpwho.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

FCC_BASE_URL = "https://broadbandmap.fcc.gov/api/public/map"

# Dec 2024 = previous snapshot; Jun 2025 = current snapshot already in Supabase.
# If Dec 2024 isn't available the script will fall back to the next earlier date.
PREV_PERIOD_TARGET = "2024-12-31"

# Momentum thresholds (6-month fiber location growth %)
THRESHOLDS = [
    (0.0,  'Stalled'),
    (5.0,  'Steady'),
    (15.0, 'Growing'),
    (float('inf'), 'Surging'),
]

BATCH_SIZE = 200

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

# ── FCC API ───────────────────────────────────────────────────────────────────

def fcc_headers():
    return {
        'username':   FCC_USERNAME,
        'hash_value': FCC_API_TOKEN,
        'Accept':     'application/json',
        'User-Agent': 'FiberMapUSA/1.0 (fibermapusa.com)',
    }


def list_as_of_dates():
    resp = requests.get(f"{FCC_BASE_URL}/listAsOfDates", headers=fcc_headers(), timeout=30)
    resp.raise_for_status()
    return sorted(
        [d['as_of_date'] for d in resp.json().get('data', [])
         if d.get('data_type') == 'availability'],
        reverse=True
    )


def pick_prev_date(dates, target=PREV_PERIOD_TARGET):
    """
    Return the most recent filing date that is <= target.
    Falls back to the oldest available if none qualify.
    """
    candidates = [d for d in dates if d <= target]
    if candidates:
        return candidates[0]  # already sorted desc
    # All dates are newer than target — use oldest available as fallback
    return dates[-1]


def list_fiber_files(as_of_date, state_fips):
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{as_of_date}"
    resp = requests.get(url, headers=fcc_headers(), params={'category': 'State'}, timeout=60)
    resp.raise_for_status()
    files = resp.json().get('data', [])
    return [
        f for f in files
        if f.get('technology_type') == 'Fixed Broadband'
        and f.get('subcategory') == 'Location Coverage'
        and f.get('state_fips') == state_fips
        and str(f.get('technology_code')) == '50'   # fiber only
    ]


def stream_fiber_csv(file_id, label):
    url = f"{FCC_BASE_URL}/downloads/downloadFile/availability/{file_id}"
    print(f"    Streaming {label} ...", end=' ', flush=True)
    resp = requests.get(url, headers=fcc_headers(), timeout=600, stream=True)
    resp.raise_for_status()
    buf = io.BytesIO()
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        buf.write(chunk)
    size_mb = buf.tell() / 1024 / 1024
    buf.seek(0)
    print(f'{size_mb:.1f} MB', flush=True)
    with zipfile.ZipFile(buf) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
        if not csv_names:
            raise ValueError(f"No CSV in zip for {label}")
        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f, dtype={
                'location_id': str, 'block_geoid': str,
                'business_residential_code': str,
            }, low_memory=False)
    return df


def county_fiber_counts(df, fips_prefix):
    """Return {county_fips: unique_residential_location_count}."""
    df = df.copy()
    df['county_fips'] = df['block_geoid'].str[:5]
    df = df[
        df['county_fips'].str.startswith(fips_prefix) &
        df['business_residential_code'].isin(['R', 'X'])
    ]
    return df.groupby('county_fips')['location_id'].nunique().to_dict()


# ── Momentum logic ────────────────────────────────────────────────────────────

def momentum_class(growth_pct):
    """Classify a growth percentage into a momentum label."""
    if growth_pct < 0:
        return 'Stalled'
    for threshold, label in THRESHOLDS:
        if growth_pct < threshold:
            return label
    return 'Surging'


# ── Supabase ──────────────────────────────────────────────────────────────────

def get_supabase():
    try:
        from supabase import create_client
    except ImportError:
        print("ERROR: pip install supabase")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def fetch_current_fiber(client, state_code):
    """
    Fetch current fiber_served values from Supabase for a state.
    Returns {geoid: fiber_served}.
    """
    results = {}
    page_size = 1000
    offset = 0
    while True:
        rows = (
            client.table('counties')
            .select('geoid, fiber_served')
            .eq('state_code', state_code)
            .range(offset, offset + page_size - 1)
            .execute()
        ).data
        if not rows:
            break
        for r in rows:
            results[r['geoid']] = r['fiber_served'] or 0
        if len(rows) < page_size:
            break
        offset += page_size
    return results


def upsert_momentum(client, rows):
    # Use update (not upsert) — rows already exist, just patching two fields
    updated = 0
    for row in rows:
        client.table('counties').update({
            'fiber_growth_pct': row['fiber_growth_pct'],
            'momentum_class':   row['momentum_class'],
        }).eq('geoid', row['geoid']).execute()
        updated += 1
    print(f"    Updated {updated} momentum rows")


# ── Per-state pipeline ────────────────────────────────────────────────────────

def process_state(client, state_code, fips_prefix, prev_date):
    print(f"\n[{state_code}] (FIPS prefix {fips_prefix})")

    # 1. Fetch Dec 2024 fiber counts from FCC
    files = list_fiber_files(prev_date, fips_prefix)
    if not files:
        print(f"  No FCC fiber files found for {state_code} on {prev_date} — skipping")
        return False

    try:
        df = stream_fiber_csv(files[0]['file_id'], f"{state_code} fiber {prev_date}")
        prev_counts = county_fiber_counts(df, fips_prefix)
        del df
        print(f"  Dec 2024: {len(prev_counts)} counties with fiber data")
    except Exception as e:
        print(f"  ERROR fetching FCC data: {e}")
        return False

    # 2. Fetch Jun 2025 fiber_served from Supabase
    curr_counts = fetch_current_fiber(client, state_code)
    print(f"  Jun 2025: {len(curr_counts)} counties in Supabase")

    if not curr_counts:
        print(f"  No Supabase data for {state_code} — run fcc_to_supabase.py first")
        return False

    # 3. Compute growth and build upsert rows
    rows = []
    # Only update counties already in Supabase — don't insert new rows (missing state_code etc.)
    all_fips = set(curr_counts)
    stats = {'stalled': 0, 'steady': 0, 'growing': 0, 'surging': 0, 'no_prev': 0}

    for geoid in sorted(all_fips):
        prev = prev_counts.get(geoid, 0)
        curr = curr_counts.get(geoid, 0)

        if prev == 0:
            # No previous fiber data — can't compute meaningful growth
            # Could be new service areas; store null to avoid misleading display
            rows.append({
                'geoid': geoid,
                'fiber_growth_pct': None,
                'momentum_class': None,
            })
            stats['no_prev'] += 1
            continue

        growth_pct = round((curr - prev) / prev * 100, 2)
        mc = momentum_class(growth_pct)
        rows.append({
            'geoid': geoid,
            'fiber_growth_pct': growth_pct,
            'momentum_class': mc,
        })
        key = mc.lower()
        if key in stats:
            stats[key] += 1

    print(f"  Results: Stalled={stats['stalled']} Steady={stats['steady']} "
          f"Growing={stats['growing']} Surging={stats['surging']} "
          f"NoPrev={stats['no_prev']}")

    upsert_momentum(client, rows)
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Compute FCC fiber build momentum (Dec 2024 → Jun 2025) and write to Supabase'
    )
    parser.add_argument('--states', nargs='+', metavar='STATE',
                        help='State codes to process (e.g. MO TX NC). Default: all.')
    parser.add_argument('--prev-date', default=None, metavar='DATE',
                        help=f'Override previous filing date (default: closest to {PREV_PERIOD_TARGET}).')
    args = parser.parse_args()

    if not FCC_USERNAME or not FCC_API_TOKEN:
        print("ERROR: Set FCC_USERNAME and FCC_API_TOKEN environment variables.")
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

    print("Fetching available FCC filing dates...")
    dates = list_as_of_dates()
    print(f"  Available: {dates[:6]}{'...' if len(dates) > 6 else ''}")

    if args.prev_date:
        prev_date = args.prev_date
        print(f"  Using override: {prev_date}")
    else:
        prev_date = pick_prev_date(dates, PREV_PERIOD_TARGET)
        print(f"  Using previous period: {prev_date} (target was {PREV_PERIOD_TARGET})")

    ok, failed = 0, 0
    for state_code, fips_prefix in states.items():
        try:
            if process_state(client, state_code, fips_prefix, prev_date):
                ok += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [{state_code}] ERROR: {e}")
            import traceback; traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"Done. Success: {ok} | Failed/Skipped: {failed}")
    print(f"{'='*50}")
    print()
    print("Next: re-run map to verify Build Momentum layer shows real classes.")
    print("      No code changes needed — map.js already reads fiber_growth_pct")
    print("      when non-null (falls back to competitive_intensity proxy otherwise).")


if __name__ == '__main__':
    main()
