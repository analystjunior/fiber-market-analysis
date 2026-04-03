#!/usr/bin/env python3
"""
FCC BDC → Supabase pipeline (no local downloads).

Streams FCC location CSVs directly into memory, aggregates county-level
operator passings, and upserts into the Supabase counties table.

Run manually or via GitHub Actions. Never touches your local disk for FCC data.

Prerequisites:
    pip install requests pandas supabase

Usage:
    # All states
    python3 pipeline/fcc_to_supabase.py

    # Specific states
    python3 pipeline/fcc_to_supabase.py --states MO TX NC

    # Tech codes: 50=fiber, 40=cable, 10=DSL (default: all three)
    python3 pipeline/fcc_to_supabase.py --states MO --tech 40 10

Environment variables:
    FCC_USERNAME          your FCC account email
    FCC_API_TOKEN         your 44-char FCC API token
    SUPABASE_URL          https://sveqgyhncdrjemohpwho.supabase.co
    SUPABASE_SERVICE_KEY  sb_secret_... (service role key — never use anon here)

FCC token: broadbandmap.fcc.gov → login → Account → Manage API Access → Generate
"""

import argparse
import io
import os
import sys
import zipfile
from pathlib import Path

import requests
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

FCC_USERNAME        = os.environ.get("FCC_USERNAME", "")
FCC_API_TOKEN       = os.environ.get("FCC_API_TOKEN", "")
SUPABASE_URL        = os.environ.get("SUPABASE_URL", "https://sveqgyhncdrjemohpwho.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

FCC_BASE_URL = "https://broadbandmap.fcc.gov/api/public/map"

TECH_LABELS = {'50': 'fiber', '40': 'cable', '10': 'dsl'}

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

BATCH_SIZE = 200

# ── FCC API ───────────────────────────────────────────────────────────────────

def fcc_headers():
    return {
        'username':   FCC_USERNAME,
        'hash_value': FCC_API_TOKEN,
        'Accept':     'application/json',
        'User-Agent': 'FiberMapUSA/1.0 (fibermapusa.com)',
    }


def get_latest_filing_date():
    resp = requests.get(f"{FCC_BASE_URL}/listAsOfDates", headers=fcc_headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    dates = sorted(
        [d['as_of_date'] for d in data.get('data', []) if d.get('data_type') == 'availability'],
        reverse=True
    )
    return dates[0] if dates else '2025-06-30'


def list_location_files(as_of_date, state_fips, tech_codes):
    """Return FCC file metadata for the given state + tech codes."""
    url = f"{FCC_BASE_URL}/downloads/listAvailabilityData/{as_of_date}"
    resp = requests.get(url, headers=fcc_headers(), params={'category': 'State'}, timeout=60)
    resp.raise_for_status()
    files = resp.json().get('data', [])
    return [
        f for f in files
        if f.get('technology_type') == 'Fixed Broadband'
        and f.get('subcategory') == 'Location Coverage'
        and f.get('state_fips') == state_fips
        and str(f.get('technology_code')) in tech_codes
    ]


def stream_fcc_csv(file_id, label):
    """
    Download an FCC zip file in memory and return a DataFrame.
    No local files are written.
    """
    url = f"{FCC_BASE_URL}/downloads/downloadFile/availability/{file_id}"
    print(f"    Streaming {label}...", end=' ', flush=True)

    resp = requests.get(url, headers=fcc_headers(), timeout=600, stream=True)
    resp.raise_for_status()

    buf = io.BytesIO()
    for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
        buf.write(chunk)
    size_mb = buf.tell() / 1024 / 1024
    buf.seek(0)
    print(f'{size_mb:.1f} MB', flush=True)

    with zipfile.ZipFile(buf) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
        if not csv_names:
            raise ValueError(f"No CSV found in zip for {label}")
        with zf.open(csv_names[0]) as csv_file:
            df = pd.read_csv(csv_file, dtype={
                'frn': str, 'provider_id': str, 'brand_name': str,
                'location_id': str, 'technology': str,
                'business_residential_code': str, 'state_usps': str,
                'block_geoid': str,
            }, low_memory=False)

    return df


# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate_by_county(df, fips_prefix):
    """
    Filter residential records, group by county FIPS, count unique location_ids.
    Returns: {county_fips: {'total': int, 'operators': {brand_name: int}}}
    """
    df = df.copy()
    df['county_fips'] = df['block_geoid'].str[:5]
    df = df[
        df['county_fips'].str.startswith(fips_prefix) &
        df['business_residential_code'].isin(['R', 'X'])
    ]

    result = {}
    for fips, group in df.groupby('county_fips'):
        total = group['location_id'].nunique()
        by_provider = (
            group.groupby('brand_name')['location_id']
            .nunique()
            .sort_values(ascending=False)
        )
        result[fips] = {
            'total': int(total),
            'operators': {
                name.strip().strip('"'): int(count)
                for name, count in by_provider.items()
                if name and str(name).strip()
            }
        }
    return result


def build_county_rows(state_code, fips_prefix, tech_data):
    """
    Build Supabase upsert rows from aggregated tech data.
    tech_data: {'fiber': {...}, 'cable': {...}, 'dsl': {...}}
    Each value is {county_fips: {total, operators}}.
    Returns list of dicts ready for upsert.
    """
    fiber_data = tech_data.get('fiber', {})
    cable_data = tech_data.get('cable', {})
    dsl_data   = tech_data.get('dsl',   {})

    all_fips = set(fiber_data) | set(cable_data) | set(dsl_data)
    rows = []

    for fips in sorted(all_fips):
        fiber = fiber_data.get(fips, {})
        cable = cable_data.get(fips, {})
        dsl   = dsl_data.get(fips, {})

        fiber_ops_by_name = fiber.get('operators', {})
        cable_ops_by_name = cable.get('operators', {})
        dsl_ops_by_name   = dsl.get('operators', {})

        # Merge all operator names
        all_names = set(fiber_ops_by_name) | set(cable_ops_by_name) | set(dsl_ops_by_name)
        operators = []
        for name in sorted(all_names, key=lambda n: -(fiber_ops_by_name.get(n, 0))):
            fp = fiber_ops_by_name.get(name, 0)
            cp = cable_ops_by_name.get(name, 0)
            dp = dsl_ops_by_name.get(name, 0)
            operators.append({
                'name':           name,
                'passings':       fp,
                'fiber_passings': fp,
                'cable_passings': cp,
                'dsl_passings':   dp,
            })

        cable_operators = sorted(
            [{'name': n, 'passings': p} for n, p in cable_ops_by_name.items()],
            key=lambda x: x['passings'], reverse=True
        )
        dsl_operators = sorted(
            [{'name': n, 'passings': p} for n, p in dsl_ops_by_name.items()],
            key=lambda x: x['passings'], reverse=True
        )

        rows.append({
            'geoid':               fips,
            'state_code':          state_code,
            'fiber_served':        fiber.get('total', 0),
            'cable_served':        cable.get('total', 0),
            'dsl_served':          dsl.get('total', 0),
            'operators':           operators,
            'cable_operators':     cable_operators,
            'dsl_operators':       dsl_operators,
            'cable_operator_count': len(cable_operators),
            'dsl_operator_count':  len(dsl_operators),
            'data_as_of':          '2025-06-01',
        })

    return rows


# ── Supabase ──────────────────────────────────────────────────────────────────

def get_supabase():
    try:
        from supabase import create_client
    except ImportError:
        print("ERROR: supabase package not installed. Run: pip install supabase")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def upsert_rows(client, rows):
    for i in range(0, len(rows), BATCH_SIZE):
        client.table('counties').upsert(
            rows[i:i + BATCH_SIZE], on_conflict='geoid'
        ).execute()
    print(f"    Upserted {len(rows)} county rows to Supabase")


# ── Main ──────────────────────────────────────────────────────────────────────

def process_state(client, state_code, fips_prefix, as_of_date, tech_codes):
    print(f"\n[{state_code}]")

    files = list_location_files(as_of_date, fips_prefix, tech_codes)
    if not files:
        print(f"  No FCC files found for {state_code} (tech codes {tech_codes})")
        return False

    tech_data = {}
    for fcc_file in files:
        tech_code = str(fcc_file['technology_code'])
        label = TECH_LABELS.get(tech_code, tech_code)
        try:
            df = stream_fcc_csv(fcc_file['file_id'], f"{state_code} {label}")
            aggregated = aggregate_by_county(df, fips_prefix)
            tech_data[label] = aggregated
            print(f"    {len(aggregated)} counties with {label} data")
            del df
        except Exception as e:
            print(f"    WARNING: Failed to process {label} for {state_code}: {e}")

    if not tech_data:
        return False

    rows = build_county_rows(state_code, fips_prefix, tech_data)
    upsert_rows(client, rows)
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Stream FCC BDC data directly to Supabase (no local downloads)'
    )
    parser.add_argument('--states', nargs='+', metavar='STATE',
                        help='State codes to process (e.g. MO TX NC). Default: all.')
    parser.add_argument('--tech', nargs='+', default=['50', '40', '10'],
                        metavar='CODE',
                        help='FCC tech codes. 50=fiber 40=cable 10=DSL. Default: all three.')
    args = parser.parse_args()

    if not FCC_USERNAME or not FCC_API_TOKEN:
        print("ERROR: Set FCC_USERNAME and FCC_API_TOKEN environment variables.")
        print("Token: broadbandmap.fcc.gov → Account → Manage API Access → Generate")
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

    tech_codes = [str(t) for t in args.tech]

    print(f"Connecting to Supabase...")
    client = get_supabase()

    print(f"Getting latest FCC filing date...")
    as_of_date = get_latest_filing_date()
    print(f"  Using: {as_of_date}")

    print(f"\nProcessing {len(states)} state(s), tech codes: {tech_codes}")

    ok, failed = 0, 0
    for state_code, fips_prefix in states.items():
        try:
            if process_state(client, state_code, fips_prefix, as_of_date, tech_codes):
                ok += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [{state_code}] ERROR: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Done. Success: {ok} | Failed: {failed}")
    print(f"{'='*50}")


if __name__ == '__main__':
    main()
