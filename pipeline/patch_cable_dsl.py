#!/usr/bin/env python3
"""
Patch cable (tech 40) + DSL (tech 10) passings into existing Supabase county rows.

Streams FCC BDC CSVs directly into memory — no local files written.
Fetches existing operators[] from Supabase and merges cable/DSL passings in,
then upserts ONLY the cable/DSL fields (fiber data is untouched).

Prerequisites:
    pip install requests pandas supabase

Usage:
    # All states missing cable data
    python3 pipeline/patch_cable_dsl.py

    # Specific states
    python3 pipeline/patch_cable_dsl.py --states FL CA IL

Environment (copy .env.example → .env and fill in values):
    FCC_USERNAME          your FCC account email
    FCC_API_TOKEN         your 44-char FCC API token
    SUPABASE_URL          https://sveqgyhncdrjemohpwho.supabase.co
    SUPABASE_SERVICE_KEY  your Supabase service role key (sb_secret_...)
"""

import argparse
import io
import os
import sys
import zipfile

import requests
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

FCC_USERNAME         = os.environ.get('FCC_USERNAME',         '')
FCC_API_TOKEN        = os.environ.get('FCC_API_TOKEN',        '')
SUPABASE_URL         = os.environ.get('SUPABASE_URL',         'https://sveqgyhncdrjemohpwho.supabase.co')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

FCC_BASE_URL = 'https://broadbandmap.fcc.gov/api/public/map'

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

# ── FCC ───────────────────────────────────────────────────────────────────────

def fcc_headers():
    return {
        'username':   FCC_USERNAME,
        'hash_value': FCC_API_TOKEN,
        'Accept':     'application/json',
        'User-Agent': 'FiberMapUSA/1.0 (fibermapusa.com)',
    }


def get_latest_filing_date():
    resp = requests.get(f'{FCC_BASE_URL}/listAsOfDates', headers=fcc_headers(), timeout=30)
    resp.raise_for_status()
    dates = sorted(
        [d['as_of_date'] for d in resp.json().get('data', [])
         if d.get('data_type') == 'availability'],
        reverse=True
    )
    return dates[0] if dates else '2025-06-30'


def list_files_for_state(as_of_date, fips_prefix, tech_codes):
    url = f'{FCC_BASE_URL}/downloads/listAvailabilityData/{as_of_date}'
    resp = requests.get(url, headers=fcc_headers(),
                        params={'category': 'State'}, timeout=60)
    resp.raise_for_status()
    return [
        f for f in resp.json().get('data', [])
        if f.get('technology_type') == 'Fixed Broadband'
        and f.get('subcategory') == 'Location Coverage'
        and f.get('state_fips') == fips_prefix
        and str(f.get('technology_code')) in tech_codes
    ]


def stream_to_df(file_id, label):
    """Download FCC zip in memory, return DataFrame. No disk writes."""
    url = f'{FCC_BASE_URL}/downloads/downloadFile/availability/{file_id}'
    print(f'    Streaming {label}...', end=' ', flush=True)
    resp = requests.get(url, headers=fcc_headers(), timeout=600, stream=True)
    resp.raise_for_status()
    buf = io.BytesIO()
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        buf.write(chunk)
    size_mb = buf.tell() / 1024 / 1024
    buf.seek(0)
    print(f'{size_mb:.1f} MB')
    with zipfile.ZipFile(buf) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith('.csv'))
        with zf.open(csv_name) as f:
            return pd.read_csv(f, dtype={
                'frn': str, 'provider_id': str, 'brand_name': str,
                'location_id': str, 'technology': str,
                'business_residential_code': str, 'state_usps': str,
                'block_geoid': str,
            }, low_memory=False)


def aggregate(df, fips_prefix):
    """County-level unique location counts per operator."""
    df = df.copy()
    df['county_fips'] = df['block_geoid'].str[:5]
    df = df[
        df['county_fips'].str.startswith(fips_prefix) &
        df['business_residential_code'].isin(['R', 'X'])
    ]
    result = {}
    for fips, grp in df.groupby('county_fips'):
        total = grp['location_id'].nunique()
        by_op = (grp.groupby('brand_name')['location_id']
                 .nunique().sort_values(ascending=False))
        result[fips] = {
            'total': int(total),
            'operators': {
                n.strip().strip('"'): int(c)
                for n, c in by_op.items() if n and str(n).strip()
            }
        }
    return result


# ── Supabase ──────────────────────────────────────────────────────────────────

def get_supabase():
    try:
        from supabase import create_client
    except ImportError:
        print('ERROR: pip install supabase')
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def fetch_existing_counties(client, state_code):
    """Fetch geoid, name + operators for all counties in a state (paginated)."""
    rows = []
    page = 1000
    offset = 0
    while True:
        r = client.table('counties') \
            .select('geoid,name,operators') \
            .eq('state_code', state_code) \
            .range(offset, offset + page - 1) \
            .execute()
        rows.extend(r.data)
        if len(r.data) < page:
            break
        offset += page
    # Returns {geoid: {name, operators}}
    return {row['geoid']: {'name': row['name'], 'operators': row['operators'] or []}
            for row in rows}


def build_patch_rows(state_code, existing, cable_data, dsl_data):
    """
    Merge cable/DSL into existing operators arrays.
    Only processes counties already in Supabase (skips FCC-only counties).
    Returns list of rows with only cable/DSL fields (fiber data untouched).
    """
    # Only patch counties we already have — never insert incomplete rows
    all_fips = set(existing)
    rows = []

    for fips in sorted(all_fips):
        cable = cable_data.get(fips, {})
        dsl   = dsl_data.get(fips, {})
        cable_by_name = cable.get('operators', {})
        dsl_by_name   = dsl.get('operators', {})

        county = existing[fips]
        county_name = county['name']

        # Start from existing operators, add cable/DSL passings
        ops = list(county['operators'])
        seen = {op.get('name', '') for op in ops}

        for op in ops:
            name = op.get('name', '')
            op['cable_passings'] = cable_by_name.get(name, 0)
            op['dsl_passings']   = dsl_by_name.get(name, 0)

        # Add cable-only operators not already in fiber list
        for name, p in cable_by_name.items():
            if name not in seen:
                seen.add(name)
                ops.append({'name': name, 'passings': 0,
                            'fiber_passings': 0, 'cable_passings': p, 'dsl_passings': 0})

        # Add DSL-only operators
        for name, p in dsl_by_name.items():
            if name not in seen:
                seen.add(name)
                ops.append({'name': name, 'passings': 0,
                            'fiber_passings': 0, 'cable_passings': 0, 'dsl_passings': p})

        cable_ops = sorted(
            [{'name': n, 'passings': p} for n, p in cable_by_name.items()],
            key=lambda x: x['passings'], reverse=True
        )
        dsl_ops = sorted(
            [{'name': n, 'passings': p} for n, p in dsl_by_name.items()],
            key=lambda x: x['passings'], reverse=True
        )

        rows.append({
            'geoid':               fips,
            'state_code':          state_code,
            'name':                county_name,
            'cable_served':        cable.get('total', 0),
            'dsl_served':          dsl.get('total', 0),
            'operators':           ops,
            'cable_operators':     cable_ops,
            'dsl_operators':       dsl_ops,
            'cable_operator_count': len(cable_ops),
            'dsl_operator_count':  len(dsl_ops),
        })

    return rows


def upsert_rows(client, rows):
    for i in range(0, len(rows), BATCH_SIZE):
        client.table('counties').upsert(
            rows[i:i + BATCH_SIZE], on_conflict='geoid'
        ).execute()


# ── Main ──────────────────────────────────────────────────────────────────────

def process_state(client, state_code, fips_prefix, as_of_date):
    print(f'\n[{state_code}]')

    files = list_files_for_state(as_of_date, fips_prefix, ['40', '10'])
    if not files:
        print(f'  No cable/DSL files found in FCC for {state_code}')
        return False

    cable_data, dsl_data = {}, {}
    for f in files:
        tech = str(f['technology_code'])
        label = {'40': 'cable', '10': 'DSL'}.get(tech, tech)
        try:
            df = stream_to_df(f['file_id'], f'{state_code} {label}')
            agg = aggregate(df, fips_prefix)
            if tech == '40':
                cable_data = agg
            else:
                dsl_data = agg
            print(f'    {len(agg)} counties with {label} data')
            del df
        except Exception as e:
            print(f'    WARNING: {label} failed: {e}')

    if not cable_data and not dsl_data:
        return False

    print(f'  Fetching existing county operators from Supabase...')
    existing = fetch_existing_counties(client, state_code)
    print(f'  {len(existing)} counties fetched')

    rows = build_patch_rows(state_code, existing, cable_data, dsl_data)
    upsert_rows(client, rows)
    print(f'  Patched {len(rows)} counties → Supabase')
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--states', nargs='+', metavar='STATE',
                        help='States to patch (default: all missing cable data)')
    args = parser.parse_args()

    # Default: the 45 states missing cable data
    MISSING = [
        'AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','HI','IA','ID',
        'IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MS','MT','ND',
        'NE','NH','NJ','NM','NV','OH','OK','OR','RI','SC','SD','TN','UT',
        'VA','VT','WA','WI','WV','WY'
    ]

    if args.states:
        states = {s.upper(): ALL_STATES[s.upper()] for s in args.states
                  if s.upper() in ALL_STATES}
    else:
        states = {s: ALL_STATES[s] for s in MISSING}

    print(f'Connecting to Supabase...')
    client = get_supabase()

    print(f'Getting latest FCC filing date...')
    as_of_date = get_latest_filing_date()
    print(f'  Using: {as_of_date}')

    print(f'\nPatching cable + DSL for {len(states)} states...')
    ok, failed = 0, 0
    for state_code, fips_prefix in states.items():
        try:
            if process_state(client, state_code, fips_prefix, as_of_date):
                ok += 1
            else:
                failed += 1
        except Exception as e:
            print(f'  [{state_code}] ERROR: {e}')
            failed += 1

    print(f'\n{"="*50}')
    print(f'Done. Patched: {ok} | Failed: {failed}')
    print(f'{"="*50}')


if __name__ == '__main__':
    main()
