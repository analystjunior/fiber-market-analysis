#!/usr/bin/env python3
"""
One-time migration: upload all existing county JSONs + fiber-data.json to Supabase.

Prerequisites:
    pip install supabase

Usage:
    # All states
    python3 scripts/upload_to_supabase.py

    # Single state (for testing)
    python3 scripts/upload_to_supabase.py --state MO

    # State summary only (fiber-data.json)
    python3 scripts/upload_to_supabase.py --summary-only

Environment variables (required):
    SUPABASE_URL          https://sveqgyhncdrjemohpwho.supabase.co
    SUPABASE_SERVICE_KEY  sb_secret_... (service role — bypasses RLS)
"""

import argparse
import glob
import json
import os
import sys
from pathlib import Path

SUPABASE_URL        = os.environ.get("SUPABASE_URL", "https://sveqgyhncdrjemohpwho.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

SCRIPT_DIR  = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR    = PROJECT_DIR / "data"

BATCH_SIZE = 200  # rows per upsert call

# Maps state abbr → state_code (derived from filename, but explicit for clarity)
FIPS_TO_STATE = {
    '01':'AL','02':'AK','04':'AZ','05':'AR','06':'CA','08':'CO','09':'CT',
    '10':'DE','11':'DC','12':'FL','13':'GA','15':'HI','16':'ID','17':'IL',
    '18':'IN','19':'IA','20':'KS','21':'KY','22':'LA','23':'ME','24':'MD',
    '25':'MA','26':'MI','27':'MN','28':'MS','29':'MO','30':'MT','31':'NE',
    '32':'NV','33':'NH','34':'NJ','35':'NM','36':'NY','37':'NC','38':'ND',
    '39':'OH','40':'OK','41':'OR','42':'PA','44':'RI','45':'SC','46':'SD',
    '47':'TN','48':'TX','49':'UT','50':'VT','51':'VA','53':'WA','54':'WV',
    '55':'WI','56':'WY',
}


def get_client():
    try:
        from supabase import create_client
    except ImportError:
        print("ERROR: supabase package not installed. Run: pip install supabase")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def transform_county(fips, county, state_code):
    """
    Normalize a county dict for Supabase insertion.
    Adds state_code, ensures JSONB fields are lists/dicts (not strings),
    and drops any fields not in the schema.
    """
    SCHEMA_FIELDS = {
        'geoid', 'state_code', 'name', 'is_metro_county',
        'total_bsls', 'fiber_served', 'fiber_unserved', 'fiber_penetration',
        'cable_served', 'dsl_served',
        'cable_coverage_pct', 'fwa_coverage_pct', 'broadband_coverage_pct',
        'broadband_gap_pct', 'cable_present', 'fwa_present',
        'operators', 'cable_operators', 'dsl_operators',
        'cable_operator_count', 'dsl_operator_count',
        'wireline_providers', 'total_broadband_providers',
        'competitive_intensity', 'competitive_label',
        'population_2023', 'population_2018', 'pop_growth_pct',
        'housing_units', 'housing_growth_pct', 'land_area_sqmi',
        'pop_density', 'housing_density', 'median_hhi', 'median_rent',
        'median_home_value', 'owner_occupied_pct', 'wfh_pct',
        'demo_score', 'opportunity_score', 'attractiveness_index', 'segment',
        'bead_status', 'bead_eligible_locations', 'bead_state_allocation',
        'bead_dollars_per_eligible_loc', 'bead_implied_county_award',
        'bead_dollars_awarded', 'bead_awardees', 'bead_locations_covered',
        'bead_claimed_pct',
        'elevation_mean_ft', 'elevation_std_ft', 'terrain_roughness',
        'construction_cost_tier', 'build_difficulty',
        'rucc_code', 'rucc_description', 'rural_class',
        'fiber_growth_pct', 'momentum_class', 'data_as_of',
    }

    row = {k: v for k, v in county.items() if k in SCHEMA_FIELDS}
    row['geoid']      = fips
    row['state_code'] = state_code
    row.setdefault('data_as_of', '2025-06-01')

    # Ensure JSONB fields are always lists (never None)
    for jsonb_field in ('operators', 'cable_operators', 'dsl_operators',
                        'wireline_providers', 'bead_awardees'):
        if not isinstance(row.get(jsonb_field), list):
            row[jsonb_field] = []

    return row


def upsert_batch(client, table, rows):
    """Upsert a batch of rows, return number inserted."""
    result = client.table(table).upsert(rows, on_conflict='geoid' if table == 'counties' else 'state_code').execute()
    return len(rows)


def upload_state(client, state_lower):
    """Upload one state's unified JSON to the counties table."""
    state_code = state_lower.upper()
    filepath = DATA_DIR / f"{state_lower}-unified-data.json"

    if not filepath.exists():
        print(f"  [{state_code}] SKIP — no file at {filepath.name}")
        return 0

    with open(filepath) as f:
        data = json.load(f)

    rows = [transform_county(fips, county, state_code)
            for fips, county in data.items()]

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        upsert_batch(client, 'counties', batch)
        total += len(batch)
        print(f"  [{state_code}] {total}/{len(rows)} rows upserted...")

    print(f"  [{state_code}] Done — {len(rows)} counties uploaded")
    return len(rows)


def upload_state_summary(client):
    """Upload fiber-data.json to the state_summary table."""
    filepath = DATA_DIR / "fiber-data.json"
    if not filepath.exists():
        print("  SKIP — fiber-data.json not found")
        return

    with open(filepath) as f:
        state_data = json.load(f)

    rows = []
    for state_code, d in state_data.items():
        rows.append({
            'state_code':           state_code,
            'state_name':           d.get('state', state_code),
            'total_housing_units':  d.get('totalHousingUnits'),
            'total_fiber_passings': d.get('totalFiberPassings'),
            'fiber_penetration':    d.get('fiberPenetration'),
            'operators':            d.get('operators', []),
        })

    for i in range(0, len(rows), BATCH_SIZE):
        client.table('state_summary').upsert(
            rows[i:i + BATCH_SIZE], on_conflict='state_code'
        ).execute()

    print(f"  state_summary — {len(rows)} states uploaded")


def main():
    parser = argparse.ArgumentParser(
        description='Upload existing county JSONs to Supabase'
    )
    parser.add_argument('--state', help='Upload a single state (e.g. MO)')
    parser.add_argument('--summary-only', action='store_true',
                        help='Upload only fiber-data.json → state_summary')
    args = parser.parse_args()

    print(f"Connecting to Supabase: {SUPABASE_URL}")
    client = get_client()

    if args.summary_only:
        print("\nUploading state summary...")
        upload_state_summary(client)
        print("\nDone.")
        return

    if args.state:
        states = [args.state.lower()]
    else:
        # Find all unified JSON files
        files = sorted(glob.glob(str(DATA_DIR / '*-unified-data.json')))
        states = [Path(f).name.replace('-unified-data.json', '') for f in files]

    print(f"\nUploading {len(states)} state(s) to counties table...\n")
    total_counties = 0
    for state_lower in states:
        total_counties += upload_state(client, state_lower)

    print(f"\nUploading state summary (fiber-data.json)...")
    upload_state_summary(client)

    print(f"\n{'='*50}")
    print(f"Done. {total_counties:,} counties uploaded across {len(states)} states.")
    print(f"{'='*50}")


if __name__ == '__main__':
    main()
