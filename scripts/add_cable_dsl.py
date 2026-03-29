#!/usr/bin/env python3
"""
Patch existing unified county JSON files with cable (tech 40) and DSL (tech 10)
operator-level passings data from FCC BDC location files.

This script does NOT re-run the full pipeline — it reads the already-built
unified JSON, adds cable_operators / dsl_operators arrays and updates the
operators[] array with per-tech passings, then writes it back.

Usage:
    # Single state
    python3 scripts/add_cable_dsl.py --state MO

    # All states that have cable/DSL files downloaded
    python3 scripts/add_cable_dsl.py --all

Data prep:
    Download cable and DSL location files from FCC BDC first:
        python3 scripts/fcc_download.py --tech 40 10 --no-drive --no-summary

    Files must be saved to:
        data/raw/fcc/jun2025/cable_locations_{state_lower}.csv   (tech 40)
        data/raw/fcc/jun2025/dsl_locations_{state_lower}.csv     (tech 10)

Output:
    Updates data/{state_lower}-unified-data.json in place.
    Adds fields:
        county.cable_served          — unique locations with cable service
        county.dsl_served            — unique locations with DSL/copper service
        county.cable_operator_count  — number of cable operators
        county.dsl_operator_count    — number of DSL operators
        county.operators[].fiber_passings  — split out from existing passings
        county.operators[].cable_passings  — new
        county.operators[].dsl_passings    — new
        county.cable_operators[]     — [{name, passings}] sorted by passings
        county.dsl_operators[]       — [{name, passings}] sorted by passings
"""

import argparse
import json
import os
import sys

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, '..')
RAW_DIR = os.path.join(PROJECT_DIR, 'data', 'raw', 'fcc', 'jun2025')
DATA_DIR = os.path.join(PROJECT_DIR, 'data')

ALL_STATES = [
    'al','ak','az','ar','ca','co','ct','dc','de','fl','ga','hi','id','il',
    'in','ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne',
    'nv','nh','nj','nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd',
    'tn','tx','ut','vt','va','wa','wv','wi','wy',
]

# State abbreviation → 2-digit FIPS prefix
STATE_FIPS = {
    'al':'01','ak':'02','az':'04','ar':'05','ca':'06','co':'08','ct':'09',
    'dc':'11','de':'10','fl':'12','ga':'13','hi':'15','id':'16','il':'17',
    'in':'18','ia':'19','ks':'20','ky':'21','la':'22','me':'23','md':'24',
    'ma':'25','mi':'26','mn':'27','ms':'28','mo':'29','mt':'30','ne':'31',
    'nv':'32','nh':'33','nj':'34','nm':'35','ny':'36','nc':'37','nd':'38',
    'oh':'39','ok':'40','or':'41','pa':'42','ri':'44','sc':'45','sd':'46',
    'tn':'47','tx':'48','ut':'49','vt':'50','va':'51','wa':'53','wv':'54',
    'wi':'55','wy':'56',
}


def load_location_file(path, fips_prefix, tech_label):
    """
    Load an FCC BDC location CSV for a single tech type.
    Returns dict: { county_fips: { 'total': int, 'operators': {brand_name: int} } }
    """
    if not os.path.exists(path):
        return None

    print(f"  Loading {tech_label} locations from {os.path.basename(path)}...")
    df = pd.read_csv(path, dtype={
        'frn': str, 'provider_id': str, 'brand_name': str,
        'location_id': str, 'technology': str,
        'business_residential_code': str, 'state_usps': str,
        'block_geoid': str,
    }, low_memory=False)

    df['county_fips'] = df['block_geoid'].str[:5]
    df = df[
        df['county_fips'].str.startswith(fips_prefix) &
        df['business_residential_code'].isin(['R', 'X'])
    ].copy()

    print(f"  {len(df):,} residential {tech_label} records")

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
                if name and name.strip()
            }
        }

    return result


def patch_state(state_lower):
    """Patch one state's unified JSON with cable and DSL data."""
    fips_prefix = STATE_FIPS.get(state_lower)
    if not fips_prefix:
        print(f"  ERROR: Unknown state '{state_lower}'")
        return False

    unified_path = os.path.join(DATA_DIR, f'{state_lower}-unified-data.json')
    if not os.path.exists(unified_path):
        print(f"  SKIP: No unified JSON at {unified_path}")
        return False

    cable_path = os.path.join(RAW_DIR, f'cable_locations_{state_lower}.csv')
    dsl_path   = os.path.join(RAW_DIR, f'dsl_locations_{state_lower}.csv')

    cable_data = load_location_file(cable_path, fips_prefix, 'Cable')
    dsl_data   = load_location_file(dsl_path,   fips_prefix, 'DSL')

    if cable_data is None and dsl_data is None:
        print(f"  SKIP: No cable or DSL location files found for {state_lower.upper()}")
        print(f"    Expected: {cable_path}")
        print(f"    Expected: {dsl_path}")
        return False

    cable_data = cable_data or {}
    dsl_data   = dsl_data   or {}

    print(f"  Loading existing unified data...")
    with open(unified_path) as f:
        counties = json.load(f)

    patched = 0
    for fips, county in counties.items():
        cable = cable_data.get(fips, {})
        dsl   = dsl_data.get(fips, {})

        # County-level totals
        county['cable_served'] = cable.get('total', 0)
        county['dsl_served']   = dsl.get('total', 0)

        # Build cable_operators list
        cable_ops = sorted(
            [{'name': n, 'passings': p} for n, p in cable.get('operators', {}).items()],
            key=lambda x: x['passings'], reverse=True
        )
        county['cable_operators'] = cable_ops
        county['cable_operator_count'] = len(cable_ops)

        # Build dsl_operators list
        dsl_ops = sorted(
            [{'name': n, 'passings': p} for n, p in dsl.get('operators', {}).items()],
            key=lambda x: x['passings'], reverse=True
        )
        county['dsl_operators'] = dsl_ops
        county['dsl_operator_count'] = len(dsl_ops)

        # Enrich existing operators[] with fiber_passings (= existing passings)
        # and add cable_passings + dsl_passings where names match
        cable_by_name = cable.get('operators', {})
        dsl_by_name   = dsl.get('operators', {})

        existing_ops = county.get('operators', [])
        for op in existing_ops:
            name = op.get('name', '')
            # Keep backward-compat 'passings' field as fiber passings
            fiber_p = op.get('passings', op.get('fiber_passings', 0))
            op['fiber_passings'] = fiber_p
            op['passings'] = fiber_p  # backward compat
            op['cable_passings'] = cable_by_name.get(name, 0)
            op['dsl_passings']   = dsl_by_name.get(name, 0)

        # Also add cable/DSL-only operators that aren't in the fiber list
        existing_names = {op['name'] for op in existing_ops}

        for name, p in cable_by_name.items():
            if name not in existing_names:
                existing_ops.append({
                    'name': name,
                    'passings': 0,
                    'fiber_passings': 0,
                    'cable_passings': p,
                    'dsl_passings': 0,
                })
                existing_names.add(name)

        for name, p in dsl_by_name.items():
            if name not in existing_names:
                existing_ops.append({
                    'name': name,
                    'passings': 0,
                    'fiber_passings': 0,
                    'cable_passings': 0,
                    'dsl_passings': p,
                })
                existing_names.add(name)

        county['operators'] = existing_ops
        patched += 1

    with open(unified_path, 'w') as f:
        json.dump(counties, f, indent=2)

    print(f"  Patched {patched} counties → {unified_path}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Add cable and DSL operator passings to existing unified county JSONs'
    )
    parser.add_argument('--state', help='Single state abbreviation (e.g. MO)')
    parser.add_argument('--all', action='store_true',
                        help='Process all states that have cable/DSL files downloaded')
    args = parser.parse_args()

    if not args.state and not args.all:
        parser.print_help()
        sys.exit(1)

    states = ALL_STATES if args.all else [args.state.lower()]

    for state_lower in states:
        print(f"\n{'='*50}")
        print(f"  Processing {state_lower.upper()}")
        print(f"{'='*50}")
        patch_state(state_lower)

    print("\nDone.")


if __name__ == '__main__':
    main()
