#!/usr/bin/env python3
"""
Reusable state data pipeline for the Fiber Market Analysis tool.

Generates a county-level unified JSON dataset for any US state using:
  - FCC BDC (latest available): fiber, cable, FWA technology coverage
  - Census ACS 5-year: demographics, income, housing
  - USDA RUCC 2023: rural-urban classification
  - USGS TRI: terrain ruggedness (if available)

Usage:
    python3 scripts/process_state_data.py --state NC --fips-prefix 37

Output:
    data/<state_lower>-unified-data.json

Data Prep (manual steps before running):
  1. Download FCC BDC FTTP locations for your state:
       https://broadbandmap.fcc.gov/data-download/bulk-fixed-availability-data
       Save to: data/raw/fcc/jun2025/fttp_locations_<state_lower>.csv

  2. Ensure you have (shared files already in repo):
       data/raw/fcc/jun2025/broadband_summary_place_<state_lower>.csv
       data/raw/fcc/jun2025/provider_summary_by_geography.csv
       data/raw/census/census_acs_<state_lower>.json
       data/raw/census/census_acs_<state_lower>_2018.json
       data/raw/census/place_county_crosswalk_<state_lower>.txt
       data/raw/usda/rucc2023.csv  (national file, already present)
       data/raw/usgs/ruggedness-scales-2020-tracts.xlsx  (national, already present)

  3. Run this script:
       python3 scripts/process_state_data.py --state NC --fips-prefix 37
"""

import argparse
import json
import csv
import math
import os
import sys

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, '..')
RAW_DIR = os.path.join(PROJECT_DIR, 'data', 'raw')
OUT_DIR = os.path.join(PROJECT_DIR, 'data')
QA_DIR = os.path.join(PROJECT_DIR, 'data', 'processed')

# NTIA BEAD allocations announced June 2023
BEAD_ALLOCATIONS = {
    'MO': 1_736_302_708,
    'NY':   664_618_251,
    'TX': 3_312_616_455,
    'NC': 1_532_999_481,
    'GA': 1_307_214_371,
    'PA': 1_161_778_272,
}


def parse_args():
    parser = argparse.ArgumentParser(description='Process state broadband data')
    parser.add_argument('--state', required=True, help='State abbreviation (e.g. NC, GA, TX)')
    parser.add_argument('--fips-prefix', required=True, help='2-digit state FIPS prefix (e.g. 37 for NC)')
    return parser.parse_args()


def safe_int(val):
    if val is None:
        return None
    try:
        v = int(val)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


# ─── Data Loaders ────────────────────────────────────────────────────────────

def load_census_acs(state_lower):
    """Load Census ACS 2023 + 2018. Returns dict keyed by 5-digit county FIPS."""
    path_2023 = os.path.join(RAW_DIR, 'census', f'census_acs_{state_lower}.json')
    path_2018 = os.path.join(RAW_DIR, 'census', f'census_acs_{state_lower}_2018.json')

    with open(path_2023) as f:
        raw_2023 = json.load(f)
    with open(path_2018) as f:
        raw_2018 = json.load(f)

    headers_2023 = raw_2023[0]
    headers_2018 = raw_2018[0]

    acs = {}
    for row in raw_2023[1:]:
        d = dict(zip(headers_2023, row))
        fips = d['state'] + d['county']
        pop = safe_int(d.get('B01003_001E'))
        housing = safe_int(d.get('B25001_001E'))
        total_occ = safe_int(d.get('B25003_001E'))
        owner_occ = safe_int(d.get('B25003_002E'))
        total_workers = safe_int(d.get('B08006_001E'))
        wfh = safe_int(d.get('B08006_017E'))

        # Strip state name suffix from county name
        # Census API returns e.g. "Durham County, North Carolina"
        name = d['NAME'].replace(' County', '').strip()
        if ',' in name:
            name = name.split(',')[0].strip()
        # Handle "St. Louis city" edge case and similar independent cities

        acs[fips] = {
            'name': name,
            'population_2023': pop,
            'housing_units': housing,
            'median_hhi': safe_int(d.get('B19013_001E')),
            'median_rent': safe_int(d.get('B25064_001E')),
            'median_home_value': safe_int(d.get('B25077_001E')),
            'owner_occupied_pct': round(owner_occ / total_occ * 100, 1) if total_occ else None,
            'wfh_pct': round(wfh / total_workers * 100, 1) if total_workers else None,
        }

    for row in raw_2018[1:]:
        d = dict(zip(headers_2018, row))
        fips = d['state'] + d['county']
        if fips in acs:
            acs[fips]['population_2018'] = safe_int(d.get('B01003_001E'))
            acs[fips]['housing_units_2018'] = safe_int(d.get('B25001_001E'))

    return acs


def load_usda_rucc(fips_prefix):
    """Load USDA RUCC 2023. Returns dict keyed by FIPS, filtered to state."""
    rucc = {}
    with open(os.path.join(RAW_DIR, 'usda', 'rucc2023.csv'), newline='', encoding='latin-1') as f:
        reader = csv.DictReader(f)
        for row in reader:
            fips = str(row['FIPS']).zfill(5)
            if not fips.startswith(fips_prefix):
                continue
            attr = row['Attribute']
            val = row['Value']
            if fips not in rucc:
                rucc[fips] = {}
            if attr == 'RUCC_2023':
                rucc[fips]['rucc_code'] = int(val)
            elif attr == 'Description':
                rucc[fips]['rucc_description'] = val
            elif attr == 'Population_2020':
                rucc[fips]['pop_2020'] = int(val)

    for fips, data in rucc.items():
        code = data.get('rucc_code', 9)
        if code <= 3:
            data['rural_class'] = 'Metro'
            data['is_metro_county'] = True
        elif code <= 5:
            data['rural_class'] = 'Micro'
            data['is_metro_county'] = False
        else:
            data['rural_class'] = 'Rural'
            data['is_metro_county'] = False

    return rucc


def load_terrain_data(fips_prefix):
    """Load USGS TRI, aggregate tracts to county level. Returns dict keyed by county FIPS."""
    xlsx_path = os.path.join(RAW_DIR, 'usgs', 'ruggedness-scales-2020-tracts.xlsx')
    if not os.path.exists(xlsx_path):
        print("  WARNING: USGS terrain file not found — terrain fields will be null")
        return {}

    df = pd.read_excel(xlsx_path, sheet_name='Ruggedness Scales 2020 Data', header=None, skiprows=1)
    real_headers = df.iloc[0].tolist()
    df = df.iloc[1:].copy()
    df.columns = real_headers

    df = df[df['CountyFIPS23'].astype(str).str.startswith(fips_prefix)].copy()
    for col in ['AreaTRI_Mean', 'AreaTRI_StdDev', 'Population', 'LandArea', 'ARS']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['CountyFIPS23'] = df['CountyFIPS23'].astype(str).str.zfill(5)

    county_terrain = df.groupby('CountyFIPS23').agg(
        tri_mean=('AreaTRI_Mean', 'mean'),
        tri_std=('AreaTRI_StdDev', 'mean'),
        ars_max=('ARS', 'max'),
        land_area=('LandArea', 'sum'),
        n_tracts=('CountyFIPS23', 'count'),
    ).reset_index()

    return county_terrain.set_index('CountyFIPS23').to_dict('index')


def load_fttp_locations(state_lower, fips_prefix):
    """Load FCC FTTP location data, aggregate to county. Returns dict keyed by county FIPS."""
    fttp_path = os.path.join(RAW_DIR, 'fcc', 'jun2025', f'fttp_locations_{state_lower}.csv')
    if not os.path.exists(fttp_path):
        print(f"  WARNING: FTTP locations file not found at {fttp_path}")
        print(f"  Download from FCC BDC and save to that path. Fiber fields will be zero.")
        return {}

    print(f"  Loading FTTP locations (may take a moment)...")
    df = pd.read_csv(fttp_path, dtype={
        'frn': str, 'provider_id': str, 'brand_name': str,
        'location_id': str, 'technology': str,
        'business_residential_code': str, 'state_usps': str,
        'block_geoid': str,
    })

    df['county_fips'] = df['block_geoid'].str[:5]
    df = df[
        df['county_fips'].str.startswith(fips_prefix) &
        df['business_residential_code'].isin(['R', 'X'])
    ].copy()

    print(f"  {len(df):,} residential FTTP records")

    result = {}
    for fips, group in df.groupby('county_fips'):
        unique_locations = group['location_id'].nunique()
        provider_stats = group.groupby('brand_name').agg(
            passings=('location_id', 'nunique'),
        ).reset_index().sort_values('passings', ascending=False)

        operators = []
        for _, prow in provider_stats.iterrows():
            name = prow['brand_name'].strip().strip('"')
            if name:
                operators.append({
                    'name': name,
                    'passings': int(prow['passings']),
                    'served': int(prow['passings']),
                })

        result[fips] = {
            'fiber_served': unique_locations,
            'operators': operators,
            'wireline_providers': [op['name'] for op in operators],
            'fiber_provider_count': len(operators),
        }

    return result


def load_provider_summary(fips_prefix):
    """Load FCC all-tech provider count per county."""
    path = os.path.join(RAW_DIR, 'fcc', 'jun2025', 'provider_summary_by_geography.csv')
    result = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['geography_type'] != 'County':
                continue
            geo_id = str(row['geography_id']).zfill(5)
            if not geo_id.startswith(fips_prefix):
                continue
            if geo_id not in result:
                result[geo_id] = set()
            result[geo_id].add(row['provider_id'])
    return {fips: {'total_provider_count': len(ps)} for fips, ps in result.items()}


def load_place_summary(state_lower, fips_prefix, acs):
    """Load FCC place-level BSL totals, aggregate to county."""
    crosswalk_path = os.path.join(RAW_DIR, 'census', f'place_county_crosswalk_{state_lower}.txt')
    if not os.path.exists(crosswalk_path):
        print(f"  WARNING: Place crosswalk not found at {crosswalk_path} — using Census housing units as BSL fallback")
        return {}

    with open(crosswalk_path) as f:
        content = f.read()

    name_to_fips = {data['name']: fips for fips, data in acs.items()}
    place_to_fips = {}
    for line in content.strip().split('\n')[1:]:
        parts = line.split('|')
        if len(parts) < 9:
            continue
        try:
            place_fips = int(parts[1] + parts[2])
        except ValueError:
            continue
        county_name = parts[8].replace(' County', '').strip()
        if ',' in county_name:
            county_name = county_name.split(',')[0].strip()
        cfips = name_to_fips.get(county_name)
        if cfips:
            place_to_fips[place_fips] = cfips

    fcc_path = os.path.join(RAW_DIR, 'fcc', 'jun2025', f'broadband_summary_place_{state_lower}.csv')
    if not os.path.exists(fcc_path):
        print(f"  WARNING: Place summary not found at {fcc_path}")
        return {}

    fcc = pd.read_csv(fcc_path)
    any_tech = fcc[
        (fcc['technology'] == 'Any Technology') &
        (fcc['biz_res'] == 'R') &
        (fcc['area_data_type'] == 'Total')
    ].copy()
    any_tech['county_fips'] = any_tech['geography_id'].apply(lambda p: place_to_fips.get(int(p)))
    any_tech = any_tech.dropna(subset=['county_fips'])
    county_bsls = any_tech.groupby('county_fips')['total_units'].sum().to_dict()
    return {fips: {'place_total_bsls': int(total)} for fips, total in county_bsls.items()}


def load_technology_coverage(state_lower, fips_prefix, acs):
    """Load cable and FWA coverage per county from place-level summary."""
    crosswalk_path = os.path.join(RAW_DIR, 'census', f'place_county_crosswalk_{state_lower}.txt')
    fcc_path = os.path.join(RAW_DIR, 'fcc', 'jun2025', f'broadband_summary_place_{state_lower}.csv')

    if not os.path.exists(crosswalk_path) or not os.path.exists(fcc_path):
        print("  WARNING: Missing crosswalk or place summary — tech coverage fields will be null")
        return {}

    with open(crosswalk_path) as f:
        content = f.read()

    name_to_fips = {data['name']: fips for fips, data in acs.items()}
    place_to_fips = {}
    for line in content.strip().split('\n')[1:]:
        parts = line.split('|')
        if len(parts) < 9:
            continue
        try:
            place_fips = int(parts[1] + parts[2])
        except ValueError:
            continue
        county_name = parts[8].replace(' County', '').strip()
        if ',' in county_name:
            county_name = county_name.split(',')[0].strip()
        cfips = name_to_fips.get(county_name)
        if cfips:
            place_to_fips[place_fips] = cfips

    fcc = pd.read_csv(fcc_path)
    fcc_res = fcc[(fcc['biz_res'] == 'R') & (fcc['area_data_type'] == 'Total')].copy()
    fcc_res['county_fips'] = fcc_res['geography_id'].apply(lambda p: place_to_fips.get(int(p)))
    fcc_res = fcc_res.dropna(subset=['county_fips'])
    fcc_res['speed_25_3'] = pd.to_numeric(fcc_res['speed_25_3'], errors='coerce').fillna(0)
    fcc_res['total_units'] = pd.to_numeric(fcc_res['total_units'], errors='coerce').fillna(0)
    fcc_res['weighted_units'] = fcc_res['total_units'] * fcc_res['speed_25_3']

    result = {}
    for county_fips, group in fcc_res.groupby('county_fips'):
        any_tech = group[group['technology'] == 'Any Technology']
        total_bsls = any_tech['total_units'].sum()
        if total_bsls == 0:
            continue

        def safe_pct(tech_name):
            rows = group[group['technology'] == tech_name]
            return round(min(1.0, rows['weighted_units'].sum() / total_bsls), 3)

        bb_pct = safe_pct('Any Technology')
        result[county_fips] = {
            'cable_coverage_pct': safe_pct('Cable'),
            'fwa_coverage_pct': safe_pct('All Fixed Wireless'),
            'broadband_coverage_pct': bb_pct,
            'broadband_gap_pct': round(max(0.0, 1.0 - bb_pct), 3),
        }

    return result


# ─── County Record Builder ────────────────────────────────────────────────────

def build_county(fips, acs_data, fttp_data, provider_data, place_data,
                 terrain_data, rucc_data, tech_data):
    """Build a unified county record from all real data sources."""
    name = acs_data['name']

    # USDA RUCC
    rucc = rucc_data.get(fips, {})
    rucc_code = rucc.get('rucc_code')
    rucc_description = rucc.get('rucc_description')
    rural_class = rucc.get('rural_class', 'Unknown')
    is_metro = rucc.get('is_metro_county', False)

    # Census ACS
    pop_2023 = acs_data['population_2023']
    pop_2018 = acs_data.get('population_2018')
    pop_growth = round((pop_2023 - pop_2018) / pop_2018 * 100, 2) if pop_2018 and pop_2018 > 0 else None
    housing_units = acs_data['housing_units']
    housing_2018 = acs_data.get('housing_units_2018')
    housing_growth = round((housing_units - housing_2018) / housing_2018 * 100, 2) if housing_2018 and housing_2018 > 0 else None
    median_hhi = acs_data['median_hhi']
    median_rent = acs_data['median_rent']
    median_home_value = acs_data['median_home_value']
    owner_occ = acs_data['owner_occupied_pct']
    wfh_pct = acs_data['wfh_pct']

    # Terrain
    terrain = terrain_data.get(fips, {})
    tri_mean = terrain.get('tri_mean')
    tri_std = terrain.get('tri_std')
    land_area = terrain.get('land_area')
    terrain_roughness = round(min(1.0, max(0, (tri_mean or 0) / 60)), 2) if tri_mean else None

    if land_area and land_area > 0:
        pop_density = round(pop_2023 / land_area, 1) if pop_2023 else None
        housing_density = round(housing_units / land_area, 1) if housing_units else None
    else:
        pop_density = None
        housing_density = None

    # FCC FTTP
    fttp = fttp_data.get(fips, {})
    fiber_served = fttp.get('fiber_served', 0)
    operators = fttp.get('operators', [])
    wireline_providers = fttp.get('wireline_providers', [])
    fiber_provider_count = fttp.get('fiber_provider_count', 0)

    # Total BSLs
    place = place_data.get(fips, {})
    place_bsls = place.get('place_total_bsls', 0)
    total_bsls = max(place_bsls, housing_units or 0)
    fiber_served = min(fiber_served, total_bsls)
    fiber_unserved = max(0, total_bsls - fiber_served)
    fiber_penetration = round(fiber_served / total_bsls, 3) if total_bsls > 0 else 0

    # Provider count
    prov = provider_data.get(fips, {})
    total_provider_count = prov.get('total_provider_count', 0)

    # Technology coverage
    tech = tech_data.get(fips, {})
    cable_coverage_pct = float(tech.get('cable_coverage_pct', 0) or 0)
    fwa_coverage_pct = float(tech.get('fwa_coverage_pct', 0) or 0)
    broadband_coverage_pct = float(tech.get('broadband_coverage_pct', 0) or 0)
    broadband_gap_pct = float(tech.get('broadband_gap_pct', 0) or 0)
    cable_present = bool(cable_coverage_pct > 0.05)
    fwa_present = bool(fwa_coverage_pct > 0.05)

    # Competitive intensity (fiber-based)
    if fiber_provider_count >= 4:
        comp_intensity = 3
        comp_label = "High"
    elif fiber_provider_count >= 3:
        comp_intensity = 2
        comp_label = "Moderate"
    elif fiber_provider_count >= 2:
        comp_intensity = 1
        comp_label = "Low"
    elif fiber_provider_count == 1:
        comp_intensity = 0
        comp_label = "Monopoly"
    else:
        comp_intensity = 0
        comp_label = "None"

    # Scores
    income_score = min(1, max(0, ((median_hhi or 30000) - 30000) / 60000))
    density_score = min(1, max(0, math.log10(max(1, housing_density or 1)) / 3))
    growth_score = min(1, max(0, ((pop_growth or 0) + 5) / 15))
    wfh_score = min(1, max(0, (wfh_pct or 0) / 25))
    demo_score = round(income_score * 0.35 + density_score * 0.25 + growth_score * 0.25 + wfh_score * 0.15, 3)
    opportunity_score = round(1 - fiber_penetration, 3)
    attractiveness_index = round(demo_score * 0.55 + opportunity_score * 0.45, 3)

    if attractiveness_index >= 0.45:
        segment = "Most Attractive"
    elif attractiveness_index >= 0.30:
        segment = "Neutral"
    else:
        segment = "Least Attractive"

    # Terrain classification
    if terrain_roughness is not None:
        if terrain_roughness >= 0.6:
            cost_tier = "Very High"
            build_diff = "Challenging"
        elif terrain_roughness >= 0.4:
            cost_tier = "High"
            build_diff = "Moderate-Hard"
        elif terrain_roughness >= 0.2:
            cost_tier = "Medium"
            build_diff = "Moderate"
        else:
            cost_tier = "Low"
            build_diff = "Easy"
    else:
        cost_tier = None
        build_diff = None

    # BEAD — eligible locations proxied by fiber_unserved
    # (locations without fiber = unserved + underserved, both eligible for BEAD funding)
    bead_eligible_locations = fiber_unserved  # max(0, total_bsls - fiber_served)
    bead_status = "Eligible" if bead_eligible_locations > 0 else "Not Eligible"

    # Momentum — null pending Dec 2024 BDC filing
    fiber_growth_pct = None
    momentum_class = None

    return {
        "geoid": fips,
        "name": name,
        "is_metro_county": is_metro,
        "total_bsls": total_bsls,
        "fiber_served": fiber_served,
        "fiber_unserved": fiber_unserved,
        "fiber_penetration": fiber_penetration,
        "operators": operators,
        "population_2023": pop_2023,
        "population_2018": pop_2018,
        "pop_growth_pct": pop_growth,
        "housing_units": housing_units,
        "housing_growth_pct": housing_growth,
        "land_area_sqmi": round(land_area, 1) if land_area else None,
        "pop_density": pop_density,
        "housing_density": housing_density,
        "median_hhi": median_hhi,
        "median_rent": median_rent,
        "median_home_value": median_home_value,
        "owner_occupied_pct": owner_occ,
        "wfh_pct": wfh_pct,
        "demo_score": demo_score,
        "opportunity_score": opportunity_score,
        "attractiveness_index": attractiveness_index,
        "segment": segment,
        "bead_status": bead_status,
        "bead_eligible_locations": bead_eligible_locations,
        "bead_state_allocation": None,           # filled in post-processing
        "bead_dollars_per_eligible_loc": None,   # filled in post-processing
        "bead_implied_county_award": None,       # filled in post-processing
        "bead_dollars_awarded": None,
        "bead_awardees": [],
        "bead_locations_covered": bead_eligible_locations if bead_eligible_locations > 0 else None,
        "bead_claimed_pct": None,
        "competitive_intensity": comp_intensity,
        "competitive_label": comp_label,
        "wireline_providers": wireline_providers,
        "total_broadband_providers": total_provider_count,
        "cable_coverage_pct": cable_coverage_pct,
        "fwa_coverage_pct": fwa_coverage_pct,
        "broadband_coverage_pct": broadband_coverage_pct,
        "broadband_gap_pct": broadband_gap_pct,
        "cable_present": cable_present,
        "fwa_present": fwa_present,
        "fiber_growth_pct": fiber_growth_pct,
        "momentum_class": momentum_class,
        "elevation_mean_ft": round(tri_mean, 1) if tri_mean else None,
        "elevation_std_ft": round(tri_std, 1) if tri_std else None,
        "terrain_roughness": terrain_roughness,
        "construction_cost_tier": cost_tier,
        "build_difficulty": build_diff,
        "rucc_code": rucc_code,
        "rucc_description": rucc_description,
        "rural_class": rural_class,
    }


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    state = args.state.upper()
    state_lower = state.lower()
    fips_prefix = args.fips_prefix.zfill(2)

    print("=" * 60)
    print(f"State Data Pipeline: {state} (FIPS prefix: {fips_prefix})")
    print("=" * 60)

    print("\n1. Loading Census ACS data...")
    acs = load_census_acs(state_lower)
    print(f"   {len(acs)} counties")

    print("\n2. Loading USDA RUCC 2023...")
    rucc = load_usda_rucc(fips_prefix)
    print(f"   {len(rucc)} counties with RUCC codes")

    print("\n3. Loading USGS terrain data...")
    terrain = load_terrain_data(fips_prefix)
    print(f"   {len(terrain)} counties with terrain data")

    print("\n4. Loading FCC FTTP location data...")
    fttp = load_fttp_locations(state_lower, fips_prefix)
    print(f"   {len(fttp)} counties with fiber location data")

    print("\n5. Loading FCC provider summary...")
    providers = load_provider_summary(fips_prefix)
    print(f"   {len(providers)} counties with provider data")

    print("\n6. Loading FCC place-level summary (BSL totals)...")
    place = load_place_summary(state_lower, fips_prefix, acs)
    print(f"   {len(place)} counties with place-level BSL data")

    print("\n7. Loading technology coverage (cable/FWA)...")
    tech = load_technology_coverage(state_lower, fips_prefix, acs)
    print(f"   {len(tech)} counties with tech coverage data")

    print("\n8. Building county records...")
    data = {}
    for fips in sorted(acs.keys()):
        data[fips] = build_county(
            fips, acs[fips], fttp, providers, place, terrain, rucc, tech
        )

    # Post-process: add BEAD state-level metrics
    state_allocation = BEAD_ALLOCATIONS.get(state)
    total_eligible = sum(c.get('bead_eligible_locations', 0) for c in data.values())
    dollars_per_loc = round(state_allocation / total_eligible, 2) if state_allocation and total_eligible > 0 else None
    for county in data.values():
        county['bead_state_allocation'] = state_allocation
        county['bead_dollars_per_eligible_loc'] = dollars_per_loc
        eligible = county.get('bead_eligible_locations', 0)
        county['bead_implied_county_award'] = int(round(eligible * dollars_per_loc)) if dollars_per_loc and eligible else 0

    if state_allocation:
        print(f"\n   BEAD: ${state_allocation/1e9:.2f}B state allocation | "
              f"{total_eligible:,} eligible locations | "
              f"${dollars_per_loc:,.0f}/location" if dollars_per_loc else "")

    # Output
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, f'{state_lower}-unified-data.json')
    with open(out_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"Generated {len(data)} {state} county records → {out_path}")

    # Summary
    avg_pen = sum(c["fiber_penetration"] for c in data.values()) / len(data)
    total_bsls = sum(c["total_bsls"] for c in data.values())
    total_served = sum(c["fiber_served"] for c in data.values())
    cable_counties = sum(1 for c in data.values() if c.get("cable_present"))
    fwa_counties = sum(1 for c in data.values() if c.get("fwa_present"))

    print(f"\nSummary:")
    print(f"  Total counties: {len(data)}")
    print(f"  Total BSLs: {total_bsls:,}")
    print(f"  Total fiber served: {total_served:,}")
    print(f"  Avg fiber penetration: {avg_pen:.1%}")
    print(f"  Counties with cable: {cable_counties} ({cable_counties/len(data):.0%})")
    print(f"  Counties with FWA: {fwa_counties} ({fwa_counties/len(data):.0%})")

    print(f"\nNext steps:")
    print(f"  1. Add '{state}' to DataHandler.loadData() in js/data.js")
    print(f"  2. Add '{state}' to FEATURED_STATES in js/map.js")
    print(f"  3. Add fiber-data.json entry for {state}")
    print(f"  4. Rebuild site and test")


if __name__ == '__main__':
    main()
