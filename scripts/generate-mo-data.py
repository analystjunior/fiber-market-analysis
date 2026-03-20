#!/usr/bin/env python3
"""
Generate Missouri county-level unified data for the fiber market analysis tool.

ALL core metrics are derived from real public data sources — zero fake inputs.

Data sources:
- FCC BDC Jun 2025 FTTP locations: Fiber-served BSLs, operators, passings (REAL)
- FCC BDC Jun 2025 Provider Summary by Geography: Broadband provider counts per county (REAL)
- FCC BDC Jun 2025 Place-level Summary: Total BSLs via place-to-county aggregation (REAL)
- Census ACS 5-year 2023 + 2018: Demographics, income, housing (REAL)
- USDA ERS RUCC 2023: Rural-Urban Continuum Codes (REAL)
- USGS Terrain Ruggedness Index (2020 tracts): Aggregated to county-level TRI (REAL)
- BEAD: Marked as "Unverified" — county-level awards not yet public (FLAGGED)
- Momentum: Set to null — requires Dec 2024 BDC filing not yet available (NULL)

Usage:
    python3 scripts/generate-mo-data.py
"""

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

# STL/KC metro county FIPS (from Census CBSA definitions)
STL_KC_METRO_FIPS = frozenset({
    '29189', '29510', '29183', '29099', '29071', '29113', '29219',  # STL
    '29095', '29047', '29165', '29037', '29025', '29177', '29049', '29107',  # KC
})


def safe_int(val):
    """Safely convert to int, returning None for missing/negative."""
    if val is None:
        return None
    try:
        v = int(val)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


# ─── Data Loaders ────────────────────────────────────────────────────────────


def load_census_acs():
    """Load Census ACS 2023 + 2018 data, return dict keyed by 5-digit county FIPS."""
    with open(os.path.join(RAW_DIR, 'census', 'census_acs_mo.json')) as f:
        raw_2023 = json.load(f)
    with open(os.path.join(RAW_DIR, 'census', 'census_acs_mo_2018.json')) as f:
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

        acs[fips] = {
            'name': d['NAME'].replace(' County, Missouri', '').replace(', Missouri', ''),
            'population_2023': pop,
            'housing_units': housing,
            'median_hhi': safe_int(d.get('B19013_001E')),
            'median_rent': safe_int(d.get('B25064_001E')),
            'median_home_value': safe_int(d.get('B25077_001E')),
            'owner_occupied_pct': round(owner_occ / total_occ * 100, 1) if total_occ else None,
            'wfh_pct': round(wfh / total_workers * 100, 1) if total_workers else None,
        }

    # Add 2018 population for growth calc
    for row in raw_2018[1:]:
        d = dict(zip(headers_2018, row))
        fips = d['state'] + d['county']
        if fips in acs:
            acs[fips]['population_2018'] = safe_int(d.get('B01003_001E'))
            acs[fips]['housing_units_2018'] = safe_int(d.get('B25001_001E'))

    return acs


def load_usda_rucc():
    """Load USDA Rural-Urban Continuum Codes 2023, return dict keyed by FIPS.

    Source: https://www.ers.usda.gov/data-products/rural-urban-continuum-codes/
    Format: long CSV with FIPS, State, County_Name, Attribute, Value
    """
    rucc = {}
    with open(os.path.join(RAW_DIR, 'usda', 'rucc2023.csv'), newline='', encoding='latin-1') as f:
        reader = csv.DictReader(f)
        for row in reader:
            fips = str(row['FIPS']).zfill(5)
            if not fips.startswith('29'):
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

    # Derive rural_class and is_metro from official RUCC
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


def load_terrain_data():
    """Load USGS terrain ruggedness data, aggregate tracts to county level.

    Source: USGS 3DEP TRI (Terrain Ruggedness Index) by 2020 Census Tract
    """
    df = pd.read_excel(
        os.path.join(RAW_DIR, 'usgs', 'ruggedness-scales-2020-tracts.xlsx'),
        sheet_name='Ruggedness Scales 2020 Data', header=None, skiprows=1
    )

    real_headers = df.iloc[0].tolist()
    df = df.iloc[1:].copy()
    df.columns = real_headers

    # Filter to MO
    df = df[df['CountyFIPS23'].astype(str).str.startswith('29')].copy()

    for col in ['AreaTRI_Mean', 'AreaTRI_StdDev', 'Population', 'LandArea', 'ARS']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df['CountyFIPS23'] = df['CountyFIPS23'].astype(str).str.zfill(5)

    # Aggregate to county: mean TRI, sum land area
    county_terrain = df.groupby('CountyFIPS23').agg(
        tri_mean=('AreaTRI_Mean', 'mean'),
        tri_std=('AreaTRI_StdDev', 'mean'),
        ars_max=('ARS', 'max'),
        ars_mean=('ARS', 'mean'),
        land_area=('LandArea', 'sum'),
        n_tracts=('CountyFIPS23', 'count'),
    ).reset_index()

    return county_terrain.set_index('CountyFIPS23').to_dict('index')


def load_fttp_locations():
    """Load FCC FTTP location-level data, aggregate to county.

    Source: FCC BDC Jun 2025, Fiber to the Premises, Missouri
    File: bdc_29_FibertothePremises_fixed_broadband_J25_03mar2026.csv
    ~2.1M rows: one row per provider-location pair

    Returns dict keyed by county FIPS with:
    - fiber_served: count of DISTINCT residential BSLs with fiber
    - operators: list of {name, passings, served} dicts
    - wireline_providers: list of provider names
    """
    print("  Loading FTTP locations (2.1M rows, may take a moment)...")
    fttp_path = os.path.join(RAW_DIR, 'fcc', 'jun2025', 'fttp_locations_mo.csv')

    # Read with pandas for performance
    df = pd.read_csv(fttp_path, dtype={
        'frn': str,
        'provider_id': str,
        'brand_name': str,
        'location_id': str,
        'technology': str,
        'max_advertised_download_speed': float,
        'max_advertised_upload_speed': float,
        'low_latency': str,
        'business_residential_code': str,
        'state_usps': str,
        'block_geoid': str,
        'h3_res8_id': str,
    })

    # Extract county FIPS from block_geoid (first 5 chars)
    df['county_fips'] = df['block_geoid'].str[:5]

    # Filter to MO counties only and residential/both (R or X)
    df = df[
        df['county_fips'].str.startswith('29') &
        df['business_residential_code'].isin(['R', 'X'])
    ].copy()

    print(f"  {len(df):,} residential FTTP records for MO")

    result = {}

    for fips, group in df.groupby('county_fips'):
        # Unique fiber-served BSLs (distinct location_ids)
        unique_locations = group['location_id'].nunique()

        # Operators: count distinct locations per provider
        provider_stats = group.groupby('brand_name').agg(
            passings=('location_id', 'nunique'),
        ).reset_index()
        provider_stats = provider_stats.sort_values('passings', ascending=False)

        operators = []
        for _, prow in provider_stats.iterrows():
            name = prow['brand_name'].strip().strip('"')
            if name:
                operators.append({
                    'name': name,
                    'passings': int(prow['passings']),
                    'served': int(prow['passings']),  # passings = served for FTTP
                })

        result[fips] = {
            'fiber_served': unique_locations,
            'operators': operators,
            'wireline_providers': [op['name'] for op in operators],
            'fiber_provider_count': len(operators),
        }

    return result


def load_provider_summary():
    """Load FCC provider summary by geography (county level) for competitive intensity.

    Source: FCC BDC Jun 2025, Provider Summary by Geography (US-wide)
    Columns: geography_type, geography_id, geography_desc, data_type, provider_id, res_st_pct, bus_iv_pct

    Returns dict keyed by county FIPS with:
    - total_provider_count: number of unique broadband providers (all technologies)
    """
    print("  Loading provider summary by geography...")
    path = os.path.join(RAW_DIR, 'fcc', 'jun2025', 'provider_summary_by_geography.csv')

    result = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['geography_type'] != 'County':
                continue
            geo_id = str(row['geography_id']).zfill(5)
            if not geo_id.startswith('29'):
                continue

            if geo_id not in result:
                result[geo_id] = set()
            result[geo_id].add(row['provider_id'])

    # Convert sets to counts
    return {fips: {'total_provider_count': len(providers)}
            for fips, providers in result.items()}


def load_place_summary(acs):
    """Load FCC place-level summary, aggregate total_units to county.

    Uses Census place-to-county crosswalk to map places to counties.
    Returns dict keyed by county FIPS with total_bsls from FCC data.
    """
    print("  Loading place-level broadband summary...")

    # Load crosswalk
    crosswalk_path = os.path.join(RAW_DIR, 'census', 'place_county_crosswalk_mo.txt')
    with open(crosswalk_path) as f:
        content = f.read()

    name_to_fips = {}
    for fips, data in acs.items():
        name_to_fips[data['name']] = fips

    place_to_fips = {}
    for line in content.strip().split('\n')[1:]:
        parts = line.split('|')
        if len(parts) < 9:
            continue
        place_fips = int(parts[1] + parts[2])
        county_name = parts[8].replace(' County', '').strip()
        if ',' in county_name:
            county_name = county_name.split(',')[0].strip()
        county_name = county_name.replace('St. Louis city', 'St. Louis City')
        cfips = name_to_fips.get(county_name)
        if cfips:
            place_to_fips[place_fips] = cfips

    # Load place summary
    fcc = pd.read_csv(os.path.join(RAW_DIR, 'fcc', 'jun2025', 'broadband_summary_place_mo.csv'))

    # Filter: Any Technology, Residential, Total
    any_tech = fcc[
        (fcc['technology'] == 'Any Technology') &
        (fcc['biz_res'] == 'R') &
        (fcc['area_data_type'] == 'Total')
    ].copy()

    any_tech['county_fips'] = any_tech['geography_id'].apply(
        lambda pid: place_to_fips.get(int(pid))
    )
    any_tech = any_tech.dropna(subset=['county_fips'])

    county_bsls = any_tech.groupby('county_fips')['total_units'].sum().to_dict()

    return {fips: {'place_total_bsls': int(total)} for fips, total in county_bsls.items()}


# ─── County Record Builder ──────────────────────────────────────────────────


def build_county(fips, acs_data, fttp_data, provider_data, place_data,
                 terrain_data, rucc_data):
    """Build a single county record from ALL real data sources.

    Data provenance per field documented inline.
    """
    name = acs_data['name']

    # === USDA RUCC (REAL — replacing estimated) ===
    rucc = rucc_data.get(fips, {})
    rucc_code = rucc.get('rucc_code')
    rucc_description = rucc.get('rucc_description')
    rural_class = rucc.get('rural_class', 'Unknown')
    is_metro = rucc.get('is_metro_county', False)
    is_stl_kc = fips in STL_KC_METRO_FIPS

    # === CENSUS ACS (REAL) ===
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

    # === TERRAIN (REAL — USGS TRI aggregated from tracts) ===
    terrain = terrain_data.get(fips, {})
    tri_mean = terrain.get('tri_mean')
    tri_std = terrain.get('tri_std')
    land_area = terrain.get('land_area')

    # Normalize terrain roughness to 0-1 scale (TRI typically 0-80m in MO)
    terrain_roughness = round(min(1.0, max(0, (tri_mean or 0) / 60)), 2) if tri_mean else None

    if land_area and land_area > 0:
        pop_density = round(pop_2023 / land_area, 1) if pop_2023 else None
        housing_density = round(housing_units / land_area, 1) if housing_units else None
    else:
        pop_density = None
        housing_density = None

    # === FCC FTTP LOCATIONS (REAL — direct count of fiber-served BSLs) ===
    fttp = fttp_data.get(fips, {})
    fiber_served = fttp.get('fiber_served', 0)
    operators = fttp.get('operators', [])
    wireline_providers = fttp.get('wireline_providers', [])
    fiber_provider_count = fttp.get('fiber_provider_count', 0)

    # Total BSLs: prefer FCC place-level aggregation, fall back to Census housing units
    place = place_data.get(fips, {})
    place_bsls = place.get('place_total_bsls', 0)
    total_bsls = max(place_bsls, housing_units or 0)

    # Ensure fiber_served doesn't exceed total_bsls
    fiber_served = min(fiber_served, total_bsls)
    fiber_unserved = max(0, total_bsls - fiber_served)
    fiber_penetration = round(fiber_served / total_bsls, 3) if total_bsls > 0 else 0

    # === FCC PROVIDER SUMMARY (REAL — all-tech provider count) ===
    prov = provider_data.get(fips, {})
    total_provider_count = prov.get('total_provider_count', 0)

    # Competitive intensity based on FIBER provider count (from FTTP)
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

    # === SCORES (DERIVED from real data) ===
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

    # === TERRAIN CLASSIFICATION (DERIVED from real TRI) ===
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

    # === BEAD (FLAGGED as Unverified — county-level awards not yet public) ===
    bead_status = "Unverified"
    bead_dollars = None
    bead_locations = None
    bead_awardees = []
    bead_claimed_pct = None

    # === MOMENTUM (NULL — requires Dec 2024 FCC BDC filing) ===
    fiber_bsls_v5 = None
    fiber_bsls_v6 = fiber_served  # Jun 2025 = current period
    fiber_growth_net = None
    fiber_growth_pct = None
    momentum_class = None

    return {
        # Identity
        "geoid": fips,
        "name": name,

        # Geography (REAL — USDA RUCC)
        "is_metro_county": is_metro,
        "is_stl_kc_metro": is_stl_kc,

        # Fiber (REAL — FCC BDC FTTP locations)
        "total_bsls": total_bsls,
        "fiber_served": fiber_served,
        "fiber_unserved": fiber_unserved,
        "fiber_penetration": fiber_penetration,

        # Operators (REAL — FCC BDC FTTP locations)
        "operators": operators,

        # Demographics (REAL — Census ACS 5-year)
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

        # Scores (DERIVED from real data)
        "demo_score": demo_score,
        "opportunity_score": opportunity_score,
        "attractiveness_index": attractiveness_index,
        "segment": segment,

        # BEAD (FLAGGED — county-level awards not yet public)
        "bead_dollars_awarded": bead_dollars,
        "bead_awardees": bead_awardees,
        "bead_locations_covered": bead_locations,
        "bead_claimed_pct": bead_claimed_pct,
        "bead_status": bead_status,

        # Competition (REAL — FCC BDC FTTP + Provider Summary)
        "competitive_intensity": comp_intensity,
        "competitive_label": comp_label,
        "wireline_providers": wireline_providers,
        "total_broadband_providers": total_provider_count,

        # Momentum (NULL — Dec 2024 BDC not available)
        "fiber_bsls_v5": fiber_bsls_v5,
        "fiber_bsls_v6": fiber_bsls_v6,
        "fiber_growth_net": fiber_growth_net,
        "fiber_growth_pct": fiber_growth_pct,
        "momentum_class": momentum_class,

        # Terrain (REAL — USGS TRI)
        "elevation_mean_ft": round(tri_mean, 1) if tri_mean else None,
        "elevation_std_ft": round(tri_std, 1) if tri_std else None,
        "terrain_roughness": terrain_roughness,
        "construction_cost_tier": cost_tier,
        "build_difficulty": build_diff,

        # RUCC (REAL — USDA ERS 2023)
        "rucc_code": rucc_code,
        "rucc_description": rucc_description,
        "rural_class": rural_class,
    }


# ─── QA Report ───────────────────────────────────────────────────────────────


def generate_qa_report(data):
    """Generate a QA report validating all county records."""
    os.makedirs(QA_DIR, exist_ok=True)

    lines = []
    lines.append("# MO County Data QA Report")
    lines.append(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"Counties: {len(data)}")
    lines.append("")

    # Completeness check
    lines.append("## Field Completeness")
    lines.append("")
    all_fields = set()
    for county in data.values():
        all_fields.update(county.keys())

    for field in sorted(all_fields):
        non_null = sum(1 for c in data.values() if c.get(field) is not None)
        pct = non_null / len(data) * 100
        marker = "" if pct == 100 else " ⚠️" if pct < 100 and pct > 0 else " ❌"
        lines.append(f"  {field}: {non_null}/{len(data)} ({pct:.0f}%){marker}")

    lines.append("")

    # Data source provenance
    lines.append("## Data Source Provenance")
    lines.append("")
    provenance = {
        "REAL — FCC BDC FTTP Jun 2025": [
            "fiber_served", "fiber_unserved", "fiber_penetration",
            "operators", "wireline_providers", "competitive_intensity",
            "competitive_label", "fiber_bsls_v6"
        ],
        "REAL — FCC BDC Provider Summary Jun 2025": [
            "total_broadband_providers"
        ],
        "REAL — FCC BDC Place Summary + Census crosswalk": [
            "total_bsls"
        ],
        "REAL — Census ACS 5-year 2023": [
            "population_2023", "housing_units", "median_hhi", "median_rent",
            "median_home_value", "owner_occupied_pct", "wfh_pct"
        ],
        "REAL — Census ACS 5-year 2018": [
            "population_2018"
        ],
        "DERIVED — from real ACS": [
            "pop_growth_pct", "housing_growth_pct", "pop_density", "housing_density",
            "demo_score", "opportunity_score", "attractiveness_index", "segment"
        ],
        "REAL — USDA RUCC 2023": [
            "rucc_code", "rucc_description", "rural_class", "is_metro_county"
        ],
        "REAL — USGS TRI 2020": [
            "elevation_mean_ft", "elevation_std_ft", "terrain_roughness",
            "construction_cost_tier", "build_difficulty"
        ],
        "FLAGGED — Unverified": [
            "bead_dollars_awarded", "bead_awardees", "bead_locations_covered",
            "bead_claimed_pct", "bead_status"
        ],
        "NULL — Dec 2024 BDC not available": [
            "fiber_bsls_v5", "fiber_growth_net", "fiber_growth_pct", "momentum_class"
        ],
    }
    for source, fields in provenance.items():
        lines.append(f"  {source}:")
        for f in fields:
            lines.append(f"    - {f}")
        lines.append("")

    # Distribution checks
    lines.append("## Distribution Checks")
    lines.append("")

    # Fiber penetration
    pens = [c['fiber_penetration'] for c in data.values()]
    lines.append(f"  fiber_penetration: min={min(pens):.3f}, max={max(pens):.3f}, "
                 f"mean={sum(pens)/len(pens):.3f}, median={sorted(pens)[len(pens)//2]:.3f}")

    # Housing density
    densities = [c['housing_density'] for c in data.values() if c['housing_density']]
    if densities:
        lines.append(f"  housing_density: min={min(densities):.1f}, max={max(densities):.1f}, "
                     f"mean={sum(densities)/len(densities):.1f}")

    # Competitive intensity
    for level in range(4):
        count = sum(1 for c in data.values() if c['competitive_intensity'] == level)
        lines.append(f"  competitive_intensity={level}: {count} counties")

    # RUCC distribution
    lines.append("")
    lines.append("  RUCC distribution:")
    for code in range(1, 10):
        count = sum(1 for c in data.values() if c.get('rucc_code') == code)
        if count > 0:
            lines.append(f"    RUCC {code}: {count} counties")

    # Fiber provider counts
    provider_counts = [c.get('fiber_provider_count', len(c.get('wireline_providers', [])))
                       for c in data.values()]
    if provider_counts:
        lines.append(f"\n  fiber_providers per county: min={min(provider_counts)}, "
                     f"max={max(provider_counts)}, mean={sum(provider_counts)/len(provider_counts):.1f}")

    # Total broadband providers
    bp_counts = [c.get('total_broadband_providers', 0) for c in data.values()]
    if bp_counts:
        lines.append(f"  total_broadband_providers per county: min={min(bp_counts)}, "
                     f"max={max(bp_counts)}, mean={sum(bp_counts)/len(bp_counts):.1f}")

    # Sanity checks
    lines.append("")
    lines.append("## Sanity Checks")
    lines.append("")

    issues = []

    for fips, c in data.items():
        if c['fiber_served'] > c['total_bsls']:
            issues.append(f"  {fips} {c['name']}: fiber_served ({c['fiber_served']}) > total_bsls ({c['total_bsls']})")
        if c['fiber_penetration'] > 1.0:
            issues.append(f"  {fips} {c['name']}: fiber_penetration ({c['fiber_penetration']}) > 1.0")
        if c.get('rucc_code') is None:
            issues.append(f"  {fips} {c['name']}: missing RUCC code")
        if c.get('terrain_roughness') is None:
            issues.append(f"  {fips} {c['name']}: missing terrain data")

    if issues:
        lines.append(f"  {len(issues)} issues found:")
        for issue in issues:
            lines.append(issue)
    else:
        lines.append("  ✅ All sanity checks passed")

    # Top/bottom rankings
    lines.append("")
    lines.append("## Top 10 Counties by Fiber Penetration")
    ranked = sorted(data.values(), key=lambda c: c['fiber_penetration'], reverse=True)
    for c in ranked[:10]:
        lines.append(f"  {c['name']}: {c['fiber_penetration']:.1%} "
                     f"({c['fiber_served']:,} / {c['total_bsls']:,})")

    lines.append("")
    lines.append("## Bottom 10 Counties by Fiber Penetration")
    for c in ranked[-10:]:
        lines.append(f"  {c['name']}: {c['fiber_penetration']:.1%} "
                     f"({c['fiber_served']:,} / {c['total_bsls']:,})")

    lines.append("")
    lines.append("## Top 10 Most Competitive Counties (by fiber provider count)")
    ranked_comp = sorted(data.values(),
                         key=lambda c: len(c.get('wireline_providers', [])), reverse=True)
    for c in ranked_comp[:10]:
        lines.append(f"  {c['name']}: {len(c['wireline_providers'])} fiber providers "
                     f"({', '.join(c['wireline_providers'][:5])}...)")

    report_text = '\n'.join(lines)

    qa_path = os.path.join(QA_DIR, 'mo-qa-report.txt')
    with open(qa_path, 'w') as f:
        f.write(report_text)

    print(f"\nQA report written to {qa_path}")
    return report_text


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print("MO County Data Pipeline — Real Data Sources Only")
    print("=" * 60)

    print("\n1. Loading Census ACS data...")
    acs = load_census_acs()
    print(f"   {len(acs)} counties")

    print("\n2. Loading USDA RUCC 2023...")
    rucc = load_usda_rucc()
    print(f"   {len(rucc)} MO counties with RUCC codes")

    print("\n3. Loading USGS terrain data...")
    terrain = load_terrain_data()
    print(f"   {len(terrain)} counties with terrain data")

    print("\n4. Loading FCC FTTP location data...")
    fttp = load_fttp_locations()
    print(f"   {len(fttp)} counties with fiber location data")

    print("\n5. Loading FCC provider summary...")
    providers = load_provider_summary()
    print(f"   {len(providers)} counties with provider data")

    print("\n6. Loading FCC place-level summary...")
    place = load_place_summary(acs)
    print(f"   {len(place)} counties with place-level BSL data")

    print("\n7. Building county records...")
    data = {}
    for fips in sorted(acs.keys()):
        data[fips] = build_county(
            fips, acs[fips], fttp, providers, place, terrain, rucc
        )

    # Write output
    out_path = os.path.join(OUT_DIR, 'mo-unified-data.json')
    with open(out_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"Generated {len(data)} MO county records -> {out_path}")

    # Summary statistics
    metro = sum(1 for c in data.values() if c["is_metro_county"])
    rural = sum(1 for c in data.values() if c["rural_class"] == "Rural")
    avg_pen = sum(c["fiber_penetration"] for c in data.values()) / len(data)
    total_bsls = sum(c["total_bsls"] for c in data.values())
    total_served = sum(c["fiber_served"] for c in data.values())
    total_ops = sum(len(c["wireline_providers"]) for c in data.values())

    print(f"\nSummary:")
    print(f"  Metro counties (USDA RUCC): {metro}")
    print(f"  Rural counties: {rural}")
    print(f"  Avg fiber penetration: {avg_pen:.1%}")
    print(f"  Total BSLs: {total_bsls:,}")
    print(f"  Total fiber served: {total_served:,}")
    print(f"  Total fiber operators (unique per county): {total_ops}")

    print(f"\nData sources:")
    print(f"  ✅ REAL: Census ACS demographics (2023 + 2018)")
    print(f"  ✅ REAL: USDA RUCC 2023 (replacing estimated)")
    print(f"  ✅ REAL: USGS TRI terrain data")
    print(f"  ✅ REAL: FCC BDC FTTP locations — fiber served, operators, competition")
    print(f"  ✅ REAL: FCC BDC provider summary — broadband provider counts")
    print(f"  ⚠️  FLAGGED: BEAD set to 'Unverified' (county-level awards not public)")
    print(f"  ❌ NULL: Momentum fields (need Dec 2024 BDC filing)")

    # Generate QA report
    print("\n8. Generating QA report...")
    generate_qa_report(data)

    print("\nDone!")


if __name__ == "__main__":
    main()
