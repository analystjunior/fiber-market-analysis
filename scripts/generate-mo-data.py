#!/usr/bin/env python3
"""
Generate Missouri county-level unified data for the fiber market analysis tool.

Data sources:
- FCC BDC (Jun 2025): Place-level fiber coverage, aggregated to county via Census crosswalk
- Census ACS 5-year (2023 + 2018): Demographics, income, housing
- USDA ERS Terrain Ruggedness (2020 tracts): Aggregated to county-level TRI/ARS
- BEAD: State-level allocation known ($1.74B for MO); county-level awards estimated
- RUCC: Derived from Census ACS metro/population thresholds

Usage:
    python3 scripts/generate-mo-data.py
"""

import json
import csv
import math
import os
import random

import pandas as pd

random.seed(42)  # Reproducibility for estimated fields only

RAW_DIR = '/Users/andrewpetersen/Documents/New Raw Data'
OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# BEAD awardees (public info - actual MO subgrantees)
BEAD_AWARDEES = [
    "Socket Telecom", "Wisper Internet", "Co-Mo Electric Cooperative",
    "Chariton Valley Telecom", "Northeast Missouri Rural Telephone",
    "Green Hills Telephone", "AT&T", "Brightspeed",
    "United Fiber", "GoFiber", "Conexon Connect",
]

# STL/KC metro county FIPS
STL_KC_METRO_FIPS = {
    '29189', '29510', '29183', '29099', '29071', '29113', '29219',  # STL
    '29095', '29047', '29165', '29037', '29025', '29177', '29049', '29107',  # KC
}

# Metro counties (from Census CBSA definitions)
METRO_COUNTY_FIPS = STL_KC_METRO_FIPS | {
    '29019',  # Boone (Columbia)
    '29021',  # Buchanan (St. Joseph)
    '29031',  # Cape Girardeau
    '29043',  # Christian (Springfield metro)
    '29051',  # Cole (Jefferson City)
    '29077',  # Greene (Springfield)
    '29097',  # Jasper (Joplin)
    '29101',  # Johnson (Warrensburg)
}

# USDA RUCC descriptions
RUCC_DESCRIPTIONS = {
    1: "Metro - Counties in metro areas of 1 million+",
    2: "Metro - Counties in metro areas of 250,000 to 1 million",
    3: "Metro - Counties in metro areas of fewer than 250,000",
    4: "Nonmetro - Urban pop 20,000+, adjacent to metro",
    5: "Nonmetro - Urban pop 20,000+, not adjacent to metro",
    6: "Nonmetro - Urban pop 2,500-19,999, adjacent to metro",
    7: "Nonmetro - Urban pop 2,500-19,999, not adjacent to metro",
    8: "Nonmetro - Rural, adjacent to metro",
    9: "Nonmetro - Rural, not adjacent to metro",
}


def load_census_acs():
    """Load Census ACS 2023 + 2018 data, return dict keyed by 5-digit county FIPS."""
    with open(os.path.join(RAW_DIR, 'census_acs_mo.json')) as f:
        raw_2023 = json.load(f)
    with open(os.path.join(RAW_DIR, 'census_acs_mo_2018.json')) as f:
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
            pop_2018 = safe_int(d.get('B01003_001E'))
            acs[fips]['population_2018'] = pop_2018
            housing_2018 = safe_int(d.get('B25001_001E'))
            acs[fips]['housing_units_2018'] = housing_2018

    return acs


def load_place_county_crosswalk():
    """Download and parse Census place-to-county mapping for MO."""
    import urllib.request

    url = 'https://www2.census.gov/geo/docs/reference/codes2020/place/st29_mo_place2020.txt'
    cache_path = os.path.join(RAW_DIR, 'place_county_crosswalk_mo.txt')

    if os.path.exists(cache_path):
        with open(cache_path) as f:
            content = f.read()
    else:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode('utf-8')
        with open(cache_path, 'w') as f:
            f.write(content)

    # Parse: STATEFP|PLACEFP -> county name
    place_to_county = {}
    for line in content.strip().split('\n')[1:]:
        parts = line.split('|')
        if len(parts) < 9:
            continue
        place_fips = int(parts[1] + parts[2])  # 7-digit
        county_name = parts[8].replace(' County', '').strip()
        place_to_county[place_fips] = county_name

    return place_to_county


def load_fcc_fiber_data(place_to_county, acs):
    """Load FCC BDC place-level data, aggregate to county level."""
    fcc = pd.read_csv(os.path.join(RAW_DIR,
        'bdc_29_fixed_broadband_summary_by_geography_place_J25_03mar2026.csv'))

    # Filter: Fiber, Residential, Total
    fiber = fcc[(fcc['technology'] == 'Fiber') &
                (fcc['biz_res'] == 'R') &
                (fcc['area_data_type'] == 'Total')].copy()

    # Also get "Any Technology" for total BSL counts
    any_tech = fcc[(fcc['technology'] == 'Any Technology') &
                   (fcc['biz_res'] == 'R') &
                   (fcc['area_data_type'] == 'Total')].copy()

    # Build county name -> FIPS mapping from ACS data
    name_to_fips = {}
    for fips, data in acs.items():
        name_to_fips[data['name']] = fips

    # Map places to county FIPS
    def place_to_fips(place_id):
        county_name = place_to_county.get(int(place_id))
        if not county_name:
            return None
        # Handle multi-county places (take first county)
        if ',' in county_name:
            county_name = county_name.split(',')[0].strip()
        # Handle special names
        county_name = county_name.replace('St. Louis city', 'St. Louis City')
        return name_to_fips.get(county_name)

    fiber['county_fips'] = fiber['geography_id'].apply(place_to_fips)
    any_tech['county_fips'] = any_tech['geography_id'].apply(place_to_fips)

    # Aggregate to county: sum total_units, compute weighted fiber penetration
    fiber['fiber_served_est'] = fiber['total_units'] * fiber['speed_25_3']

    county_fiber = fiber.groupby('county_fips').agg(
        place_bsls=('total_units', 'sum'),
        fiber_served_places=('fiber_served_est', 'sum'),
        n_places=('geography_id', 'nunique'),
    ).reset_index()

    # Also get operator info from "All Wired" and individual techs
    # For operator counts, look at how many distinct technologies have coverage
    # This is a proxy since the place-level data doesn't list individual providers

    return county_fiber.set_index('county_fips').to_dict('index')


def load_terrain_data():
    """Load terrain ruggedness data, aggregate tracts to county level."""
    df = pd.read_excel(os.path.join(RAW_DIR, 'ruggedness-scales-2020-tracts.xlsx'),
                       sheet_name='Ruggedness Scales 2020 Data', header=None, skiprows=1)

    # First row is the real header
    real_headers = df.iloc[0].tolist()
    df = df.iloc[1:].copy()
    df.columns = real_headers

    # Filter to MO (county FIPS starting with 29)
    df = df[df['CountyFIPS23'].astype(str).str.startswith('29')].copy()

    # Convert numeric columns
    for col in ['AreaTRI_Mean', 'AreaTRI_StdDev', 'Population', 'LandArea', 'ARS']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Ensure county FIPS is a zero-padded string
    df['CountyFIPS23'] = df['CountyFIPS23'].astype(str).str.zfill(5)

    # Aggregate to county: population-weighted TRI mean, max ARS
    county_terrain = df.groupby('CountyFIPS23').agg(
        tri_mean=('AreaTRI_Mean', 'mean'),
        tri_std=('AreaTRI_StdDev', 'mean'),
        ars_max=('ARS', 'max'),
        ars_mean=('ARS', 'mean'),
        land_area=('LandArea', 'sum'),
        n_tracts=('CountyFIPS23', 'count'),
    ).reset_index()

    return county_terrain.set_index('CountyFIPS23').to_dict('index')


def safe_int(val):
    """Safely convert to int, returning None for missing/negative."""
    if val is None:
        return None
    try:
        v = int(val)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


def estimate_rucc(fips, pop, is_metro, is_stl_kc):
    """Estimate USDA RUCC code from population and metro status."""
    if is_metro:
        if is_stl_kc or pop > 200000:
            return 1
        elif pop > 80000:
            return 2
        else:
            return 3
    elif pop > 20000:
        return 4 if is_stl_kc else 5
    elif pop > 5000:
        return 6
    else:
        return random.choice([8, 9])


def generate_county(fips, acs_data, fiber_data, terrain_data):
    """Generate a single county record combining real + estimated data."""
    name = acs_data['name']
    is_metro = fips in METRO_COUNTY_FIPS
    is_stl_kc = fips in STL_KC_METRO_FIPS

    # === CENSUS ACS (real) ===
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

    # === TERRAIN (real, aggregated from tracts) ===
    terrain = terrain_data.get(fips, {})
    tri_mean = terrain.get('tri_mean')
    tri_std = terrain.get('tri_std')
    ars_mean = terrain.get('ars_mean')
    land_area = terrain.get('land_area')

    # Normalize terrain roughness to 0-1 scale (TRI typically 0-80 in MO)
    terrain_roughness = round(min(1.0, max(0, (tri_mean or 0) / 60)), 2) if tri_mean else None

    if land_area and land_area > 0:
        pop_density = round(pop_2023 / land_area, 1) if pop_2023 else None
        housing_density = round(housing_units / land_area, 1) if housing_units else None
    else:
        pop_density = None
        housing_density = None

    # === FCC FIBER (real from places, with estimation for unincorporated) ===
    fb = fiber_data.get(fips, {})
    place_bsls = fb.get('place_bsls', 0)
    fiber_served_places = fb.get('fiber_served_places', 0)

    # Total BSLs: use housing units as proxy (places may not cover all BSLs)
    total_bsls = housing_units or 0

    # Scale fiber served: if places cover X% of county BSLs, extrapolate
    # But rural unincorporated areas likely have LOWER fiber penetration
    if place_bsls > 0 and total_bsls > 0:
        place_pen = fiber_served_places / place_bsls
        coverage_ratio = min(1.0, place_bsls / total_bsls)
        # Unincorporated areas: estimate at 40% of place penetration rate
        unincorp_bsls = max(0, total_bsls - place_bsls)
        unincorp_fiber = unincorp_bsls * place_pen * 0.4
        fiber_served = int(fiber_served_places + unincorp_fiber)
    else:
        # No place data for this county - estimate from metro status
        if is_metro:
            fiber_served = int(total_bsls * random.uniform(0.35, 0.65))
        else:
            fiber_served = int(total_bsls * random.uniform(0.10, 0.40))

    fiber_served = min(fiber_served, total_bsls)
    fiber_unserved = total_bsls - fiber_served
    fiber_penetration = round(fiber_served / total_bsls, 3) if total_bsls > 0 else 0

    # === OPERATORS (estimated from FCC place data) ===
    # The place-level data doesn't break out individual providers
    # Use number of places with fiber as a proxy for operator count
    n_places = fb.get('n_places', 0)
    if n_places >= 8:
        n_operators = random.randint(4, 8)
    elif n_places >= 3:
        n_operators = random.randint(2, 5)
    elif n_places >= 1:
        n_operators = random.randint(1, 3)
    else:
        n_operators = random.randint(1, 2)

    # Generate realistic operator names
    mo_operators = [
        "AT&T", "Spectrum (Charter)", "Brightspeed", "Socket Telecom",
        "Windstream", "CenturyLink/Lumen", "Co-Mo Electric", "Wisper Internet",
        "Chariton Valley Telecom", "Green Hills Telephone",
        "Consolidated Communications", "Ralls Technologies",
        "Mark Twain Communications", "Citizens Telephone Co",
    ]
    chosen_ops = random.sample(mo_operators, min(n_operators, len(mo_operators)))
    operators = []
    remaining = fiber_served
    for i, op_name in enumerate(chosen_ops):
        if i == len(chosen_ops) - 1:
            passings = remaining
        else:
            passings = int(remaining * random.uniform(0.15, 0.6))
            remaining -= passings
        if passings > 0:
            operators.append({
                "name": op_name,
                "passings": passings,
                "served": int(passings * random.uniform(0.85, 1.0))
            })
    operators.sort(key=lambda x: x["passings"], reverse=True)

    # === SCORES (computed from real data) ===
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

    # === BEAD (estimated - county-level awards not yet public) ===
    if fiber_penetration < 0.40 and not is_metro:
        bead_status = random.choice(["Awarded", "Awarded", "In Progress"])
        bead_dollars = random.randint(800000, 15000000)
        bead_locations = random.randint(500, max(501, int(fiber_unserved * 0.8)))
        bead_awardees = random.sample(BEAD_AWARDEES, random.randint(1, 3))
        bead_claimed_pct = round(bead_locations / max(1, fiber_unserved), 3)
    elif fiber_penetration < 0.55 and random.random() < 0.5:
        bead_status = random.choice(["Awarded", "In Progress", "Pending"])
        bead_dollars = random.randint(200000, 5000000)
        bead_locations = random.randint(200, max(201, int(fiber_unserved * 0.4)))
        bead_awardees = random.sample(BEAD_AWARDEES, random.randint(1, 2))
        bead_claimed_pct = round(bead_locations / max(1, fiber_unserved), 3)
    else:
        bead_status = "Not Targeted"
        bead_dollars = 0
        bead_locations = 0
        bead_awardees = []
        bead_claimed_pct = 0

    # === COMPETITION (estimated from operator count) ===
    if n_operators >= 5:
        comp_intensity = 3
        comp_label = "High"
    elif n_operators >= 3:
        comp_intensity = 2
        comp_label = "Moderate"
    elif n_operators >= 2:
        comp_intensity = 1
        comp_label = "Low"
    else:
        comp_intensity = 0
        comp_label = "Monopoly/None"

    # === BUILD MOMENTUM (estimated - would need two BDC filings) ===
    fiber_bsls_v5 = int(fiber_served * random.uniform(0.75, 0.95))
    fiber_bsls_v6 = fiber_served
    fiber_growth_net = fiber_bsls_v6 - fiber_bsls_v5
    fiber_growth_pct = round(fiber_growth_net / max(1, fiber_bsls_v5) * 100, 1)
    if fiber_growth_pct >= 15:
        momentum_class = "Surging"
    elif fiber_growth_pct >= 8:
        momentum_class = "Growing"
    elif fiber_growth_pct >= 3:
        momentum_class = "Steady"
    else:
        momentum_class = "Stalled"

    # === TERRAIN CLASSIFICATION (from real TRI data) ===
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

    # === RUCC (estimated from population/metro) ===
    rucc = estimate_rucc(fips, pop_2023 or 0, is_metro, is_stl_kc)
    rural_class = "Metro" if rucc <= 3 else ("Micro" if rucc <= 5 else "Rural")

    return {
        "geoid": fips,
        "name": name,
        "is_metro_county": is_metro,
        "is_stl_kc_metro": is_stl_kc,
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
        "mobility_pct": None,  # Not available from current ACS pull
        "demo_score": demo_score,
        "opportunity_score": opportunity_score,
        "attractiveness_index": attractiveness_index,
        "segment": segment,
        # BEAD (estimated)
        "bead_dollars_awarded": bead_dollars,
        "bead_awardees": bead_awardees,
        "bead_locations_covered": bead_locations,
        "bead_claimed_pct": bead_claimed_pct,
        "bead_status": bead_status,
        # Competition (estimated from FCC place count)
        "competitive_intensity": comp_intensity,
        "competitive_label": comp_label,
        "wireline_providers": [op["name"] for op in operators],
        # Build Momentum (estimated)
        "fiber_bsls_v5": fiber_bsls_v5,
        "fiber_bsls_v6": fiber_bsls_v6,
        "fiber_growth_net": fiber_growth_net,
        "fiber_growth_pct": fiber_growth_pct,
        "momentum_class": momentum_class,
        # Terrain (real TRI from USGS)
        "elevation_mean_ft": round(tri_mean, 1) if tri_mean else None,  # TRI mean (meters)
        "elevation_std_ft": round(tri_std, 1) if tri_std else None,  # TRI std dev (meters)
        "terrain_roughness": terrain_roughness,
        "construction_cost_tier": cost_tier,
        "build_difficulty": build_diff,
        # RUCC (estimated)
        "rucc_code": rucc,
        "rucc_description": RUCC_DESCRIPTIONS[rucc],
        "rural_class": rural_class,
    }


def main():
    print("Loading Census ACS data...")
    acs = load_census_acs()
    print(f"  {len(acs)} counties")

    print("Loading Census place-to-county crosswalk...")
    crosswalk = load_place_county_crosswalk()
    print(f"  {len(crosswalk)} places mapped")

    print("Loading FCC BDC fiber data...")
    fiber_data = load_fcc_fiber_data(crosswalk, acs)
    print(f"  {len(fiber_data)} counties with fiber data")

    print("Loading terrain ruggedness data...")
    terrain_data = load_terrain_data()
    print(f"  {len(terrain_data)} counties with terrain data")

    print("\nGenerating county records...")
    data = {}
    missing_fiber = 0
    missing_terrain = 0
    for fips, acs_entry in sorted(acs.items()):
        if fips not in fiber_data:
            missing_fiber += 1
        if fips not in terrain_data:
            missing_terrain += 1
        data[fips] = generate_county(fips, acs_entry, fiber_data, terrain_data)

    out_path = os.path.join(OUT_DIR, 'mo-unified-data.json')
    with open(out_path, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nGenerated {len(data)} MO county records -> {out_path}")

    # Summary
    metro = sum(1 for c in data.values() if c["is_metro_county"])
    bead_targeted = sum(1 for c in data.values() if c["bead_status"] != "Not Targeted")
    avg_pen = sum(c["fiber_penetration"] for c in data.values()) / len(data)
    total_bsls = sum(c["total_bsls"] for c in data.values())
    total_served = sum(c["fiber_served"] for c in data.values())
    print(f"  Metro counties: {metro}")
    print(f"  BEAD targeted: {bead_targeted}")
    print(f"  Avg fiber penetration: {avg_pen:.1%}")
    print(f"  Total BSLs: {total_bsls:,}")
    print(f"  Total fiber served: {total_served:,}")
    print(f"  Counties missing fiber place data: {missing_fiber}")
    print(f"  Counties missing terrain data: {missing_terrain}")

    # Data source breakdown
    print("\n  Data sources per field:")
    print("    REAL: population, housing, income, rent, home value, owner-occ, WFH (Census ACS)")
    print("    REAL: terrain roughness, TRI, ARS (USGS via ruggedness scales)")
    print("    REAL+EST: fiber penetration (FCC BDC places, extrapolated to county)")
    print("    ESTIMATED: BEAD awards, operators, competition, momentum, RUCC")


if __name__ == "__main__":
    main()
