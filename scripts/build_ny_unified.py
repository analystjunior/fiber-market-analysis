#!/usr/bin/env python3
"""
Build NY unified county data from existing NY source files + national reference data.

Sources (already present):
  data/ny-county-fiber.json          -> fiber penetration, operators, BSLs
  data/ny-acs-data.json              -> demographics (ACS)
  data/raw/usda/rucc2023.csv         -> RUCC rural classification
  data/raw/usgs/ruggedness-scales-2020-tracts.xlsx -> terrain
  data/raw/fcc/jun2025/provider_summary_by_geography.csv -> broadband provider counts

Output:
  data/ny-unified-data.json (replaces old format, matches MO schema)

Run:
  python3 scripts/build_ny_unified.py
"""

import json
import csv
import os
import math
import sys

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, '..')
RAW_DIR = os.path.join(PROJECT_DIR, 'data', 'raw')
DATA_DIR = os.path.join(PROJECT_DIR, 'data')

FIPS_PREFIX = '36'
STATE = 'NY'

NYC_BOROUGHS = {'36005', '36047', '36061', '36081', '36085'}

# ── Loaders ──────────────────────────────────────────────────────────────────

def load_fiber():
    path = os.path.join(DATA_DIR, 'ny-county-fiber.json')
    with open(path) as f:
        raw = json.load(f)
    out = {}
    for fips_key, d in raw.items():
        fips = str(fips_key).zfill(5)
        ops = []
        for op in (d.get('operators') or []):
            if isinstance(op, dict):
                ops.append({'name': op.get('name', ''), 'passings': op.get('passings', 0)})
        out[fips] = {
            'total_bsls':        d.get('totalBSLs'),
            'fiber_served':      d.get('fiberServed'),
            'fiber_unserved':    (d.get('totalBSLs') or 0) - (d.get('fiberServed') or 0),
            'fiber_penetration': d.get('fiberPenetration'),
            'operators':         ops,
        }
    print(f"  Fiber: {len(out)} counties")
    return out


def load_acs():
    path = os.path.join(DATA_DIR, 'ny-acs-data.json')
    with open(path) as f:
        raw = json.load(f)
    out = {}
    for fips_key, d in raw.items():
        fips = str(fips_key).zfill(5)
        out[fips] = {
            'name':              d.get('name', '').replace(' County', '').strip(),
            'population_2023':   d.get('population_2023'),
            'population_2018':   d.get('population_2018'),
            'housing_units':     d.get('housing_units_2023'),
            'housing_units_2018':d.get('housing_units_2018'),
            'median_hhi':        d.get('median_hhi'),
            'median_rent':       d.get('median_rent'),
            'median_home_value': d.get('median_home_value'),
            'owner_occupied_pct':d.get('owner_occupied_pct'),
            'wfh_pct':           d.get('wfh_pct'),
        }
    print(f"  ACS: {len(out)} counties")
    return out


def load_rucc():
    rucc = {}
    path = os.path.join(RAW_DIR, 'usda', 'rucc2023.csv')
    with open(path, newline='', encoding='latin-1') as f:
        for row in csv.DictReader(f):
            fips = str(row['FIPS']).zfill(5)
            if not fips.startswith(FIPS_PREFIX):
                continue
            attr, val = row['Attribute'], row['Value']
            if fips not in rucc:
                rucc[fips] = {}
            if attr == 'RUCC_2023':
                rucc[fips]['rucc_code'] = int(val)
            elif attr == 'Description':
                rucc[fips]['rucc_description'] = val
    for fips, d in rucc.items():
        code = d.get('rucc_code', 9)
        if code <= 3:
            d['rural_class'] = 'Metro'
            d['is_metro_county'] = True
        elif code <= 5:
            d['rural_class'] = 'Micro'
            d['is_metro_county'] = False
        else:
            d['rural_class'] = 'Rural'
            d['is_metro_county'] = False
    print(f"  RUCC: {len(rucc)} NY counties")
    return rucc


def load_terrain():
    xlsx = os.path.join(RAW_DIR, 'usgs', 'ruggedness-scales-2020-tracts.xlsx')
    if not os.path.exists(xlsx):
        print("  WARNING: USGS terrain file not found — terrain null")
        return {}
    df = pd.read_excel(xlsx, sheet_name='Ruggedness Scales 2020 Data', header=None, skiprows=1)
    real_headers = df.iloc[0].tolist()
    df = df.iloc[1:].copy()
    df.columns = real_headers
    df = df[df['CountyFIPS23'].astype(str).str.startswith(FIPS_PREFIX)].copy()
    for col in ['AreaTRI_Mean', 'AreaTRI_StdDev', 'Population', 'LandArea', 'ARS']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['CountyFIPS23'] = df['CountyFIPS23'].astype(str).str.zfill(5)
    agg = df.groupby('CountyFIPS23').agg(
        tri_mean=('AreaTRI_Mean', 'mean'),
        tri_std=('AreaTRI_StdDev', 'mean'),
        ars_max=('ARS', 'max'),
        land_area=('LandArea', 'sum'),
    ).reset_index()

    terrain = {}
    for _, row in agg.iterrows():
        fips = str(row['CountyFIPS23']).zfill(5)
        tri = row['tri_mean']
        if pd.isna(tri):
            continue
        ars = row.get('ars_max', 1)
        if pd.isna(ars):
            ars = 1

        # Roughness 0-1 scale
        roughness = min(1.0, tri / 150.0)

        if ars <= 1:
            cost_tier, build_diff = 'Low', 'Easy'
        elif ars <= 2:
            cost_tier, build_diff = 'Medium', 'Moderate'
        elif ars <= 3:
            cost_tier, build_diff = 'High', 'Difficult'
        else:
            cost_tier, build_diff = 'Very High', 'Very Difficult'

        terrain[fips] = {
            'elevation_mean_ft': round(float(tri) * 3.28084, 1),
            'elevation_std_ft':  round(float(row['tri_std']) * 3.28084, 1) if not pd.isna(row['tri_std']) else None,
            'terrain_roughness': round(roughness, 4),
            'construction_cost_tier': cost_tier,
            'build_difficulty':  build_diff,
        }
    print(f"  Terrain: {len(terrain)} NY counties")
    return terrain


def load_provider_counts():
    path = os.path.join(RAW_DIR, 'fcc', 'jun2025', 'provider_summary_by_geography.csv')
    if not os.path.exists(path):
        print("  WARNING: National provider summary not found — provider counts null")
        return {}
    counts = {}
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        for row in csv.DictReader(f):
            if row.get('geography_type') != 'County':
                continue
            geo_id = str(row.get('geography_id', '')).zfill(5)
            if not geo_id.startswith(FIPS_PREFIX):
                continue
            if geo_id not in counts:
                counts[geo_id] = set()
            counts[geo_id].add(row.get('provider_id', ''))
    result = {fips: len(pids) for fips, pids in counts.items()}
    print(f"  Provider counts: {len(result)} NY counties")
    return result


# ── Score helpers ─────────────────────────────────────────────────────────────

def clamp(v, lo=0.0, hi=1.0):
    if v is None or not math.isfinite(v):
        return 0.0
    return max(lo, min(hi, v))


def score_demo(d):
    hhi       = clamp((d.get('median_hhi') or 0) / 120000)
    density   = clamp(math.log1p(d.get('housing_density') or 0) / math.log1p(3000))
    pop_g     = clamp(((d.get('pop_growth_pct') or 0) + 5) / 15)
    wfh       = clamp((d.get('wfh_pct') or 0) / 20)
    own       = clamp((d.get('owner_occupied_pct') or 0) / 80)
    return round(hhi * 0.30 + density * 0.25 + pop_g * 0.20 + wfh * 0.15 + own * 0.10, 4)


def score_opportunity(fiber_pen):
    if fiber_pen is None:
        return 0.5
    return round(1.0 - clamp(fiber_pen), 4)


def segment(attr):
    if attr >= 0.67:
        return 'Most Attractive'
    elif attr >= 0.34:
        return 'Neutral'
    return 'Least Attractive'


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Building NY unified data...")

    print("\nLoading source files:")
    fiber    = load_fiber()
    acs      = load_acs()
    rucc     = load_rucc()
    terrain  = load_terrain()
    provider = load_provider_counts()

    all_fips = set(acs.keys())
    print(f"\nBuilding {len(all_fips)} county records...")

    out = {}
    for fips in sorted(all_fips):
        a = acs.get(fips, {})
        fi = fiber.get(fips, {})
        r = rucc.get(fips, {})
        t = terrain.get(fips, {})

        pop23 = a.get('population_2023')
        pop18 = a.get('population_2018')
        hu23  = a.get('housing_units')
        hu18  = a.get('housing_units_2018')
        area  = None  # not readily available from ACS JSON; will be null

        pop_growth = round((pop23 - pop18) / pop18 * 100, 2) if pop23 and pop18 else None
        hu_growth  = round((hu23  - hu18)  / hu18  * 100, 2) if hu23  and hu18  else None

        # Housing density: need land area — approximate from RUCC pop if area unavailable
        housing_density = None  # will be null without area

        d = {
            'geoid':         fips,
            'name':          a.get('name', fips),
            'is_nyc_borough': fips in NYC_BOROUGHS,
            'is_metro_county': r.get('is_metro_county'),

            # Fiber
            'total_bsls':       fi.get('total_bsls'),
            'fiber_served':     fi.get('fiber_served'),
            'fiber_unserved':   fi.get('fiber_unserved'),
            'fiber_penetration':fi.get('fiber_penetration'),
            'operators':        fi.get('operators', []),

            # Demographics
            'population_2023':    pop23,
            'population_2018':    pop18,
            'pop_growth_pct':     pop_growth,
            'housing_units':      hu23,
            'housing_growth_pct': hu_growth,
            'land_area_sqmi':     area,
            'pop_density':        None,
            'housing_density':    housing_density,
            'median_hhi':         a.get('median_hhi'),
            'median_rent':        a.get('median_rent'),
            'median_home_value':  a.get('median_home_value'),
            'owner_occupied_pct': a.get('owner_occupied_pct'),
            'wfh_pct':            a.get('wfh_pct'),

            # BEAD (not available at county level)
            'bead_status':           'Unverified',
            'bead_dollars_awarded':  None,
            'bead_awardees':         None,
            'bead_locations_covered':None,
            'bead_claimed_pct':      None,

            # Competition
            'competitive_intensity':    None,
            'competitive_label':        None,
            'wireline_providers':       None,
            'total_broadband_providers':provider.get(fips),

            # Cable / FWA (need FCC place summary — download separately)
            'cable_coverage_pct':    None,
            'fwa_coverage_pct':      None,
            'broadband_coverage_pct':None,
            'broadband_gap_pct':     None,
            'cable_present':         None,
            'fwa_present':           None,

            # Momentum (needs two BDC periods)
            'fiber_bsls_v5':    None,
            'fiber_bsls_v6':    None,
            'fiber_growth_net': None,
            'fiber_growth_pct': None,
            'momentum_class':   None,

            # Terrain
            'elevation_mean_ft':      t.get('elevation_mean_ft'),
            'elevation_std_ft':       t.get('elevation_std_ft'),
            'terrain_roughness':      t.get('terrain_roughness'),
            'construction_cost_tier': t.get('construction_cost_tier'),
            'build_difficulty':       t.get('build_difficulty'),

            # RUCC
            'rucc_code':        r.get('rucc_code'),
            'rucc_description': r.get('rucc_description'),
            'rural_class':      r.get('rural_class'),
        }

        # Derived scores
        d['demo_score']         = score_demo(d)
        d['opportunity_score']  = score_opportunity(d.get('fiber_penetration'))
        d['attractiveness_index'] = round(d['demo_score'] * 0.5 + d['opportunity_score'] * 0.5, 4)
        d['segment']            = segment(d['attractiveness_index'])

        out[fips] = d

    out_path = os.path.join(DATA_DIR, 'ny-unified-data.json')
    with open(out_path, 'w') as f:
        json.dump(out, f, indent=2)

    print(f"\nSaved {len(out)} counties -> {out_path}")
    print(f"File size: {os.path.getsize(out_path) // 1024}KB")

    # QA summary
    has_fiber    = sum(1 for c in out.values() if c.get('fiber_penetration') is not None)
    has_terrain  = sum(1 for c in out.values() if c.get('terrain_roughness') is not None)
    has_rucc     = sum(1 for c in out.values() if c.get('rucc_code') is not None)
    has_providers= sum(1 for c in out.values() if c.get('total_broadband_providers') is not None)
    print(f"\nQA Summary ({len(out)} counties total):")
    print(f"  Fiber penetration:  {has_fiber}/{len(out)}")
    print(f"  RUCC classification:{has_rucc}/{len(out)}")
    print(f"  Terrain data:       {has_terrain}/{len(out)}")
    print(f"  Provider counts:    {has_providers}/{len(out)}")
    print(f"\nMissing (requires FCC BDC download):")
    print(f"  Cable/FWA coverage: download broadband_summary_place_ny.csv from broadbandmap.fcc.gov")
    print(f"  Fiber operators (county-level): already loaded from ny-county-fiber.json ✓")


if __name__ == '__main__':
    main()
