#!/usr/bin/env python3
"""
Generate place-county crosswalk files for each state using:
  - Census Gazetteer (place centroids)
  - Census Geocoder REST API (reverse geocode centroid → county)

Output: data/raw/census/place_county_crosswalk_{state}.txt
Format matches existing MO crosswalk (pipe-delimited).

Usage:
    python3 scripts/generate_crosswalks.py
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, '..')
CENSUS_DIR = os.path.join(PROJECT_DIR, 'data', 'raw', 'census')

# States to generate crosswalks for (abbrev → state FIPS)
STATES = {
    'ny': '36',
    'tx': '48',
    'nc': '37',
    'ga': '13',
    'pa': '42',
}

GEOCODER_URL = (
    'https://geocoding.geo.census.gov/geocoder/geographies/coordinates'
    '?x={lon}&y={lat}&benchmark=Public_AR_Current&vintage=Current_Current'
    '&layers=Counties&format=json'
)

GAZ_URL = (
    'https://www2.census.gov/geo/docs/maps-data/data/gazetteer/'
    '2023_Gazetteer/2023_gaz_place_{fips}.txt'
)

COUNTY_API_URL = (
    'https://api.census.gov/data/2023/acs/acs5'
    '?get=NAME&for=county:*&in=state:{fips}'
)


def fetch_gazetteer(state_fips):
    url = GAZ_URL.format(fips=state_fips)
    print(f'    Fetching gazetteer from {url}')
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            text = r.read().decode('utf-8')
        places = []
        lines = text.strip().split('\n')
        for line in lines[1:]:
            parts = line.strip().split('\t')
            if len(parts) < 11:
                continue
            geoid = parts[1].strip().zfill(7)
            name = parts[3].strip()
            try:
                lat = float(parts[10].strip())
                lon = float(parts[11].strip())
            except (ValueError, IndexError):
                continue
            places.append({'geoid': geoid, 'name': name, 'lat': lat, 'lon': lon})
        print(f'    {len(places)} places in gazetteer')
        return places
    except Exception as e:
        print(f'    ERROR fetching gazetteer: {e}')
        return []


def geocode_place(place):
    """Returns (geoid, county_fips, county_name) or None on failure."""
    url = GEOCODER_URL.format(lat=place['lat'], lon=place['lon'])
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read().decode('utf-8'))
        result = data.get('result', {})
        geographies = result.get('geographies', {})
        counties = geographies.get('Counties', [])
        if counties:
            c = counties[0]
            county_fips = c.get('STATE', '') + c.get('COUNTY', '')
            county_name = c.get('NAME', '')
            return (place['geoid'], county_fips, county_name)
    except Exception:
        pass
    return None


def fetch_county_names(state_fips):
    """Returns dict: county_fips → county name (without ' County')."""
    url = COUNTY_API_URL.format(fips=state_fips)
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data = json.loads(r.read().decode('utf-8'))
        result = {}
        headers = data[0]
        for row in data[1:]:
            d = dict(zip(headers, row))
            fips = d['state'] + d['county']
            name = d['NAME'].replace(' County', '').split(',')[0].strip()
            result[fips] = name
        return result
    except Exception as e:
        print(f'    WARNING: Could not fetch county names: {e}')
        return {}


def generate_crosswalk(abbrev, state_fips):
    out_path = os.path.join(CENSUS_DIR, f'place_county_crosswalk_{abbrev}.txt')
    if os.path.exists(out_path):
        print(f'  {abbrev.upper()}: Crosswalk already exists — skipping')
        return True

    print(f'\n  {abbrev.upper()} (FIPS {state_fips}):')

    places = fetch_gazetteer(state_fips)
    if not places:
        return False

    county_names = fetch_county_names(state_fips)
    print(f'    {len(county_names)} counties from ACS')
    print(f'    Geocoding {len(places)} place centroids (concurrent)...')

    place_to_county = {}
    failed = 0

    # Batch geocode with thread pool
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(geocode_place, p): p for p in places}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f'      {done}/{len(places)} geocoded...')
            result = future.result()
            if result:
                geoid, county_fips, _ = result
                place_to_county[geoid] = county_fips
            else:
                failed += 1

    print(f'    Mapped {len(place_to_county)} places ({failed} failed)')

    # Write crosswalk file in pipe-delimited format
    state_abbrev = abbrev.upper()
    with open(out_path, 'w') as f:
        f.write('STATE|STATEFP|PLACEFP|PLACENS|PLACENAME|TYPE|CLASSFP|FUNCSTAT|COUNTIES\n')
        for place in sorted(places, key=lambda p: p['geoid']):
            geoid = place['geoid']
            county_fips = place_to_county.get(geoid)
            if not county_fips:
                continue
            county_name = county_names.get(county_fips, '')
            if not county_name:
                continue
            place_fips = geoid[2:]  # strip state prefix → 5-digit place code
            county_display = county_name + ' County'
            f.write(
                f'{state_abbrev}|{state_fips}|{place_fips}|'
                f'00000000|{place["name"]}|PLACE|C1|A|{county_display}\n'
            )

    lines = sum(1 for _ in open(out_path)) - 1  # minus header
    print(f'    Wrote {lines} place-county mappings → {out_path}')
    return True


def main():
    print('Generating place-county crosswalk files...')
    os.makedirs(CENSUS_DIR, exist_ok=True)

    for abbrev, fips in STATES.items():
        generate_crosswalk(abbrev, fips)
        time.sleep(1)  # brief pause between states

    print('\nDone.')


if __name__ == '__main__':
    main()
