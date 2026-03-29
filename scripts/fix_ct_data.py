#!/usr/bin/env python3
"""
Fix Connecticut unified data.

CT's 2022 restructuring replaced 8 counties with 9 planning regions.
The FCC BDC data still uses old county block GEOIDs (09001-09015).
The us-counties.json TopoJSON also uses old county FIPS.

This script rebuilds ct-unified-data.json using old county FIPS:
  - FCC FTTP locations aggregated by block_geoid[:5]
  - Census ACS 2021 5-year (last available for old CT counties)
"""

import csv
import json
import urllib.request
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"

CT_COUNTIES = {
    "09001": "Fairfield",
    "09003": "Hartford",
    "09005": "Litchfield",
    "09007": "Middlesex",
    "09009": "New Haven",
    "09011": "New London",
    "09013": "Tolland",
    "09015": "Windham",
}

def get_census_acs():
    url = ("https://api.census.gov/data/2021/acs/acs5"
           "?get=NAME,B25001_001E,B01003_001E,B19013_001E,"
           "B25003_001E,B25003_002E"
           "&for=county:*&in=state:09")
    print("Fetching Census ACS 2021 for CT old counties...")
    with urllib.request.urlopen(url, timeout=30) as r:
        data = json.loads(r.read())

    headers = data[0]
    result = {}
    for row in data[1:]:
        d = dict(zip(headers, row))
        county_fips = "09" + d["county"]
        result[county_fips] = {
            "total_units": int(d["B25001_001E"]),
            "population": int(d["B01003_001E"]),
            "median_income": int(d["B19013_001E"]) if d["B19013_001E"] != "-666666666" else None,
            "owner_occupied": int(d["B25003_002E"]),
            "total_occupied": int(d["B25003_001E"]),
        }
    return result

def aggregate_ftp_by_county():
    fttp_file = RAW_DIR / "fcc" / "jun2025" / "fttp_locations_ct.csv"
    print(f"Aggregating FTTP locations from {fttp_file.name}...")

    county_locs = defaultdict(set)   # county_fips → set of location_ids
    county_providers = defaultdict(set)

    with open(fttp_file, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            county_fips = row["block_geoid"][:5]
            if county_fips in CT_COUNTIES:
                county_locs[county_fips].add(row["location_id"])
                county_providers[county_fips].add(row["brand_name"])

    return {
        fips: {
            "fiber_served": len(locs),
            "providers": sorted(county_providers[fips]),
        }
        for fips, locs in county_locs.items()
    }

def aggregate_bsls_by_county():
    """Get total BSLs per county from the FTTP file (all unique location_ids)."""
    # The FTTP file only contains fiber-capable locations.
    # For total BSLs we need to get them from the broader broadband summary.
    # Since we don't have a county-level BSL file for CT, we'll use the
    # planning region totals (which sum to state total) redistributed by
    # housing units, OR we can use housing units from ACS as proxy.
    #
    # Actually: the total_bsls value is best derived from FCC's national
    # location fabric. As a reasonable proxy we use ACS housing units * 1.15
    # to account for commercial/vacant units being in the fabric.
    # (MO: 2.824M BSLs vs 2.8M housing units → ratio ≈ 1.009)
    return {}  # Use ACS housing units directly

def main():
    acs = get_census_acs()
    fttp = aggregate_ftp_by_county()

    # Use planning region totals to calibrate total BSLs
    # CT planning regions have: total_bsls = 1,536,049 total
    # ACS 2021 housing units total = sum across counties
    acs_total_units = sum(v["total_units"] for v in acs.values())
    planning_region_bsls = 1_536_049  # from existing ct-unified-data.json

    output = {}
    for fips, name in sorted(CT_COUNTIES.items()):
        county_acs = acs.get(fips, {})
        county_fttp = fttp.get(fips, {"fiber_served": 0, "providers": []})

        # Scale ACS housing units to match FCC BSL total
        acs_units = county_acs.get("total_units", 0)
        bsl_scale = planning_region_bsls / acs_total_units if acs_total_units else 1.0
        total_bsls = round(acs_units * bsl_scale)

        fiber_served = county_fttp["fiber_served"]
        fiber_penetration = round(fiber_served / total_bsls, 3) if total_bsls > 0 else 0.0

        operators = [
            {"name": p, "passings": None, "served": None}
            for p in county_fttp["providers"][:10]
        ]

        output[fips] = {
            "geoid": fips,
            "name": name,
            "is_metro_county": fips in {"09001", "09003", "09009"},
            "total_bsls": total_bsls,
            "fiber_served": fiber_served,
            "fiber_unserved": max(0, total_bsls - fiber_served),
            "fiber_penetration": fiber_penetration,
            "operators": operators,
            "total_broadband_providers": len(county_fttp["providers"]),
            "cable_coverage_pct": None,
            "fwa_coverage_pct": None,
            "broadband_coverage_pct": None,
            "broadband_gap_pct": None,
            "cable_present": None,
            "fwa_present": None,
            "population": county_acs.get("population"),
            "median_income": county_acs.get("median_income"),
            "owner_occupied_pct": (
                round(county_acs["owner_occupied"] / county_acs["total_occupied"] * 100, 1)
                if county_acs.get("total_occupied") else None
            ),
            "rucc_code": None,
            "rural_class": None,
            "terrain_ruggedness": None,
            "bead_eligible_locations": None,
            "bead_funding": None,
            "bead_status": "Unverified",
            "fiber_momentum": None,
        }

        print(f"  {fips} {name}: {fiber_served:,} fiber / {total_bsls:,} BSLs "
              f"= {fiber_penetration*100:.1f}%  ({len(county_fttp['providers'])} providers)")

    out_path = DATA_DIR / "ct-unified-data.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    total_fiber = sum(v["fiber_served"] for v in output.values())
    total_bsls = sum(v["total_bsls"] for v in output.values())
    print(f"\nWrote {out_path.name}: {len(output)} counties, "
          f"{total_fiber:,}/{total_bsls:,} BSLs = {total_fiber/total_bsls*100:.1f}% fiber")

if __name__ == "__main__":
    main()
