#!/usr/bin/env python3
"""
Aggregate county-level unified data into state-level fiber-data.json.
Updates totalHousingUnits, totalFiberPassings, and fiberPenetration
with real FCC BDC numbers. Preserves existing operator lists.
"""

import json
import os
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
FIBER_DATA_PATH = DATA_DIR / "fiber-data.json"

STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}

def aggregate_state(state_code):
    filename = DATA_DIR / f"{state_code.lower()}-unified-data.json"
    if not filename.exists():
        print(f"  WARNING: {filename.name} not found, skipping")
        return None

    with open(filename) as f:
        counties = json.load(f)

    total_bsls = 0
    fiber_served = 0
    county_count = 0

    for fips, county in counties.items():
        bsls = county.get("total_bsls") or 0
        served = county.get("fiber_served") or 0
        total_bsls += bsls
        fiber_served += served
        county_count += 1

    if total_bsls == 0:
        pct = 0.0
    else:
        pct = round(fiber_served / total_bsls * 100, 1)

    return {
        "total_bsls": total_bsls,
        "fiber_served": fiber_served,
        "fiberPenetration": pct,
        "county_count": county_count,
    }

def main():
    with open(FIBER_DATA_PATH) as f:
        fiber_data = json.load(f)

    print(f"Updating fiber-data.json with real FCC BDC aggregates\n")

    results = []
    for state_code in sorted(STATE_NAMES.keys()):
        agg = aggregate_state(state_code)
        if agg is None:
            continue

        old_pen = fiber_data.get(state_code, {}).get("fiberPenetration", "N/A")
        new_pen = agg["fiberPenetration"]

        # Update the entry
        if state_code not in fiber_data:
            fiber_data[state_code] = {"state": STATE_NAMES[state_code], "operators": []}

        fiber_data[state_code]["state"] = STATE_NAMES[state_code]
        fiber_data[state_code]["totalHousingUnits"] = agg["total_bsls"]
        fiber_data[state_code]["totalFiberPassings"] = agg["fiber_served"]
        fiber_data[state_code]["fiberPenetration"] = new_pen

        results.append((state_code, STATE_NAMES[state_code], agg["county_count"],
                        agg["total_bsls"], agg["fiber_served"], old_pen, new_pen))

        print(f"  {state_code}: {old_pen}% → {new_pen}%  "
              f"({agg['fiber_served']:,} / {agg['total_bsls']:,} BSLs, "
              f"{agg['county_count']} counties)")

    # Write updated file
    with open(FIBER_DATA_PATH, "w") as f:
        json.dump(fiber_data, f, indent=2)

    print(f"\nDone. Updated {len(results)} states in {FIBER_DATA_PATH.name}")

    # Highlight states with 0% or suspiciously low
    low = [(s, n, p) for s, n, c, tb, fs, op, p in results if p < 5]
    if low:
        print(f"\nStates with <5% fiber penetration (possible data issues):")
        for sc, name, pct in low:
            print(f"  {sc} ({name}): {pct}%")

if __name__ == "__main__":
    main()
