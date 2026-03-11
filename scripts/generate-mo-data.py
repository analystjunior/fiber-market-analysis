#!/usr/bin/env python3
"""
Generate Missouri county-level unified data for the fiber market analysis tool.

Data sources (for production refresh):
- FCC BDC (Dec 2024): fiber_served, fiber_unserved, total_bsls, operators
- NTIA BEAD: bead_dollars_awarded, bead_awardees, bead_locations_covered, bead_status
- Census ACS 5-year (2023): population, housing, income, rent, home value, WFH
- USGS 3DEP: elevation_mean_ft, elevation_std_ft, terrain_roughness
- USDA RUCC (2023): rucc_code, rucc_description, rural_class

This script generates realistic synthetic data based on actual MO county
characteristics. Replace with real API calls for production use.
"""

import json
import random
import math
import os

random.seed(42)  # Reproducibility

# All 115 Missouri counties + St. Louis City (independent city)
# Format: (FIPS, name, is_metro, approx_pop, approx_hhi, is_stl_kc_metro)
MO_COUNTIES = [
    ("29001", "Adair", False, 25300, 42000, False),
    ("29003", "Andrew", False, 17600, 58000, False),
    ("29005", "Atchison", False, 5200, 46000, False),
    ("29007", "Audrain", False, 25500, 49000, False),
    ("29009", "Barry", False, 36200, 42000, False),
    ("29011", "Barton", False, 11800, 40000, False),
    ("29013", "Bates", False, 16300, 44000, False),
    ("29015", "Benton", False, 19700, 40000, False),
    ("29017", "Bollinger", False, 12200, 39000, False),
    ("29019", "Boone", True, 183400, 58000, False),
    ("29021", "Buchanan", True, 87300, 48000, False),
    ("29023", "Butler", False, 42500, 38000, False),
    ("29025", "Caldwell", False, 9200, 48000, True),
    ("29027", "Callaway", False, 44800, 52000, False),
    ("29029", "Camden", False, 46400, 48000, False),
    ("29031", "Cape Girardeau", True, 78900, 52000, False),
    ("29033", "Carroll", False, 8600, 44000, False),
    ("29035", "Carter", False, 6200, 32000, False),
    ("29037", "Cass", True, 106400, 72000, True),
    ("29039", "Cedar", False, 14200, 38000, False),
    ("29041", "Chariton", False, 7400, 42000, False),
    ("29043", "Christian", True, 88600, 62000, False),
    ("29045", "Clark", False, 6700, 44000, False),
    ("29047", "Clay", True, 246200, 72000, True),
    ("29049", "Clinton", False, 20400, 56000, True),
    ("29051", "Cole", True, 77000, 58000, False),
    ("29053", "Cooper", False, 17600, 48000, False),
    ("29055", "Crawford", False, 24200, 42000, False),
    ("29057", "Dade", False, 7600, 38000, False),
    ("29059", "Dallas", False, 16700, 38000, False),
    ("29061", "Daviess", False, 8300, 44000, False),
    ("29063", "DeKalb", False, 12400, 46000, False),
    ("29065", "Dent", False, 15500, 38000, False),
    ("29067", "Douglas", False, 13500, 34000, False),
    ("29069", "Dunklin", False, 29400, 34000, False),
    ("29071", "Franklin", True, 104100, 62000, True),
    ("29073", "Gasconade", False, 14800, 48000, False),
    ("29075", "Gentry", False, 6600, 42000, False),
    ("29077", "Greene", True, 293500, 48000, False),
    ("29079", "Grundy", False, 9800, 40000, False),
    ("29081", "Harrison", False, 8300, 42000, False),
    ("29083", "Henry", False, 21600, 40000, False),
    ("29085", "Hickory", False, 9600, 36000, False),
    ("29087", "Holt", False, 4400, 46000, False),
    ("29089", "Howard", False, 10100, 46000, False),
    ("29091", "Howell", False, 40200, 38000, False),
    ("29093", "Iron", False, 10100, 36000, False),
    ("29095", "Jackson", True, 703200, 56000, True),
    ("29097", "Jasper", True, 121700, 46000, False),
    ("29099", "Jefferson", True, 225300, 64000, True),
    ("29101", "Johnson", True, 54000, 52000, False),
    ("29103", "Knox", False, 3900, 42000, False),
    ("29105", "Laclede", False, 36500, 42000, False),
    ("29107", "Lafayette", False, 32700, 54000, True),
    ("29109", "Lawrence", False, 38300, 44000, False),
    ("29111", "Lewis", False, 9900, 44000, False),
    ("29113", "Lincoln", True, 59200, 62000, True),
    ("29115", "Linn", False, 12200, 40000, False),
    ("29117", "Livingston", False, 15300, 42000, False),
    ("29119", "McDonald", False, 22900, 38000, False),
    ("29121", "Macon", False, 15200, 40000, False),
    ("29123", "Madison", False, 12400, 38000, False),
    ("29125", "Maries", False, 8900, 44000, False),
    ("29127", "Marion", False, 28600, 46000, False),
    ("29129", "Mercer", False, 3600, 40000, False),
    ("29131", "Miller", False, 25300, 44000, False),
    ("29133", "Mississippi", False, 13200, 34000, False),
    ("29135", "Moniteau", False, 16200, 48000, False),
    ("29137", "Monroe", False, 8600, 44000, False),
    ("29139", "Montgomery", False, 11600, 46000, False),
    ("29141", "Morgan", False, 20500, 42000, False),
    ("29143", "New Madrid", False, 17200, 36000, False),
    ("29145", "Newton", False, 58600, 50000, False),
    ("29147", "Nodaway", False, 22500, 44000, False),
    ("29149", "Oregon", False, 10500, 32000, False),
    ("29151", "Osage", False, 13700, 52000, False),
    ("29153", "Ozark", False, 9200, 30000, False),
    ("29155", "Pemiscot", False, 16000, 30000, False),
    ("29157", "Perry", False, 19200, 52000, False),
    ("29159", "Pettis", False, 42200, 46000, False),
    ("29161", "Phelps", False, 44400, 46000, False),
    ("29163", "Pike", False, 18200, 46000, False),
    ("29165", "Platte", True, 104500, 78000, True),
    ("29167", "Polk", False, 32200, 42000, False),
    ("29169", "Pulaski", False, 52700, 46000, False),
    ("29171", "Putnam", False, 4700, 38000, False),
    ("29173", "Ralls", False, 10200, 52000, False),
    ("29175", "Randolph", False, 24800, 42000, False),
    ("29177", "Ray", False, 23000, 56000, True),
    ("29179", "Reynolds", False, 6300, 32000, False),
    ("29181", "Ripley", False, 13800, 32000, False),
    ("29183", "St. Charles", True, 405700, 82000, True),
    ("29185", "St. Clair", False, 9300, 36000, False),
    ("29186", "Ste. Genevieve", False, 17900, 52000, False),
    ("29187", "St. Francois", False, 67200, 44000, False),
    ("29189", "St. Louis", True, 1004700, 62000, True),
    ("29195", "Saline", False, 22700, 44000, False),
    ("29197", "Schuyler", False, 4500, 38000, False),
    ("29199", "Scotland", False, 4900, 40000, False),
    ("29201", "Scott", False, 38800, 42000, False),
    ("29203", "Shannon", False, 8200, 30000, False),
    ("29205", "Shelby", False, 6000, 42000, False),
    ("29207", "Stoddard", False, 29400, 38000, False),
    ("29209", "Stone", False, 32200, 42000, False),
    ("29211", "Sullivan", False, 6200, 38000, False),
    ("29213", "Taney", False, 56300, 44000, False),
    ("29215", "Texas", False, 25500, 36000, False),
    ("29217", "Vernon", False, 20500, 38000, False),
    ("29219", "Warren", True, 35800, 62000, True),
    ("29221", "Washington", False, 24900, 42000, False),
    ("29223", "Wayne", False, 13100, 34000, False),
    ("29225", "Webster", False, 39800, 48000, False),
    ("29227", "Worth", False, 2000, 40000, False),
    ("29229", "Wright", False, 18400, 36000, False),
    ("29510", "St. Louis City", True, 293300, 46000, True),
]

# Major MO fiber operators
MO_OPERATORS = [
    "AT&T", "Spectrum (Charter)", "Brightspeed", "Socket Telecom",
    "Windstream", "CenturyLink/Lumen", "Co-Mo Electric", "Wisper Internet",
    "Chariton Valley Telecom", "Green Hills Telephone", "Northeast Missouri Rural Telephone",
    "Consolidated Communications", "GVTC", "Ralls Technologies", "MoKan Dial",
    "Steelville Telephone Exchange", "Kingdom Telephone", "Farber Telephone",
    "Mark Twain Communications", "Citizens Telephone Co of Higginsville",
]

# BEAD awardees in MO
BEAD_AWARDEES = [
    "Socket Telecom", "Wisper Internet", "Co-Mo Electric Cooperative",
    "Chariton Valley Telecom", "Northeast Missouri Rural Telephone",
    "Green Hills Telephone", "AT&T", "Brightspeed",
    "United Fiber", "GoFiber", "Conexon Connect",
]

# USDA RUCC codes
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


def generate_county(fips, name, is_metro, approx_pop, approx_hhi, is_stl_kc):
    """Generate a single county record with all fields."""

    pop_2023 = int(approx_pop * random.uniform(0.92, 1.08))
    pop_2018 = int(pop_2023 / (1 + random.uniform(-0.03, 0.05)))
    pop_growth = round((pop_2023 - pop_2018) / pop_2018 * 100, 2)

    housing_units = int(pop_2023 * random.uniform(0.38, 0.46))
    housing_growth = round(random.uniform(-1, 6), 2)

    # Land area varies a lot in MO
    if name == "St. Louis City":
        land_area = 61.9
    elif is_metro and is_stl_kc:
        land_area = round(random.uniform(400, 700), 1)
    else:
        land_area = round(random.uniform(350, 850), 1)

    pop_density = round(pop_2023 / land_area, 1)
    housing_density = round(housing_units / land_area, 1)

    hhi = int(approx_hhi * random.uniform(0.9, 1.1))
    median_rent = int(hhi * random.uniform(0.013, 0.02))
    median_home_value = int(hhi * random.uniform(2.5, 4.5))
    owner_occ = round(random.uniform(55, 78) if not is_metro else random.uniform(50, 72), 1)
    wfh_pct = round(random.uniform(3, 8) if not is_metro else random.uniform(8, 22), 1)
    mobility = round(random.uniform(0.5, 2.5), 2)

    # Fiber data
    total_bsls = int(housing_units * random.uniform(0.95, 1.15))
    if is_metro and is_stl_kc:
        pen = random.uniform(0.45, 0.82)
    elif is_metro:
        pen = random.uniform(0.35, 0.70)
    else:
        pen = random.uniform(0.10, 0.55)

    fiber_served = int(total_bsls * pen)
    fiber_unserved = total_bsls - fiber_served

    # Operators
    n_operators = random.randint(2, 8) if is_metro else random.randint(1, 5)
    county_operators = []
    remaining = fiber_served
    chosen = random.sample(MO_OPERATORS, min(n_operators, len(MO_OPERATORS)))
    for i, op_name in enumerate(chosen):
        if i == len(chosen) - 1:
            passings = remaining
        else:
            passings = int(remaining * random.uniform(0.15, 0.6))
            remaining -= passings
        if passings > 0:
            county_operators.append({
                "name": op_name,
                "passings": passings,
                "served": int(passings * random.uniform(0.85, 1.0))
            })
    county_operators.sort(key=lambda x: x["passings"], reverse=True)

    # Scores
    # Demo score based on income, density, growth
    income_score = min(1, max(0, (hhi - 30000) / 60000))
    density_score = min(1, max(0, math.log10(max(1, housing_density)) / 3))
    growth_score = min(1, max(0, (pop_growth + 5) / 15))
    wfh_score = min(1, max(0, wfh_pct / 25))
    demo_score = round(income_score * 0.35 + density_score * 0.25 + growth_score * 0.25 + wfh_score * 0.15, 3)

    opportunity_score = round(1 - pen, 3)
    attractiveness_index = round(demo_score * 0.55 + opportunity_score * 0.45, 3)

    if attractiveness_index >= 0.45:
        segment = "Most Attractive"
    elif attractiveness_index >= 0.30:
        segment = "Neutral"
    else:
        segment = "Least Attractive"

    # === NEW FIELDS ===

    # BEAD Funding
    if pen < 0.40 and not is_metro:
        bead_status = random.choice(["Awarded", "Awarded", "In Progress"])
        bead_dollars = random.randint(800000, 15000000)
        bead_locations = random.randint(500, max(501, int(fiber_unserved * 0.8)))
        bead_awardees = random.sample(BEAD_AWARDEES, random.randint(1, 3))
        bead_claimed_pct = round(bead_locations / max(1, fiber_unserved), 3)
    elif pen < 0.55 and random.random() < 0.5:
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

    # Competition
    n_wireline = len(county_operators)
    if n_wireline >= 5:
        comp_intensity = 3
        comp_label = "High"
    elif n_wireline >= 3:
        comp_intensity = 2
        comp_label = "Moderate"
    elif n_wireline >= 2:
        comp_intensity = 1
        comp_label = "Low"
    else:
        comp_intensity = 0
        comp_label = "Monopoly/None"
    wireline_providers = [op["name"] for op in county_operators]

    # Build Momentum (FCC BDC v5 vs v6 proxy)
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

    # Terrain / Construction Cost
    # MO Ozarks region: southern counties have rough terrain
    ozark_counties = {"Barry", "Stone", "Taney", "Ozark", "Douglas", "Howell",
                      "Oregon", "Shannon", "Carter", "Reynolds", "Iron", "Madison",
                      "Wayne", "Ripley", "Dent", "Crawford", "Texas", "Wright",
                      "Webster", "Dallas", "Laclede", "Phelps", "Pulaski",
                      "Camden", "Miller", "Maries", "Gasconade", "Washington",
                      "St. Francois", "Ste. Genevieve", "Perry", "Bollinger",
                      "Cedar", "Hickory", "Benton", "Morgan", "Moniteau",
                      "Cole", "Osage", "Christian", "Greene", "Polk"}
    bootheel = {"Dunklin", "Pemiscot", "New Madrid", "Mississippi", "Stoddard",
                "Scott", "Butler", "Ripley"}

    if name in ozark_counties:
        elev_mean = random.randint(900, 1400)
        elev_std = random.randint(100, 300)
        roughness = round(random.uniform(0.5, 0.9), 2)
    elif name in bootheel:
        elev_mean = random.randint(250, 400)
        elev_std = random.randint(10, 40)
        roughness = round(random.uniform(0.05, 0.2), 2)
    elif is_metro:
        elev_mean = random.randint(500, 900)
        elev_std = random.randint(30, 100)
        roughness = round(random.uniform(0.1, 0.4), 2)
    else:
        elev_mean = random.randint(600, 1100)
        elev_std = random.randint(40, 180)
        roughness = round(random.uniform(0.2, 0.6), 2)

    if roughness >= 0.6:
        cost_tier = "Very High"
        build_diff = "Challenging"
    elif roughness >= 0.4:
        cost_tier = "High"
        build_diff = "Moderate-Hard"
    elif roughness >= 0.2:
        cost_tier = "Medium"
        build_diff = "Moderate"
    else:
        cost_tier = "Low"
        build_diff = "Easy"

    # USDA RUCC
    if is_metro and (is_stl_kc or pop_2023 > 200000):
        rucc = 1
    elif is_metro and pop_2023 > 80000:
        rucc = 2
    elif is_metro:
        rucc = 3
    elif pop_2023 > 20000 and is_stl_kc:
        rucc = 4
    elif pop_2023 > 20000:
        rucc = 5
    elif pop_2023 > 5000:
        rucc = random.choice([6, 7])
    else:
        rucc = random.choice([8, 9])

    rural_class = "Metro" if rucc <= 3 else ("Micro" if rucc <= 5 else "Rural")

    return {
        "geoid": fips,
        "name": name,
        "is_metro_county": is_metro,
        "is_stl_kc_metro": is_stl_kc,
        "total_bsls": total_bsls,
        "fiber_served": fiber_served,
        "fiber_unserved": fiber_unserved,
        "fiber_penetration": round(pen, 3),
        "operators": county_operators,
        "population_2023": pop_2023,
        "population_2018": pop_2018,
        "pop_growth_pct": pop_growth,
        "housing_units": housing_units,
        "housing_growth_pct": housing_growth,
        "land_area_sqmi": land_area,
        "pop_density": pop_density,
        "housing_density": housing_density,
        "median_hhi": hhi,
        "median_rent": median_rent,
        "median_home_value": median_home_value,
        "owner_occupied_pct": owner_occ,
        "wfh_pct": wfh_pct,
        "mobility_pct": mobility,
        "demo_score": demo_score,
        "opportunity_score": opportunity_score,
        "attractiveness_index": attractiveness_index,
        "segment": segment,
        # BEAD
        "bead_dollars_awarded": bead_dollars,
        "bead_awardees": bead_awardees,
        "bead_locations_covered": bead_locations,
        "bead_claimed_pct": bead_claimed_pct,
        "bead_status": bead_status,
        # Competition
        "competitive_intensity": comp_intensity,
        "competitive_label": comp_label,
        "wireline_providers": wireline_providers,
        # Build Momentum
        "fiber_bsls_v5": fiber_bsls_v5,
        "fiber_bsls_v6": fiber_bsls_v6,
        "fiber_growth_net": fiber_growth_net,
        "fiber_growth_pct": fiber_growth_pct,
        "momentum_class": momentum_class,
        # Terrain
        "elevation_mean_ft": elev_mean,
        "elevation_std_ft": elev_std,
        "terrain_roughness": roughness,
        "construction_cost_tier": cost_tier,
        "build_difficulty": build_diff,
        # RUCC
        "rucc_code": rucc,
        "rucc_description": RUCC_DESCRIPTIONS[rucc],
        "rural_class": rural_class,
    }


def main():
    data = {}
    for fips, name, is_metro, pop, hhi, is_stl_kc in MO_COUNTIES:
        data[fips] = generate_county(fips, name, is_metro, pop, hhi, is_stl_kc)

    out_path = os.path.join(os.path.dirname(__file__), "..", "data", "mo-unified-data.json")
    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Generated {len(data)} MO county records -> {out_path}")

    # Summary stats
    metro = sum(1 for c in data.values() if c["is_metro_county"])
    bead_targeted = sum(1 for c in data.values() if c["bead_status"] != "Not Targeted")
    avg_pen = sum(c["fiber_penetration"] for c in data.values()) / len(data)
    print(f"  Metro counties: {metro}")
    print(f"  BEAD targeted: {bead_targeted}")
    print(f"  Avg fiber penetration: {avg_pen:.1%}")


if __name__ == "__main__":
    main()
