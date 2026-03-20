# Current Data Contract — MO County Dataset

## Status: Real Data Pipeline (v2)

All core metrics are now sourced from real public datasets. Zero fake inputs.

## Field Audit

| Field | Source | Status | Used In |
|-------|--------|--------|---------|
| `geoid` | Census FIPS | REAL | All joins, map data-fips |
| `name` | Census ACS | REAL | InfoPanel, table |
| `is_metro_county` | USDA RUCC 2023 (code <= 3) | REAL | Filters |
| `is_stl_kc_metro` | Hardcoded FIPS set | REAL | Filters |
| `total_bsls` | FCC BDC Place Summary (aggregated to county via Census crosswalk) | REAL | InfoPanel, NPV |
| `fiber_served` | FCC BDC FTTP Locations (distinct location_ids per county) | REAL | InfoPanel, map color, NPV |
| `fiber_unserved` | total_bsls - fiber_served | DERIVED | InfoPanel, table, NPV |
| `fiber_penetration` | fiber_served / total_bsls | DERIVED | Map layer, score bars, table |
| `operators` | FCC BDC FTTP Locations (brand_name, passings per county) | REAL | InfoPanel operators list |
| `wireline_providers` | FCC BDC FTTP Locations (distinct brand_names per county) | REAL | InfoPanel |
| `competitive_intensity` | FCC BDC FTTP fiber provider count (0-3 scale) | REAL | Map layer |
| `competitive_label` | Bucket from fiber provider count | DERIVED | InfoPanel |
| `total_broadband_providers` | FCC BDC Provider Summary by Geography (all-tech, county) | REAL | InfoPanel |
| `population_2023` | Census ACS 5-year 2023 | REAL | InfoPanel, filters |
| `population_2018` | Census ACS 5-year 2018 | REAL | Growth calc |
| `pop_growth_pct` | (pop_2023 - pop_2018) / pop_2018 | DERIVED | InfoPanel |
| `housing_units` | Census ACS 5-year 2023 | REAL | InfoPanel |
| `housing_growth_pct` | (housing_2023 - housing_2018) / housing_2018 | DERIVED | (unused in UI) |
| `land_area_sqmi` | USGS TRI tract file, aggregated to county | REAL | Density calc |
| `pop_density` | pop / area | DERIVED | InfoPanel |
| `housing_density` | housing / area | DERIVED | InfoPanel, table, filters |
| `median_hhi` | Census ACS 5-year 2023 | REAL | InfoPanel, table, score |
| `median_rent` | Census ACS 5-year 2023 | REAL | InfoPanel |
| `median_home_value` | Census ACS 5-year 2023 | REAL | InfoPanel |
| `owner_occupied_pct` | Census ACS 5-year 2023 | REAL | InfoPanel |
| `wfh_pct` | Census ACS 5-year 2023 | REAL | InfoPanel |
| `demo_score` | Composite of real ACS fields | DERIVED | Map layer, score bar, table |
| `opportunity_score` | 1 - fiber_penetration | DERIVED | (internal) |
| `attractiveness_index` | demo * 0.55 + opp * 0.45 | DERIVED | Map layer, score bar, table |
| `segment` | Threshold on attractiveness | DERIVED | (internal) |
| `bead_status` | Set to "Unverified" | FLAGGED | InfoPanel BEAD section |
| `bead_dollars_awarded` | null | NULL | InfoPanel BEAD section |
| `bead_awardees` | [] | NULL | InfoPanel BEAD section |
| `bead_locations_covered` | null | NULL | InfoPanel BEAD section |
| `bead_claimed_pct` | null | NULL | Map layer (inactive) |
| `fiber_bsls_v5` | null (Dec 2024 BDC not available) | NULL | Momentum calc |
| `fiber_bsls_v6` | = fiber_served (Jun 2025) | REAL | Momentum calc |
| `fiber_growth_net` | null | NULL | InfoPanel |
| `fiber_growth_pct` | null | NULL | Map layer (inactive) |
| `momentum_class` | null | NULL | InfoPanel |
| `elevation_mean_ft` | USGS TRI mean (meters, aggregated from tracts) | REAL | InfoPanel |
| `elevation_std_ft` | USGS TRI std dev (meters) | REAL | (unused in UI) |
| `terrain_roughness` | TRI / 60, clamped 0-1 | DERIVED | Map layer, InfoPanel |
| `construction_cost_tier` | Threshold on roughness | DERIVED | InfoPanel |
| `build_difficulty` | Threshold on roughness | DERIVED | InfoPanel |
| `rucc_code` | USDA RUCC 2023 | REAL | InfoPanel |
| `rucc_description` | USDA RUCC 2023 | REAL | (internal) |
| `rural_class` | Derived from RUCC code (1-3=Metro, 4-5=Micro, 6-9=Rural) | DERIVED | InfoPanel |

## Summary

- **REAL**: 25 fields (FCC FTTP locations, Provider Summary, Census ACS, USDA RUCC, USGS TRI)
- **DERIVED from real**: 13 fields (density, scores, growth, competitive label, rural class)
- **FLAGGED**: 1 field (bead_status = "Unverified")
- **NULL (pending data)**: 5 fields (BEAD details, momentum — pending county BEAD release + Dec 2024 BDC)
- **ESTIMATED**: 0 fields
- **FAKE**: 0 fields

## Raw Data Sources

| Source | File | Records | Coverage |
|--------|------|---------|----------|
| FCC BDC FTTP Locations Jun 2025 | `data/raw/fcc/jun2025/fttp_locations_mo.csv` | ~2.1M rows | 115 MO counties |
| FCC BDC Provider Summary Jun 2025 | `data/raw/fcc/jun2025/provider_summary_by_geography.csv` | ~516K rows (US) | 115 MO counties |
| FCC BDC Place Summary Jun 2025 | `data/raw/fcc/jun2025/broadband_summary_place_mo.csv` | ~102K rows | 114 counties (via crosswalk) |
| FCC BDC Provider Summary National | `data/raw/fcc/jun2025/provider_summary_national.csv` | ~7K rows | Provider lookup |
| Census ACS 5-year 2023 | `data/raw/census/census_acs_mo.json` | 115 records | 115 counties |
| Census ACS 5-year 2018 | `data/raw/census/census_acs_mo_2018.json` | 115 records | 115 counties |
| Census Place-County Crosswalk | `data/raw/census/place_county_crosswalk_mo.txt` | ~1,081 places | MO places |
| USDA RUCC 2023 | `data/raw/usda/rucc2023.csv` | ~9.4K rows (US) | 115 MO counties |
| USGS TRI 2020 Tracts | `data/raw/usgs/ruggedness-scales-2020-tracts.xlsx` | ~1,654 MO tracts | 115 counties |

## Pending Data

1. **FCC BDC County Summary — Dec 2024**: Needed for momentum (fiber_bsls_v5 delta). When available, add to `data/raw/fcc/dec2024/` and update `generate-mo-data.py`.
2. **NTIA BEAD County Awards**: Needed for BEAD section. When publicly released, replace "Unverified" status with real award data.

## Pipeline

```bash
python3 scripts/generate-mo-data.py
```

Reads all raw sources from `data/raw/`, generates `data/mo-unified-data.json` and QA report at `data/processed/mo-qa-report.txt`.
