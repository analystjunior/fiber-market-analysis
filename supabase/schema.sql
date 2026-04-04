-- ============================================================
-- FiberMapUSA — Supabase Schema
-- Run this in the Supabase SQL Editor (Dashboard → SQL Editor)
-- ============================================================

-- ── Counties ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS counties (
  -- Identity
  geoid                        text         PRIMARY KEY,  -- '29001' (5-digit FIPS)
  state_code                   text         NOT NULL,     -- 'MO'
  name                         text         NOT NULL,

  -- Broadband locations
  total_bsls                   integer,
  fiber_served                 integer,
  fiber_unserved               integer,
  fiber_penetration            numeric(8,4),
  cable_served                 integer,
  dsl_served                   integer,

  -- Technology coverage (0.0–1.0)
  cable_coverage_pct           numeric(8,4),
  fwa_coverage_pct             numeric(8,4),
  broadband_coverage_pct       numeric(8,4),
  broadband_gap_pct            numeric(8,4),
  cable_present                boolean,
  fwa_present                  boolean,

  -- Operators (JSONB — always read together with parent county)
  operators                    jsonb        NOT NULL DEFAULT '[]',
  cable_operators              jsonb        NOT NULL DEFAULT '[]',
  dsl_operators                jsonb        NOT NULL DEFAULT '[]',
  cable_operator_count         smallint,
  dsl_operator_count           smallint,

  -- Provider lists
  wireline_providers           jsonb        NOT NULL DEFAULT '[]',
  total_broadband_providers    integer,
  competitive_intensity        smallint,
  competitive_label            text,

  -- Demographics
  population_2023              integer,
  population_2018              integer,
  pop_growth_pct               numeric(8,2),
  housing_units                integer,
  housing_growth_pct           numeric(8,2),
  land_area_sqmi               numeric(12,2),
  pop_density                  numeric(10,2),
  housing_density              numeric(10,2),
  median_hhi                   integer,
  median_rent                  integer,
  median_home_value            integer,
  owner_occupied_pct           numeric(6,2),
  wfh_pct                      numeric(6,2),

  -- Scores
  demo_score                   numeric(8,4),
  opportunity_score            numeric(8,4),
  attractiveness_index         numeric(8,4),
  segment                      text,

  -- BEAD
  bead_status                  text,
  bead_eligible_locations      integer,
  bead_state_allocation        bigint,
  bead_dollars_per_eligible_loc numeric(14,2),
  bead_implied_county_award    bigint,
  bead_dollars_awarded         bigint,
  bead_awardees                jsonb        NOT NULL DEFAULT '[]',
  bead_locations_covered       integer,
  bead_claimed_pct             numeric(8,4),

  -- Terrain / build
  elevation_mean_ft            numeric(10,2),
  elevation_std_ft             numeric(10,2),
  terrain_roughness            numeric(8,4),
  construction_cost_tier       text,
  build_difficulty             text,

  -- Seasonal construction window (NOAA Climate Normals 1991-2020, threshold 32°F)
  buildable_months             smallint,     -- months/year avg temp > 32°F (0–12)
  winter_severity              text,         -- 'None' / 'Mild' / 'Moderate' / 'Severe'
  coldest_month_f              real,         -- average temp of coldest month (°F)

  -- Classification
  rucc_code                    smallint,
  rucc_description             text,
  rural_class                  text,
  is_metro_county              boolean,

  -- Momentum
  fiber_growth_pct             numeric(8,2),
  momentum_class               text,

  -- Pipeline tracking
  data_as_of                   date,
  updated_at                   timestamptz  NOT NULL DEFAULT now()
);

-- Fast state-level fetch (primary access pattern)
CREATE INDEX IF NOT EXISTS idx_counties_state_code
  ON counties (state_code);

-- Ranking queries by attractiveness
CREATE INDEX IF NOT EXISTS idx_counties_attractiveness
  ON counties (state_code, attractiveness_index DESC NULLS LAST);

-- Provider-mode: filter by operator name inside JSONB
CREATE INDEX IF NOT EXISTS idx_counties_operators_gin
  ON counties USING gin (operators);


-- ── State Summary ─────────────────────────────────────────────
-- Replaces fiber-data.json (51 rows, loaded once at startup)
CREATE TABLE IF NOT EXISTS state_summary (
  state_code             text         PRIMARY KEY,  -- 'MO'
  state_name             text         NOT NULL,
  total_housing_units    integer,
  total_fiber_passings   integer,
  fiber_penetration      numeric(6,2),
  operators              jsonb        NOT NULL DEFAULT '[]',
  updated_at             timestamptz  NOT NULL DEFAULT now()
);


-- ── Row Level Security ────────────────────────────────────────
-- Public read-only. Service role key (used only in pipeline) bypasses RLS.

ALTER TABLE counties       ENABLE ROW LEVEL SECURITY;
ALTER TABLE state_summary  ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "public_read_counties"    ON counties;
DROP POLICY IF EXISTS "public_read_state_summary" ON state_summary;

CREATE POLICY "public_read_counties"
  ON counties FOR SELECT TO anon USING (true);

CREATE POLICY "public_read_state_summary"
  ON state_summary FOR SELECT TO anon USING (true);
