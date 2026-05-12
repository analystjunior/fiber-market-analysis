-- ============================================================
-- FiberMapUSA — Provider Passings History
-- Run in: Supabase Dashboard → SQL Editor
-- ============================================================
-- Stores exact FTTP location counts (distinct location_ids) per
-- provider per county per BDC filing period.
-- Sourced from FCC BDC tech-50 location coverage CSVs.
-- ============================================================

CREATE TABLE IF NOT EXISTS provider_passings_history (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  geoid        text   NOT NULL,  -- 5-digit county FIPS e.g. '29189'
  provider_id  text   NOT NULL,  -- FCC numeric provider_id e.g. '130077'
  brand_name   text   NOT NULL,  -- FCC brand_name e.g. 'AT&T'
  filing_date  date   NOT NULL,  -- BDC as-of date e.g. '2025-06-30'
  passings     integer NOT NULL, -- distinct residential location_ids
  UNIQUE (geoid, provider_id, filing_date)
);

-- Primary query: all history for a county (county panel load)
CREATE INDEX IF NOT EXISTS idx_pph_geoid_date
  ON provider_passings_history (geoid, filing_date);

-- Secondary: all counties for a provider (provider map view, future use)
CREATE INDEX IF NOT EXISTS idx_pph_provider_date
  ON provider_passings_history (provider_id, filing_date);

-- Public read-only (same RLS pattern as counties table)
ALTER TABLE provider_passings_history ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "public_read_provider_passings_history" ON provider_passings_history;
CREATE POLICY "public_read_provider_passings_history"
  ON provider_passings_history FOR SELECT TO anon, authenticated USING (true);

-- Service role (pipeline scripts) can insert/upsert without restriction.
-- No explicit policy needed — service role bypasses RLS.
