-- Add technology column to provider_passings_history and update unique constraint.
-- Run in: Supabase Dashboard → SQL Editor

-- 1. Drop old unique constraint (Postgres auto-names it)
ALTER TABLE provider_passings_history
  DROP CONSTRAINT IF EXISTS provider_passings_history_geoid_provider_id_filing_date_key;

-- 2. Add technology column — existing fiber rows get 'fiber' by default
ALTER TABLE provider_passings_history
  ADD COLUMN IF NOT EXISTS technology text NOT NULL DEFAULT 'fiber';

-- 3. New unique constraint includes technology so cable/dsl rows don't collide
ALTER TABLE provider_passings_history
  ADD CONSTRAINT pph_unique_geoid_provider_date_tech
  UNIQUE (geoid, provider_id, filing_date, technology);

-- 4. Replace primary index to cover technology for efficient per-tech county queries
DROP INDEX IF EXISTS idx_pph_geoid_date;
CREATE INDEX IF NOT EXISTS idx_pph_geoid_date_tech
  ON provider_passings_history (geoid, filing_date, technology);
