-- =============================================================================
-- Migration 001: Ranking Schema
--
-- Context:
--   Supabase exposes the 'public' schema by default via its REST API.
--   'core' schema is accessed only through the explicit .schema("core") client
--   call. Two tables were historically placed (or have views) in 'public' so
--   they can be queried without a schema prefix in index.js:
--     - public.country_normalized
--     - public.university_composite_ranking   ← this migration touches this one
--
-- Strategy:
--   - Preserve existing public.university_composite_ranking as _legacy (data safe)
--   - New ranking tables go into 'core' (consistent with all other data tables)
--   - Replacement VIEW lives in 'public' so index.js needs zero changes
--   - trust_tier: 1=domestic (highest trust), 2=global, 3=govt (lowest trust)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Step 1: Preserve existing data
-- Handles both TABLE and VIEW cases (public schema, not core)
-- -----------------------------------------------------------------------------

DO $$
BEGIN
  -- If it is a plain table
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name   = 'university_composite_ranking'
      AND table_type   = 'BASE TABLE'
  ) THEN
    ALTER TABLE public.university_composite_ranking
      RENAME TO university_composite_ranking_legacy;

  -- If it is a view
  ELSIF EXISTS (
    SELECT 1 FROM information_schema.views
    WHERE table_schema = 'public'
      AND table_name   = 'university_composite_ranking'
  ) THEN
    ALTER VIEW public.university_composite_ranking
      RENAME TO university_composite_ranking_legacy;
  END IF;
END;
$$;


-- -----------------------------------------------------------------------------
-- Step 2: Ranking frameworks master table  (core schema)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.ranking_frameworks (
  id            UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  name          TEXT    NOT NULL,                   -- 'QS World University Rankings'
  short_name    TEXT    NOT NULL,                   -- 'QS World'
  type          TEXT    NOT NULL                    -- 'global' | 'domestic' | 'govt'
                        CHECK (type IN ('global', 'domestic', 'govt')),
  region        TEXT,                               -- NULL for global; 'IN','UK','US','AU' for domestic/govt
  trust_tier    INTEGER NOT NULL                    -- 1=domestic, 2=global, 3=govt
                        CHECK (trust_tier BETWEEN 1 AND 3),
  subject_scope TEXT    NOT NULL DEFAULT 'overall'  -- 'overall' | 'subject'
                        CHECK (subject_scope IN ('overall', 'subject')),
  active        BOOLEAN NOT NULL DEFAULT TRUE,
  notes         TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE core.ranking_frameworks IS
  'Master list of ranking systems. trust_tier: 1=domestic (highest), 2=global, 3=govt.';


-- -----------------------------------------------------------------------------
-- Step 3: Per-year editions of each framework  (core schema)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.ranking_editions (
  id            UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  framework_id  UUID    NOT NULL REFERENCES core.ranking_frameworks(id) ON DELETE CASCADE,
  year          INTEGER NOT NULL,
  published_date DATE,
  is_latest     BOOLEAN NOT NULL DEFAULT FALSE,     -- maintained via trigger below
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (framework_id, year)
);

COMMENT ON TABLE core.ranking_editions IS
  'One row per annual edition of a ranking framework. is_latest = TRUE for most recent year.';

-- Trigger: keep is_latest accurate when a new edition is inserted/updated
CREATE OR REPLACE FUNCTION core.refresh_latest_edition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  UPDATE core.ranking_editions
  SET    is_latest = FALSE
  WHERE  framework_id = NEW.framework_id;

  UPDATE core.ranking_editions
  SET    is_latest = TRUE
  WHERE  id = (
    SELECT id FROM core.ranking_editions
    WHERE  framework_id = NEW.framework_id
    ORDER  BY year DESC
    LIMIT  1
  );

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_refresh_latest_edition ON core.ranking_editions;
CREATE TRIGGER trg_refresh_latest_edition
AFTER INSERT OR UPDATE ON core.ranking_editions
FOR EACH ROW EXECUTE FUNCTION core.refresh_latest_edition();


-- -----------------------------------------------------------------------------
-- Step 4: Composite rank per university per edition  (core schema)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.university_rankings (
  id                UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  university_id     UUID    NOT NULL REFERENCES core.universities(id) ON DELETE CASCADE,
  edition_id        UUID    NOT NULL REFERENCES core.ranking_editions(id) ON DELETE CASCADE,
  rank_number       INTEGER,             -- exact rank (NULL if banded, e.g. 501-600)
  rank_band         TEXT,                -- '501-600'; NULL if exact rank is known
  composite_score   NUMERIC(6,4),        -- 0-1 normalised score (we compute via helper fn)
  raw_overall_score NUMERIC(7,3),        -- score as published (e.g. QS 0-100, THE 0-100)
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (university_id, edition_id),
  CHECK (rank_number IS NOT NULL OR rank_band IS NOT NULL)
);

COMMENT ON COLUMN core.university_rankings.composite_score IS
  '0-1 normalised rank score. Computed via core.refresh_composite_scores(edition_id). Uses log scale.';
COMMENT ON COLUMN core.university_rankings.raw_overall_score IS
  'Score as published by the framework (e.g. QS overall score 0-100).';

CREATE INDEX IF NOT EXISTS idx_university_rankings_university ON core.university_rankings(university_id);
CREATE INDEX IF NOT EXISTS idx_university_rankings_edition    ON core.university_rankings(edition_id);


-- -----------------------------------------------------------------------------
-- Step 5: Sub-indicator (pillar) definitions per framework  (core schema)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.ranking_sub_indicators (
  id                   UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  framework_id         UUID    NOT NULL REFERENCES core.ranking_frameworks(id) ON DELETE CASCADE,
  indicator_key        TEXT    NOT NULL,
  indicator_label      TEXT    NOT NULL,
  official_weight_pct  NUMERIC(5,2),
  parameter_category   TEXT
                        CHECK (parameter_category IN (
                          'research', 'employability', 'teaching',
                          'prestige', 'international', 'experience'
                        )),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (framework_id, indicator_key)
);

COMMENT ON TABLE core.ranking_sub_indicators IS
  'Pillar definitions per framework. parameter_category bridges to quiz-driven weighting later.';


-- -----------------------------------------------------------------------------
-- Step 6: Sub-indicator scores per university per edition  (core schema)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.university_sub_indicator_scores (
  id                    UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  university_ranking_id UUID    NOT NULL REFERENCES core.university_rankings(id) ON DELETE CASCADE,
  sub_indicator_id      UUID    NOT NULL REFERENCES core.ranking_sub_indicators(id) ON DELETE CASCADE,
  raw_score             NUMERIC(7,3),
  normalized_score      NUMERIC(6,4),
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (university_ranking_id, sub_indicator_id)
);

COMMENT ON TABLE core.university_sub_indicator_scores IS
  'Sub-indicator scores per university per edition. raw_score as published; normalized_score 0-1.';

CREATE INDEX IF NOT EXISTS idx_sub_scores_ranking   ON core.university_sub_indicator_scores(university_ranking_id);
CREATE INDEX IF NOT EXISTS idx_sub_scores_indicator ON core.university_sub_indicator_scores(sub_indicator_id);


-- -----------------------------------------------------------------------------
-- Step 7: Replacement VIEW in PUBLIC schema
--
-- Must live in public so index.js can query it without .schema() prefix.
-- Weighted average of composite_score across latest editions per framework,
-- weighted by trust_tier (tier 1 = weight 3, tier 2 = weight 2, tier 3 = weight 1).
-- Falls back to legacy score until real ranking data is loaded.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.university_composite_ranking AS
WITH latest_scores AS (
  SELECT
    ur.university_id,
    ur.composite_score,
    (4 - rf.trust_tier) AS tier_weight    -- tier 1→3, tier 2→2, tier 3→1
  FROM core.university_rankings  ur
  JOIN core.ranking_editions     re ON re.id = ur.edition_id
  JOIN core.ranking_frameworks   rf ON rf.id = re.framework_id
  WHERE re.is_latest          = TRUE
    AND rf.active             = TRUE
    AND ur.composite_score IS NOT NULL
),
aggregated AS (
  SELECT
    university_id,
    SUM(composite_score * tier_weight) / SUM(tier_weight) AS computed_score
  FROM latest_scores
  GROUP BY university_id
)
SELECT
  u.id,
  COALESCE(
    agg.computed_score,   -- weighted score from new ranking tables (once data is loaded)
    leg.final_score,      -- legacy flat score in the meantime
    0.5                   -- absolute fallback
  ) AS final_score
FROM core.universities u
LEFT JOIN aggregated                                   agg ON agg.university_id = u.id
LEFT JOIN public.university_composite_ranking_legacy   leg ON leg.id            = u.id;

COMMENT ON VIEW public.university_composite_ranking IS
  'Public-schema drop-in replacement for the old table. Consumed by index.js without schema prefix.
   Computes final_score as trust-tier-weighted average of composite_score across latest ranking editions.
   Falls back to legacy score until core.university_rankings is populated.';


-- -----------------------------------------------------------------------------
-- Step 8: Display view in public schema
--
-- Exposes per-framework rank data for the frontend (QS #10, THE #15, etc.)
-- Also in public so it can be queried directly if needed.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.university_ranking_summary AS
SELECT
  u.id                  AS university_id,
  u.name                AS university_name,
  rf.short_name         AS framework,
  rf.type               AS framework_type,
  rf.trust_tier,
  re.year,
  ur.rank_number,
  ur.rank_band,
  ur.composite_score,
  ur.raw_overall_score
FROM core.universities        u
JOIN core.university_rankings ur ON ur.university_id = u.id
JOIN core.ranking_editions    re ON re.id            = ur.edition_id
JOIN core.ranking_frameworks  rf ON rf.id            = re.framework_id
ORDER BY u.name, rf.trust_tier, re.year DESC;

COMMENT ON VIEW public.university_ranking_summary IS
  'Per-framework ranking display view. Use to show QS #10, THE #15 etc. alongside recommendations.';
