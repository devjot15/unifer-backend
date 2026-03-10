-- =============================================================================
-- Migration 001: Ranking Schema
--
-- Strategy:
--   - Keeps existing university_composite_ranking table intact (no breaking changes)
--   - Adds new tables for multi-framework ranking data (composite + sub-indicators)
--   - Replaces university_composite_ranking with a VIEW derived from new tables,
--     with a fallback to the legacy score until real data is loaded
--   - trust_tier: 1=domestic (highest trust), 2=global, 3=govt (lowest trust)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Step 1: Preserve existing data
-- -----------------------------------------------------------------------------

ALTER TABLE core.university_composite_ranking
  RENAME TO university_composite_ranking_legacy;


-- -----------------------------------------------------------------------------
-- Step 2: Ranking frameworks master table
-- -----------------------------------------------------------------------------

CREATE TABLE core.ranking_frameworks (
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
-- Step 3: Per-year editions of each framework
-- -----------------------------------------------------------------------------

CREATE TABLE core.ranking_editions (
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

-- Trigger: keep is_latest accurate when a new edition is inserted
CREATE OR REPLACE FUNCTION core.refresh_latest_edition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  -- Mark all editions for this framework as not latest
  UPDATE core.ranking_editions
  SET    is_latest = FALSE
  WHERE  framework_id = NEW.framework_id;

  -- Mark the highest year as latest
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

CREATE TRIGGER trg_refresh_latest_edition
AFTER INSERT OR UPDATE ON core.ranking_editions
FOR EACH ROW EXECUTE FUNCTION core.refresh_latest_edition();


-- -----------------------------------------------------------------------------
-- Step 4: Composite rank per university per edition
-- -----------------------------------------------------------------------------

CREATE TABLE core.university_rankings (
  id                UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  university_id     UUID    NOT NULL REFERENCES core.universities(id) ON DELETE CASCADE,
  edition_id        UUID    NOT NULL REFERENCES core.ranking_editions(id) ON DELETE CASCADE,
  rank_number       INTEGER,             -- exact rank (NULL if banded, e.g. 501-600)
  rank_band         TEXT,                -- '501-600'; NULL if exact rank is known
  composite_score   NUMERIC(6,4),        -- 0-1 normalised score (we compute, see note)
  raw_overall_score NUMERIC(7,3),        -- score as published (e.g. QS publishes 0-100)
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (university_id, edition_id),
  -- Either rank_number or rank_band must be present
  CHECK (rank_number IS NOT NULL OR rank_band IS NOT NULL)
);

COMMENT ON COLUMN core.university_rankings.composite_score IS
  '0-1 normalised rank score we compute. For ranked universities: 1 - ((rank-1) / max_rank_in_edition). For banded, use mid-point of band.';
COMMENT ON COLUMN core.university_rankings.raw_overall_score IS
  'Score as published by the framework (e.g. QS publishes 0-100 overall score, THE publishes 0-100).';

CREATE INDEX idx_university_rankings_university ON core.university_rankings(university_id);
CREATE INDEX idx_university_rankings_edition    ON core.university_rankings(edition_id);


-- -----------------------------------------------------------------------------
-- Step 5: Sub-indicator (pillar) definitions per framework
-- -----------------------------------------------------------------------------

CREATE TABLE core.ranking_sub_indicators (
  id                   UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  framework_id         UUID    NOT NULL REFERENCES core.ranking_frameworks(id) ON DELETE CASCADE,
  indicator_key        TEXT    NOT NULL,  -- 'academic_reputation', 'citations_per_faculty'
  indicator_label      TEXT    NOT NULL,  -- human-readable display name
  official_weight_pct  NUMERIC(5,2),      -- official weight % in the framework (may be NULL if undisclosed)
  parameter_category   TEXT               -- bridge to quiz preference vectors later:
                                          -- 'research' | 'employability' | 'teaching'
                                          -- | 'prestige' | 'international' | 'experience'
                        CHECK (parameter_category IN (
                          'research', 'employability', 'teaching',
                          'prestige', 'international', 'experience'
                        )),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (framework_id, indicator_key)
);

COMMENT ON TABLE core.ranking_sub_indicators IS
  'Pillar/sub-indicator definitions per framework. parameter_category is the future bridge to quiz-driven weighting.';


-- -----------------------------------------------------------------------------
-- Step 6: Actual sub-indicator scores per university per edition
-- -----------------------------------------------------------------------------

CREATE TABLE core.university_sub_indicator_scores (
  id                    UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  university_ranking_id UUID    NOT NULL REFERENCES core.university_rankings(id) ON DELETE CASCADE,
  sub_indicator_id      UUID    NOT NULL REFERENCES core.ranking_sub_indicators(id) ON DELETE CASCADE,
  raw_score             NUMERIC(7,3),    -- as published by the framework
  normalized_score      NUMERIC(6,4),    -- 0-1 normalised (we compute)
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (university_ranking_id, sub_indicator_id)
);

COMMENT ON TABLE core.university_sub_indicator_scores IS
  'Actual sub-indicator scores per university per ranking edition. Both raw and 0-1 normalised.';

CREATE INDEX idx_sub_scores_ranking ON core.university_sub_indicator_scores(university_ranking_id);
CREATE INDEX idx_sub_scores_indicator ON core.university_sub_indicator_scores(sub_indicator_id);


-- -----------------------------------------------------------------------------
-- Step 7: Replace university_composite_ranking with a VIEW
--
-- Logic:
--   For each university, weighted average of composite_score across latest editions,
--   weighted by trust_tier (tier 1 = weight 3, tier 2 = weight 2, tier 3 = weight 1).
--   Falls back to legacy score if no new ranking data exists for that university.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW core.university_composite_ranking AS
WITH latest_scores AS (
  SELECT
    ur.university_id,
    ur.composite_score,
    rf.trust_tier,
    -- tier weight: domestic(1) → 3, global(2) → 2, govt(3) → 1
    (4 - rf.trust_tier) AS tier_weight
  FROM core.university_rankings ur
  JOIN core.ranking_editions    re ON re.id          = ur.edition_id
  JOIN core.ranking_frameworks  rf ON rf.id          = re.framework_id
  WHERE re.is_latest = TRUE
    AND rf.active    = TRUE
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
    agg.computed_score,            -- new weighted score from new ranking tables
    leg.final_score,               -- legacy score if no new data yet
    0.5                            -- absolute fallback
  ) AS final_score
FROM core.universities u
LEFT JOIN aggregated                          agg ON agg.university_id = u.id
LEFT JOIN core.university_composite_ranking_legacy leg ON leg.id        = u.id;

COMMENT ON VIEW core.university_composite_ranking IS
  'Drop-in replacement for the old table. Weighted average of composite_score across latest ranking editions (trust_tier weighted). Falls back to legacy score until new data is loaded.';


-- -----------------------------------------------------------------------------
-- Step 8: Convenience view — per-university ranking summary (for display)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW core.university_ranking_summary AS
SELECT
  u.id                          AS university_id,
  u.name                        AS university_name,
  rf.short_name                 AS framework,
  rf.type                       AS framework_type,
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

COMMENT ON VIEW core.university_ranking_summary IS
  'Flat view of all ranking data per university for display/export. Use this to show QS #10, THE #15 etc. alongside recommendations.';
