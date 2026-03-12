-- =============================================================================
-- Migration 003: Helper Functions for Ranking Data Ingestion
--
-- Functions:
--   core.normalize_rank(rank, max_rank)       → 0-1 score from a rank number
--   core.normalize_band(band_text, max_rank)  → 0-1 score from a band string like '501-600'
--   core.refresh_composite_scores(edition_id) → recomputes composite_score column
--                                               for all universities in an edition
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Normalize rank number to 0-1
-- rank 1 → 1.0, rank max_rank → ~0.0
-- Uses log scale so difference between #1 and #10 is larger than #490 and #500
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.normalize_rank(
  p_rank     INTEGER,
  p_max_rank INTEGER
)
RETURNS NUMERIC(6,4) LANGUAGE sql IMMUTABLE AS $$
  SELECT ROUND(
    CAST(
      1.0 - (LN(p_rank) / NULLIF(LN(p_max_rank), 0))
    AS NUMERIC(6,4)
  ), 4)
  WHERE p_rank > 0 AND p_max_rank > 0;
$$;

COMMENT ON FUNCTION core.normalize_rank IS
  'Log-scale normalisation: rank 1 → ~1.0, rank max → ~0.0. Larger gap at top, smaller at bottom.';


-- -----------------------------------------------------------------------------
-- Normalize a rank band string to 0-1 using its midpoint
-- e.g. '501-600' with max_rank 1500 → midpoint 550 → normalize_rank(550, 1500)
-- Also handles formats like '601+' or '1000+'
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.normalize_band(
  p_band     TEXT,
  p_max_rank INTEGER
)
RETURNS NUMERIC(6,4) LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  parts    TEXT[];
  low_end  INTEGER;
  high_end INTEGER;
  midpoint INTEGER;
BEGIN
  -- Handle open-ended bands like '1001+' or '601+'
  IF p_band ~ '^\d+\+$' THEN
    low_end  := CAST(regexp_replace(p_band, '\+', '') AS INTEGER);
    midpoint := low_end + 100;  -- assume ~100 wide band
    RETURN core.normalize_rank(midpoint, p_max_rank);
  END IF;

  -- Handle range bands like '501-600'
  IF p_band ~ '^\d+-\d+$' THEN
    parts    := string_to_array(p_band, '-');
    low_end  := CAST(parts[1] AS INTEGER);
    high_end := CAST(parts[2] AS INTEGER);
    midpoint := (low_end + high_end) / 2;
    RETURN core.normalize_rank(midpoint, p_max_rank);
  END IF;

  -- Fallback: try to cast directly as a number
  BEGIN
    RETURN core.normalize_rank(CAST(p_band AS INTEGER), p_max_rank);
  EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
  END;
END;
$$;

COMMENT ON FUNCTION core.normalize_band IS
  'Normalises banded rank strings (e.g. 501-600, 1001+) to 0-1 using log-scale midpoint.';


-- -----------------------------------------------------------------------------
-- Refresh composite_score for all university_rankings rows in a given edition
-- Call this after bulk-inserting rank data for an edition.
--
-- Approach:
--   composite_score = normalize_rank(rank_number, max_rank_in_edition)
--   or normalize_band(rank_band, max_rank_in_edition) for banded entries
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.refresh_composite_scores(p_edition_id UUID)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  v_max_rank INTEGER;
  v_updated  INTEGER;
BEGIN
  -- Find the maximum known rank in this edition (ignore banded for max calculation)
  SELECT MAX(rank_number)
  INTO   v_max_rank
  FROM   core.university_rankings
  WHERE  edition_id   = p_edition_id
    AND  rank_number IS NOT NULL;

  -- Fallback if all are banded
  IF v_max_rank IS NULL THEN
    v_max_rank := 1500;
  END IF;

  -- Update composite_score for exact ranks
  UPDATE core.university_rankings
  SET    composite_score = core.normalize_rank(rank_number, v_max_rank),
         updated_at      = NOW()
  WHERE  edition_id      = p_edition_id
    AND  rank_number    IS NOT NULL;

  GET DIAGNOSTICS v_updated = ROW_COUNT;

  -- Update composite_score for banded ranks
  UPDATE core.university_rankings
  SET    composite_score = core.normalize_band(rank_band, v_max_rank),
         updated_at      = NOW()
  WHERE  edition_id      = p_edition_id
    AND  rank_number    IS NULL
    AND  rank_band      IS NOT NULL;

  GET DIAGNOSTICS v_updated = v_updated + ROW_COUNT;

  RETURN v_updated;
END;
$$;

COMMENT ON FUNCTION core.refresh_composite_scores IS
  'Recomputes composite_score (0-1) for all rows in an edition using log-scale normalisation. Call after bulk data load.';


-- -----------------------------------------------------------------------------
-- Refresh normalized_score for sub-indicator scores in a given edition
-- Normalises raw_score relative to the max raw_score for that indicator in the edition
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.refresh_sub_indicator_scores(p_edition_id UUID)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  v_updated INTEGER := 0;
  rec       RECORD;
BEGIN
  -- For each sub_indicator in this edition, compute max raw_score and normalise
  FOR rec IN
    SELECT DISTINCT sis.sub_indicator_id,
                    MAX(sis.raw_score) OVER (PARTITION BY sis.sub_indicator_id) AS max_score
    FROM   core.university_sub_indicator_scores sis
    JOIN   core.university_rankings             ur  ON ur.id = sis.university_ranking_id
    WHERE  ur.edition_id = p_edition_id
      AND  sis.raw_score IS NOT NULL
  LOOP
    CONTINUE WHEN rec.max_score IS NULL OR rec.max_score = 0;

    UPDATE core.university_sub_indicator_scores sis
    SET    normalized_score = ROUND(CAST(sis.raw_score / rec.max_score AS NUMERIC(6,4)), 4),
           updated_at       = NOW()
    FROM   core.university_rankings ur
    WHERE  ur.id              = sis.university_ranking_id
      AND  ur.edition_id      = p_edition_id
      AND  sis.sub_indicator_id = rec.sub_indicator_id;

    GET DIAGNOSTICS v_updated = v_updated + ROW_COUNT;
  END LOOP;

  RETURN v_updated;
END;
$$;

COMMENT ON FUNCTION core.refresh_sub_indicator_scores IS
  'Normalises raw sub-indicator scores to 0-1 relative to the max in each edition. Call after bulk data load.';


-- =============================================================================
-- USAGE GUIDE (run in order when loading a new ranking dataset):
--
--   1. Insert into core.ranking_editions (year + framework_id)
--      → is_latest trigger fires automatically
--
--   2. Bulk insert into core.university_rankings (rank_number or rank_band,
--      raw_overall_score). Leave composite_score NULL for now.
--
--   3. SELECT core.refresh_composite_scores('<edition_id>');
--      → fills composite_score column
--
--   4. Bulk insert into core.university_sub_indicator_scores (raw_score).
--      Leave normalized_score NULL.
--
--   5. SELECT core.refresh_sub_indicator_scores('<edition_id>');
--      → fills normalized_score column
--
--   6. university_composite_ranking VIEW auto-updates — no further action needed.
-- =============================================================================
