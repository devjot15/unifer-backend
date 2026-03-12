-- =============================================================================
-- Migration 005: Scoring Rules — Schema of Logic
--
-- Purpose:
--   Formalises how quiz answers map to ranking sub-indicator weights.
--   Currently the weight logic is hardcoded in index.js. This table makes it
--   data-driven: easy to tune weights without code deploys, and queryable
--   for explainability ("why was this university recommended?").
--
-- Architecture:
--   Quiz answer (question_key + answer_value)
--     → dimension (country | course | institution)
--       → parameter_category (matches ranking_sub_indicators.parameter_category)
--         → weight (0.0–1.0)
--
--   The scoring engine reads these rules to dynamically weight sub-indicator
--   scores when computing the institution dimension score.
--
--   For country and course dimensions, rules map to named score components
--   (e.g. psw_score, cost_score) rather than ranking sub-indicators.
--
-- How it will be consumed (Phase 2 of scoring engine):
--   1. Load rules for student's answer set at query time
--   2. For institution score: weighted sum of sub-indicator normalized_scores
--      where weight = scoring_rules.weight for that indicator's parameter_category
--   3. For country/course: weights already in index.js, but rules make them
--      auditable and tuneable without code changes
--
-- Current state: seeded but not yet consumed by index.js scoring loop.
--   index.js still uses hardcoded weights. Phase 2 will switch to rule lookups.
--   Seeds here define the INTENDED logic — treat as the authoritative source.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Table definition
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.scoring_rules (
  id                 UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  quiz_question_key  TEXT    NOT NULL,
  answer_value       TEXT    NOT NULL,
  dimension          TEXT    NOT NULL
                       CHECK (dimension IN ('country', 'course', 'institution')),
  -- For institution dimension: maps to ranking_sub_indicators.parameter_category
  -- For country/course: maps to named score component (see notes column)
  parameter_category TEXT,
  weight             NUMERIC(4,3) NOT NULL CHECK (weight BETWEEN 0 AND 1),
  notes              TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (quiz_question_key, answer_value, dimension, parameter_category)
);

COMMENT ON TABLE core.scoring_rules IS
  'Maps quiz answer values to sub-indicator weights for the scoring engine.
   dimension=institution rules weight ranking_sub_indicators.parameter_category values.
   dimension=country/course rules weight named score components (documented in notes).
   Phase 2 of scoring engine will read these instead of hardcoded JS weights.';

COMMENT ON COLUMN core.scoring_rules.parameter_category IS
  'For institution dimension: must match ranking_sub_indicators.parameter_category
   (research | employability | teaching | prestige | international | experience).
   For country/course dimension: named component (psw_score, pr_score, cost_score,
   english_score, gov_score, internship_score, scholarship_score).';


-- -----------------------------------------------------------------------------
-- INSTITUTION DIMENSION RULES
-- These weight the 6 parameter_categories used in ranking sub-indicators.
-- Higher weight = that category's sub-indicator scores count more.
-- -----------------------------------------------------------------------------

INSERT INTO core.scoring_rules
  (quiz_question_key, answer_value, dimension, parameter_category, weight, notes)
VALUES

  -- ── ranking_importance ─────────────────────────────────────────────────────
  -- "Only want top institutions" → prestige & research indicators carry most weight
  ('ranking_importance', 'Only want to apply in top institutions',
   'institution', 'prestige',       1.000, 'Full weight on prestige sub-indicators (academic reputation)'),
  ('ranking_importance', 'Only want to apply in top institutions',
   'institution', 'research',       0.800, 'Strong weight on research quality'),
  ('ranking_importance', 'Only want to apply in top institutions',
   'institution', 'employability',  0.600, 'Moderate employability weight'),
  ('ranking_importance', 'Only want to apply in top institutions',
   'institution', 'teaching',       0.500, 'Moderate teaching weight'),
  ('ranking_importance', 'Only want to apply in top institutions',
   'institution', 'international',  0.400, 'Lower international weight'),
  ('ranking_importance', 'Only want to apply in top institutions',
   'institution', 'experience',     0.300, 'Lower experience weight'),

  -- "Top and middle institutions are fine"
  ('ranking_importance', 'Top and middle institutions are fine',
   'institution', 'prestige',       0.700, NULL),
  ('ranking_importance', 'Top and middle institutions are fine',
   'institution', 'research',       0.600, NULL),
  ('ranking_importance', 'Top and middle institutions are fine',
   'institution', 'employability',  0.700, NULL),
  ('ranking_importance', 'Top and middle institutions are fine',
   'institution', 'teaching',       0.600, NULL),
  ('ranking_importance', 'Top and middle institutions are fine',
   'institution', 'international',  0.500, NULL),
  ('ranking_importance', 'Top and middle institutions are fine',
   'institution', 'experience',     0.400, NULL),

  -- "All institutions irrespective of ranking"
  ('ranking_importance', 'All institution irrespective of ranking',
   'institution', 'prestige',       0.300, 'Low prestige weight — ranking matters little'),
  ('ranking_importance', 'All institution irrespective of ranking',
   'institution', 'research',       0.400, NULL),
  ('ranking_importance', 'All institution irrespective of ranking',
   'institution', 'employability',  0.700, 'Boost employability — still want good outcomes'),
  ('ranking_importance', 'All institution irrespective of ranking',
   'institution', 'teaching',       0.700, 'Boost teaching — quality of learning matters'),
  ('ranking_importance', 'All institution irrespective of ranking',
   'institution', 'international',  0.500, NULL),
  ('ranking_importance', 'All institution irrespective of ranking',
   'institution', 'experience',     0.500, NULL),

  -- ── career_importance ──────────────────────────────────────────────────────
  ('career_importance', 'Very strongly (placement driven institutions)',
   'institution', 'employability',  1.000, 'Max weight on employer reputation & employment outcomes'),
  ('career_importance', 'Very strongly (placement driven institutions)',
   'institution', 'prestige',       0.700, 'Employer prestige matters for placement'),
  ('career_importance', 'Very strongly (placement driven institutions)',
   'institution', 'research',       0.300, 'Research less important for placement-focused student'),
  ('career_importance', 'Very strongly (placement driven institutions)',
   'institution', 'teaching',       0.500, NULL),
  ('career_importance', 'Very strongly (placement driven institutions)',
   'institution', 'international',  0.400, NULL),
  ('career_importance', 'Very strongly (placement driven institutions)',
   'institution', 'experience',     0.600, 'Industry income / experience-related indicators'),

  ('career_importance', 'Moderately (academics driven institutions)',
   'institution', 'employability',  0.500, NULL),
  ('career_importance', 'Moderately (academics driven institutions)',
   'institution', 'prestige',       0.600, NULL),
  ('career_importance', 'Moderately (academics driven institutions)',
   'institution', 'research',       0.700, 'Research quality signals strong academics'),
  ('career_importance', 'Moderately (academics driven institutions)',
   'institution', 'teaching',       0.800, NULL),
  ('career_importance', 'Moderately (academics driven institutions)',
   'institution', 'international',  0.500, NULL),
  ('career_importance', 'Moderately (academics driven institutions)',
   'institution', 'experience',     0.400, NULL),

  ('career_importance', 'Not that much',
   'institution', 'employability',  0.300, NULL),
  ('career_importance', 'Not that much',
   'institution', 'prestige',       0.400, NULL),
  ('career_importance', 'Not that much',
   'institution', 'research',       0.700, 'Student likely research/academia focused'),
  ('career_importance', 'Not that much',
   'institution', 'teaching',       0.700, NULL),
  ('career_importance', 'Not that much',
   'institution', 'international',  0.600, NULL),
  ('career_importance', 'Not that much',
   'institution', 'experience',     0.400, NULL)

ON CONFLICT (quiz_question_key, answer_value, dimension, parameter_category)
  DO NOTHING;


-- -----------------------------------------------------------------------------
-- COUNTRY DIMENSION RULES
-- These weight named score components on the country_normalized view.
-- Component names: psw_score, pr_pathway_clarity_score, government_support_score,
--                  english_score, cost_score
-- -----------------------------------------------------------------------------

INSERT INTO core.scoring_rules
  (quiz_question_key, answer_value, dimension, parameter_category, weight, notes)
VALUES

  -- ── work_permit_importance ─────────────────────────────────────────────────
  ('work_permit_importance', 'Very strongly (3 years and above)',
   'country', 'psw_score', 1.000, 'Post-study work score weighted at max'),
  ('work_permit_importance', 'Wouldn''t mind (1 to 3 years)',
   'country', 'psw_score', 0.600, NULL),
  ('work_permit_importance', 'Not an immediate priority',
   'country', 'psw_score', 0.300, NULL),

  -- ── pr_importance ──────────────────────────────────────────────────────────
  ('pr_importance', 'Very strongly',
   'country', 'pr_pathway_clarity_score', 1.000, NULL),
  ('pr_importance', 'Wouldn''t mind',
   'country', 'pr_pathway_clarity_score', 0.600, NULL),
  ('pr_importance', 'Don''t care',
   'country', 'pr_pathway_clarity_score', 0.300, NULL),

  -- ── gov_support_importance ────────────────────────────────────────────────
  ('gov_support_importance', 'Very strongly',
   'country', 'government_support_score', 1.000, NULL),
  ('gov_support_importance', 'Wouldn''t mind',
   'country', 'government_support_score', 0.600, NULL),
  ('gov_support_importance', 'Don''t mind',
   'country', 'government_support_score', 0.300, NULL),

  -- ── english_preference ────────────────────────────────────────────────────
  ('english_preference', 'Yes',
   'country', 'english_score', 1.000, NULL),
  ('english_preference', 'Prefer but flexible',
   'country', 'english_score', 0.600, NULL),
  ('english_preference', 'No preference',
   'country', 'english_score', 0.300, NULL)

ON CONFLICT (quiz_question_key, answer_value, dimension, parameter_category)
  DO NOTHING;


-- -----------------------------------------------------------------------------
-- COURSE DIMENSION RULES
-- Weight named score components on course-level signals.
-- Component names: internship_score, scholarship_score
-- -----------------------------------------------------------------------------

INSERT INTO core.scoring_rules
  (quiz_question_key, answer_value, dimension, parameter_category, weight, notes)
VALUES

  -- ── internship_importance ─────────────────────────────────────────────────
  ('internship_importance', 'Very strongly',
   'course', 'internship_score', 1.000, NULL),
  ('internship_importance', 'Wouldn''t mind',
   'course', 'internship_score', 0.600, NULL),
  ('internship_importance', 'Don''t care',
   'course', 'internship_score', 0.300, NULL),

  -- ── scholarship_importance ────────────────────────────────────────────────
  ('scholarship_importance', 'Very strongly (more than 20% of tuition)',
   'course', 'scholarship_score', 1.000, NULL),
  ('scholarship_importance', 'Wouldn''t mind getting one (less than 20% of tuition or none)',
   'course', 'scholarship_score', 0.600, NULL),
  ('scholarship_importance', 'Don''t care',
   'course', 'scholarship_score', 0.300, NULL)

ON CONFLICT (quiz_question_key, answer_value, dimension, parameter_category)
  DO NOTHING;


-- =============================================================================
-- DESIGN NOTES: How scoring rules get applied (Phase 2 implementation guide)
--
-- Institution score computation (replaces current flat composite_ranking):
--
--   1. For a given student's answers, load all dimension=institution rules:
--        SELECT parameter_category, weight
--        FROM core.scoring_rules
--        WHERE quiz_question_key = 'ranking_importance'
--          AND answer_value = <student's answer>
--          AND dimension = 'institution'
--      Repeat for 'career_importance' and any future institution-dimension questions.
--      Merge: where multiple rules cover same parameter_category, take MAX weight
--      (or average — TBD).
--
--   2. For the university being scored, load sub-indicator normalized scores:
--        SELECT si.parameter_category, sis.normalized_score
--        FROM core.university_sub_indicator_scores sis
--        JOIN core.ranking_sub_indicators si ON si.id = sis.sub_indicator_id
--        JOIN core.university_rankings ur ON ur.id = sis.university_ranking_id
--        JOIN core.ranking_editions re ON re.id = ur.edition_id
--        WHERE ur.university_id = <id>
--          AND re.is_latest = TRUE
--          -- Prefer subject-specific edition if course.subject_id is set
--
--   3. Compute weighted average:
--        score = SUM(weight[cat] * avg(normalized_score[cat])) / SUM(weights used)
--
--   This replaces: rankingWeight * compositeRanking
--   With:         Σ(categoryWeight * categoryAvgScore) / Σ(categoryWeights)
--
-- Until sub-indicator data is loaded, the existing composite_score fallback
-- in public.university_composite_ranking continues to work unchanged.
-- =============================================================================
