-- =============================================================================
-- Migration 004: Subject Rankings Infrastructure
--
-- Problem being solved:
--   core.courses.field_category is too broad (10 buckets). QS/THE subject
--   rankings operate at a much finer granularity (55 QS subjects, 11 THE subjects).
--   A CS student should be matched against QS Computer Science rank, not a
--   blended "engineering & tech" composite. This migration adds the subject
--   taxonomy and hooks it into the existing ranking infrastructure.
--
-- What this adds:
--   1. core.subjects          — canonical subject taxonomy (~27 subjects)
--   2. subject_id FK on core.ranking_editions   — makes QS Subject editions
--                                                 per-subject (not one blob)
--   3. subject_id FK on core.courses            — links each course to its
--                                                 precise subject (nullable,
--                                                 backward-compatible)
--   4. QS Subject sub-indicators seed           — 4 indicators QS uses for
--                                                 subject-level rankings
--   5. public.university_subject_ranking VIEW   — fast lookup: university rank
--                                                 in a specific subject
--   6. core.get_subject_ranking_score()         — helper fn used by app layer:
--                                                 returns subject score if
--                                                 available, else overall score
--
-- Backward compatibility:
--   - All new columns are nullable; existing rows unaffected
--   - public.university_composite_ranking view unchanged
--   - Existing field_category col on courses remains; subject_id is additive
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. core.subjects  — canonical subject taxonomy
--    field_category_hint links to existing broad categories so auto-assignment
--    scripts can populate subject_id from field_category as a starting point
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.subjects (
  id                    UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
  name                  TEXT    NOT NULL UNIQUE,
  short_name            TEXT    NOT NULL,
  broad_area            TEXT    NOT NULL
                          CHECK (broad_area IN (
                            'Engineering & Technology',
                            'Natural Sciences',
                            'Life Sciences & Medicine',
                            'Business & Economics',
                            'Social Sciences & Management',
                            'Arts & Design',
                            'Agriculture & Environment',
                            'Hospitality & Tourism'
                          )),
  -- Maps back to the existing field_category values for backward-compat
  field_category_hint   TEXT,
  -- Slug keys for matching against QS/THE source data files
  qs_subject_key        TEXT,   -- e.g. 'computer-science-information-systems'
  the_subject_key       TEXT,   -- e.g. 'computer-science'
  active                BOOLEAN NOT NULL DEFAULT TRUE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE core.subjects IS
  'Canonical subject taxonomy bridging broad field_category to QS/THE subject-level rankings.
   subject_id on courses + ranking_editions enables per-subject rank lookups in scoring.';

COMMENT ON COLUMN core.subjects.field_category_hint IS
  'Matches the field_category value on core.courses. Used for bulk auto-assigning
   subject_id to courses that only have a broad field_category set.';

COMMENT ON COLUMN core.subjects.qs_subject_key IS
  'URL slug used in QS subject ranking data files (e.g. computer-science-information-systems).
   Use this to join when importing QS subject ranking CSVs.';


-- -----------------------------------------------------------------------------
-- Seed: ~27 subjects aligned with QS World University Rankings by Subject
-- -----------------------------------------------------------------------------

INSERT INTO core.subjects
  (id, name, short_name, broad_area, field_category_hint, qs_subject_key, the_subject_key)
VALUES

  -- Engineering & Technology
  ('10000000-0000-0000-0000-000000000001',
   'Computer Science & Information Systems',
   'CS & IT',
   'Engineering & Technology',
   'engineering & tech',
   'computer-science-information-systems',
   'computer-science'),

  ('10000000-0000-0000-0000-000000000002',
   'Electrical & Electronic Engineering',
   'EEE',
   'Engineering & Technology',
   'engineering & tech',
   'electrical-electronic-engineering',
   'engineering'),

  ('10000000-0000-0000-0000-000000000003',
   'Mechanical, Aeronautical & Manufacturing Engineering',
   'Mech Eng',
   'Engineering & Technology',
   'engineering & tech',
   'mechanical-aeronautical-manufacturing-engineering',
   'engineering'),

  ('10000000-0000-0000-0000-000000000004',
   'Civil & Structural Engineering',
   'Civil Eng',
   'Engineering & Technology',
   'engineering & tech',
   'civil-structural-engineering',
   'engineering'),

  ('10000000-0000-0000-0000-000000000005',
   'Chemical Engineering',
   'Chem Eng',
   'Engineering & Technology',
   'engineering & tech',
   'chemical-engineering',
   'engineering'),

  ('10000000-0000-0000-0000-000000000006',
   'Data Science & Artificial Intelligence',
   'DS & AI',
   'Engineering & Technology',
   'engineering & tech',
   'data-science-artificial-intelligence',
   'computer-science'),

  ('10000000-0000-0000-0000-000000000007',
   'Materials Science',
   'Materials',
   'Engineering & Technology',
   'engineering & tech',
   'materials-science',
   'engineering'),

  -- Business & Economics
  ('10000000-0000-0000-0000-000000000011',
   'Business & Management Studies',
   'Business',
   'Business & Economics',
   'business, management and economics',
   'business-management-studies',
   'business-economics'),

  ('10000000-0000-0000-0000-000000000012',
   'Accounting & Finance',
   'Accounting',
   'Business & Economics',
   'business, management and economics',
   'accounting-finance',
   'business-economics'),

  ('10000000-0000-0000-0000-000000000013',
   'Economics & Econometrics',
   'Economics',
   'Business & Economics',
   'business, management and economics',
   'economics-econometrics',
   'business-economics'),

  -- Natural Sciences
  ('10000000-0000-0000-0000-000000000021',
   'Mathematics & Statistics',
   'Maths',
   'Natural Sciences',
   'science & applied science',
   'mathematics',
   'physical-sciences'),

  ('10000000-0000-0000-0000-000000000022',
   'Physics & Astronomy',
   'Physics',
   'Natural Sciences',
   'science & applied science',
   'physics-astronomy',
   'physical-sciences'),

  ('10000000-0000-0000-0000-000000000023',
   'Chemistry',
   'Chemistry',
   'Natural Sciences',
   'science & applied science',
   'chemistry',
   'physical-sciences'),

  ('10000000-0000-0000-0000-000000000024',
   'Environmental Sciences',
   'Env Science',
   'Natural Sciences',
   'agriculture, sustainability & environmental studies',
   'environmental-sciences',
   'physical-sciences'),

  -- Life Sciences & Medicine
  ('10000000-0000-0000-0000-000000000031',
   'Medicine',
   'Medicine',
   'Life Sciences & Medicine',
   'medicine, health and life science',
   'medicine',
   'clinical-health'),

  ('10000000-0000-0000-0000-000000000032',
   'Pharmacy & Pharmacology',
   'Pharmacy',
   'Life Sciences & Medicine',
   'medicine, health and life science',
   'pharmacy-pharmacology',
   'clinical-health'),

  ('10000000-0000-0000-0000-000000000033',
   'Biological Sciences',
   'Bio Sciences',
   'Life Sciences & Medicine',
   'medicine, health and life science',
   'biological-sciences',
   'life-sciences'),

  ('10000000-0000-0000-0000-000000000034',
   'Psychology',
   'Psychology',
   'Life Sciences & Medicine',
   'medicine, health and life science',
   'psychology',
   'psychology'),

  ('10000000-0000-0000-0000-000000000035',
   'Nursing & Midwifery',
   'Nursing',
   'Life Sciences & Medicine',
   'medicine, health and life science',
   'nursing',
   'clinical-health'),

  -- Social Sciences & Management
  ('10000000-0000-0000-0000-000000000041',
   'Law & Legal Studies',
   'Law',
   'Social Sciences & Management',
   'law, public policy & governance',
   'law',
   'social-sciences'),

  ('10000000-0000-0000-0000-000000000042',
   'Politics & International Studies',
   'Politics',
   'Social Sciences & Management',
   'social science & humanities',
   'politics-international-studies',
   'social-sciences'),

  ('10000000-0000-0000-0000-000000000043',
   'Media & Communication Studies',
   'Media',
   'Social Sciences & Management',
   'social science & humanities',
   'communication-media-studies',
   'social-sciences'),

  ('10000000-0000-0000-0000-000000000044',
   'Education & Training',
   'Education',
   'Social Sciences & Management',
   'education & teaching',
   'education-training',
   'education'),

  -- Arts & Design
  ('10000000-0000-0000-0000-000000000051',
   'Architecture & Built Environment',
   'Architecture',
   'Arts & Design',
   'arts, design & creative studies',
   'architecture-built-environment',
   'arts-humanities'),

  ('10000000-0000-0000-0000-000000000052',
   'Art & Design',
   'Art & Design',
   'Arts & Design',
   'arts, design & creative studies',
   'art-design',
   'arts-humanities'),

  -- Agriculture & Environment
  ('10000000-0000-0000-0000-000000000061',
   'Agriculture & Forestry',
   'Agriculture',
   'Agriculture & Environment',
   'agriculture, sustainability & environmental studies',
   'agriculture-forestry',
   'life-sciences'),

  -- Hospitality & Tourism
  ('10000000-0000-0000-0000-000000000071',
   'Hospitality & Leisure Management',
   'Hospitality',
   'Hospitality & Tourism',
   'hospitality, tourism & service industry',
   'hospitality-leisure',
   NULL)

ON CONFLICT (name) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 2. Add subject_id to core.ranking_editions
--    NULL = overall ranking (QS World, THE World, ARWU, NIRF, etc.)
--    Non-NULL = subject-specific ranking (QS Subject by discipline)
--
-- Old UNIQUE was (framework_id, year).
-- New UNIQUE must be (framework_id, year, subject_id) treating NULLs as equal
-- so that two "QS World 2024 overall" editions can't coexist.
-- NULLS NOT DISTINCT requires PostgreSQL 15+ (Supabase default).
-- -----------------------------------------------------------------------------

ALTER TABLE core.ranking_editions
  ADD COLUMN IF NOT EXISTS subject_id UUID REFERENCES core.subjects(id);

COMMENT ON COLUMN core.ranking_editions.subject_id IS
  'NULL for overall rankings. Set to a core.subjects.id for subject-level ranking editions
   (e.g. QS Subject CS 2024). Enables per-subject composite scores on university_rankings.';

-- Drop old unique constraint (name may vary; handle both possibilities)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_schema = 'core'
      AND table_name   = 'ranking_editions'
      AND constraint_name = 'ranking_editions_framework_id_year_key'
  ) THEN
    ALTER TABLE core.ranking_editions
      DROP CONSTRAINT ranking_editions_framework_id_year_key;
  END IF;
END;
$$;

-- New unique index: treats NULL subject_id as equal (NULLS NOT DISTINCT = PG15+)
DROP INDEX IF EXISTS core.idx_ranking_editions_framework_year_subject;
CREATE UNIQUE INDEX idx_ranking_editions_framework_year_subject
  ON core.ranking_editions (framework_id, year, subject_id)
  NULLS NOT DISTINCT;


-- -----------------------------------------------------------------------------
-- 3. Add subject_id to core.courses
--    More granular than field_category; NULL is fine (backward-compatible).
--    Auto-assign script can populate using field_category_hint mapping.
-- -----------------------------------------------------------------------------

ALTER TABLE core.courses
  ADD COLUMN IF NOT EXISTS subject_id UUID REFERENCES core.subjects(id);

COMMENT ON COLUMN core.courses.subject_id IS
  'Links to core.subjects for subject-level ranking lookups. More precise than field_category.
   NULL = not yet assigned. Populate via: UPDATE core.courses SET subject_id = s.id
   FROM core.subjects s WHERE courses.field_category = s.field_category_hint
   (then refine for courses that span multiple subjects).';

CREATE INDEX IF NOT EXISTS idx_courses_subject ON core.courses(subject_id);


-- -----------------------------------------------------------------------------
-- 4. QS Subject Rankings sub-indicators
--    QS uses 4 indicators for subject rankings (weights vary by subject;
--    stored here as typical/average weights across most disciplines).
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_sub_indicators
  (framework_id, indicator_key, indicator_label, official_weight_pct, parameter_category)
VALUES
  -- Academic Reputation (40–50% typical)
  ('00000000-0000-0000-0000-000000000004',
   'academic_reputation',    'Academic Reputation (Subject)',       45.0, 'prestige'),

  -- Employer Reputation (10–30% typical)
  ('00000000-0000-0000-0000-000000000004',
   'employer_reputation',    'Employer Reputation (Subject)',       20.0, 'employability'),

  -- Citations per Paper (10–25% typical) — research impact per published paper
  ('00000000-0000-0000-0000-000000000004',
   'citations_per_paper',    'Citations per Paper',                 25.0, 'research'),

  -- H-Index Citations (10–25% typical) — breadth + depth of research impact
  ('00000000-0000-0000-0000-000000000004',
   'h_index_citations',      'H-Index Citations',                   10.0, 'research')

ON CONFLICT (framework_id, indicator_key) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 5. public.university_subject_ranking VIEW
--    Exposes each university's rank in a specific subject (latest edition only).
--    Consumed by index.js to build the subjectRankMap at query time.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW public.university_subject_ranking AS
SELECT
  ur.university_id,
  u.name                  AS university_name,
  re.subject_id,
  s.name                  AS subject_name,
  s.broad_area,
  s.field_category_hint,
  rf.short_name           AS framework,
  re.year,
  ur.rank_number,
  ur.rank_band,
  ur.composite_score
FROM core.university_rankings  ur
JOIN core.universities         u   ON u.id   = ur.university_id
JOIN core.ranking_editions     re  ON re.id  = ur.edition_id
JOIN core.ranking_frameworks   rf  ON rf.id  = re.framework_id
JOIN core.subjects              s   ON s.id  = re.subject_id
WHERE re.is_latest      = TRUE
  AND rf.active         = TRUE
  AND re.subject_id     IS NOT NULL
  AND ur.composite_score IS NOT NULL
ORDER BY s.name, ur.composite_score DESC NULLS LAST;

COMMENT ON VIEW public.university_subject_ranking IS
  'Per-subject university rankings (latest edition only). Used by /recommend endpoint
   to look up university subject rank when a course has a subject_id set.
   Falls back to public.university_composite_ranking when no subject data exists.';


-- -----------------------------------------------------------------------------
-- 6. core.get_subject_ranking_score(university_id, subject_id)
--    Returns the best subject-specific composite score if available,
--    otherwise NULL (caller falls back to overall composite score).
--    Used by the /recommend scoring logic.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION core.get_subject_ranking_score(
  p_university_id UUID,
  p_subject_id    UUID
)
RETURNS NUMERIC(6,4) LANGUAGE sql STABLE AS $$
  SELECT ur.composite_score
  FROM   core.university_rankings  ur
  JOIN   core.ranking_editions     re ON re.id = ur.edition_id
  JOIN   core.ranking_frameworks   rf ON rf.id = re.framework_id
  WHERE  ur.university_id = p_university_id
    AND  re.subject_id    = p_subject_id
    AND  re.is_latest     = TRUE
    AND  rf.active        = TRUE
    AND  ur.composite_score IS NOT NULL
  ORDER  BY rf.trust_tier ASC   -- prefer domestic (tier 1) over global (tier 2)
  LIMIT  1;
$$;

COMMENT ON FUNCTION core.get_subject_ranking_score IS
  'Returns the best available subject-specific composite_score (0-1) for a university
   in a given subject. Returns NULL if no subject ranking exists (use overall score as fallback).
   Prefers lower trust_tier (higher reliability) when multiple frameworks cover the same subject.';


-- =============================================================================
-- USAGE NOTES
--
-- Loading subject ranking data (e.g. QS CS 2024):
--   1. INSERT INTO core.ranking_editions
--        (framework_id, year, subject_id, published_date)
--      VALUES
--        ('00000000-0000-0000-0000-000000000004', 2024,
--         '10000000-0000-0000-0000-000000000001', '2024-04-01');
--      → is_latest trigger fires automatically
--
--   2. Bulk INSERT into core.university_rankings
--        (university_id, edition_id, rank_number, raw_overall_score)
--      → same structure as overall rankings
--
--   3. SELECT core.refresh_composite_scores('<edition_id>');
--
--   4. public.university_subject_ranking view updates automatically
--
-- Auto-assigning subject_id to courses from field_category:
--   UPDATE core.courses c
--   SET    subject_id = s.id
--   FROM   core.subjects s
--   WHERE  c.field_category = s.field_category_hint
--     AND  c.subject_id IS NULL;
--   -- This sets the most common subject per field_category.
--   -- For multi-subject fields (e.g. 'engineering & tech'), refine manually
--   -- or use program name keyword matching.
-- =============================================================================
