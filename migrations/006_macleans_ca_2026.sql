-- =============================================================================
-- Migration 006: Maclean's University Rankings — Canada 2026
--                Comprehensive Category (15 universities)
--
-- Source file : attached_assets/MC_canada.xlsx
-- Framework   : Maclean's University Rankings (domestic, Canada)
-- Year        : 2026
-- Category    : Comprehensive (one of three Maclean's tiers;
--               Medical Doctoral and Primarily Undergraduate pending)
--
-- Sub-indicators stored as inverted rank scores so that higher raw_score
-- = better performance (rank 1 → raw_score 15, rank 15 → raw_score 1).
-- core.refresh_sub_indicator_scores() then normalises to 0-1.
--
-- Steps:
--   1. Upsert Maclean's framework into core.ranking_frameworks
--   2. Seed 12 sub-indicators
--   3. Insert 2026 edition
--   4. Insert university rankings (rank 1-15, Comprehensive category)
--   5. Insert sub-indicator scores (inverted ranks)
--   6. Refresh composite and sub-indicator normalised scores
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Ensure framework exists in core.ranking_frameworks
--    (may already exist if inserted via Supabase dashboard; ON CONFLICT is safe)
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_frameworks
  (id, name, short_name, type, region, trust_tier, subject_scope, notes)
VALUES
  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'Maclean''s University Rankings',
   'Maclean''s CA',
   'domestic',
   'CA',
   1,
   'overall',
   'Annual Canadian university ranking by Maclean''s magazine. Three categories: '
   'Medical Doctoral, Comprehensive, Primarily Undergraduate. '
   'Uses 12 sub-indicators including research dollars, faculty awards, reputation.')
ON CONFLICT (id) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 2. Sub-indicators (12 Maclean's Comprehensive indicators)
--    parameter_category mapped to our 6 canonical categories
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_sub_indicators
  (framework_id, indicator_key, indicator_label, official_weight_pct, parameter_category)
VALUES
  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'student_awards',          'Student Awards',                           NULL, 'prestige'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'student_faculty_ratio',   'Student/Faculty Ratio',                    NULL, 'teaching'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'faculty_awards',          'Faculty Awards',                           NULL, 'prestige'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'ssh_grants',              'Social Sciences & Humanities Grants',      NULL, 'research'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'medical_science_grants',  'Medical/Science Grants',                   NULL, 'research'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'total_research_dollars',  'Total Research Dollars',                   NULL, 'research'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'operating_budget',        'Operating Budget',                         NULL, 'experience'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'library_expenses',        'Library Expenses',                         NULL, 'experience'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'library_acquisitions',    'Library Acquisitions',                     NULL, 'experience'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'scholarships_bursaries',  'Scholarships & Bursaries',                 NULL, 'experience'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'student_services',        'Student Services',                         NULL, 'experience'),

  ('538510c2-8f4b-4e00-bca8-f0640f1bcf47',
   'reputational_survey',     'Reputational Survey',                      NULL, 'prestige')

ON CONFLICT (framework_id, indicator_key) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 3. 2026 Edition
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_editions (framework_id, year, is_latest)
VALUES ('538510c2-8f4b-4e00-bca8-f0640f1bcf47', 2026, TRUE)
ON CONFLICT (framework_id, year) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 4. University rankings — Comprehensive category 2026
--
-- Universities matched to core.universities by ILIKE on name.
-- composite_score left NULL; filled by refresh_composite_scores() below.
-- NOTE: All 15 universities are in the Maclean's Comprehensive tier.
--       Medical Doctoral & Primarily Undergraduate data pending.
-- -----------------------------------------------------------------------------

DO $$
DECLARE
  v_edition_id   UUID;
  v_university_id UUID;

  -- Maclean's 2026 Comprehensive rankings: (university name, rank)
  rankings TEXT[][] := ARRAY[
    ARRAY['Simon Fraser University',              '1'],
    ARRAY['University of Victoria',               '2'],
    ARRAY['University of Waterloo',               '3'],
    ARRAY['Carleton University',                  '4'],
    ARRAY['York University',                      '5'],
    ARRAY['University of Guelph',                 '6'],
    ARRAY['University of New Brunswick',          '7'],
    ARRAY['Memorial University of Newfoundland',  '8'],
    ARRAY['Concordia University',                 '9'],
    ARRAY['Toronto Metropolitan University',      '10'],
    ARRAY['University of Quebec in Montreal',     '11'],
    ARRAY['Wilfrid Laurier University',           '12'],
    ARRAY['Brock University',                     '13'],
    ARRAY['University of Regina',                 '14'],
    ARRAY['University of Windsor',                '15']
  ];

  rec TEXT[];
BEGIN
  -- Get edition id
  SELECT id INTO v_edition_id
  FROM core.ranking_editions
  WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47'
    AND year = 2026;

  FOREACH rec SLICE 1 IN ARRAY rankings LOOP
    -- Match university by name (case-insensitive)
    SELECT id INTO v_university_id
    FROM core.universities
    WHERE name ILIKE rec[1]
    LIMIT 1;

    IF v_university_id IS NULL THEN
      -- Try partial match for universities with alternate spellings
      SELECT id INTO v_university_id
      FROM core.universities
      WHERE name ILIKE '%' || rec[1] || '%'
      LIMIT 1;
    END IF;

    IF v_university_id IS NOT NULL THEN
      INSERT INTO core.university_rankings
        (university_id, edition_id, rank_number, composite_score, raw_overall_score)
      VALUES
        (v_university_id, v_edition_id, rec[2]::INTEGER, NULL, NULL)
      ON CONFLICT (university_id, edition_id) DO UPDATE
        SET rank_number = EXCLUDED.rank_number,
            updated_at  = NOW();
    ELSE
      RAISE NOTICE 'University not found in core.universities: %', rec[1];
    END IF;
  END LOOP;
END;
$$;


-- -----------------------------------------------------------------------------
-- 5. Sub-indicator scores (inverted rank: rank 1 → score 15, rank 15 → score 1)
--    Data: (university name, student_awards, student_faculty_ratio, faculty_awards,
--           ssh_grants, medical_science_grants, total_research_dollars,
--           operating_budget, library_expenses, library_acquisitions,
--           scholarships_bursaries, student_services, reputational_survey)
-- -----------------------------------------------------------------------------

DO $$
DECLARE
  v_edition_id UUID;
  v_max_rank   INTEGER := 15;  -- 15 universities in Comprehensive category

  -- Sub-indicator UUIDs (looked up by key)
  sid_student_awards        UUID;
  sid_student_faculty_ratio UUID;
  sid_faculty_awards        UUID;
  sid_ssh_grants            UUID;
  sid_medical_science_grants UUID;
  sid_total_research_dollars UUID;
  sid_operating_budget      UUID;
  sid_library_expenses      UUID;
  sid_library_acquisitions  UUID;
  sid_scholarships_bursaries UUID;
  sid_student_services      UUID;
  sid_reputational_survey   UUID;

  v_university_id UUID;
  v_ranking_id    UUID;

  -- Raw data: (name, sa, sfr, fa, ssh, msg, trd, ob, le, la, sb, ss, rs)
  -- Values are the published sub-ranks (1=best, 15=worst)
  raw_data TEXT[][] := ARRAY[
    ARRAY['Simon Fraser University',             '1',  '3',  '4',  '3',  '1',  '3',  '1',  '1',  '1',  '7',  '2',  '2'],
    ARRAY['University of Victoria',              '2',  '4',  '1',  '7',  '3',  '2',  '4',  '3',  '4',  '2',  '8',  '3'],
    ARRAY['University of Waterloo',              '3',  '8',  '2',  '2',  '2',  '5',  '8', '15',  '8',  '1', '12',  '1'],
    ARRAY['Carleton University',                 '6',  '7',  '3',  '1',  '6',  '6', '10',  '5', '13',  '4',  '8',  '8'],
    ARRAY['York University',                     '8',  '9',  '6',  '4',  '7', '13',  '5',  '9',  '7',  '3', '10',  '4'],
    ARRAY['University of Guelph',                '8', '15',  '5',  '9',  '4',  '1', '13', '12',  '2',  '8',  '7',  '6'],
    ARRAY['University of New Brunswick',         '5',  '1', '14', '15', '14',  '7',  '3',  '2',  '6', '13', '11', '11'],
    ARRAY['Memorial University of Newfoundland', '10',  '2',  '8', '12', '15',  '4',  '2', '12',  '5',  '8', '14', '10'],
    ARRAY['Concordia University',                '7',  '9', '10',  '8',  '8',  '9', '14',  '7', '10',  '8', '13',  '5'],
    ARRAY['Toronto Metropolitan University',    '14', '12', '13',  '5', '12', '10',  '6', '14', '11', '14',  '1',  '7'],
    ARRAY['University of Quebec in Montreal',    '3',  '5',  '8', '10',  '9',  '8', '12',  '9', '15', '12', '15', '13'],
    ARRAY['Wilfrid Laurier University',         '11', '14', '14',  '6',  '5', '14', '11',  '7', '12',  '6',  '3',  '9'],
    ARRAY['Brock University',                   '13',  '9', '11', '11', '13', '15',  '9',  '6',  '3',  '5',  '5', '12'],
    ARRAY['University of Regina',               '15',  '6', '12', '14', '10', '11',  '7', '11', '14', '11',  '6', '14'],
    ARRAY['University of Windsor',              '12', '13',  '7', '13', '10', '12', '15',  '4',  '9', '15',  '4', '15']
  ];

  rec TEXT[];
BEGIN
  -- Get edition id
  SELECT id INTO v_edition_id
  FROM core.ranking_editions
  WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47'
    AND year = 2026;

  -- Look up sub-indicator IDs
  SELECT id INTO sid_student_awards        FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'student_awards';
  SELECT id INTO sid_student_faculty_ratio FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'student_faculty_ratio';
  SELECT id INTO sid_faculty_awards        FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'faculty_awards';
  SELECT id INTO sid_ssh_grants            FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'ssh_grants';
  SELECT id INTO sid_medical_science_grants FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'medical_science_grants';
  SELECT id INTO sid_total_research_dollars FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'total_research_dollars';
  SELECT id INTO sid_operating_budget      FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'operating_budget';
  SELECT id INTO sid_library_expenses      FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'library_expenses';
  SELECT id INTO sid_library_acquisitions  FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'library_acquisitions';
  SELECT id INTO sid_scholarships_bursaries FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'scholarships_bursaries';
  SELECT id INTO sid_student_services      FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'student_services';
  SELECT id INTO sid_reputational_survey   FROM core.ranking_sub_indicators WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND indicator_key = 'reputational_survey';

  FOREACH rec SLICE 1 IN ARRAY raw_data LOOP
    -- Resolve university
    SELECT id INTO v_university_id
    FROM core.universities
    WHERE name ILIKE rec[1]
    LIMIT 1;

    IF v_university_id IS NULL THEN
      SELECT id INTO v_university_id
      FROM core.universities
      WHERE name ILIKE '%' || rec[1] || '%'
      LIMIT 1;
    END IF;

    IF v_university_id IS NULL THEN
      RAISE NOTICE 'Skipping sub-indicators — university not found: %', rec[1];
      CONTINUE;
    END IF;

    -- Get the university_rankings row id for this edition
    SELECT id INTO v_ranking_id
    FROM core.university_rankings
    WHERE university_id = v_university_id
      AND edition_id    = v_edition_id;

    IF v_ranking_id IS NULL THEN
      RAISE NOTICE 'No ranking row for university: %', rec[1];
      CONTINUE;
    END IF;

    -- Insert inverted ranks as raw_score (rank 1 → 15, rank 15 → 1)
    INSERT INTO core.university_sub_indicator_scores
      (university_ranking_id, sub_indicator_id, raw_score)
    VALUES
      (v_ranking_id, sid_student_awards,         v_max_rank - rec[2]::INTEGER  + 1),
      (v_ranking_id, sid_student_faculty_ratio,  v_max_rank - rec[3]::INTEGER  + 1),
      (v_ranking_id, sid_faculty_awards,         v_max_rank - rec[4]::INTEGER  + 1),
      (v_ranking_id, sid_ssh_grants,             v_max_rank - rec[5]::INTEGER  + 1),
      (v_ranking_id, sid_medical_science_grants, v_max_rank - rec[6]::INTEGER  + 1),
      (v_ranking_id, sid_total_research_dollars, v_max_rank - rec[7]::INTEGER  + 1),
      (v_ranking_id, sid_operating_budget,       v_max_rank - rec[8]::INTEGER  + 1),
      (v_ranking_id, sid_library_expenses,       v_max_rank - rec[9]::INTEGER  + 1),
      (v_ranking_id, sid_library_acquisitions,   v_max_rank - rec[10]::INTEGER + 1),
      (v_ranking_id, sid_scholarships_bursaries, v_max_rank - rec[11]::INTEGER + 1),
      (v_ranking_id, sid_student_services,       v_max_rank - rec[12]::INTEGER + 1),
      (v_ranking_id, sid_reputational_survey,    v_max_rank - rec[13]::INTEGER + 1)
    ON CONFLICT (university_ranking_id, sub_indicator_id) DO UPDATE
      SET raw_score  = EXCLUDED.raw_score,
          updated_at = NOW();
  END LOOP;
END;
$$;


-- -----------------------------------------------------------------------------
-- 6. Refresh normalised scores
-- -----------------------------------------------------------------------------

SELECT core.refresh_composite_scores(
  (SELECT id FROM core.ranking_editions
   WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND year = 2026)
);

SELECT core.refresh_sub_indicator_scores(
  (SELECT id FROM core.ranking_editions
   WHERE framework_id = '538510c2-8f4b-4e00-bca8-f0640f1bcf47' AND year = 2026)
);
