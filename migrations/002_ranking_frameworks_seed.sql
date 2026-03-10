-- =============================================================================
-- Migration 002: Seed Ranking Frameworks & Sub-Indicators
--
-- Frameworks seeded:
--   Global  : QS World, THE World, ARWU (Shanghai)
--   Global  : QS Subject Rankings (subject_scope = 'subject')
--   Domestic: NIRF (India), REF/TEF (UK), US News (USA), Good Universities Guide (AU)
--   Govt    : British Council, Campus France, DAAD (Germany)
--
-- Sub-indicators seeded for: QS World, THE World, ARWU
-- Others TBD when data is provided.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Ranking Frameworks
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_frameworks
  (id, name, short_name, type, region, trust_tier, subject_scope, notes)
VALUES

  -- Global rankings (trust_tier = 2)
  ('00000000-0000-0000-0000-000000000001',
   'QS World University Rankings',         'QS World',     'global', NULL, 2, 'overall',
   'Published by Quacquarelli Symonds. Uses 6 indicators. Widely cited globally.'),

  ('00000000-0000-0000-0000-000000000002',
   'Times Higher Education World University Rankings', 'THE World', 'global', NULL, 2, 'overall',
   'Published by Times Higher Education. Uses 5 pillars, 18 indicators.'),

  ('00000000-0000-0000-0000-000000000003',
   'Academic Ranking of World Universities (Shanghai)', 'ARWU',    'global', NULL, 2, 'overall',
   'Published by ShanghaiRanking Consultancy. Heavily research/Nobel-prize weighted.'),

  ('00000000-0000-0000-0000-000000000004',
   'QS World University Rankings by Subject',          'QS Subject', 'global', NULL, 2, 'subject',
   'Subject-level rankings by QS. Shares methodology with QS World but at discipline level.'),

  -- Domestic rankings (trust_tier = 1)
  ('00000000-0000-0000-0000-000000000010',
   'National Institutional Ranking Framework',         'NIRF',     'domestic', 'IN', 1, 'overall',
   'Published by Ministry of Education, India. Covers Teaching, Research, Outreach, Perception.'),

  ('00000000-0000-0000-0000-000000000011',
   'Research Excellence Framework',                   'REF',      'domestic', 'GB', 1, 'overall',
   'UK govt assessment of research quality in higher education institutions.'),

  ('00000000-0000-0000-0000-000000000012',
   'US News Best Colleges Rankings',                  'US News',  'domestic', 'US', 1, 'overall',
   'Published by US News & World Report. Widely used by US students.'),

  ('00000000-0000-0000-0000-000000000013',
   'Good Universities Guide',                         'GUG AU',   'domestic', 'AU', 1, 'overall',
   'Published by Hobsons Australia. Graduate outcomes and student experience focused.'),

  ('00000000-0000-0000-0000-000000000014',
   'Complete University Guide',                       'CUG UK',   'domestic', 'GB', 1, 'overall',
   'UK-focused. Entry standards, student satisfaction, graduate prospects.'),

  -- Govt-endorsed rankings (trust_tier = 3)
  ('00000000-0000-0000-0000-000000000020',
   'British Council Partner Institutions',            'BC Partners', 'govt', 'GB', 3, 'overall',
   'British Council endorsed/recognised institution list.'),

  ('00000000-0000-0000-0000-000000000021',
   'Campus France Partner Universities',              'Campus France', 'govt', 'FR', 3, 'overall',
   'French government recognised partner universities for international students.'),

  ('00000000-0000-0000-0000-000000000022',
   'DAAD Partner Universities',                       'DAAD',     'govt', 'DE', 3, 'overall',
   'German Academic Exchange Service partner institutions.')

ON CONFLICT (id) DO NOTHING;


-- -----------------------------------------------------------------------------
-- Sub-Indicators: QS World University Rankings
-- Official weights sourced from QS methodology (2024 edition)
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_sub_indicators
  (framework_id, indicator_key, indicator_label, official_weight_pct, parameter_category)
VALUES
  -- Academic Reputation (40%) — survey of 130k+ academics
  ('00000000-0000-0000-0000-000000000001',
   'academic_reputation',      'Academic Reputation',          40.0, 'prestige'),

  -- Employer Reputation (10%) — survey of 75k+ employers
  ('00000000-0000-0000-0000-000000000001',
   'employer_reputation',      'Employer Reputation',          10.0, 'employability'),

  -- Faculty Student Ratio (20%) — proxy for teaching quality
  ('00000000-0000-0000-0000-000000000001',
   'faculty_student_ratio',    'Faculty/Student Ratio',        20.0, 'teaching'),

  -- Citations per Faculty (20%) — research impact proxy
  ('00000000-0000-0000-0000-000000000001',
   'citations_per_faculty',    'Citations per Faculty',        20.0, 'research'),

  -- International Faculty Ratio (5%)
  ('00000000-0000-0000-0000-000000000001',
   'international_faculty',    'International Faculty Ratio',   5.0, 'international'),

  -- International Student Ratio (5%)
  ('00000000-0000-0000-0000-000000000001',
   'international_students',   'International Student Ratio',   5.0, 'international'),

  -- Employment Outcomes (added in QS 2024 — 5%, replaces some older weight)
  -- NOTE: QS 2024 added this; total weight redistributed. Stored for completeness.
  ('00000000-0000-0000-0000-000000000001',
   'employment_outcomes',      'Employment Outcomes',           5.0, 'employability'),

  -- Sustainability (added in QS 2024 — part of score)
  ('00000000-0000-0000-0000-000000000001',
   'sustainability',           'Sustainability Score',          NULL, 'experience');


-- -----------------------------------------------------------------------------
-- Sub-Indicators: THE World University Rankings
-- Official weights sourced from THE methodology (2024 edition)
-- 5 pillars, 18 performance indicators
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_sub_indicators
  (framework_id, indicator_key, indicator_label, official_weight_pct, parameter_category)
VALUES
  -- TEACHING pillar (29.5% total)
  ('00000000-0000-0000-0000-000000000002',
   'teaching_reputation',         'Teaching Reputation Survey',      15.0, 'teaching'),
  ('00000000-0000-0000-0000-000000000002',
   'staff_to_student_ratio',      'Staff to Student Ratio',           4.5, 'teaching'),
  ('00000000-0000-0000-0000-000000000002',
   'doctorate_to_bachelor_ratio', 'Doctorate to Bachelor Ratio',      2.25,'teaching'),
  ('00000000-0000-0000-0000-000000000002',
   'doctorate_to_academic_staff', 'Doctorates Awarded to Academic Staff Ratio', 6.0, 'teaching'),
  ('00000000-0000-0000-0000-000000000002',
   'institutional_income',        'Institutional Income',             2.25, 'teaching'),

  -- RESEARCH ENVIRONMENT pillar (29% total)
  ('00000000-0000-0000-0000-000000000002',
   'research_reputation',         'Research Reputation Survey',      18.0, 'research'),
  ('00000000-0000-0000-0000-000000000002',
   'research_income',             'Research Income',                  6.0, 'research'),
  ('00000000-0000-0000-0000-000000000002',
   'research_productivity',       'Research Productivity',            6.0, 'research'),

  -- RESEARCH QUALITY pillar (30% total) — citation based
  ('00000000-0000-0000-0000-000000000002',
   'citation_impact',             'Citation Impact',                 15.0, 'research'),
  ('00000000-0000-0000-0000-000000000002',
   'research_strength',           'Research Strength',                5.0, 'research'),
  ('00000000-0000-0000-0000-000000000002',
   'research_excellence',         'Research Excellence',              5.0, 'research'),
  ('00000000-0000-0000-0000-000000000002',
   'research_influence',          'Research Influence',               5.0, 'research'),

  -- INTERNATIONAL OUTLOOK pillar (7.5% total)
  ('00000000-0000-0000-0000-000000000002',
   'intl_student_ratio',          'International Student Ratio',      2.5, 'international'),
  ('00000000-0000-0000-0000-000000000002',
   'intl_staff_ratio',            'International Staff Ratio',        2.5, 'international'),
  ('00000000-0000-0000-0000-000000000002',
   'intl_collaboration',          'International Collaboration',      2.5, 'international'),

  -- INDUSTRY pillar (4% total)
  ('00000000-0000-0000-0000-000000000002',
   'industry_income',             'Industry Income',                  4.0, 'employability'),

  -- PATENTS (part of research quality in some editions)
  ('00000000-0000-0000-0000-000000000002',
   'patents',                     'Patents',                          NULL, 'research');


-- -----------------------------------------------------------------------------
-- Sub-Indicators: ARWU (Shanghai Rankings)
-- Official weights sourced from ARWU methodology
-- Heavily Nobel-prize and citation weighted — best proxy for elite research
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_sub_indicators
  (framework_id, indicator_key, indicator_label, official_weight_pct, parameter_category)
VALUES
  -- Alumni Nobel & Fields (10%)
  ('00000000-0000-0000-0000-000000000003',
   'alumni_award',      'Alumni of Institution Winning Nobel/Fields', 10.0, 'prestige'),

  -- Staff Nobel & Fields (20%)
  ('00000000-0000-0000-0000-000000000003',
   'award',             'Staff of Institution Winning Nobel/Fields',  20.0, 'prestige'),

  -- Highly Cited Researchers (20%)
  ('00000000-0000-0000-0000-000000000003',
   'hici',              'Highly Cited Researchers (HiCi)',            20.0, 'research'),

  -- Papers in Nature & Science (20%)
  ('00000000-0000-0000-0000-000000000003',
   'ns',                'Papers Published in Nature & Science',       20.0, 'research'),

  -- Papers indexed in SCIE & SSCI (20%)
  ('00000000-0000-0000-0000-000000000003',
   'pub',               'Papers in Science Citation Index',           20.0, 'research'),

  -- Per Capita Academic Performance (10%)
  ('00000000-0000-0000-0000-000000000003',
   'pcp',               'Per Capita Academic Performance',            10.0, 'research');


-- -----------------------------------------------------------------------------
-- Sub-Indicators: NIRF (India)
-- Official weights sourced from NIRF methodology
-- -----------------------------------------------------------------------------

INSERT INTO core.ranking_sub_indicators
  (framework_id, indicator_key, indicator_label, official_weight_pct, parameter_category)
VALUES
  -- Teaching Learning & Resources (30%)
  ('00000000-0000-0000-0000-000000000010',
   'teaching_learning_resources', 'Teaching, Learning & Resources',  30.0, 'teaching'),

  -- Research & Professional Practice (30%)
  ('00000000-0000-0000-0000-000000000010',
   'research_professional',       'Research & Professional Practice', 30.0, 'research'),

  -- Graduation Outcomes (20%)
  ('00000000-0000-0000-0000-000000000010',
   'graduation_outcomes',         'Graduation Outcomes',              20.0, 'employability'),

  -- Outreach & Inclusivity (10%)
  ('00000000-0000-0000-0000-000000000010',
   'outreach_inclusivity',        'Outreach & Inclusivity',           10.0, 'experience'),

  -- Peer Perception (10%)
  ('00000000-0000-0000-0000-000000000010',
   'peer_perception',             'Peer Perception',                  10.0, 'prestige');
