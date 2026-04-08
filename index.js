require("dotenv").config();
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const cheerio = require("cheerio");
const puppeteer = require("puppeteer");

// Currency conversion rates to USD
const CAD_TO_USD = 0.74;
const GBP_TO_USD = 1.27;
const EUR_TO_USD = 1.08; // covers Germany and Ireland
const AUD_TO_USD = 0.65; // covers Australia
const USD_TO_USD = 1.0;

const CURRENCY_TO_USD = {
  CAD: CAD_TO_USD,
  GBP: GBP_TO_USD,
  EUR: EUR_TO_USD,
  AUD: AUD_TO_USD,
  USD: USD_TO_USD,
};

let sharedBrowser = null;
async function getBrowser() {
  if (!sharedBrowser || !sharedBrowser.connected) {
    sharedBrowser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--memory-pressure-off',
        '--max_old_space_size=512'
      ]
    });
  }
  return sharedBrowser;
}

async function fetchWithPuppeteer(url) {
  const browser = await getBrowser();
  try {
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

    // Accept cookies if present
    try {
      await new Promise((r) => setTimeout(r, 2000));
      const attrSelectors = [
        'button[id*="accept"]',
        'button[class*="accept"]',
        'button[id*="cookie"]',
        'button[class*="cookie"]',
        'a[id*="accept"]',
        "#onetrust-accept-btn-handler",
        ".cookie-accept",
        '[aria-label*="accept"]',
      ];
      let clicked = false;
      for (const selector of attrSelectors) {
        try {
          const btn = await page.$(selector);
          if (btn) {
            await btn.click();
            await new Promise((r) => setTimeout(r, 800));
            console.log(`[puppeteer] Accepted cookies with: ${selector}`);
            clicked = true;
            break;
          }
        } catch (e) {
          console.warn(`[puppeteer] Cookie selector failed (${selector}): ${e.message}`);
        }
      }
      if (!clicked) {
        // Fallback: find buttons by text content
        try {
          await page.evaluate(() => {
            const texts = ["Accept All", "Accept Cookies", "Accept", "I agree", "Allow all", "Allow"];
            const buttons = Array.from(document.querySelectorAll("button, a"));
            for (const text of texts) {
              const btn = buttons.find((b) => b.textContent.trim().startsWith(text));
              if (btn) { btn.click(); break; }
            }
          });
        } catch (e) {
          // not critical
        }
      }
    } catch (e) {
      console.warn(`[puppeteer] Cookie acceptance error: ${e.message}`);
    }

    // Wait for content to load after cookie acceptance
    await new Promise((r) => setTimeout(r, 3000));
    const html = await page.content();
    return html;
  } finally {
    await browser.close();
  }
}
const { createClient } = require("@supabase/supabase-js");
const OpenAI = require("openai");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

// Connect to Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY,
);

// ── Subject sub-indicator constants ──────────────────────────────────────

const SUBJECT_FIELD_MAP = {
  "engineering & tech": "Engineering & Tech",
  "business, management and economics": "Business Management & Economics",
  "science & applied science": "Science & Applied Science",
  "medicine, health and life science": "Medicine Health & Life Science",
  "social science & humanities": "Social Science & Humanities",
  "arts, design & creative studies": "Arts Design & Creative Studies",
  "law, public policy & governance": "Law Public Policy & Governance",
  "hospitality, tourism & service industry": "Hospitality Tourism & Service Industry",
  "education & teaching": "Education & Teaching",
  "agriculture, sustainability & environmental studies": "Agriculture Sustainability & Environmental Studies"
};

const GLOBAL_FW_BASE_WEIGHTS = { QS: 5.25, THE: 4.96, ARWU: 4.97, CUG: 4.86, Guardian: 4.81, GUG: 4.81 };

const GLOBAL_FW_NAME_MAP = {
  'QS World University Rankings':                  'QS',
  'Times Higher Education (THE)':                  'THE',
  'Academic Ranking of World Universities (ARWU)': 'ARWU',
  'Complete University Guide':                     'CUG',
  'Guardian University Guide':                     'Guardian',
  'Good Universities Guide (GUG)':                 'GUG',
};

const GLOBAL_FW_MULTIPLIERS = {
  research: { QS: 0.8, THE: 1.2, ARWU: 1.4, CUG: 0.9, Guardian: 0.7, GUG: 0.7 },
  balanced: { QS: 1.0, THE: 1.0, ARWU: 1.0, CUG: 1.0, Guardian: 1.0, GUG: 1.0 },
  industry: { QS: 1.3, THE: 0.9, ARWU: 0.7, CUG: 1.1, Guardian: 1.2, GUG: 1.2 },
};

const SUBJECT_FW_BASE_WEIGHTS = { QS: 5.25, THE: 4.96, ARWU: 4.97, CUG: 4.86, Guardian: 4.81 };

const SUBJECT_FW_MULTIPLIERS = {
  research: { QS: 0.8, THE: 1.2, ARWU: 1.4, CUG: 0.9, Guardian: 0.7 },
  balanced: { QS: 1.0, THE: 1.0, ARWU: 1.0, CUG: 1.0, Guardian: 1.0 },
  industry: { QS: 1.3, THE: 0.9, ARWU: 0.7, CUG: 1.1, Guardian: 1.2 }
};

const SUBJECT_CONCEPT_GROUPS = {
  employability: {
    employer_reputation: { QS: ['employer_score_norm'] },
    graduate_outcomes: { CUG: ['graduate_prospects_outcomes_norm', 'graduate_prospects_on_track_norm'], Guardian: ['career_prospects_norm'] },
    industry_link: { THE: ['industry_score_norm'] }
  },
  research: {
    citation_impact: { QS: ['citations_score_norm', 'h_index_score_norm'], THE: ['research_quality_norm'], ARWU: ['research_impact_norm'] },
    research_env: { THE: ['research_environment_norm'], CUG: ['research_quality_norm'] },
    top_journal: { ARWU: ['world_class_output_norm', 'high_quality_research_norm'] }
  },
  teaching: {
    teaching_quality: { THE: ['teaching_score_norm'] },
    student_satisfaction: { CUG: ['student_satisfaction_norm'], Guardian: ['satisfied_teaching_norm', 'satisfied_assessment_norm'] },
    staff_ratio: { Guardian: ['student_staff_ratio_norm'] },
    value_added: { Guardian: ['value_added_score_norm'] }
  },
  student_experience: {
    continuation: { CUG: ['continuation_norm'], Guardian: ['continuation_norm'] },
    spend: { Guardian: ['expenditure_per_student_norm'] }
  },
  international: {
    intl_research: { QS: ['irn_score_norm'], ARWU: ['international_collab_norm'] },
    intl_outlook: { THE: ['international_outlook_norm'] }
  },
  prestige: {
    academic_rep: { QS: ['academic_score_norm'] },
    elite_faculty: { ARWU: ['world_class_faculty_norm'] }
  },
  selectivity: {
    entry_tariff: { CUG: ['entry_standards_norm'], Guardian: ['average_entry_tariff_norm'] }
  }
};

function getResearchIntent(research_importance) {
  if (!research_importance) return 'balanced';
  if (research_importance.startsWith('Very important')) return 'research';
  if (research_importance.startsWith('Not important')) return 'industry';
  return 'balanced';
}

function getSubjectDimWeights(answers) {
  const w = { high: 0.25, medium: 0.15, low: 0.05 };
  const intent = getResearchIntent(answers.research_importance);
  return {
    employability:      w[answers.career_importance] || 0.15,
    research:           intent === 'research' ? 0.25 : intent === 'industry' ? 0.05 : 0.15,
    teaching:           w[answers.teaching_importance] || 0.15,
    student_experience: w[answers.student_experience_importance] || 0.15,
    international:      w[answers.international_importance] || 0.15,
    prestige:           w[answers.prestige_importance] || 0.15,
    selectivity:        w[answers.selectivity_importance] || 0.15
  };
}

function getCoverageConfidence(frameworkCount) {
  return { 5: 1.0, 4: 0.95, 3: 0.85, 2: 0.70, 1: 0.55 }[frameworkCount] || 0;
}

function computeSubjectSubScore(fwScores, answers) {
  const intent = getResearchIntent(answers.research_importance);
  const multipliers = SUBJECT_FW_MULTIPLIERS[intent];
  const dimWeights = getSubjectDimWeights(answers);
  let weightedSum = 0, totalWeight = 0;

  for (const [dim, concepts] of Object.entries(SUBJECT_CONCEPT_GROUPS)) {
    const conceptScores = [];
    for (const fwCols of Object.values(concepts)) {
      const vals = [];
      for (const [fw, cols] of Object.entries(fwCols)) {
        const fwData = fwScores[fw];
        if (!fwData) continue;
        const adjW = SUBJECT_FW_BASE_WEIGHTS[fw] * multipliers[fw];
        for (const col of cols) {
          const v = fwData[col];
          if (v !== null && v !== undefined) vals.push({ value: v, weight: adjW });
        }
      }
      if (vals.length > 0) {
        const totalW = vals.reduce((s, x) => s + x.weight, 0);
        conceptScores.push(vals.reduce((s, x) => s + x.value * x.weight, 0) / totalW);
      }
    }
    if (conceptScores.length > 0) {
      const dimScore = conceptScores.reduce((a, b) => a + b, 0) / conceptScores.length;
      const w = dimWeights[dim] || 0.10;
      weightedSum += w * dimScore;
      totalWeight += w;
    }
  }
  return totalWeight > 0 ? weightedSum / totalWeight : null;
}

// ─────────────────────────────────────────────────────────────────────────────

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// ── OpenAI rate-limit wrapper ────────────────────────────────────────────────
// Max 3 concurrent calls, 500 ms stagger between acquisitions.
// On 429: parse retry-after from the error message and wait exactly that long.
// On RPD limit: throw a hard stop so callers can bail out entirely.
// Max 3 retries with exponential backoff (1 s → 2 s → 4 s).
// ─────────────────────────────────────────────────────────────────────────────
let _openaiActive = 0;
const _openaiQueue = [];
function _openaiAcquire() {
  return new Promise((resolve) => {
    function tryAcquire() {
      if (_openaiActive < 3) {
        _openaiActive++;
        resolve();
      } else {
        _openaiQueue.push(tryAcquire);
      }
    }
    tryAcquire();
  });
}
function _openaiRelease() {
  _openaiActive--;
  if (_openaiQueue.length > 0) {
    setTimeout(() => {
      const next = _openaiQueue.shift();
      if (next) next();
    }, 500); // 500 ms stagger between acquired slots
  }
}

async function callOpenAI(params, timeoutMs = 60000) {
  const MAX_RETRIES = 3;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    await _openaiAcquire();
    try {
      const result = await Promise.race([
        openai.chat.completions.create(params),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("OpenAI timeout after " + timeoutMs + "ms")), timeoutMs)
        ),
      ]);
      _openaiRelease();
      return result;
    } catch (err) {
      _openaiRelease();
      const msg = err.message || "";

      // Hard stop on daily request limit
      if (msg.toLowerCase().includes("requests per day")) {
        console.error("[openai] RPD limit reached — halting OpenAI processing:", msg);
        const rpdErr = new Error("OPENAI_RPD_LIMIT: " + msg);
        rpdErr.isRpdLimit = true;
        throw rpdErr;
      }

      // Retry on 429 / rate limit
      if ((err.status === 429 || msg.includes("429") || msg.toLowerCase().includes("rate limit")) && attempt < MAX_RETRIES) {
        // Parse retry-after seconds from message, e.g. "Please try again in 12s" or "retry after 30"
        const retryMatch = msg.match(/(?:try again in|retry after)\s*(\d+(?:\.\d+)?)\s*s/i)
          || msg.match(/(\d+(?:\.\d+)?)\s*s(?:econds?)?/i);
        const retryMs = retryMatch
          ? Math.ceil(parseFloat(retryMatch[1])) * 1000
          : Math.pow(2, attempt) * 1000; // exponential fallback: 1s, 2s, 4s
        console.warn(`[openai] 429 rate limit — waiting ${retryMs}ms before retry ${attempt + 1}/${MAX_RETRIES}`);
        await new Promise((r) => setTimeout(r, retryMs));
        continue;
      }

      throw err;
    }
  }
}
// ─────────────────────────────────────────────────────────────────────────────

app.get("/", (req, res) => {
  res.send("Study Abroad Engine API running 🚀");
});

app.get("/test-core", async (req, res) => {
  const { data, error } = await supabase
    .schema("core")
    .from("universities")
    .select("id, name, countries(name)")
    .limit(1);

  if (error) return res.status(500).json({ error });
  res.json({ message: "Core schema works", data });
});

// Test route to fetch countries
app.get("/countries", async (req, res) => {
  const { data, error } = await supabase
    .schema("core")
    .from("countries")
    .select("*");

  if (error) {
    return res.status(500).json({ error });
  }

  res.json(data);
});

async function getSubScore(universityId, body) {
  // Step 1: Fetch sub-indicator scores joined to dimension and concept_group
  const { data: rows, error } = await supabase.rpc("get_sub_indicator_scores", {
    p_university_id: universityId
  });

  if (error) {
    return 0.5;
  }
  if (!rows) {
    return 0.5;
  }
  if (rows.length === 0) {
    return 0.5;
  }

  // Step 2: Group by (dimension, concept_group) with framework-weighted scores
  const intent = getResearchIntent(body.research_importance);
  const multipliers = GLOBAL_FW_MULTIPLIERS[intent] || GLOBAL_FW_MULTIPLIERS.balanced;

  const conceptBuckets = {}; // { dimension: { concept_group: { value, weight }[] } }
  for (const row of rows) {
    const { dimension, concept_group, framework } = row;
    const score = row.normalized_score;
    if (score == null) continue;
    const shortName = GLOBAL_FW_NAME_MAP[framework] || null;
    const baseWeight = shortName ? (GLOBAL_FW_BASE_WEIGHTS[shortName] || 1.0) : 1.0;
    const intentMultiplier = shortName ? (multipliers[shortName] || 1.0) : 1.0;
    const adjWeight = baseWeight * intentMultiplier;
    if (!conceptBuckets[dimension]) conceptBuckets[dimension] = {};
    if (!conceptBuckets[dimension][concept_group]) conceptBuckets[dimension][concept_group] = [];
    conceptBuckets[dimension][concept_group].push({ value: Number(score), weight: adjWeight });
  }

  const dimConceptScore = {}; // { dimension: { concept_group: weighted_avg } }
  for (const [dim, concepts] of Object.entries(conceptBuckets)) {
    dimConceptScore[dim] = {};
    for (const [concept, entries] of Object.entries(concepts)) {
      const totalW = entries.reduce((s, x) => s + x.weight, 0);
      dimConceptScore[dim][concept] = entries.reduce((s, x) => s + x.value * x.weight, 0) / totalW;
    }
  }

  // Step 3 & 4: Compute dim_score per dimension
  const dimScore = {}; // { dimension: number }
  for (const [dim, concepts] of Object.entries(dimConceptScore)) {
    const conceptNames = Object.keys(concepts);
    if (dim === "employability") {
      // Step 3: Apply career_type boost (2x weight for matching concept)
      const careerType = body.career_type || null;
      const weights = {};
      for (const c of conceptNames) weights[c] = 1;

      if (careerType === "graduate_salary" && concepts["graduate_salary"] == null) {
        // Fallback: equal weighting of employer_reputation and employment_rate
      } else if (
        careerType === "employment_rate" ||
        careerType === "employer_reputation" ||
        careerType === "graduate_salary"
      ) {
        if (concepts[careerType] != null) weights[careerType] = 2;
      }
      // null/missing careerType → equal weighting (all weights stay 1)

      const totalWeight = Object.values(weights).reduce((a, b) => a + b, 0);
      const weightedSum = conceptNames.reduce((acc, c) => acc + weights[c] * concepts[c], 0);
      dimScore[dim] = totalWeight > 0 ? weightedSum / totalWeight : null;
    } else {
      // Step 4: Simple average of all concept scores
      const scores = Object.values(concepts);
      dimScore[dim] = scores.reduce((a, b) => a + b, 0) / scores.length;
    }
  }

  // Step 5: Preference vector
  const DIM_WEIGHT = { high: 0.25, medium: 0.15, low: 0.05 };
  const answerMap = {
    employability: body.career_importance,
    teaching: body.teaching_importance,
    research: body.research_importance,
    student_experience: body.student_experience_importance,
    international: body.international_importance,
    selectivity: body.selectivity_importance,
    prestige: body.prestige_importance,
  };

  // Step 6: Weighted average across dimensions with non-null dim_score
  let numerator = 0;
  let denominator = 0;
  for (const [dim, score] of Object.entries(dimScore)) {
    if (score == null) continue;
    const answer = answerMap[dim] || "low";
    const w = DIM_WEIGHT[answer] || DIM_WEIGHT.low;
    numerator += w * score;
    denominator += w;
  }
  const subScore = denominator > 0 ? numerator / denominator : 0.5;
  return subScore;
}

async function bulkFetchSubjectScores(universityIds, answers, supabase) {
  if (!answers.sub_field || !answers.field || universityIds.length === 0) return {};

  const taxonomyField = SUBJECT_FIELD_MAP[answers.field];
  if (!taxonomyField) return {};

  // Lookup framework→subject_name from taxonomy
  const { data: taxRows, error: taxErr } = await supabase
    .rpc('get_subject_taxonomy', {
      p_field: taxonomyField,
      p_sub_field: answers.sub_field
    });

  if (taxErr || !taxRows || taxRows.length === 0) {
    console.log('[subject] taxonomy lookup returned no rows for', taxonomyField, answers.sub_field);
    return {};
  }

  const subjectMap = {};
  taxRows.forEach(r => {
    if (!subjectMap[r.framework]) subjectMap[r.framework] = [];
    subjectMap[r.framework].push(r.subject_name);
  });
  console.log('[subject] taxonomy matches:', subjectMap);

  const FW_COLS = {
    QS: 'university_id,academic_score_norm,employer_score_norm,citations_score_norm,h_index_score_norm,irn_score_norm',
    THE: 'university_id,research_quality_norm,industry_score_norm,international_outlook_norm,research_environment_norm,teaching_score_norm',
    ARWU: 'university_id,world_class_faculty_norm,world_class_output_norm,high_quality_research_norm,research_impact_norm,international_collab_norm',
    CUG: 'university_id,entry_standards_norm,student_satisfaction_norm,research_quality_norm,continuation_norm,graduate_prospects_outcomes_norm,graduate_prospects_on_track_norm',
    Guardian: 'university_id,satisfied_teaching_norm,continuation_norm,expenditure_per_student_norm,student_staff_ratio_norm,career_prospects_norm,value_added_score_norm,average_entry_tariff_norm,satisfied_assessment_norm'
  };
  const FW_SCHEMAS = { QS: 'qs', THE: 'the', ARWU: 'arwu', CUG: 'cug', Guardian: 'guardian' };

  const FW_COL_NAMES = {
    QS: ['academic_score_norm','employer_score_norm','citations_score_norm','h_index_score_norm','irn_score_norm'],
    THE: ['research_quality_norm','industry_score_norm','international_outlook_norm','research_environment_norm','teaching_score_norm'],
    ARWU: ['world_class_faculty_norm','world_class_output_norm','high_quality_research_norm','research_impact_norm','international_collab_norm'],
    CUG: ['entry_standards_norm','student_satisfaction_norm','research_quality_norm','continuation_norm','graduate_prospects_outcomes_norm','graduate_prospects_on_track_norm'],
    Guardian: ['satisfied_teaching_norm','continuation_norm','expenditure_per_student_norm','student_staff_ratio_norm','career_prospects_norm','value_added_score_norm','average_entry_tariff_norm','satisfied_assessment_norm']
  };

  const queries = Object.entries(subjectMap).map(async ([fw, subjectNames]) => {
    const fwLower = FW_SCHEMAS[fw];
    if (!fwLower || !FW_COL_NAMES[fw]) return [fw, []];
    const allRows = [];
    for (const subjectName of subjectNames) {
      const { data, error } = await supabase
        .rpc('get_subject_scores', {
          p_framework: fwLower,
          p_subject: subjectName,
          p_university_ids: universityIds
        });
      if (error) {
        console.log(`[subject] ${fw} rpc error:`, error.message);
        continue;
      }
      if (data) allRows.push(...data);
    }
    const colNames = FW_COL_NAMES[fw];
    const remapped = allRows.map(row => {
      const obj = { university_id: row.university_id };
      colNames.forEach((name, i) => { obj[name] = row[`c${i+1}`]; });
      return obj;
    });
    return [fw, remapped];
  });

  const results = await Promise.all(queries);

  // Build map: universityId → { QS: {col:val,...}, THE: {...}, ... }
  const scoreMap = {};
  results.forEach(([fw, rows]) => {
    rows.forEach(row => {
      const uid = row.university_id;
      if (!scoreMap[uid]) scoreMap[uid] = {};
      if (!scoreMap[uid][fw]) {
        scoreMap[uid][fw] = { ...row };
      } else {
        const existing = scoreMap[uid][fw];
        const merged = { university_id: uid };
        Object.keys(row).forEach(k => {
          if (k === 'university_id') return;
          const a = existing[k], b = row[k];
          merged[k] = (a !== null && a !== undefined && b !== null && b !== undefined)
            ? (a + b) / 2
            : (a !== null && a !== undefined ? a : b);
        });
        scoreMap[uid][fw] = merged;
      }
    });
  });

  console.log(`[subject] scoreMap populated for ${Object.keys(scoreMap).length} universities`);
  return scoreMap;
}

async function bulkFetchCourseRelevance(eligibleCourses, answers, supabase) {
  if (!answers.sub_field || !answers.field || eligibleCourses.length === 0) return {};

  const courseIds = eligibleCourses.map(c => c.id).filter(Boolean);
  if (courseIds.length === 0) return {};

  const { data: embeddingData, error: embErr } = await supabase
    .rpc('get_sub_field_embedding', {
      p_field: answers.field,
      p_sub_field: answers.sub_field
    });

  if (embErr || !embeddingData) {
    console.log('[relevance] sub-field embedding not found for', answers.field, answers.sub_field);
    return {};
  }

  // Build BM25 search terms from sub_field name
  // Convert "Computer Science & IT" → "Computer & Science & IT" for tsquery
  const bm25Terms = (answers.sub_field || '')
    .replace(/[^a-zA-Z0-9\s]/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(w => w.length > 2)
    .join(' & ');

  const { data: similarities, error: simErr } = await supabase
    .rpc('get_course_similarities', {
      p_query_embedding: embeddingData,
      p_course_ids: courseIds,
      p_search_terms: bm25Terms
    });

  if (simErr || !similarities) {
    console.log('[relevance] similarity query error:', simErr?.message);
    return {};
  }

  // Build raw map
  const rawMap = {};
  similarities.forEach(row => {
    rawMap[row.course_id] = Math.max(0, row.similarity);
  });

  // Min-max normalise within batch to spread scores across 0–1
  const scores = Object.values(rawMap);
  const minScore = Math.min(...scores);
  const maxScore = Math.max(...scores);
  const range = maxScore - minScore;

  const relevanceMap = {};
  Object.keys(rawMap).forEach(id => {
    relevanceMap[id] = range > 0.05
      ? (rawMap[id] - minScore) / range
      : rawMap[id];
  });

  console.log(`[relevance] scores computed for ${Object.keys(relevanceMap).length} courses — raw range: ${minScore.toFixed(3)}–${maxScore.toFixed(3)} → normalised 0–1`);
  return relevanceMap;
}

app.post("/embed-new-courses", async (req, res) => {
  try {
    const macMiniUrl = process.env.MAC_MINI_EMBED_URL;
    if (!macMiniUrl) {
      console.log("[embed] MAC_MINI_EMBED_URL not set — skipping auto-embed");
      return res.json({ success: true, message: "Auto-embed skipped — MAC_MINI_EMBED_URL not configured" });
    }
    const response = await fetch(macMiniUrl, { method: "POST" });
    const result = await response.json();
    console.log("[embed] Mac Mini embed job triggered:", result);
    return res.json({ success: true, message: "Embed job triggered on Mac Mini" });
  } catch (err) {
    console.error("[embed] error triggering embed job:", err.message);
    return res.status(500).json({ error: err.message });
  }
});

app.post("/recommend", async (req, res) => {
  try {
    const answers = req.body;
    const {
      career_importance,
      career_type,
      teaching_importance,
      research_importance,
      student_experience_importance,
      international_importance,
      selectivity_importance,
      prestige_importance,
      sub_field,
      subject_ranking_importance,
    } = req.body;

    const { priority_1, priority_2, priority_3 } = req.body;

    if (!priority_1 || !priority_2 || !priority_3) {
      return res.status(400).json({ error: 'All three priorities must be provided.' });
    }
    if (priority_1 === priority_2 || priority_1 === priority_3 || priority_2 === priority_3) {
      return res.status(400).json({ error: 'Priority 1, 2 and 3 must all be different. Please select three distinct priorities.' });
    }

    // 1️⃣ Fetch all data
    const { data: countries, error: cErr } = await supabase
      .schema("core")
      .from("countries")
      .select("*");

    const { data: universities, error: uErr } = await supabase
      .schema("core")
      .from("universities")
      .select("*");

    const tuitionBounds = {
      "Less than $12k": { min: 0, max: 11999 },
      "$12k - $25k": { min: 12000, max: 25000 },
      "More than $25K": { min: 25001, max: 999999 },
    };
    const tBand = tuitionBounds[answers.tuition_band] || {
      min: 0,
      max: 999999,
    };

    const durationBounds = {
      "1 year or less": { min: 0, max: 1 },
      "More than 1 year": { min: 1, max: 99 },
      "3 years or less": { min: 0, max: 3 },
      "More than 3 years": { min: 3, max: 99 },
    };
    const dBand = durationBounds[answers.duration] || { min: 0, max: 99 };

    let courseQuery = supabase
      .schema("core")
      .from("courses")
      .select("*")
      .eq("degree_level", answers.level)
      .eq("field_category", answers.field)
      .gte("tuition_usd", tBand.min)
      .lte("tuition_usd", tBand.max);

    if (answers.selected_country) {
      const { data: countryRow } = await supabase
        .schema('core')
        .from('countries')
        .select('id')
        .eq('name', answers.selected_country)
        .single();
      if (countryRow) {
        const { data: uniIds } = await supabase
          .schema('core')
          .from('universities')
          .select('id')
          .eq('country_id', countryRow.id);
        if (uniIds && uniIds.length > 0) {
          courseQuery = courseQuery.in('university_id', uniIds.map(u => u.id));
        }
      }
    }

    if (answers.sub_field) {
      courseQuery = courseQuery.eq("sub_field", answers.sub_field);
    }

    if (answers.gre_filter === "Without GRE or GMAT") {
      courseQuery = courseQuery
        .eq("gre_required", false)
        .eq("gmat_required", false);
    } else if (answers.gre_filter === "Without GRE") {
      courseQuery = courseQuery.eq("gre_required", false);
    } else if (answers.gre_filter === "Without GMAT") {
      courseQuery = courseQuery.eq("gmat_required", false);
    }

    if (answers.profile_gpa_percentage) {
      courseQuery = courseQuery.or(
        `min_gpa_percentage.is.null,min_gpa_percentage.lte.${answers.profile_gpa_percentage}`,
      );
    }

    if (answers.profile_backlogs && parseInt(answers.profile_backlogs) > 0) {
      courseQuery = courseQuery.neq("accepts_backlogs", false);
    }

    let { data: courses, error: coErr } = await courseQuery;

    if (answers.sub_field && (!courses || courses.length === 0)) {
      console.log('[sub_field] no courses found for sub_field, falling back to field_category only');
      let fallbackQuery = supabase
        .schema("core")
        .from("courses")
        .select("*")
        .eq("degree_level", answers.level)
        .eq("field_category", answers.field)
        .gte("tuition_usd", tBand.min)
        .lte("tuition_usd", tBand.max);

      if (answers.selected_country) {
        const { data: countryRow } = await supabase
          .schema('core')
          .from('countries')
          .select('id')
          .eq('name', answers.selected_country)
          .single();
        if (countryRow) {
          const { data: uniIds } = await supabase
            .schema('core')
            .from('universities')
            .select('id')
            .eq('country_id', countryRow.id);
          if (uniIds && uniIds.length > 0) {
            fallbackQuery = fallbackQuery.in('university_id', uniIds.map(u => u.id));
          }
        }
      }

      if (answers.gre_filter === "Without GRE or GMAT") {
        fallbackQuery = fallbackQuery.eq("gre_required", false).eq("gmat_required", false);
      } else if (answers.gre_filter === "Without GRE") {
        fallbackQuery = fallbackQuery.eq("gre_required", false);
      } else if (answers.gre_filter === "Without GMAT") {
        fallbackQuery = fallbackQuery.eq("gmat_required", false);
      }

      if (answers.profile_gpa_percentage) {
        fallbackQuery = fallbackQuery.or(
          `min_gpa_percentage.is.null,min_gpa_percentage.lte.${answers.profile_gpa_percentage}`,
        );
      }

      if (answers.profile_backlogs && parseInt(answers.profile_backlogs) > 0) {
        fallbackQuery = fallbackQuery.neq("accepts_backlogs", false);
      }

      const { data: fallbackCourses, error: fallbackError } = await fallbackQuery;
      if (fallbackError) console.error("Fallback courses fetch error:", fallbackError.message);
      courses = fallbackCourses;
    }

    if (cErr) console.error("Countries fetch error:", cErr.message);
    if (uErr) console.error("Universities fetch error:", uErr.message);
    if (coErr) console.error("Courses fetch error:", coErr.message);

    if (!countries || !universities || !courses) {
      return res
        .status(500)
        .json({ error: "Failed to fetch core data from the database." });
    }

    const { data: countryData } = await supabase
      .schema("core")
      .from("countries")
      .select("id, name, avg_cost_of_living_usd, post_study_work_years, pr_pathway_clarity_score, english_primary_language");

    const { data: rankingData } = await supabase
      .from("university_composite_ranking")
      .select("id, final_score")
      .limit(2000);

    const rankingMap = {};
    if (rankingData) {
      rankingData.forEach((r) => {
        rankingMap[r.id] = r.final_score;
      });
    }

    // Subject-level ranking map: "universityId:subjectId" → composite_score
    // When a course has subject_id set, we prefer this over the blended overall score.
    // Example: MIT might be #200 overall but #3 in CS — a CS student should see #3.
    const { data: subjectRankData } = await supabase
      .from("university_subject_ranking")
      .select("university_id, subject_id, composite_score");

    const subjectRankMap = {};
    if (subjectRankData) {
      subjectRankData.forEach((r) => {
        if (r.composite_score != null) {
          subjectRankMap[`${r.university_id}:${r.subject_id}`] = r.composite_score;
        }
      });
    }

    const countryMap = {};
    if (countryData) {
      countryData.forEach((c) => {
        countryMap[c.id] = c;
      });
    }

    // 2️⃣ PROFILE ELIMINATION (remaining checks not done at DB level)
    const eligibleCourses = courses.filter((course) => {
      // Work experience check
      if (
        course.work_experience_required &&
        course.work_experience_required > 0
      ) {
        if (
          !answers.profile_work_experience ||
          parseFloat(answers.profile_work_experience) <
            course.work_experience_required
        )
          return false;
      }

      // English score check
      if (
        answers.profile_english_test &&
        answers.profile_english_test !== "None"
      ) {
        const score = parseFloat(answers.profile_english_score);
        if (answers.profile_english_test === "IELTS" && course.ielts_minimum) {
          if (score < course.ielts_minimum) return false;
        }
        if (answers.profile_english_test === "TOEFL" && course.toefl_minimum) {
          if (score < course.toefl_minimum) return false;
        }
        if (answers.profile_english_test === "PTE" && course.pte_minimum) {
          if (score < course.pte_minimum) return false;
        }
      }

      // GRE/GMAT — if student has no score, eliminate programs that require it
      if (
        !answers.profile_gre_score ||
        parseFloat(answers.profile_gre_score) === 0
      ) {
        if (course.gre_required) return false;
      }
      if (
        !answers.profile_gmat_score ||
        parseFloat(answers.profile_gmat_score) === 0
      ) {
        if (course.gmat_required) return false;
      }

      return true;
    });

    const durationFilteredCourses = eligibleCourses.filter(c => c.duration_years >= dBand.min && c.duration_years <= dBand.max);
    const softDurationCourses = eligibleCourses.filter(c => c.duration_years < dBand.min || c.duration_years > dBand.max);

    if (durationFilteredCourses.length === 0 && softDurationCourses.length === 0) {
      return res.json({
        empty: true,
        message: "No courses matched your profile and filters.",
        suggestion:
          "Try adjusting your tuition band, duration, or English score — some programs may require a higher score than entered.",
      });
    }

    // 3️⃣ MACRO WEIGHTS
    function computeMacroWeights(p1, p2, p3) {
      const base = { 1: 0.5, 2: 0.32, 3: 0.18 };

      const normalise = (val) => {
        if (!val) return null;
        const map = {
          country: "Country",
          course: "Course",
          institution: "Institution",
        };
        return map[val.toLowerCase()] || val;
      };

      const weights = {};
      weights[normalise(p1)] = base[1];
      weights[normalise(p2)] = base[2];
      weights[normalise(p3)] = base[3];

      const total = Object.values(weights).reduce((a, b) => a + b, 0);
      if (Math.abs(total - 1.0) > 0.01) {
        console.error("WEIGHT SUM ERROR:", total, weights);
      }

      return weights;
    }

    const weights = computeMacroWeights(
      answers.priority_1,
      answers.priority_2,
      answers.priority_3,
    );

    function clamp(value) {
      if (value == null || isNaN(value)) return 0;
      return Math.max(0, Math.min(1, value));
    }

    function computeFinalScore(weights, scores) {
      return (
        weights.Country * scores.country +
        weights.Course * scores.course +
        weights.Institution * scores.university
      );
    }

    // --- COUNTRY NORMALIZATION BASE ---

    const maxCost = Math.max(...countries.map((c) => c.avg_cost_of_living_usd));
    const minCost = Math.min(...countries.map((c) => c.avg_cost_of_living_usd));
    const maxWorkYears = Math.max(
      ...countries.map((c) => c.post_study_work_years),
    );

    function normalizeCost(cost) {
      if (maxCost === minCost) return 1;
      return 1 - (cost - minCost) / (maxCost - minCost);
    }

    function normalizeWorkYears(years) {
      if (maxWorkYears === 0) return 0;
      return years / maxWorkYears;
    }

    function normalizeRank(rank, maxRank) {
      if (!rank || !maxRank) return null;
      return 1 - (rank - 1) / (maxRank - 1);
    }

    function computeCountryScore(country, answers, countryMap) {
      const c = countryMap[country.id];
      if (!c) {
        return 0;
      }

      const psw_score = c.post_study_work_years != null ? clamp((c.post_study_work_years - 1.5) / (3 - 1.5)) : 0.5;
      const pr_score = c.pr_pathway_clarity_score != null ? c.pr_pathway_clarity_score : 0.5;
      const english_score = c.english_primary_language === true ? 1.0 : c.english_primary_language === false ? 0.0 : 0.5;

      let pswWeight =
        answers.work_permit_importance === "Very strongly (3 years and above)"
          ? 1
          : answers.work_permit_importance.includes("Wouldn't mind")
            ? 0.6
            : 0.3;

      let prWeight =
        answers.pr_importance === "Very strongly"
          ? 1
          : answers.pr_importance === "Wouldn't mind"
            ? 0.6
            : 0.3;

      let englishWeight =
        answers.english_preference === "Yes"
          ? 1
          : answers.english_preference === "Prefer but flexible"
            ? 0.6
            : 0.3;

      let weightedSum = pswWeight * psw_score + prWeight * pr_score + englishWeight * english_score;

      let totalWeight = pswWeight + prWeight + englishWeight;

      return clamp(weightedSum / totalWeight);
    }

    function computeIntentAlignment(course, answers) {
      const ri = answers.research_importance || '';
      let intent = 'balanced';
      if (ri.startsWith('Very important')) intent = 'research';
      else if (ri.startsWith('Not important')) intent = 'industry';

      const pType = course.program_type;
      if (intent === 'research') {
        if (pType === 'research') return 1.0;
        if (pType === 'professional') return 0.4;
        return 0.7;
      }
      if (intent === 'industry') {
        if (pType === 'professional') return 1.0;
        if (pType === 'research') return 0.3;
        return 0.7;
      }
      if (pType === 'research') return 0.6;
      if (pType === 'professional') return 0.8;
      return 0.7;
    }

    function computeLogisticsFit(course, answers) {
      let components = [];
      let weights = [];

      const iMap = {
        'Very strongly': 1,
        'Wouldn\u2019t mind': 0.6,
        'Don\u2019t care': 0.3,
      };
      const internshipWeight = iMap[answers.internship_importance] || 0;
      components.push(internshipWeight * (course.internship_available ? 1 : 0));
      weights.push(internshipWeight);

      const sMap = {
        'Very strongly (more than 20% of tuition)': 1,
        'Wouldn\u2019t mind getting one (less than 20% of tuition or none)': 0.6,
        'Don\u2019t care': 0.3,
      };
      const scholarshipWeight = sMap[answers.scholarship_importance] || 0;
      const scholarshipScore = course.scholarship_available ? 0.8 : 0.2;
      components.push(scholarshipWeight * scholarshipScore);
      weights.push(scholarshipWeight);

      const totalWeight = weights.reduce((a, b) => a + b, 0);
      return totalWeight > 0 ? clamp(components.reduce((a, b) => a + b, 0) / totalWeight) : 0.5;
    }

    function computeCourseScore(course, answers, relevanceMap) {
      const contentRelevance = (relevanceMap && relevanceMap[course.id] !== undefined) ? relevanceMap[course.id] : 0.5;
      const intentAlignment = computeIntentAlignment(course, answers);
      const logisticsFit = computeLogisticsFit(course, answers);
      return clamp(0.50 * contentRelevance + 0.25 * intentAlignment + 0.25 * logisticsFit);
    }

    function computeUniversityScore(university, country, answers, rankingMap, subjectRankMap, courseSubjectId) {
      // Use subject-specific ranking if available — much more accurate signal.
      // e.g. MIT #200 overall but #3 in CS: a CS student should see the #3 score.
      const overallRanking = rankingMap[university.id] ?? 0.5;
      const subjectRanking =
        courseSubjectId && subjectRankMap
          ? subjectRankMap[`${university.id}:${courseSubjectId}`] ?? null
          : null;
      // Blend: if subject ranking exists, weight it 70% vs 30% overall.
      // This preserves the overall signal (faculty ratio, intl outlook, etc.)
      // while prioritising the discipline-specific rank.
      const compositeRanking =
        subjectRanking != null
          ? 0.7 * subjectRanking + 0.3 * overallRanking
          : overallRanking;

      let rankingWeight = parseFloat(answers.ranking_importance) || 0;

      let rankingScore = rankingWeight * compositeRanking;

      const uniNumerator = rankingScore;
      const uniDenominator = rankingWeight;
      return clamp(uniNumerator / (uniDenominator || 1));
    }

    // 4️⃣ SCORE PATHWAYS
    const allUniversityIds = [...new Set(eligibleCourses.map(c => c.university_id).filter(Boolean))];
    const subjectScoreMap = await bulkFetchSubjectScores(allUniversityIds, answers, supabase);
    const courseRelevanceMap = await bulkFetchCourseRelevance(eligibleCourses, answers, supabase);

    const pathways = await Promise.all(durationFilteredCourses.map(async (course) => {
      const university = universities.find(
        (u) => u.id === course.university_id,
      );
      if (!university) return null;
      const country = countries.find((c) => c.id === university.country_id);
      if (!country) return null;

      let countryScore = computeCountryScore(country, answers, countryMap);
      let courseScore = computeCourseScore(course, answers, courseRelevanceMap);

      // Step 7: Blend composite ranking score with sub-indicator score
      const subScore = await getSubScore(university.id, answers);
      const compositeScore = rankingMap[university.id] ?? null;
      const alpha = parseFloat(answers.ranking_importance) || 0;
      const beta = 1 - alpha;
      const delta = { high: 0.60, medium: 0.35, low: 0.10 }[answers.subject_ranking_importance] || 0.10;
      const fwScores = subjectScoreMap[university.id] || subjectScoreMap[course.university_id] || null;
      const subjectSubScore = fwScores ? computeSubjectSubScore(fwScores, answers) : null;
      const coverageConf = fwScores ? getCoverageConfidence(Object.keys(fwScores).length) : 0;
      const blendedSubScore = (subjectSubScore !== null)
        ? (1 - delta) * subScore + delta * subjectSubScore * coverageConf
        : subScore;
      const universityScore = compositeScore != null
        ? alpha * compositeScore + beta * blendedSubScore
        : 0.70 * blendedSubScore;

      console.log(`[score] ${university?.canonical_name || course?.university_id} composite=${compositeScore?.toFixed(3)} globalSub=${subScore?.toFixed(3)} subjectSub=${subjectSubScore?.toFixed(3)} delta=${delta} final=${universityScore?.toFixed(3)}`);

      // FINAL ADDITIVE SCORE
      let finalScore = computeFinalScore(weights, {
        country: countryScore,
        course: courseScore,
        university: universityScore,
      });

      if (!isFinite(finalScore)) {
        finalScore = 0;
      }

      const explanation = [];

      if (countryScore >= 0.7)
        explanation.push(
          "Strong country match based on your living and work preferences",
        );
      else if (countryScore >= 0.4)
        explanation.push("Moderate country alignment with your preferences");

      if (
        answers.pr_importance === "Very strongly" &&
        country.pr_pathway_clarity_score >= 0.7
      ) {
        explanation.push("Strong permanent residency pathway available");
      }

      if (
        answers.english_preference === "Yes" &&
        country.english_primary_language
      ) {
        explanation.push("English-speaking country matches your preference");
      }

      if (
        answers.work_permit_importance.includes("Very strongly") &&
        country.post_study_work_years >= 3
      ) {
        explanation.push("Post-study work permit of 3+ years available");
      }

      if (
        course.internship_available &&
        answers.internship_importance !== "Don't care"
      ) {
        explanation.push("Includes internship as part of the curriculum");
      }

      if (courseScore >= 0.7)
        explanation.push("Strong match with your subject area and academic goals");
      else if (courseScore >= 0.4)
        explanation.push("Good alignment with your field and course preferences");
      else
        explanation.push("Course is within your selected field category");

      // If subject-specific ranking was used, surface that as an explanation
      if (course.subject_id && subjectRankMap) {
        const subjectScore = subjectRankMap[`${university.id}:${course.subject_id}`];
        if (subjectScore != null && subjectScore >= 0.7) {
          explanation.push("Highly ranked in your specific subject area");
        } else if (subjectScore != null && subjectScore >= 0.5) {
          explanation.push("Well ranked in your specific subject area");
        }
      }

      if (universityScore >= 0.7)
        explanation.push(
          "Institution scores well on ranking, location, and services",
        );
      else if (universityScore >= 0.4)
        explanation.push("Institution meets your core university preferences");

      if (explanation.length === 0) {
        explanation.push(
          "Balanced match across country, course, and institution factors",
        );
      }

      return {
        country: country.name,
        university: university.name,
        course: course.name,
        duration: course.duration_years ?? null,
        tuition_usd: course.tuition_usd ?? null,
        finalScore,
        scores: {
          country: Math.round(countryScore * 100) / 100,
          course: Math.round(courseScore * 100) / 100,
          university: Math.round(universityScore * 100) / 100,
        },
        explanation,
      };
    }));

    // 5️⃣ Sort & Return Top 5
    const seenUniversities = new Set();
    const primaryTop = pathways
      .filter((p) => p !== null)
      .sort((a, b) => (b.finalScore || 0) - (a.finalScore || 0))
      .filter((p) => {
        if (seenUniversities.has(p.university)) return false;
        seenUniversities.add(p.university);
        return true;
      })
      .slice(0, 5);

    if (primaryTop.length < 5 && softDurationCourses.length > 0) {
      const excludedUniversities = new Set(primaryTop.map(p => p.university));
      const extraCourses = softDurationCourses
        .filter(c => {
          const uni = universities.find(u => u.id === c.university_id);
          return uni && !excludedUniversities.has(uni.name);
        })
        .slice(0, 30);

      let softPathways;
      try {
        softPathways = await Promise.all(extraCourses.map(async (course) => {
          const university = universities.find(u => u.id === course.university_id);
          if (!university) return null;
          const country = countries.find(c => c.id === university.country_id);
          if (!country) return null;

          const subScore = await getSubScore(university.id, answers);
          const compositeScore = rankingMap[university.id] ?? null;
          const alpha = parseFloat(answers.ranking_importance) || 0;
          const beta = 1 - alpha;
          const delta = { high: 0.60, medium: 0.35, low: 0.10 }[answers.subject_ranking_importance] || 0.10;
          const fwScores = subjectScoreMap[university.id] || subjectScoreMap[course.university_id] || null;
          const subjectSubScore = fwScores ? computeSubjectSubScore(fwScores, answers) : null;
          const coverageConf = fwScores ? getCoverageConfidence(Object.keys(fwScores).length) : 0;
          const blendedSubScore = (subjectSubScore !== null)
            ? (1 - delta) * subScore + delta * subjectSubScore * coverageConf
            : subScore;
          const universityScore = compositeScore != null
            ? alpha * compositeScore + beta * blendedSubScore
            : 0.70 * blendedSubScore;

          const countryScore = computeCountryScore(country, answers, countryMap);
          const courseScore = computeCourseScore(course, answers, courseRelevanceMap);

          const rawFinalScore = weights.Country * countryScore + weights.Course * courseScore + weights.Institution * universityScore;

          const durationDistance = Math.max(0,
            course.duration_years < dBand.min
              ? dBand.min - course.duration_years
              : course.duration_years - dBand.max
          );
          const durationPenalty = Math.max(0.70, 1 - durationDistance * 0.10);
          const finalScore = rawFinalScore * durationPenalty;

          return {
            ...course,
            university: university.name,
            university_id: university.id,
            country: country.name,
            countryScore,
            courseScore,
            institutionScore: universityScore,
            finalScore,
            softDuration: true
          };
        }));
      } catch (softErr) {
        console.error('[soft-duration] error:', softErr.message);
        console.error('[soft-duration] stack:', softErr.stack);
        return res.json(primaryTop);
      }

      const softSeen = new Set(primaryTop.map(p => p.university));
      const softDeduped = softPathways
        .filter(p => p !== null)
        .sort((a, b) => (b.finalScore || 0) - (a.finalScore || 0))
        .filter(p => {
          if (softSeen.has(p.university)) return false;
          softSeen.add(p.university);
          return true;
        });

      const combined = [...primaryTop, ...softDeduped].slice(0, 5);
      return res.json(combined);
    }

    return res.json(primaryTop);
  } catch (error) {
    console.error(error);
    res.status(500).send("Recommendation failed");
  }
});

// --------------------------------------
// SCRAPE PROGRAM PAGE
// --------------------------------------

app.post("/scrape-program", async (req, res) => {
  try {
    const { university_id, program_url } = req.body;

    if (!university_id || !program_url) {
      return res.status(400).json({
        error: "university_id and program_url are required",
      });
    }

    console.log("Scraping:", program_url);

    // Fetch page
    const response = await axios.get(program_url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)",
      },
      timeout: 30000,
    });

    const html = response.data;

    if (!html || html.length < 1000) {
      return res.status(400).json({
        error: "Page content too small or invalid",
      });
    }

    // Insert raw HTML into ingestion schema
    const { data, error } = await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .insert({
        university_id: university_id,
        source_url: program_url,
        raw_html: html,
        parse_status: "pending",
      })
      .select();

    if (error) {
      console.error("Insert error:", error);
      return res.status(500).json({ error });
    }

    res.json({
      message: "Program page scraped successfully",
      inserted_id: data[0].id,
    });
  } catch (err) {
    console.error("Scrape failed:", err.message);

    await supabase
      .schema("ingestion")
      .from("scrape_logs")
      .insert({
        university_id: req.body.university_id || null,
        status: "failed",
        error_message: err.message,
      });

    res.status(500).json({
      error: "Scraping failed",
      details: err.message,
    });
  }
});

// ============================================================
// LISTING PAGE DETECTOR
// ============================================================
const PROGRAM_URL_SIGNALS_GLOBAL = [
  "program","programme","programmes","postgraduate","taught",
  "degree","graduate","master","phd","doctoral","course",
  "faculty","school","department","study","academic",
  "msc","mba","med","llm","meng","certificate","diploma",
];

function isListingPage(html, sourceUrl) {
  try {
    const $ = cheerio.load(html);

    $("script, style, nav, footer, header, .menu, .navigation, .breadcrumb").remove();

    const mainSelectors = ["main", "article", ".content", "#content", "[role='main']", ".page-content", ".main-content"];
    let mainText = "";
    for (const sel of mainSelectors) {
      const el = $(sel);
      if (el.length && el.text().trim().length > 100) {
        mainText = el.text().replace(/\s+/g, " ").trim();
        break;
      }
    }
    if (!mainText) mainText = $("body").text().replace(/\s+/g, " ").trim();

    const wordCount = mainText.split(/\s+/).length;

    // Detail-page headings — if present, this is almost certainly a program detail page
    const detailHeadings = [
      "admission requirement", "requirements", "curriculum", "degree requirement",
      "program structure", "course requirement", "application process",
      "tuition", "duration", "thesis", "dissertation", "supervisor",
      "learning outcomes", "course outline", "program overview",
    ];
    const headingText = $("h1, h2, h3").text().toLowerCase();
    const hasDetailHeadings = detailHeadings.some((h) => headingText.includes(h));

    // Count links that look like program pages
    let programLinkCount = 0;
    $("a[href]").each(function () {
      const href = ($(this).attr("href") || "").toLowerCase();
      const text = $(this).text().trim();
      if (
        text.length > 5 &&
        PROGRAM_URL_SIGNALS_GLOBAL.some((s) => href.includes(s))
      ) {
        programLinkCount++;
      }
    });

    // A page is a listing page if:
    // - Has many outbound program links AND lacks program-detail headings
    // - OR is very short (< 200 words) with no detail headings
    if (hasDetailHeadings) {
      return false; // definitely a detail page
    }

    if (programLinkCount >= 8 && wordCount < 2000) {
      console.log(`[parse] Detected listing page (programLinks: ${programLinkCount}, words: ${wordCount}): ${sourceUrl}`);
      return true;
    }

    if (wordCount < 200 && programLinkCount >= 3) {
      console.log(`[parse] Detected listing page (short+links — words: ${wordCount}, programLinks: ${programLinkCount}): ${sourceUrl}`);
      return true;
    }

    return false;
  } catch (e) {
    return false;
  }
}

// ----------------------
// PARSE SINGLE PROGRAM
// ----------------------

async function parseProgramPage(pageId, { prefetchedFeeStructures = null, prefetchedUni = null } = {}) {
  const { data: raw, error: fetchError } = await supabase
    .schema("ingestion")
    .from("raw_program_pages")
    .select("*")
    .eq("id", pageId)
    .single();

  if (fetchError || !raw) throw new Error("Page not found");

  await supabase
    .schema("ingestion")
    .from("raw_program_pages")
    .update({ parse_status: "processing" })
    .eq("id", pageId);

  try {
    if (isListingPage(raw.raw_html, raw.source_url)) {
      // ── STEP A: seed individual program links for detail-page scraping ────────
      // For universities whose individual program pages ARE scrappable, this seeds
      // them into scrape_queue so they get processed as detail pages.
      let seeded = 0;
      try {
        const $listing = cheerio.load(raw.raw_html);
        const baseHostname = new URL(raw.source_url).hostname;
        const toSeed = [];
        const seen = new Set();
        // Signal 1 (anchor text): link text must name a specific degree type.
        // "graduate" alone is intentionally excluded — it appears in news headlines,
        // admissions copy, award names, etc. Only explicit degree abbreviations and
        // words that cannot appear outside a program name are allowed.
        const DEGREE_KEYWORDS = [
          "master", "msc", "m.sc", "mba", "m.b.a", "mfa", "meng", "m.eng",
          "med ", "m.ed", "mpa", "llm", "l.l.m", "mph", "phd", "ph.d",
          "doctor of ", "doctoral", "graduate certificate", "graduate diploma",
          "postgraduate", "post-graduate",
        ];

        // Signal 2 (URL path): reject URLs whose path contains any of these segments.
        // These are section-level paths that will never be an individual program page,
        // regardless of what the anchor text says.
        const JUNK_PATH_SEGMENTS = [
          "/news/", "/events/", "/event/", "/admissions/", "/admission/",
          "/student-experience/", "/student-life/", "/student-services/",
          "/about/", "/people/", "/faculty/", "/staff/", "/alumni/",
          "/awards/", "/scholarships/", "/funding/", "/giving/",
          "/apply/", "/contact/", "/faq/", "/resources/", "/handbook/",
          "/current-students/", "/prospective-students/",
        ];

        // Signal 2 (URL terminal slug): the last path segment must not be a generic
        // section name. Program slugs are specific (msc-finance, ma-communication);
        // section slugs are generic and finite (overview, admissions, requirements).
        const GENERIC_TERMINAL_SLUGS = new Set([
          "overview", "admissions", "admission", "apply", "application",
          "news", "events", "event", "about", "contact", "index",
          "funding", "requirements", "deadlines", "deadline", "faq",
          "resources", "resource", "faculty", "people", "staff", "team",
          "student-experience", "experience", "life", "community", "support",
          "handbook", "forms", "awards", "award", "medals", "medal",
          "scholarships", "scholarship", "bursaries", "bursary",
          "ambassadors", "ambassador", "current-students", "prospective-students",
          "why-sfu", "visit", "programs", "graduate", "grad", "home",
        ]);

        $listing("a[href]").each(function () {
          const href = $listing(this).attr("href");
          if (!href || href.startsWith("mailto:") || href.startsWith("tel:")) return;
          let full;
          try { full = new URL(href, raw.source_url).toString(); } catch { return; }
          if (full.includes("#") || seen.has(full)) return;

          let parsedUrl;
          try { parsedUrl = new URL(full); } catch { return; }
          if (parsedUrl.hostname !== baseHostname) return;

          const lowerPath = parsedUrl.pathname.toLowerCase();
          if (lowerPath.endsWith(".pdf")) return;

          // Signal 1: anchor text must name a specific degree type
          const linkText = $listing(this).text().replace(/\s+/g, " ").trim().toLowerCase();
          if (linkText.length < 6) return;
          if (!DEGREE_KEYWORDS.some((k) => linkText.includes(k))) return;

          // Signal 2a: URL path must not pass through a known section directory
          if (JUNK_PATH_SEGMENTS.some((s) => lowerPath.includes(s))) return;

          // Signal 2b: terminal path slug must not be a generic section name
          const terminalSlug = lowerPath.replace(/\.html?$/, "").split("/").filter(Boolean).pop() ?? "";
          if (GENERIC_TERMINAL_SLUGS.has(terminalSlug)) return;

          seen.add(full);
          toSeed.push({ university_id: raw.university_id, program_url: full, status: "pending" });
        });
        if (toSeed.length > 0) {
          const { error: seedErr } = await supabase.schema("ingestion").from("scrape_queue")
            .upsert(toSeed, { onConflict: "program_url" });
          if (!seedErr) seeded = toSeed.length;
        }
      } catch (e) {
        console.warn(`[parse] Failed to seed links from listing page ${raw.source_url}: ${e.message}`);
      }

      // ── STEP B: extract program entries directly from this listing page ───────
      // Many universities render individual program detail pages client-side
      // (React/Vue), so those URLs are unscrappable. The listing page itself is
      // often the only static HTML we have. We extract whatever is visible in the
      // program cards/rows — name, degree level, type, field — leaving fields that
      // only appear on detail pages (tuition, deadlines, requirements) as null.
      let listingExtracted = 0;
      try {
        const $lp = cheerio.load(raw.raw_html);
        $lp("script, style, nav, footer, header, .menu, .navigation, .breadcrumb").remove();

        let listingText = "";
        for (const sel of ["main", "article", ".content", "#content", "[role='main']", ".page-content", ".main-content"]) {
          const el = $lp(sel);
          if (el.length && el.text().trim().length > 100) {
            listingText = el.text().replace(/\s+/g, " ").trim();
            break;
          }
        }
        if (!listingText) listingText = $lp("body").text().replace(/\s+/g, " ").trim();

        const listingPrompt = `You are extracting a list of graduate programs from a university programs directory page.
Return STRICT JSON only. No markdown, no explanation.

This page is a programs index — it lists many programs in summary cards or rows.
Extract EVERY distinct program entry you can find. One object per program.

For each program return ONLY these fields (omit everything else):
- program_name: Full official name exactly as shown. Include the degree abbreviation in parentheses if visible, e.g. "Chemical Engineering (MSc)", "Business Administration (MBA)", "Philosophy (PhD)".
- degree_level: "PG" for Masters / PhD / Graduate Certificate / Graduate Diploma. "UG" for Bachelor. Default to "PG" for graduate school pages.
- program_type: "research" (thesis-based), "professional" (coursework/no thesis), "doctoral" (any PhD or EdD), or null if unclear.
- field_category: exactly one of:
    "engineering & tech", "business, management and economics", "science & applied science",
    "medicine, health and life science", "social science & humanities", "arts, design & creative studies",
    "law, public policy & governance", "hospitality, tourism & service industry",
    "education & teaching", "agriculture, sustainability & environmental studies"

Do NOT guess or include tuition, deadlines, IELTS, GRE, duration, or any field not visible on this listing page.

Example output:
[
  {"program_name": "Chemical Engineering (MSc)", "degree_level": "PG", "program_type": "research", "field_category": "engineering & tech"},
  {"program_name": "Business Administration (MBA)", "degree_level": "PG", "program_type": "professional", "field_category": "business, management and economics"},
  {"program_name": "Philosophy (PhD)", "degree_level": "PG", "program_type": "doctoral", "field_category": "social science & humanities"}
]

Page content:
${listingText.substring(0, 10000)}`;

        const listingCompletion = await callOpenAI({
          model: "gpt-4o-mini",
          messages: [{ role: "user", content: listingPrompt }],
          temperature: 0,
        }, 60000);

        const rawListing = listingCompletion.choices[0].message.content;
        let listingPrograms;
        try {
          listingPrograms = JSON.parse(rawListing.replace(/```json|```/g, "").trim());
        } catch {
          const match = rawListing.match(/\[[\s\S]*\]/);
          if (match) listingPrograms = JSON.parse(match[0]);
        }

        if (Array.isArray(listingPrograms) && listingPrograms.length > 0) {
          const feeStructures = prefetchedFeeStructures !== null
            ? prefetchedFeeStructures
            : (await supabase.schema("ingestion").from("university_fee_structure").select("*").eq("university_id", raw.university_id)).data;

          const toInsert = [];
          for (const p of listingPrograms) {
            if (!p.program_name) continue;
            const { data: existing } = await supabase
              .schema("ingestion").from("parsed_programs")
              .select("id")
              .eq("university_id", raw.university_id)
              .eq("program_name", p.program_name)
              .eq("degree_level", p.degree_level || "PG")
              .limit(1);
            if (existing && existing.length > 0) continue;

            const tuitionResult = resolveTuition(p.program_name, p.program_type, raw.university_id, feeStructures);
            const tuitionUSDFromFee = tuitionResult
              ? Math.round(tuitionResult.amount * (CURRENCY_TO_USD[tuitionResult.currency] || 1.0))
              : null;

            toInsert.push({
              raw_page_id: raw.id,
              university_id: raw.university_id,
              program_name: p.program_name,
              degree_level: p.degree_level || "PG",
              program_type: p.program_type || null,
              field_category: VALID_FIELD_CATEGORIES.includes(p.field_category) ? p.field_category : null,
              tuition_usd: tuitionUSDFromFee,
              // Fields only available on detail pages — not present on a listing page
              duration_years: null,
              duration_confidence: "low",
              official_duration_value: null,
              official_duration_unit: null,
              official_duration_text: null,
              total_credits_required: null,
              credit_system: null,
              completion_time_value: null,
              completion_time_unit: null,
              tuition_raw_text: null,
              internship_available: false,
              gre_required: false,
              gmat_required: false,
              scholarship_available: false,
              scholarship_details: null,
              funding_guaranteed: false,
              ielts_minimum: null,
              pte_minimum: null,
              toefl_minimum: null,
              application_deadline_intl: null,
              application_materials: [],
              min_gpa_percentage: null,
              accepts_backlogs: true,
              subjects_required: [],
              work_experience_required: 0,
              validation_status: "pending",
              parse_status: "parsed",
            });
          }

          if (toInsert.length > 0) {
            const { error: insertErr } = await supabase
              .schema("ingestion").from("parsed_programs")
              .upsert(toInsert, { onConflict: "raw_page_id,program_name" });
            if (!insertErr) listingExtracted = toInsert.length;
          }
        }
      } catch (e) {
        console.warn(`[parse] Listing page direct extraction failed for ${raw.source_url}: ${e.message}`);
      }

      await supabase.schema("ingestion").from("raw_program_pages")
        .update({ parse_status: "parsed" }).eq("id", pageId);
      console.log(`[parse] Listing page: seeded=${seeded} links, extracted=${listingExtracted} programs: ${raw.source_url}`);
      return { success: true, programs: [], seeded, listingExtracted };
    }

    const $ = cheerio.load(raw.raw_html);

    let structuredMetadata = "";
    $("dl").each(function () {
      const pairs = [];
      $(this).find("dt").each(function () {
        const key = $(this).text().trim();
        const val = $(this).next("dd").text().trim();
        if (key && val) pairs.push(`${key}: ${val}`);
      });
      if (pairs.length) structuredMetadata += pairs.join("\n") + "\n\n";
    });
    const metaSelectors = [
      ".program-meta",".program-info",".program-details",".info-box",
      ".sidebar-info",".course-info",".aside","[class*='program-overview']",
      "[class*='program-summary']","[class*='key-info']","[class*='course-details']",
      "[class*='sidebar']",".callout",".highlight-box",".quick-facts",".program-facts",
    ];
    for (const sel of metaSelectors) {
      $(sel).each(function () {
        const text = $(this).text().replace(/\s+/g, " ").trim();
        if (text.length > 20 && text.length < 3000) structuredMetadata += text + "\n\n";
      });
    }
    let tableText = "";
    $("table").each(function () {
      $(this).find("tr").each(function () {
        const cells = [];
        $(this).find("th, td").each(function () { cells.push($(this).text().trim()); });
        if (cells.length) tableText += cells.join(" | ") + "\n";
      });
      tableText += "\n";
    });

    $(
      "script, style, nav, footer, header, aside, .menu, .sidebar, .navigation, .breadcrumb, .cookie, .banner, .advertisement",
    ).remove();

    // Prioritise main content areas
    const mainSelectors = [
      "main",
      "article",
      ".content",
      "#content",
      ".program-content",
      ".page-content",
      ".main-content",
      "[role='main']",
    ];
    let contentText = "";

    for (const selector of mainSelectors) {
      const el = $(selector);
      if (el.length && el.text().trim().length > 500) {
        contentText = el.text().replace(/\s+/g, " ").trim();
        break;
      }
    }

    // Fallback to full body if no main content found
    if (!contentText) {
      contentText = $("body").text().replace(/\s+/g, " ").trim();
    }

    const trimmedText = [
      "=== PROGRAM METADATA (Duration, Credits, Deadlines, Experiential Learning) ===",
      structuredMetadata.substring(0, 3000),
      "=== TABLES (Fees, Requirements, Deadlines) ===",
      tableText.substring(0, 2000),
      "=== MAIN CONTENT ===",
      contentText.substring(0, 7000),
    ].join("\n").substring(0, 12000);

    const prompt = `
You are extracting structured data from a university program page.
Return STRICT JSON only. No markdown, no explanation, no extra text.

IMPORTANT: Many pages list MULTIPLE degrees (e.g. MA, MSc, PhD in the same subject area).
You MUST return an ARRAY of program objects — one object per distinct degree.
Even if only one degree is found, return it as a single-element array.
Example: [{"program_name": "Master of Science", ...}, {"program_name": "Doctor of Philosophy", ...}]

FIELDS TO EXTRACT:

- program_name: Full official program name EXACTLY as shown in the page heading.
  CRITICAL: Always include the degree abbreviation in parentheses if it appears on the page.
  CORRECT: "Aerospace Engineering (MEng)", "Computer Science (MCompSc)", "Business Administration (MBA)"
  WRONG:   "Aerospace MEng", "Computer Science Masters", "Aerospace Engineering Master"
  Never shorten, abbreviate, or rearrange the subject area name.
- degree_level: Must be exactly "UG" or "PG". Masters, PhD, Graduate Certificate, Graduate Diploma, MBA = PG. Bachelor = UG.
  IMPORTANT: If this page is from a graduate studies or graduate programs section of a university website, ALL programs should be PG. Never classify a program as UG if the page context is clearly about graduate-level offerings.

PROGRAM TYPE:
- program_type: Must be exactly one of:
    research — thesis-based, research-focused, leads to academic career. Keywords: thesis, dissertation, research, supervisor, lab
    professional — coursework-based, industry-focused, no thesis. Keywords: coursework, capstone, project, industry, professional, applied
    doctoral — any PhD or doctoral degree regardless of type

DURATION:
- official_duration_value: numeric value of advertised program length. 
  Convert word-based numbers to digits: "one"=1, "two"=2, "three"=3, "four"=4, "five"=5.
  For ranges like "one or two years", "two to three years", "1-2 years" — always take the AVERAGE value.
  So "one or two years" = 1.5, "two to three years" = 2.5, "1-2 years" = 1.5.
  Look for patterns like "X years of funding" or "X-year program" as duration signals.
- official_duration_unit: "months" or "years"
- official_duration_text: exact quoted text from page describing duration
- total_credits_required: Numeric credit count if stated.
  Look in PROGRAM METADATA section FIRST for patterns like "Credits: 45 credits".
  Also look for: "(45 credits)", "45-credit program", "requires 45 credits", "minimum of 45 credits".
  Return null only if no credit count is found anywhere on the page.
- credit_system: "US", "UK", "ECTS", "AUS", "CAN"
- completion_time_value: numeric value if average completion time is mentioned
- completion_time_unit: "months" or "years"

TUITION:
- tuition_raw_text: exact fee text for INTERNATIONAL students only.
  Priority order:
  1. Annual program fee (preferred)
  2. Per-term or per-instalment fee — include the term "per term" or "per instalment" in the text
  3. Per-credit fee — only if no other fee is available, include "per credit" in the text
  Never return domestic student fees.
  If only domestic fees are shown, return null.

FIELD:
- field_category: must be exactly one of:
    engineering & tech,
    business, management and economics,
    science & applied science,
    medicine, health and life science,
    social science & humanities,
    arts, design & creative studies,
    law, public policy & governance,
    hospitality, tourism & service industry,
    education & teaching,
    agriculture, sustainability & environmental studies

INTERNSHIP:
- internship_available: true or false
  Set TRUE if the page OR the PROGRAM METADATA section mentions ANY of:
  internship, co-op, coop, practicum, fieldwork, field placement,
  field experience, work placement, work-integrated learning, industry project,
  clinical placement, clinical experience, experiential learning, applied project,
  community placement, industry internship, professional experience.
  ALSO look in PROGRAM METADATA for entries like "Experiential learning: Co-op, Internship".

GRE / GMAT:
- gre_required: true or false
  Set TRUE if GRE is mentioned as required or strongly recommended for admission.
  Set FALSE if GRE is optional, waived, not mentioned, or only recommended.
- gmat_required: true or false
  Set TRUE if GMAT is mentioned as required or strongly recommended for admission.
  Set FALSE if GMAT is optional, waived, not mentioned, or only recommended.

SCHOLARSHIP:
- scholarship_available: true or false
  Set TRUE if the page mentions ANY of:
  scholarship, bursary, fellowship, funding, award, financial aid,
  graduate award, entrance award, merit award, assistantship,
  teaching assistantship, research assistantship, tuition waiver,
  stipend, funded position
- scholarship_details: exact text describing scholarship or funding opportunity.
  Include amounts if mentioned. Return null if none found.
- funding_guaranteed: true or false
  Set TRUE only if the page explicitly states all students are funded,
  or funding is guaranteed. Common for research PhDs.
  Set FALSE if funding is competitive, optional, or not mentioned.

ENGLISH LANGUAGE REQUIREMENTS:
- ielts_minimum: numeric minimum IELTS overall band score required (e.g. 6.5). Return null if not stated.
- pte_minimum: numeric minimum PTE Academic score required (e.g. 63). Return null if not stated.
- toefl_minimum: numeric minimum TOEFL iBT score required (e.g. 90). Return null if not stated.

ACADEMIC REQUIREMENTS:
- min_gpa_percentage: minimum academic average or GPA required for admission as a percentage (0-100).
  Convert GPA to percentage if needed: 3.0/4.0 = 75%, 3.3/4.0 = 82%, 3.7/4.0 = 92%.
  Return null if not stated.
- accepts_backlogs: true or false
  Set FALSE if the page explicitly states no backlogs, no failed courses, clean academic record required.
  Set TRUE if backlogs are not mentioned or are acceptable.
- work_experience_required: number of years of work experience required.
  Return 0 if not required or not mentioned.
  Common for MBA (2-5 years) and some professional masters.

SUBJECT REQUIREMENTS (for UG programs):
- subjects_required: array of subjects required at senior secondary / high school level.
  Use only these values: ["Mathematics", "Physics", "Chemistry", "Biology", "Economics",
  "Commerce", "Computer Science", "English", "Arts/Humanities"]
  Example: ["Mathematics", "Physics"] for Engineering programs.
  Return empty array [] if no specific subjects required or if this is a PG program.

APPLICATION:
- application_deadline_intl: Deadline for INTERNATIONAL students.
  IMPORTANT — many pages use section-based formats:
    "FALL: July 1 (U.S. and international)" → return "July 1"
    "March 1 (international students)" → return "March 1"
    "January 15 for international applicants" → return "January 15"
    "Rolling admissions" → return "Rolling admissions"
  If multiple intake terms, return the Fall or primary intake deadline.
  Return null only if no deadline information exists on the page.
- application_materials: array of strings listing required application documents.
  Examples: ["CV", "Statement of Purpose", "3 Reference Letters", "Transcripts", "Writing Sample"]
  Return empty array [] if not found.

RULES:
- Return null for anything not clearly stated on the page
- Do not guess or infer
- Do not fabricate values
- If a field is ambiguous, return null
- Check the PROGRAM METADATA section first for Duration, Credits, Experiential learning, and Deadlines.

Content:
${trimmedText}
`;

    const completion = await callOpenAI({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      temperature: 0,
    }, 90000);

    const rawContent = completion.choices[0].message.content;
    let parsed;
    try {
      parsed = JSON.parse(rawContent.replace(/```json|```/g, "").trim());
    } catch (jsonErr) {
      // Try to salvage a JSON array or object from the response
      const match = rawContent.match(/\[[\s\S]*\]|\{[\s\S]*\}/);
      if (match) {
        try {
          parsed = JSON.parse(match[0]);
        } catch (e2) {
          throw new Error(`GPT returned unparseable JSON: ${rawContent.substring(0, 300)}`);
        }
      } else {
        throw new Error(`GPT returned unparseable JSON: ${rawContent.substring(0, 300)}`);
      }
    }

    // Handle both single object (legacy) and array (multi-degree pages)
    const programList = Array.isArray(parsed) ? parsed : [parsed];

    // Use pre-fetched fee structures if provided (avoids N+1 when called in batch)
    const feeStructures = prefetchedFeeStructures !== null
      ? prefetchedFeeStructures
      : (await supabase
          .schema("ingestion")
          .from("university_fee_structure")
          .select("*")
          .eq("university_id", raw.university_id)
        ).data;

    // Use pre-fetched university info if provided
    const uniInfo = prefetchedUni !== null
      ? prefetchedUni
      : (await supabase
          .schema("core")
          .from("universities")
          .select("credits_per_year")
          .eq("id", raw.university_id)
          .single()
        ).data;

    for (const program of programList) {
      if (!program.program_name) continue;
      let duration_years = null;
      if (program.official_duration_value && program.official_duration_unit) {
        duration_years =
          program.official_duration_unit === "months"
            ? program.official_duration_value / 12
            : program.official_duration_value;
      }

      // Fallback: calculate from credits using university's credits_per_year
      if (!duration_years && program.total_credits_required && uniInfo?.credits_per_year) {
        duration_years = Math.ceil(
          program.total_credits_required / uniInfo.credits_per_year,
        );
      }

      console.log(
        `[parse-fees] ${program.program_name} | type=${program.program_type} | uni=${raw.university_id} | feeStructures=${feeStructures?.length || 0}`,
      );

      const tuitionResult = resolveTuition(
        program.program_name,
        program.program_type,
        raw.university_id,
        feeStructures,
      );
      console.log(`[parse-fees] resolveTuition returned: ${JSON.stringify(tuitionResult)}`);
      const tuition_usd = tuitionResult
        ? Math.round(tuitionResult.amount * (CURRENCY_TO_USD[tuitionResult.currency] || 1.0))
        : null;

      const { data: existingProgram } = await supabase
        .schema("ingestion")
        .from("parsed_programs")
        .select("id")
        .eq("university_id", raw.university_id)
        .eq("program_name", program.program_name)
        .eq("degree_level", program.degree_level || "PG")
        .limit(1);

      if (existingProgram && existingProgram.length > 0) {
        console.log(`[parse] Skipping duplicate: ${program.program_name}`);
        continue;
      }

      const { error: insertError } = await supabase
        .schema("ingestion")
        .from("parsed_programs")
        .upsert(
          {
            raw_page_id: raw.id,
            university_id: raw.university_id,
            program_name: program.program_name,
            degree_level: program.degree_level,
            program_type: program.program_type || null,
            duration_years,
            duration_confidence: "high",
            official_duration_value: program.official_duration_value || null,
            official_duration_unit: program.official_duration_unit || null,
            official_duration_text: program.official_duration_text || null,
            total_credits_required: program.total_credits_required || null,
            credit_system: program.credit_system || null,
            completion_time_value: program.completion_time_value || null,
            completion_time_unit: program.completion_time_unit || null,
            tuition_usd,
            tuition_raw_text: program.tuition_raw_text || null,
            field_category: VALID_FIELD_CATEGORIES.includes(
              program.field_category,
            )
              ? program.field_category
              : null,
            internship_available: program.internship_available || false,
            gre_required: program.gre_required || false,
            gmat_required: program.gmat_required || false,
            scholarship_available: program.scholarship_available || false,
            scholarship_details: program.scholarship_details || null,
            funding_guaranteed: program.funding_guaranteed || false,
            ielts_minimum: program.ielts_minimum || null,
            pte_minimum: program.pte_minimum || null,
            toefl_minimum: program.toefl_minimum || null,
            application_deadline_intl:
              program.application_deadline_intl || null,
            application_materials: program.application_materials || [],
            min_gpa_percentage: program.min_gpa_percentage || null,
            accepts_backlogs: program.accepts_backlogs !== false,
            subjects_required: program.subjects_required || [],
            work_experience_required: program.work_experience_required || 0,
            validation_status: "pending",
            parse_status: "parsed",
          },
          { onConflict: "raw_page_id,program_name" },
        );

      if (insertError) {
        console.error(
          "Insert error for",
          program.program_name,
          insertError.message,
        );
      }
    }

    await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .update({ parse_status: "parsed" })
      .eq("id", pageId);

    return { success: true, programs: programList.map((p) => p.program_name) };
  } catch (err) {
    await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .update({ parse_status: "failed" })
      .eq("id", pageId);
    throw err;
  }
}

app.post("/parse-program", async (req, res) => {
  try {
    const pageId = req.body.page_id;

    let rawId;
    if (pageId) {
      rawId = pageId;
    } else {
      const { data: raw } = await supabase
        .schema("ingestion")
        .from("raw_program_pages")
        .select("id")
        .eq("parse_status", "pending")
        .order("scraped_at", { ascending: true })
        .limit(1)
        .single();
      if (!raw) return res.json({ message: "No pending pages" });
      rawId = raw.id;
    }

    const result = await parseProgramPage(rawId);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ----------------------
// PARSE BATCH
// ----------------------

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

app.post("/parse-batch", async (req, res) => {
  const limit = req.body.limit || 20;
  const concurrency = req.body.concurrency || 2;

  res.json({ message: "Started", status: "running" });

  setImmediate(async () => {
    const { data: pages } = await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .select("id")
      .eq("parse_status", "pending")
      .limit(limit);

    if (!pages || pages.length === 0) {
      console.log("No pending pages");
      return;
    }

    console.log(
      `Parsing ${pages.length} pages with concurrency ${concurrency}`,
    );

    let success = 0;
    let failed = 0;

    for (let i = 0; i < pages.length; i += concurrency) {
      const chunk = pages.slice(i, i + concurrency);

      await Promise.all(
        chunk.map(async (page) => {
          try {
            await parseProgramPage(page.id);
            success++;
          } catch (err) {
            console.error(`Failed page ${page.id}:`, err.message);
            failed++;
          }
        }),
      );

      console.log(
        `Progress: ${Math.min(i + concurrency, pages.length)}/${pages.length} — success: ${success}, failed: ${failed}`,
      );
    }

    console.log(`Batch complete — success: ${success}, failed: ${failed}`);
  })();
});

// ==============================
// CRAWLER — DISCOVER PROGRAM URLS
// ==============================

app.post("/crawl-university", async (req, res) => {
  try {
    const {
      university_id,
      directory_url,
      directory_urls,
      depth = 1,
    } = req.body;

    if (!university_id) {
      return res.status(400).json({ error: "university_id is required" });
    }

    const startUrls =
      directory_urls || (directory_url ? [directory_url] : null);
    if (!startUrls || startUrls.length === 0) {
      return res
        .status(400)
        .json({ error: "directory_url or directory_urls array is required" });
    }

    let totalDiscovered = 0;
    for (const url of startUrls) {
      const count = await crawlDirectory(university_id, url, depth);
      totalDiscovered += count;
    }

    res.json({
      message: "Crawl complete",
      discovered: totalDiscovered,
      queued: totalDiscovered,
    });
  } catch (err) {
    console.error("Crawl failed:", err.message);
    res.status(500).json({ error: "Crawl failed", details: err.message });
  }
});

// ==============================
// PROCESS SCRAPE QUEUE
// ==============================

app.post("/process-queue", async (req, res) => {
  const limit = req.body.limit || 500;
  const university_id = req.body.university_id;

  res.json({ message: "Started", status: "running" });

  setImmediate(async () => {
    try {
      // Reset any stale "processing" items back to "pending"
      if (university_id) {
        await supabase
          .schema("ingestion")
          .from("scrape_queue")
          .update({ status: "pending" })
          .eq("status", "processing")
          .eq("university_id", university_id);
      }

      let query = supabase
        .schema("ingestion")
        .from("scrape_queue")
        .select("*")
        .eq("status", "pending");

      if (university_id) {
        query = query.eq("university_id", university_id);
      }

      const { data: queueItems, error: qErr } = await query.limit(limit);

      if (qErr) { console.error("[queue] Query error:", qErr); return; }
      if (!queueItems || queueItems.length === 0) {
        console.log("[queue] Queue is empty");
        return;
      }

      console.log(`Processing ${queueItems.length} URLs from queue`);

      const results = { success: 0, failed: 0, skipped: 0 };

      for (const item of queueItems) {
        try {
          await supabase
            .schema("ingestion")
            .from("scrape_queue")
            .update({ status: "processing" })
            .eq("id", item.id);

          const scrapeResponse = await axios.get(item.program_url, {
            headers: { "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)" },
            timeout: 30000,
          });

          let html = scrapeResponse.data;

          if (!html || html.length < 500) {
            console.log(
              `[queue] Axios got small page, trying Browserless for: ${item.program_url}`,
            );
            const browser = await getBrowser();
            try {
              const page = await browser.newPage();
              await page.goto(item.program_url, {
                waitUntil: "domcontentloaded",
                timeout: 30000,
              });
              await new Promise((r) => setTimeout(r, 3000));
              html = await page.content();
              console.log(
                `[queue] Browserless got ${html.length} chars for: ${item.program_url}`,
              );
            } catch (bErr) {
              console.error(`[queue] Browserless also failed: ${bErr.message}`);
              await supabase
                .schema("ingestion")
                .from("scrape_queue")
                .update({
                  status: "failed",
                  error_message: `Browserless failed: ${bErr.message}`,
                })
                .eq("id", item.id);
              results.failed++;
              continue;
            } finally {
              await browser.close();
            }

            if (!html || html.length < 500) {
              await supabase
                .schema("ingestion")
                .from("scrape_queue")
                .update({
                  status: "failed",
                  error_message: "Page too small even after Browserless",
                })
                .eq("id", item.id);
              results.failed++;
              continue;
            }
          }

          await supabase.schema("ingestion").from("raw_program_pages").upsert(
            {
              university_id: item.university_id,
              source_url: item.program_url,
              raw_html: html,
              parse_status: "pending",
            },
            { onConflict: "source_url" },
          );

          await supabase
            .schema("ingestion")
            .from("scrape_queue")
            .update({ status: "scraped", scraped_at: new Date().toISOString() })
            .eq("id", item.id);

          results.success++;
          console.log(
            `[queue] ✓ ${results.success}/${queueItems.length} scraped: ${item.program_url}`,
          );
          await delay(1500);
        } catch (err) {
          console.error(
            `[queue] ✗ Failed to scrape: ${item.program_url}`,
            err.message,
          );
          await supabase
            .schema("ingestion")
            .from("scrape_queue")
            .update({ status: "failed", error_message: err.message })
            .eq("id", item.id);
          results.failed++;
        }
      }

      if (university_id && results.success > 0) {
        console.log(`[queue] Parsing ${results.success} newly scraped pages...`);
        try {
          const parsed = await parsePagesForUniversity(university_id);
          results.parsed = parsed;
          console.log(`[queue] Parsed ${parsed} programs`);

          const fixed = await autoFixFieldCategories(university_id);
          results.fixed = fixed;
          console.log(`[queue] Fixed ${fixed} field categories`);
        } catch (parseErr) {
          console.error("[queue] Parse step failed:", parseErr.message);
        }
      }

      console.log(`[queue] Complete — success: ${results.success}, failed: ${results.failed}`);
    } catch (err) {
      console.error("[queue] Processing error:", err.message);
    }
  });
});

// ==============================
// MIGRATE PARSED → CORE
// ==============================

app.post("/migrate", async (req, res) => {
  try {
    const { data: parsed, error: pErr } = await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .select("*")
      .eq("validation_status", "pending");

    if (pErr) return res.status(500).json({ error: pErr });
    if (!parsed || parsed.length === 0) {
      return res.json({ message: "No programs to migrate" });
    }

    console.log(`Migrating ${parsed.length} programs to core.courses...`);

    let success = 0;
    let failed = 0;
    let skipped = 0;

    const feeCache = {};

    for (const p of parsed) {
      try {
        if (!feeCache[p.university_id]) {
          const { data: fs } = await supabase
            .schema("ingestion")
            .from("university_fee_structure")
            .select("*")
            .eq("university_id", p.university_id);
          feeCache[p.university_id] = fs || [];
        }
        const feeStructures = feeCache[p.university_id];

        let finalTuitionUSD = p.tuition_usd ? Math.round(p.tuition_usd) : null;

        if (!finalTuitionUSD) {
          const tuitionResult = resolveTuition(
            p.program_name,
            p.program_type,
            p.university_id,
            feeStructures,
          );
          console.log(
            `[migrate-fees] ${p.program_name} → resolveTuition: ${JSON.stringify(tuitionResult)}`,
          );
          if (tuitionResult && tuitionResult.amount) {
            const rate = CURRENCY_TO_USD[tuitionResult.currency] || 1.0;
            finalTuitionUSD = Math.round(tuitionResult.amount * rate);
          }
        }

        if (!finalTuitionUSD) {
          console.log(`[migrate-warn] No tuition resolved for: ${p.program_name} — inserting with null`);
        }

        const { error: insertError } = await supabase
          .schema("core")
          .from("courses")
          .insert({
            name: p.program_name,
            university_id: p.university_id,
            degree_level: p.degree_level,
            duration_years: p.duration_years ?? undefined,
            tuition_usd: finalTuitionUSD,
            field_category: p.field_category,
            internship_available: p.internship_available || false,
            gre_required: p.gre_required || false,
            gmat_required: p.gmat_required || false,
            scholarship_available: p.scholarship_available || false,
            scholarship_details: p.scholarship_details || null,
            funding_guaranteed: p.funding_guaranteed || false,
            program_type: p.program_type || null,
            ielts_minimum: p.ielts_minimum || null,
            pte_minimum: p.pte_minimum || null,
            toefl_minimum: p.toefl_minimum || null,
            min_gpa_percentage: p.min_gpa_percentage || null,
            accepts_backlogs: p.accepts_backlogs !== false,
            work_experience_required: p.work_experience_required || 0,
            subjects_required: p.subjects_required || [],
            application_deadline_intl: p.application_deadline_intl || null,
            application_materials: p.application_materials || [],
            data_quality: "parsed",
          });

        if (insertError) {
          console.error(
            `Migration failed for ${p.program_name}:`,
            insertError.message,
          );
          failed++;
          continue;
        }

        await supabase
          .schema("ingestion")
          .from("parsed_programs")
          .update({ validation_status: "migrated" })
          .eq("id", p.id);

        success++;
      } catch (err) {
        console.error(`Migration error for ${p.program_name}:`, err.message);
        failed++;
      }
    }

    const migratedUniIds = [...new Set(parsed.map((p) => p.university_id))];
    let overridesApplied = 0;

    for (const uniId of migratedUniIds) {
      const { data: feeStructures } = await supabase
        .schema("ingestion")
        .from("university_fee_structure")
        .select("*")
        .eq("university_id", uniId);

      if (!feeStructures || feeStructures.length === 0) continue;

      const { data: courses } = await supabase
        .schema("core")
        .from("courses")
        .select("id, name, degree_level, program_type")
        .eq("university_id", uniId);

      for (const course of courses || []) {
        console.log(
          `[migrate-fees] ${course.name} | type=${course.program_type} | uni=${uniId} | feeStructures=${feeStructures?.length || 0}`,
        );
        const tuitionResult = resolveTuition(
          course.name,
          course.program_type,
          uniId,
          feeStructures,
        );
        console.log(`[migrate-fees] resolveTuition returned: ${JSON.stringify(tuitionResult)}`);
        if (tuitionResult) {
          const rate = CURRENCY_TO_USD[tuitionResult.currency] || 1.0;
          const tuitionUSD = Math.round(tuitionResult.amount * rate);
          await supabase
            .schema("core")
            .from("courses")
            .update({
              tuition_usd: tuitionUSD,
              data_quality: "international_rate_official",
            })
            .eq("id", course.id);
          overridesApplied++;
        }
      }
    }

    console.log(
      `Migration complete — success: ${success}, failed: ${failed}, skipped: ${skipped}, fee overrides applied: ${overridesApplied}`,
    );
    res.json({
      message: "Migration complete",
      success,
      failed,
      skipped,
      overrides_applied: overridesApplied,
    });
  } catch (err) {
    console.error("Migration error:", err.message);
    res.status(500).json({ error: "Migration failed", details: err.message });
  }
});

// ==============================
// ML TRACKING ROUTES
// ==============================

app.post("/ml/session", async (req, res) => {
  try {
    const { data, error } = await supabase
      .schema("ml")
      .from("user_sessions")
      .insert({ completed: false })
      .select()
      .single();

    if (error) throw error;
    res.json({ session_id: data.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/ml/step-complete", async (req, res) => {
  try {
    const { session_id, step_number, time_spent_seconds, answers } = req.body;

    await supabase.schema("ml").from("question_events").insert({
      session_id,
      step_number,
      time_spent_seconds,
    });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/ml/dropoff", async (req, res) => {
  try {
    const { session_id, dropped_at_step, time_spent_total_seconds } = req.body;

    await supabase.schema("ml").from("session_dropoffs").insert({
      session_id,
      dropped_at_step,
      time_spent_total_seconds,
    });

    await supabase
      .schema("ml")
      .from("user_sessions")
      .update({ completed: false })
      .eq("id", session_id);

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/ml/save-profile", async (req, res) => {
  try {
    const { session_id, ...profile } = req.body;

    await supabase
      .schema("ml")
      .from("user_sessions")
      .update(profile)
      .eq("id", session_id);

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/ml/recommendations", async (req, res) => {
  try {
    const { session_id, recommendations } = req.body;

    const rows = recommendations.map((r, index) => ({
      session_id,
      course_id: r.course_id,
      rank_shown: index + 1,
      final_score: r.finalScore,
      country_score: r.scores?.country,
      course_score: r.scores?.course,
      university_score: r.scores?.university,
    }));

    await supabase.schema("ml").from("recommendations_shown").insert(rows);

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// FIELD CATEGORY KEYWORD MAP
// ============================================================
const FIELD_CATEGORY_KEYWORDS = {
  "engineering & tech": [
    "engineer",
    "engineering",
    "software",
    "computer science",
    "computing",
    "electrical",
    "mechanical",
    "civil",
    "chemical",
    "biomedical",
    "aerospace",
    "robotics",
    "automation",
    "data science",
    "artificial intelligence",
    "machine learning",
    "cybersecurity",
    "network",
    "telecommunications",
    "materials",
    "nanotechnology",
    "systems",
    "industrial",
    "manufacturing",
    "petroleum",
    "mining",
    "structural",
    "information technology",
    "information systems",
    "digital",
    "technology",
  ],
  "business, management and economics": [
    "business",
    "management",
    "economics",
    "finance",
    "accounting",
    "mba",
    "marketing",
    "commerce",
    "entrepreneurship",
    "administration",
    "supply chain",
    "logistics",
    "operations",
    "human resources",
    "organizational",
    "leadership",
    "strategy",
    "analytics",
    "fintech",
    "banking",
    "investment",
    "taxation",
    "development studies",
    "international business",
    "project management",
  ],
  "science & applied science": [
    "physics",
    "chemistry",
    "mathematics",
    "statistics",
    "biology",
    "biochemistry",
    "molecular",
    "genetics",
    "neuroscience",
    "astronomy",
    "astrophysics",
    "geology",
    "geography",
    "oceanography",
    "meteorology",
    "applied science",
    "biophysics",
    "computational",
    "quantitative",
    "photonics",
    "optics",
    "nuclear",
  ],
  "medicine, health and life science": [
    "medicine",
    "medical",
    "health",
    "nursing",
    "pharmacy",
    "dentistry",
    "dental",
    "physiotherapy",
    "occupational therapy",
    "rehabilitation",
    "public health",
    "epidemiology",
    "nutrition",
    "dietetics",
    "speech",
    "audiology",
    "oncology",
    "cardiology",
    "psychiatry",
    "clinical",
    "healthcare",
    "life science",
    "biomedical science",
    "pathology",
    "microbiology",
    "immunology",
    "virology",
    "global health",
    "mental health",
    "kinesiology",
    "exercise science",
  ],
  "social science & humanities": [
    "psychology",
    "sociology",
    "anthropology",
    "political science",
    "history",
    "philosophy",
    "linguistics",
    "literature",
    "english",
    "french",
    "languages",
    "communication",
    "media",
    "journalism",
    "social work",
    "criminology",
    "international relations",
    "cultural studies",
    "religious studies",
    "theology",
    "jewish",
    "islamic",
    "gender studies",
    "indigenous",
    "archaeology",
    "information studies",
    "library",
    "archives",
    "knowledge management",
    "cognitive science",
    "counselling",
    "social science",
  ],
  "arts, design & creative studies": [
    "art",
    "arts",
    "design",
    "architecture",
    "music",
    "fine art",
    "visual",
    "photography",
    "film",
    "cinema",
    "theatre",
    "drama",
    "dance",
    "creative writing",
    "digital media",
    "game design",
    "animation",
    "fashion",
    "interior design",
    "urban design",
    "landscape",
    "graphic",
    "studio",
    "performing arts",
    "conducting",
    "composition",
    "musicology",
  ],
  "law, public policy & governance": [
    "law",
    "legal",
    "juris",
    "policy",
    "governance",
    "public administration",
    "public policy",
    "regulation",
    "compliance",
    "international law",
    "human rights",
    "constitutional",
    "criminal law",
    "civil law",
    "tax law",
    "environmental law",
    "j.d",
    "ll.m",
    "bcl",
  ],
  "hospitality, tourism & service industry": [
    "hospitality",
    "tourism",
    "hotel",
    "travel",
    "events management",
    "food service",
    "culinary",
    "recreation",
    "leisure",
    "resort",
    "casino",
    "service management",
  ],
  "education & teaching": [
    "education",
    "teaching",
    "pedagogy",
    "curriculum",
    "learning",
    "instruction",
    "educational",
    "teacher",
    "school psychology",
    "applied child psychology",
    "higher education",
    "adult education",
    "special education",
    "literacy",
    "immersion",
    "early childhood",
  ],
  "agriculture, sustainability & environmental studies": [
    "agriculture",
    "agricultural",
    "environmental",
    "sustainability",
    "ecology",
    "forestry",
    "natural resources",
    "conservation",
    "climate",
    "energy",
    "renewable",
    "water",
    "soil",
    "plant science",
    "animal science",
    "food science",
    "agronomy",
    "horticulture",
    "wildlife",
    "fisheries",
    "marine",
    "clean energy",
  ],
};

const VALID_FIELD_CATEGORIES = [
  "engineering & tech",
  "business, management and economics",
  "science & applied science",
  "medicine, health and life science",
  "social science & humanities",
  "arts, design & creative studies",
  "law, public policy & governance",
  "hospitality, tourism & service industry",
  "education & teaching",
  "agriculture, sustainability & environmental studies",
];

function autoAssignFieldCategory(programName) {
  if (!programName) return null;
  const lower = programName.toLowerCase();
  let bestMatch = null;
  let bestScore = 0;
  for (const [category, keywords] of Object.entries(FIELD_CATEGORY_KEYWORDS)) {
    let score = 0;
    for (const keyword of keywords) {
      if (lower.includes(keyword.toLowerCase())) {
        score += keyword.length;
      }
    }
    if (score > bestScore) {
      bestScore = score;
      bestMatch = category;
    }
  }
  return bestScore > 4 ? bestMatch : null;
}

// ============================================================
// SMART URL PATTERN DISCOVERY
// ============================================================
const COMMON_DIRECTORY_PATTERNS = [
  "/graduate/programs",
  "/graduate-programs",
  "/grad/programs",
  "/programs/graduate",
  "/future-students/graduate-degree-programs",
  "/prospective-students/graduate-degree-programs",
  "/admissions/graduate",
  "/academics/graduate",
  "/study/graduate",
  "/programs",
  "/courses",
  "/faculties",
];

async function discoverDirectoryUrls(baseUrl) {
  for (const pattern of COMMON_DIRECTORY_PATTERNS) {
    const testUrl = baseUrl.replace(/\/$/, "") + pattern;
    try {
      const response = await axios.get(testUrl, {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)" },
        timeout: 10000,
        validateStatus: (status) => status < 404,
      });
      if (response.status === 200 && response.data.length > 2000) {
        console.log(`[discovery] Found directory at: ${testUrl}`);
        return [testUrl];
      }
    } catch (e) {
      // try next
    }
  }
  return null;
}

// ============================================================
// CORE WORKER FUNCTION
// Status flow: queued → crawling → scraping → parsing → fee_scraping → fixing → ready_for_review → migrated → failed
// ============================================================
let workerRunning = false;

async function runWorker() {
  if (workerRunning) {
    console.log("[worker] Already running, skipping");
    return;
  }
  workerRunning = true;
  console.log("[worker] Starting job check...");

  try {
    const { data: job, error } = await supabase
      .schema("ingestion")
      .from("university_jobs")
      .select("*")
      .eq("status", "queued")
      .order("created_at", { ascending: true })
      .limit(1)
      .single();

    if (error && error.code !== "PGRST116") {
      console.error("[worker] Job fetch error:", error.message);
      workerRunning = false;
      return;
    }

    if (error || !job) {
      console.log("[worker] No queued jobs found");
      workerRunning = false;
      return;
    }

    console.log(
      `[worker] Processing job for university_id: ${job.university_id}`,
    );

    // ---- STEP 1: CRAWL ----
    await updateJobStatus(job.id, "crawling");
    let directoryUrls = job.directory_urls;

    if (!directoryUrls || directoryUrls.length === 0) {
      const { data: uni } = await supabase
        .schema("core")
        .from("universities")
        .select("website_url")
        .eq("id", job.university_id)
        .single();

      if (uni?.website_url) {
        directoryUrls = await discoverDirectoryUrls(uni.website_url);
      }

      if (!directoryUrls) {
        await updateJobStatus(
          job.id,
          "failed",
          "Could not discover directory URLs",
        );
        workerRunning = false;
        return;
      }
    }

    let totalDiscovered = 0;
    for (const dirUrl of directoryUrls) {
      try {
        const crawlResult = await crawlDirectory(
          job.university_id,
          dirUrl,
          job.crawl_depth,
        );
        totalDiscovered += crawlResult;
        console.log(
          `[worker] Crawled ${dirUrl} — discovered ${crawlResult} URLs`,
        );
      } catch (e) {
        console.error(`[worker] Crawl failed for ${dirUrl}:`, e.message);
      }
    }

    await supabase
      .schema("ingestion")
      .from("university_jobs")
      .update({ urls_discovered: totalDiscovered })
      .eq("id", job.id);

    if (totalDiscovered === 0) {
      await updateJobStatus(
        job.id,
        "failed",
        "No URLs discovered during crawl",
      );
      workerRunning = false;
      return;
    }

    // ---- STEP 2 & 3: SCRAPE → PARSE (loop up to 3 rounds if listing pages seed new URLs) ----
    let totalScraped = 0, totalParsed = 0;
    const MAX_PIPELINE_ROUNDS = 3;
    for (let round = 1; round <= MAX_PIPELINE_ROUNDS; round++) {
      await updateJobStatus(job.id, "scraping");
      const scraped = await scrapeQueueForUniversity(job.university_id);
      totalScraped += scraped;
      console.log(`[worker] Round ${round}: scraped ${scraped} pages`);

      await updateJobStatus(job.id, "parsing");
      const { parsed, seeded } = await parsePagesForUniversity(job.university_id);
      totalParsed += parsed;
      console.log(`[worker] Round ${round}: parsed ${parsed} pages, seeded ${seeded} new URLs from listing pages`);

      if (seeded === 0 || round === MAX_PIPELINE_ROUNDS) break;
      console.log(`[worker] Listing pages seeded ${seeded} URLs — running pipeline round ${round + 1}`);
    }
    await supabase
      .schema("ingestion")
      .from("university_jobs")
      .update({ urls_scraped: totalScraped, urls_parsed: totalParsed })
      .eq("id", job.id);

    // ---- STEP 4: SCRAPE FEE STRUCTURE (intelligent — tries form-based then static) ----
    await updateJobStatus(job.id, "fee_scraping");
    let feeResult = false;
    try {
      feeResult = await scrapeFeeStructureIntelligent(job.university_id);
    } catch (feeErr) {
      console.error(`[worker] Fee scraping threw an error for ${job.university_id}: ${feeErr.message}`);
    }
    if (!feeResult) {
      console.warn(`[worker] ⚠ Fee scraping FAILED for university ${job.university_id} — programs will have NULL tuition unless fees are added manually to university_fee_structure`);
      await supabase
        .schema("ingestion")
        .from("university_jobs")
        .update({ error_message: "Fee scraping failed — add fees manually to university_fee_structure before migrating" })
        .eq("id", job.id);
    }

    // ---- STEP 5: AUTO-FIX field_category nulls ----
    await updateJobStatus(job.id, "fixing");
    const fixResult = await autoFixFieldCategories(job.university_id);
    console.log(`[worker] Auto-fixed ${fixResult} field categories`);

    // ---- STEP 6: COUNT READY PROGRAMS ----
    const { count } = await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .select("*", { count: "exact", head: true })
      .eq("university_id", job.university_id)
      .eq("validation_status", "pending")
      .not("field_category", "is", null);

    await supabase
      .schema("ingestion")
      .from("university_jobs")
      .update({
        status: "ready_for_review",
        programs_ready: count || 0,
        completed_at: new Date().toISOString(),
      })
      .eq("id", job.id);

    console.log(`[worker] Job complete — ${count} programs ready for review`);
  } catch (err) {
    console.error("[worker] Unexpected error:", err.message);
  }
  workerRunning = false;
}

async function updateJobStatus(jobId, status, errorMessage = null) {
  const update = { status, updated_at: new Date().toISOString() };
  if (status === "crawling") update.started_at = new Date().toISOString();
  if (errorMessage) update.error_message = errorMessage;
  const { error } = await supabase
    .schema("ingestion")
    .from("university_jobs")
    .update(update)
    .eq("id", jobId);
  if (error) {
    console.error(`[worker] Failed to update job ${jobId} → ${status}: ${error.message}`);
  }
}

// ============================================================
// PIPELINE WORKER — Stage-by-stage bulk ingestion for 500+ universities
// All universities complete stage N before any university starts stage N+1.
// A university that fails stage N is automatically skipped for N+1 onward.
// ============================================================

const PIPELINE_STAGES = ['crawl', 'scrape', 'parse', 'fee_scrape', 'fix'];
const STAGE_CONCURRENCY = { crawl: 3, scrape: 5, parse: 5, fee_scrape: 3, fix: 5 };

let pipelineWorkerRunning = false;

async function runPipelineWorker() {
  if (pipelineWorkerRunning) {
    console.log('[pipeline] Already running, skipping');
    return;
  }
  pipelineWorkerRunning = true;
  let foundJobs = false;
  try {
    // Find the active stage: lowest stage that still has pending or running jobs
    let activeStage = null;
    for (const stage of PIPELINE_STAGES) {
      const { count } = await supabase
        .schema('ingestion')
        .from('pipeline_jobs')
        .select('*', { count: 'exact', head: true })
        .eq('stage', stage)
        .in('status', ['pending', 'running']);
      if (count > 0) { activeStage = stage; break; }
    }

    if (!activeStage) {
      console.log('[pipeline] No active pipeline jobs');
    } else {
      // Pick up to CONCURRENCY pending jobs at the active stage
      const limit = STAGE_CONCURRENCY[activeStage] || 3;
      const { data: jobs } = await supabase
        .schema('ingestion')
        .from('pipeline_jobs')
        .select('*')
        .eq('stage', activeStage)
        .eq('status', 'pending')
        .order('created_at', { ascending: true })
        .limit(limit);

      if (!jobs || jobs.length === 0) {
        console.log(`[pipeline] Stage "${activeStage}": waiting for running jobs to finish`);
      } else {
        foundJobs = true;
        console.log(`[pipeline] Stage "${activeStage}": processing ${jobs.length} universities concurrently`);

        // Mark all picked jobs as running before processing starts
        await Promise.all(jobs.map(job =>
          supabase.schema('ingestion').from('pipeline_jobs')
            .update({ status: 'running', started_at: new Date().toISOString(), attempts: (job.attempts || 0) + 1 })
            .eq('id', job.id)
        ));

        // Process concurrently
        const pipelineLimit = 3;
        for (let i = 0; i < jobs.length; i += pipelineLimit) {
          const chunk = jobs.slice(i, i + pipelineLimit);
          await Promise.all(chunk.map(job => executePipelineStage(job)));
        }
      }
    }
  } catch (err) {
    console.error('[pipeline] Unexpected worker error:', err.message);
  }
  pipelineWorkerRunning = false;
  // Self-schedule: 10s if jobs were processed (more likely pending), 30s if idle
  setTimeout(runPipelineWorker, foundJobs ? 10 * 1000 : 30 * 1000);
}

async function executePipelineStage(job) {
  const { id, university_id, stage, attempts, max_attempts } = job;
  try {
    let result;
    switch (stage) {
      case 'crawl':      result = await pipelineCrawl(university_id);           break;
      case 'scrape':     result = await pipelineScrape(university_id);          break;
      case 'parse':      result = await pipelineParse(university_id);           break;
      case 'fee_scrape': result = await pipelineFeeScrapeSafe(university_id);   break;
      case 'fix':        result = await pipelineFix(university_id);             break;
      default: throw new Error(`Unknown pipeline stage: ${stage}`);
    }

    await supabase.schema('ingestion').from('pipeline_jobs')
      .update({ status: 'complete', completed_at: new Date().toISOString(), error_message: null })
      .eq('id', id);

    console.log(`[pipeline] ✓ ${stage} complete for ${university_id}`, result);

  } catch (err) {
    const currentAttempts = (attempts || 0) + 1;
    const maxAttempts = max_attempts || 3;
    console.error(`[pipeline] ✗ ${stage} failed for ${university_id} (attempt ${currentAttempts}/${maxAttempts}): ${err.message}`);

    if (currentAttempts >= maxAttempts) {
      // Permanently failed — mark stage failed and skip all downstream stages
      await supabase.schema('ingestion').from('pipeline_jobs')
        .update({ status: 'failed', error_message: err.message, completed_at: new Date().toISOString() })
        .eq('id', id);

      const nextStages = PIPELINE_STAGES.slice(PIPELINE_STAGES.indexOf(stage) + 1);
      if (nextStages.length > 0) {
        await supabase.schema('ingestion').from('pipeline_jobs')
          .update({ status: 'skipped', error_message: `Skipped: upstream stage "${stage}" failed` })
          .eq('university_id', university_id)
          .in('stage', nextStages)
          .eq('status', 'pending');
        console.log(`[pipeline] Skipped [${nextStages.join(', ')}] for ${university_id}`);
      }
    } else {
      // Retry on next worker cycle
      await supabase.schema('ingestion').from('pipeline_jobs')
        .update({ status: 'pending', error_message: `Attempt ${currentAttempts} failed: ${err.message}` })
        .eq('id', id);
    }
  }
}

// ---- Stage executors ----

async function pipelineCrawl(universityId) {
  const { data: uni } = await supabase.schema('core').from('universities')
    .select('website_url, directory_url').eq('id', universityId).single();

  const { data: existingJob } = await supabase.schema('ingestion').from('university_jobs')
    .select('directory_urls, crawl_depth')
    .eq('university_id', universityId)
    .order('created_at', { ascending: false })
    .limit(1).single();

  let directoryUrls = existingJob?.directory_urls;
  const crawlDepth = existingJob?.crawl_depth || 1;

  if (!directoryUrls || directoryUrls.length === 0) {
    if (uni?.directory_url) {
      directoryUrls = [uni.directory_url];
    } else {
      if (!uni?.website_url) throw new Error('No website URL configured for this university');
      directoryUrls = await discoverDirectoryUrls(uni.website_url);
    }
  }
  if (!directoryUrls || directoryUrls.length === 0) {
    throw new Error('Could not discover any directory URLs');
  }

  let totalDiscovered = 0;
  for (const dirUrl of directoryUrls) {
    try {
      const count = await crawlDirectory(universityId, dirUrl, crawlDepth);
      totalDiscovered += count;
      console.log(`[pipeline/crawl] ${dirUrl} → ${count} URLs`);
    } catch (e) {
      console.error(`[pipeline/crawl] ${dirUrl} failed: ${e.message}`);
    }
  }

  if (totalDiscovered === 0) throw new Error('No program URLs discovered during crawl');
  return { urls_discovered: totalDiscovered };
}

async function pipelineScrape(universityId) {
  let total = 0;
  for (let round = 1; round <= 3; round++) {
    const scraped = await scrapeQueueForUniversity(universityId);
    total += scraped;
    console.log(`[pipeline/scrape] Round ${round}: ${scraped} pages`);
    if (scraped === 0) break;
  }
  return { urls_scraped: total };
}

async function pipelineParse(universityId) {
  let totalParsed = 0, totalSeeded = 0;
  for (let round = 1; round <= 3; round++) {
    const { parsed, seeded } = await parsePagesForUniversity(universityId);
    totalParsed += parsed;
    totalSeeded += seeded;
    console.log(`[pipeline/parse] Round ${round}: ${parsed} parsed, ${seeded} seeded`);
    if (seeded === 0) break;
    // Scrape newly seeded listing-page URLs before the next parse round
    if (round < 3) {
      const extra = await scrapeQueueForUniversity(universityId);
      console.log(`[pipeline/parse] Scraped ${extra} additional pages from listing pages`);
    }
  }
  return { parsed: totalParsed, seeded: totalSeeded };
}

async function pipelineFeeScrapeSafe(universityId) {
  // Fee scraping failure is non-fatal — programs will have null tuition but pipeline continues
  try {
    const result = await scrapeFeeStructureIntelligent(universityId);
    if (!result) console.warn(`[pipeline/fee_scrape] No fees found for ${universityId} — tuition will be null`);
    return { success: !!result };
  } catch (err) {
    console.warn(`[pipeline/fee_scrape] Error for ${universityId}: ${err.message} — continuing`);
    return { success: false, warning: err.message };
  }
}

async function pipelineFix(universityId) {
  const fixed = await autoFixFieldCategories(universityId);
  const { count } = await supabase.schema('ingestion').from('parsed_programs')
    .select('*', { count: 'exact', head: true })
    .eq('university_id', universityId)
    .eq('validation_status', 'pending')
    .not('field_category', 'is', null);

  // Mark the corresponding university_job as ready_for_review
  await supabase.schema('ingestion').from('university_jobs')
    .update({ status: 'ready_for_review', programs_ready: count || 0, completed_at: new Date().toISOString() })
    .eq('university_id', universityId)
    .in('status', ['queued', 'crawling', 'scraping', 'parsing', 'fee_scraping', 'fixing']);

  return { fixed, programs_ready: count || 0 };
}

const PROGRAM_URL_SIGNALS = [
  "program",
  "programme",
  "programmes",
  "postgraduate",
  "taught",
  "degree",
  "graduate",
  "master",
  "phd",
  "doctoral",
  "course",
  "faculty",
  "school",
  "department",
  "study",
  "academic",
  "msc",
  "mba",
  "med",
  "llm",
  "meng",
  "certificate",
  "diploma",
];

const SKIP_URL_SIGNALS = [
  "news",
  "event",
  "blog",
  "contact",
  "about",
  "login",
  "apply",
  "alumni",
  "giving",
  "donate",
  "campus",
  "map",
  "directory",
  "privacy",
  "accessibility",
  "copyright",
  "sitemap",
  "search",
  "career",
  "job",
  "staff",
  "faculty-staff",
  "research-staff",
];

function isProgramUrl(url) {
  const path = new URL(url).pathname.toLowerCase();
  const hasProgram = PROGRAM_URL_SIGNALS.some((s) => path.includes(s));
  const shouldSkip = SKIP_URL_SIGNALS.some((s) => path.includes(s));
  return hasProgram && !shouldSkip;
}

async function crawlDirectory(universityId, dirUrl, depth = 1) {
  const parsedBase = new URL(dirUrl);
  const baseDomain = parsedBase.hostname;

  async function fetchPage(url) {
    let html = null;

    try {
      const res = await axios.get(url, {
        headers: { "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)" },
        timeout: 20000,
      });
      html = res.data;
    } catch (e) {
      html = null;
    }

    let needsPuppeteer = false;

    if (!html || html.length < 5000) {
      needsPuppeteer = true;
    } else {
      const $test = cheerio.load(html);
      const linkCount = $test("a[href]").length;
      if (linkCount < 5) needsPuppeteer = true;
    }

    if (needsPuppeteer) {
      console.log(
        `[crawl] Auto-switching to Puppeteer for: ${url} (axios html: ${html?.length || 0} chars)`,
      );
      try {
        html = await Promise.race([
          fetchWithPuppeteer(url),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("Puppeteer timeout")), 75000),
          ),
        ]);
      } catch (e) {
        console.error(`[crawl] Puppeteer failed for ${url}:`, e.message);
        return null;
      }
    }

    return html;
  }

  const discovered = [];
  const seen = new Set();
  const toVisit = [dirUrl];
  const visited = new Set();
  seen.add(dirUrl);

  for (let d = 0; d < depth; d++) {
    const batch = [...toVisit];
    toVisit.length = 0;

    for (const url of batch) {
      if (visited.has(url)) continue;
      visited.add(url);

      const html = await fetchPage(url);
      if (!html) continue;

      const $ = cheerio.load(html);
      let foundOnPage = 0;

      $("a[href]").each(function () {
        const href = $(this).attr("href");
        if (!href) return;

        let full;
        try {
          full = new URL(href, url).toString();
        } catch (e) {
          return;
        }

        let fullHostname;
        try {
          fullHostname = new URL(full).hostname;
        } catch (e) {
          return;
        }

        if (fullHostname !== baseDomain) return;

        if (full.includes("#") || seen.has(full)) return;

        if (!isProgramUrl(full)) return;

        seen.add(full);
        foundOnPage++;

        if (d < depth - 1) toVisit.push(full);

        discovered.push({
          university_id: universityId,
          program_url: full,
          status: "pending",
        });
      });

      const allLinks = [];
      $("a[href]").each(function() {
        const href = $(this).attr("href");
        if (!href) return;
        try { allLinks.push(new URL(href, url).toString()); } catch(e) {}
      });
      const rejected = allLinks.filter(u => { try { return new URL(u).hostname === baseDomain && !u.includes('#') && !isProgramUrl(u); } catch(e) { return false; } });
      console.log(`[debug/crawl] ${url} — total links: ${allLinks.length}, rejected by isProgramUrl: ${rejected.length}`);
      console.log(`[debug/crawl] sample rejected:`, rejected.slice(0, 8));

      console.log(`[crawl] ${url} → found ${foundOnPage} program URLs`);
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  if (discovered.length === 0) return 0;

  const { error: upsertErr } = await supabase
    .schema("ingestion")
    .from("scrape_queue")
    .upsert(discovered, { onConflict: "program_url" });

  if (upsertErr) {
    console.error(`[crawl] Failed to upsert discovered URLs:`, upsertErr.message);
  }

  // Return discovered count regardless — upsert may 0 when all rows already existed
  return discovered.length;
}

async function scrapeQueueForUniversity(universityId) {
  const CONCURRENCY = 5;
  const MAX_BATCHES = 40; // safety cap — prevents infinite loop if status updates fail
  let totalScraped = 0;
  let batchCount = 0;

  // Reset any items stuck in "processing" from a previous crashed run
  const { error: resetErr } = await supabase
    .schema("ingestion")
    .from("scrape_queue")
    .update({ status: "pending", error_message: null })
    .eq("university_id", universityId)
    .eq("status", "processing");
  if (resetErr) console.warn(`[scrape] Could not reset stuck items: ${resetErr.message}`);

  while (batchCount < MAX_BATCHES) {
    batchCount++;

    const { data: items, error: fetchErr } = await supabase
      .schema("ingestion")
      .from("scrape_queue")
      .select("*")
      .eq("university_id", universityId)
      .eq("status", "pending")
      .limit(50);

    if (fetchErr) {
      console.error(`[scrape] Failed to fetch queue batch: ${fetchErr.message}`);
      break;
    }

    if (!items || items.length === 0) break;

    console.log(`[scrape] Batch ${batchCount}: processing ${items.length} URLs`);

    for (let i = 0; i < items.length; i += CONCURRENCY) {
      const chunk = items.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (item) => {
          // Mark as processing first so we don't pick it up again in the same run
          const { error: markErr } = await supabase
            .schema("ingestion")
            .from("scrape_queue")
            .update({ status: "processing" })
            .eq("id", item.id);
          if (markErr) {
            console.error(`[scrape] Could not mark item processing: ${item.program_url}`);
            return;
          }

          // Skip PDF URLs — binary content can't be stored as raw_html
          if (item.program_url.toLowerCase().endsWith(".pdf")) {
            await supabase
              .schema("ingestion")
              .from("scrape_queue")
              .update({ status: "failed", error_message: "Skipped: PDF URL not supported" })
              .eq("id", item.id);
            return;
          }

          let html = null;

          // Attempt 1: fast axios fetch
          try {
            const res = await axios.get(item.program_url, {
              headers: { "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)" },
              timeout: 30000,
            });
            html = res.data;
          } catch (e) {
            console.warn(`[scrape] Axios failed for ${item.program_url}: ${e.message}`);
            html = null;
          }

          // Attempt 2: Puppeteer fallback if page too small or empty
          if (!html || html.length < 500) {
            console.log(`[scrape] Falling back to Puppeteer for: ${item.program_url}`);
            try {
              html = await Promise.race([
                fetchWithPuppeteer(item.program_url),
                new Promise((_, reject) =>
                  setTimeout(() => reject(new Error("Puppeteer timeout after 45s")), 45000),
                ),
              ]);
            } catch (puppeteerErr) {
              console.error(`[scrape] Puppeteer failed for ${item.program_url}: ${puppeteerErr.message}`);
              await supabase
                .schema("ingestion")
                .from("scrape_queue")
                .update({ status: "failed", error_message: `Puppeteer: ${puppeteerErr.message}` })
                .eq("id", item.id);
              return;
            }
          }

          if (!html || html.length < 500) {
            await supabase
              .schema("ingestion")
              .from("scrape_queue")
              .update({ status: "failed", error_message: "Page too small after both attempts" })
              .eq("id", item.id);
            return;
          }

          // Save raw HTML
          const { error: upsertErr } = await supabase
            .schema("ingestion")
            .from("raw_program_pages")
            .upsert(
              {
                university_id: item.university_id,
                source_url: item.program_url,
                raw_html: html,
                parse_status: "pending",
              },
              { onConflict: "source_url" },
            );

          if (upsertErr) {
            console.error(`[scrape] Failed to save HTML for ${item.program_url}: ${upsertErr.message}`);
            await supabase
              .schema("ingestion")
              .from("scrape_queue")
              .update({ status: "failed", error_message: `DB save failed: ${upsertErr.message}` })
              .eq("id", item.id);
            return;
          }

          const { error: doneErr } = await supabase
            .schema("ingestion")
            .from("scrape_queue")
            .update({ status: "scraped", scraped_at: new Date().toISOString() })
            .eq("id", item.id);
          if (doneErr) {
            console.error(`[scrape] Failed to mark scraped: ${item.program_url}: ${doneErr.message}`);
          } else {
            totalScraped++;
          }
        }),
      );
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  if (batchCount >= MAX_BATCHES) {
    console.warn(`[scrape] Reached max batch limit (${MAX_BATCHES}) for university ${universityId} — stopping`);
  }

  return totalScraped;
}

async function parsePagesForUniversity(universityId) {
  const CONCURRENCY = 5;
  const MAX_BATCHES = 40; // safety cap
  let totalParsed = 0;
  let totalSeeded = 0;
  let batchCount = 0;

  // Pre-fetch shared data once — avoids N+1 queries per page
  const { data: prefetchedFeeStructures } = await supabase
    .schema("ingestion")
    .from("university_fee_structure")
    .select("*")
    .eq("university_id", universityId);

  const { data: prefetchedUni } = await supabase
    .schema("core")
    .from("universities")
    .select("credits_per_year")
    .eq("id", universityId)
    .single();

  // Reset any pages stuck in "processing" from a previous crashed run
  await supabase
    .schema("ingestion")
    .from("raw_program_pages")
    .update({ parse_status: "pending" })
    .eq("university_id", universityId)
    .eq("parse_status", "processing");

  while (batchCount < MAX_BATCHES) {
    batchCount++;

    const { data: pages, error: fetchErr } = await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .select("id")
      .eq("university_id", universityId)
      .eq("parse_status", "pending")
      .limit(50);

    if (fetchErr) {
      console.error(`[parse] Failed to fetch pages batch: ${fetchErr.message}`);
      break;
    }

    if (!pages || pages.length === 0) break;

    console.log(`[parse] Batch ${batchCount}: parsing ${pages.length} pages`);

    for (let i = 0; i < pages.length; i += CONCURRENCY) {
      const chunk = pages.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (page) => {
          try {
            const result = await parseProgramPage(page.id, {
              prefetchedFeeStructures: prefetchedFeeStructures || [],
              prefetchedUni: prefetchedUni || null,
            });
            totalParsed++;
            if (result?.seeded) totalSeeded += result.seeded;
          } catch (e) {
            console.error(`[parse] Failed page ${page.id}: ${e.message}`);
          }
        }),
      );
    }
  }

  if (batchCount >= MAX_BATCHES) {
    console.warn(`[parse] Reached max batch limit (${MAX_BATCHES}) for university ${universityId} — stopping`);
  }

  return { parsed: totalParsed, seeded: totalSeeded };
}

async function autoFixFieldCategories(universityId) {
  const JUNK_NAMES = [
    "Graduate Program", "Graduate Co-op Program", "Graduate Co-op (PG)",
    "Master's Degree", "Doctoral Studies", "Graduate Certificate",
    "Graduate Diploma", "Postdoctoral Fellowship",
    "Master's thesis programs", "Master's non-thesis programs (MSc)",
    "Individualized Program (MA)", "Individualized Program (MSc)",
    "Individualized Program (PhD)", "Individualized Program (Master's)",
    "Individualized Program (Doctoral)", "Individualized Program (INDI) PhD",
    "Individualized Program (INDI) MSc", "Individualized Program (INDI) MA",
    "Individualized Program (INDI) MA/MSc", "Individualized Program (MA, MSc)",
    "Individualized Program (MA, MSc) Thesis", "Individualized Program (PG)",
    "Individualized Program (Master's)", "Arts / Education / Science",
    "Traduction (PG)", "Health Services (PG)",
    "Principles of Nanoscience and Nanotechnology (PG)",
    "Lettres et sciences humaines+ (PG)", "Graduate Co-op Program",
    "Master's Degree", "Doctoral Studies",
  ];

  await supabase
    .schema("ingestion")
    .from("parsed_programs")
    .delete()
    .eq("university_id", universityId)
    .eq("validation_status", "pending")
    .in("program_name", JUNK_NAMES);

  console.log(`[fix] Deleted junk program names for ${universityId}`);

  const { data: allPrograms } = await supabase
    .schema("ingestion")
    .from("parsed_programs")
    .select("id, program_name")
    .eq("university_id", universityId)
    .eq("validation_status", "pending");

  const FRENCH_SIGNALS = [
    'maîtrise', 'génie', 'chimie', 'biologie', 'traduction',
    'thérapie', 'thérapies', 'études', 'analyse', 'sciences économiques',
    'microprogramme', 'andragogie', 'musicothérapie', 'géographie',
    'analytique', 'anthropologie', 'administration des affaires',
    'technologies de', 'gestion et', 'informatique', 'histoire de l',
    'arts plastiques', 'arts cinéma', 'génie de', 'génie environ',
    'génie indus', 'génie logiciel', 'génie électrique', 'sociologie',
    'philosophie', 'économique', 'évaluation', 'religions et', 'lettres',
    'certificat', 'dipl. 2e c.', 'cert. 2e c.', '2e cycle', 'doctorat',
    'linguistique', 'traductologie', 'didactique', 'enseignement',
  ];

  function isFrenchProgram(name) {
    const lower = name.toLowerCase();
    if (FRENCH_SIGNALS.some(s => lower.includes(s))) return true;
    if (/[éèêëàâùûüôîïç]/.test(name)) {
      const frenchStartWords = ['maî', 'géo', 'géni', 'chim', 'biol', 'trad',
        'thér', 'étud', 'anal', 'soci', 'phil', 'écon', 'éval', 'ling'];
      if (frenchStartWords.some(w => lower.startsWith(w))) return true;
      if (/\b(des|les|une|aux|par|sur|pour|dans|avec|sans)\b/.test(lower)) return true;
    }
    return false;
  }

  const frenchIds = (allPrograms || [])
    .filter(p => isFrenchProgram(p.program_name))
    .map(p => p.id);

  if (frenchIds.length > 0) {
    await supabase.schema("ingestion").from("parsed_programs")
      .delete()
      .eq("university_id", universityId)
      .in("id", frenchIds);
    console.log(`[fix] Deleted ${frenchIds.length} French language programs for ${universityId}`);
  }

  await supabase.schema("ingestion").from("parsed_programs")
    .update({ duration_years: 1 })
    .eq("university_id", universityId)
    .eq("validation_status", "pending")
    .is("duration_years", null)
    .eq("program_type", "professional");

  await supabase.schema("ingestion").from("parsed_programs")
    .update({ duration_years: 2 })
    .eq("university_id", universityId)
    .eq("validation_status", "pending")
    .is("duration_years", null)
    .eq("program_type", "research");

  await supabase.schema("ingestion").from("parsed_programs")
    .update({ duration_years: 4 })
    .eq("university_id", universityId)
    .eq("validation_status", "pending")
    .is("duration_years", null)
    .eq("program_type", "doctoral");

  await supabase.schema("ingestion").from("parsed_programs")
    .update({ duration_years: 1 })
    .eq("university_id", universityId)
    .eq("validation_status", "pending")
    .is("duration_years", null)
    .or("program_name.ilike.%certificate%,program_name.ilike.%diploma%,program_name.ilike.%grad. cert%");

  console.log(`[fix] Set duration defaults for ${universityId}`);

  const { data: programs } = await supabase
    .schema("ingestion")
    .from("parsed_programs")
    .select("id, program_name")
    .eq("university_id", universityId)
    .is("field_category", null)
    .eq("validation_status", "pending");

  if (!programs || programs.length === 0) return 0;

  // Group by category for batch updates instead of N+1 individual updates
  const categoryGroups = {};
  let fixed = 0;
  for (const p of programs) {
    const assigned = autoAssignFieldCategory(p.program_name);
    if (assigned) {
      if (!categoryGroups[assigned]) categoryGroups[assigned] = [];
      categoryGroups[assigned].push(p.id);
      fixed++;
      console.log(`[fix] "${p.program_name}" → "${assigned}"`);
    } else {
      console.log(`[fix] Could not assign category for: "${p.program_name}"`);
    }
  }

  for (const [category, ids] of Object.entries(categoryGroups)) {
    const { error: batchErr } = await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .update({ field_category: category })
      .in("id", ids);
    if (batchErr) {
      console.error(`[fix] Batch update failed for category "${category}": ${batchErr.message}`);
    }
  }

  const { data: stillNull } = await supabase
    .schema("ingestion")
    .from("parsed_programs")
    .select("id, program_name")
    .eq("university_id", universityId)
    .is("field_category", null)
    .eq("validation_status", "pending");

  if (stillNull && stillNull.length > 0) {
    console.log(`[fix] ${stillNull.length} programs still need GPT field category resolution`);
    const names = stillNull.map(p => p.program_name).join('\n');
    const categoryPrompt = `
You are classifying university program names into field categories.
Return STRICT JSON array only. One object per program in the same order as input.
Each object: { "program_name": "...", "field_category": "..." }

Field categories (use EXACTLY one of these):
- engineering & tech
- business, management and economics
- science & applied science
- medicine, health and life science
- social science & humanities
- arts, design & creative studies
- law, public policy & governance
- hospitality, tourism & service industry
- education & teaching
- agriculture, sustainability & environmental studies

Programs to classify:
${names}
`;
    try {
      const completion = await callOpenAI({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: categoryPrompt }],
        temperature: 0,
      }, 30000);
      const result = JSON.parse(completion.choices[0].message.content.replace(/```json|```/g, "").trim());
      if (!Array.isArray(result)) throw new Error("GPT returned non-array");
      for (let i = 0; i < Math.min(result.length, stillNull.length); i++) {
        const item = result[i];
        const target = stillNull[i];
        if (item.field_category && VALID_FIELD_CATEGORIES.includes(item.field_category)) {
          await supabase.schema("ingestion").from("parsed_programs")
            .update({ field_category: item.field_category })
            .eq("id", target.id);
          fixed++;
          console.log(`[fix-gpt] "${target.program_name}" → "${item.field_category}"`);
        }
      }
    } catch (err) {
      console.error(`[fix-gpt] GPT field category fallback failed:`, err.message);
    }
  }

  return fixed;
}

// ============================================================
// FEE SCRAPING — Auto-discover and extract university fee structure
// ============================================================

const FEE_PAGE_PATTERNS = [
  "/graduate/fees",
  "/graduate/tuition",
  "/graduate-studies/tuition",
  "/graduate-studies/fees",
  "/fees-and-funding",
  "/tuition-fees",
  "/tuition",
  "/fees",
  "/graduate/funding-and-fees",
  "/future-students/tuition-fees",
  "/admissions/tuition-fees",
  "/graduate/costs",
  "/graduate-studies/costs-and-funding",
  "/programs/tuition",
  "/financial/tuition",
  "/graduate/funding",
  "/graduate/financial-support",
  "/graduate-studies/funding",
  "/graduate-studies/financial",
  "/graduate/cost-of-studies",
  "/graduate/cost",
  "/grad/fees",
  "/grad/tuition",
  "/academics/graduate/fees",
  "/future-students/fees",
  "/future-students/tuition",
  "/graduate/international-fees",
  "/international/tuition",
  "/registrar/tuition-fees",
  "/registrar/fees",
  "/student-accounts/tuition",
  "/finance/tuition-fees",
];

function resolveTuition(programName, programType, universityId, feeStructures) {
  if (!feeStructures || feeStructures.length === 0) return null;
  const level = programType === 'doctoral' ? 'doctoral' : 'masters';
  const nameLower = programName.toLowerCase();
  const levelFees = feeStructures.filter(f => f.program_level === level);
  if (levelFees.length === 0) return null;
  function feeAmount(fee) {
    const amount = fee.fee_type === 'flat_annual'
      ? fee.international_fee
      : fee.international_fee * (fee.instalments_per_year || 2);
    return { amount, currency: fee.currency || 'CAD' };
  }
  if (programType) {
    const match = levelFees
      .filter(f =>
        f.program_type === programType &&
        f.program_name_pattern &&
        f.program_name_pattern !== `default_${level}` &&
        nameLower.includes(f.program_name_pattern.toLowerCase())
      )
      .sort((a, b) => b.program_name_pattern.length - a.program_name_pattern.length)[0];
    if (match) return feeAmount(match);
  }
  const matchNoType = levelFees
    .filter(f =>
      f.program_type === null &&
      f.program_name_pattern &&
      f.program_name_pattern !== `default_${level}` &&
      nameLower.includes(f.program_name_pattern.toLowerCase())
    )
    .sort((a, b) => b.program_name_pattern.length - a.program_name_pattern.length)[0];
  if (matchNoType) return feeAmount(matchNoType);
  if (programType) {
    const defaultWithType = levelFees.find(f =>
      f.program_type === programType &&
      f.program_name_pattern === `default_${level}`
    );
    if (defaultWithType) return feeAmount(defaultWithType);
  }
  const defaultFee = levelFees.find(f =>
    f.program_name_pattern === `default_${level}` &&
    f.program_type === null
  );
  if (defaultFee) return feeAmount(defaultFee);
  const fallbackType = programType === 'research' ? 'doctoral' : 'masters';
  const fallbackLevel = feeStructures.filter(f => f.program_level === fallbackType);
  const fallbackMatch = fallbackLevel
    .filter(f =>
      f.program_name_pattern &&
      f.program_name_pattern !== `default_${fallbackType}` &&
      nameLower.includes(f.program_name_pattern.toLowerCase())
    )
    .sort((a, b) => b.program_name_pattern.length - a.program_name_pattern.length)[0];
  if (fallbackMatch) return feeAmount(fallbackMatch);
  const fallbackDefault = fallbackLevel.find(f =>
    f.program_name_pattern === `default_${fallbackType}` ||
    f.program_name_pattern === null
  );
  if (fallbackDefault) return feeAmount(fallbackDefault);
  const anyFee = levelFees[0];
  if (anyFee) return feeAmount(anyFee);
  return null;
}

// ═══════════════════════════════════════════════════════════════
    // INTELLIGENT FEE SCRAPER — fully cascade-aware
    // Works for any university with a cascading fee calculator form.
    // Cascade order is discovered dynamically via waitForOptions().
    // ═══════════════════════════════════════════════════════════════

    async function safeSelect(page, elementId, value) {
      const ok = await page.evaluate((id, val) => {
        let el = document.getElementById(id);
        if (!el) { try { el = document.querySelector('#' + CSS.escape(id)); } catch(e) {} }
        if (!el) { try { el = document.querySelector('select[name="' + id + '"]'); } catch(e) {} }
        if (!el) return false;
        el.value = val;
        el.dispatchEvent(new Event('change', { bubbles: true }));
        el.dispatchEvent(new Event('input', { bubbles: true }));
        return true;
      }, elementId, value);
      if (!ok) throw new Error('safeSelect: not found: ' + elementId);
    }

    async function readOptions(page, elementId) {
      return page.evaluate((id) => {
        let el = document.getElementById(id);
        if (!el) { try { el = document.querySelector('#' + CSS.escape(id)); } catch(e) {} }
        if (!el) { try { el = document.querySelector('select[name="' + id + '"]'); } catch(e) {} }
        if (!el) return [];
        return Array.from(el.options)
          .filter(o => o.value && o.value.trim())
          .map(o => ({ value: o.value, label: o.textContent.trim() }));
      }, elementId);
    }

    async function waitForOptions(page, elementId, minCount = 2, timeoutMs = 6000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const opts = await readOptions(page, elementId);
        if (opts.length >= minCount) return opts;
        await new Promise(r => setTimeout(r, 300));
      }
      return readOptions(page, elementId);
    }

    async function waitForFee(page, timeoutMs = 8000) {
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        const found = await page.evaluate(() => {
          const text = document.body.innerText;
          const matches = [...text.matchAll(/(?:CA)?\$\s?([\d,]+\.?\d*)/g)];
          for (const m of matches) {
            const n = parseFloat(m[1].replace(/,/g, ''));
            if (n > 1000 && n < 100000) return true;
          }
          return false;
        });
        if (found) return true;
        await new Promise(r => setTimeout(r, 300));
      }
      return false;
    }

    async function readFeeFromDOM(page) {
      const result = await page.evaluate(() => {
        const CAD = /(?:CA)?\$\s?([\d,]+\.?\d*)/g;

        const cells = Array.from(document.querySelectorAll('td, th'));
        for (const cell of cells) {
          const text = (cell.innerText || cell.textContent || '').trim();
          const m = text.match(/(?:CA)?\$\s?([\d,]+\.?\d*)/);
          if (m) {
            const n = parseFloat(m[1].replace(/,/g, ''));
            if (n > 1000 && n < 100000) {
              return { fee_per_term: n, raw_text: text };
            }
          }
        }

        const sources = Array.from(document.querySelectorAll('[class*="result"], [class*="fee"], [class*="tuition"], [class*="cost"], [id*="result"], [id*="fee"]'));
        for (const el of sources) {
          const text = (el.innerText || el.textContent || '').replace(/\s+/g, ' ');
          const matches = [...text.matchAll(CAD)];
          for (const m of matches) {
            const n = parseFloat(m[1].replace(/,/g, ''));
            if (n > 1000 && n < 100000) {
              const ctx = text.substring(Math.max(0, m.index - 60), m.index + 80).trim();
              return { fee_per_term: n, raw_text: ctx };
            }
          }
        }

        const body = document.body.innerText;
        const all = [...body.matchAll(CAD)];
        for (const m of all) {
          const n = parseFloat(m[1].replace(/,/g, ''));
          if (n > 1000 && n < 100000) {
            const ctx = body.substring(Math.max(0, m.index - 60), m.index + 80).trim();
            return { fee_per_term: n, raw_text: ctx };
          }
        }
        return null;
      });

      if (!result) {
        const snapshot = await page.evaluate(() => {
          const text = document.body.innerText.replace(/\s+/g, ' ').trim();
          return text.substring(0, 600);
        });
        console.log('[fees] DOM snapshot (no fee found): ' + snapshot);
      }

      return result;
    }

    function classifyRole(id, label) {
      const i = id.toLowerCase(), l = label.toLowerCase();
      if (i.includes('year') || l.includes('academic year') || l.includes('year') || l.includes('session')) return 'year';
      if (i.includes('level') || l.includes('level of study') || l.includes('level') || l.includes('cycle')) return 'level';
      if (i.includes('faculty') || l.includes('faculty') || l.includes('school') || l.includes('département') || l.includes('department')) return 'faculty';
      if (i.includes('discipline') || l.includes('discipline') || l.includes('program') || l.includes('field')) return 'discipline';
      if (i.includes('student') || l.includes('student type') || i.includes('status') || l.includes('citizenship')) return 'student_type';
      if (i.includes('classif') || i.includes('load') || i.includes('time') || l.includes('course load') || l.includes('full-time') || l.includes('regime') || l.includes('billing') || l.includes('fee type') || l.includes('assessment')) return 'course_load';
      return 'unknown';
    }

    function pgLevel(label) {
      const t = label.toLowerCase();
      if (t.includes('doctor') || t.includes('phd') || t.includes('ph.d') || t.includes('doctorat')) return 'doctoral';
      if (t.includes('master') || t.includes('maîtrise') || t.includes('maitrise') || t.includes('msc') || t.includes('mba') || t.includes('meng') || t.includes('graduate diploma') || t.includes('grad dip')) return 'masters';
      if (t.includes('postgraduate') || t.includes('post-graduate') || t.includes('second cycle') || t.includes('2nd cycle') || t.includes('2e cycle')) return 'masters';
      if ((t.includes('graduate') || t.includes('grad')) && !t.includes('under')) return 'masters';
      return null;
    }

    function toPatternKeyword(label) {
      if (!label || label === 'default') return null;
      const low = label.toLowerCase().replace(/faculty of |school of |telfer |department of |college of |faculty |/gi, '').replace(/[^a-z0-9 ]/g, '').trim();
      const map = { engineering:'engineering', management:'management', arts:'arts', science:'science', law:'law', medicine:'medicine', health:'health', education:'education', business:'business', nursing:'nursing', pharmacy:'pharmacy', environment:'environment', architecture:'architecture', 'social science':'social', 'social sciences':'social', 'computer science':'computer science', information:'information', economics:'economics', psychology:'psychology' };
      for (const [k,v] of Object.entries(map)) { if (low.includes(k)) return v; }
      return low.split(' ')[0];
    }

    async function scrapeFeeStructureIntelligent(universityId, manualFeeUrl = null) {
      const { data: uni } = await supabase.schema('core').from('universities').select('name, terms_per_year').eq('id', universityId).single();
      const uniName = uni?.name || universityId;
      const termsPerYear = uni?.terms_per_year || 3;

      let feeUrl = manualFeeUrl;
      if (!feeUrl) {
        const { data: sample } = await supabase.schema('ingestion').from('scrape_queue').select('program_url').eq('university_id', universityId).limit(1).single();
        if (!sample) { console.log('[fees] No URLs for ' + uniName); return false; }
        const p = new URL(sample.program_url);
        const bases = [...new Set([
          p.origin,
          p.hostname.split('.').length > 2
            ? p.protocol + '//' + p.hostname.split('.').slice(-2).join('.')
            : null,
        ].filter(Boolean))];
        outer: for (const base of bases) {
          for (const pat of FEE_PAGE_PATTERNS) {
            try {
              const r = await axios.get(base + pat, { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 10000, maxRedirects: 3 });
              if (r.status === 200 && r.data.length > 3000 &&
                  ['tuition','fee','international'].some(w => r.data.toLowerCase().includes(w))) {
                feeUrl = base + pat; break outer;
              }
            } catch(e) {}
          }
        }
      }
      if (!feeUrl) { console.log('[fees] No fee page found for ' + uniName); return false; }
      console.log('[fees] Fee URL: ' + feeUrl);

      const browser = await getBrowser();
      const page = await browser.newPage();
      let renderedHtml = null;

      try {
        await page.goto(feeUrl, { waitUntil: 'networkidle2', timeout: 45000 });
        await new Promise(r => setTimeout(r, 2000));

        let selectCount = await page.evaluate(() => document.querySelectorAll('select').length);
        if (selectCount === 0) {
          for (const frame of page.frames()) {
            try {
              const cnt = await frame.evaluate(() => document.querySelectorAll('select').length);
              if (cnt > 0) {
                feeUrl = frame.url();
                console.log('[fees] Form in iframe: ' + feeUrl);
                await page.goto(feeUrl, { waitUntil: 'networkidle2', timeout: 30000 });
                await new Promise(r => setTimeout(r, 1500));
                break;
              }
            } catch(e) {}
          }
        }

        renderedHtml = await page.content();

        const rawSelects = await page.evaluate(() =>
          Array.from(document.querySelectorAll('select')).map((el, i) => {
            const id = el.id || el.name || '';
            let label = '';
            if (el.id) { const l = document.querySelector('label[for="' + el.id + '"]'); if (l) label = l.textContent.trim(); }
            if (!label) { const p = el.closest('div,p,td,fieldset,li'); if (p) { const l = p.querySelector('label,th,legend'); if (l) label = l.textContent.trim(); } }
            if (!label) label = el.id || el.name || 'dropdown_' + i;
            const options = Array.from(el.options).filter(o => o.value && o.value.trim())
              .map(o => ({ value: o.value, label: o.textContent.trim() }));
            return { id, label, options };
          })
        );

        let yearId = null, yearValue = null;
        let levelId = null;
        let facultyId = null;
        let disciplineId = null;
        let studentTypeId = null, studentTypeIntlValue = null;
        let loadId = null;

        for (const sel of rawSelects) {
          if (!sel.id) continue;
          const role = classifyRole(sel.id, sel.label);
          console.log('[fees] "' + sel.label + '" → ' + role + ' (' + sel.options.length + ' opts)');

          switch (role) {
            case 'year':
              yearId = sel.id;
              const sorted = [...sel.options].sort((a, b) => b.label.localeCompare(a.label));
              yearValue = sorted[0]?.value || sel.options[sel.options.length - 1]?.value;
              console.log('[fees] Year: ' + yearValue + ' (' + sorted[0]?.label + ')');
              break;
            case 'level':        levelId = sel.id; break;
            case 'faculty':      facultyId = sel.id; break;
            case 'discipline':   disciplineId = sel.id; break;
            case 'student_type':
              studentTypeId = sel.id;
              studentTypeIntlValue = sel.options.find(o => o.label.toLowerCase().includes('international'))?.value || null;
              console.log('[fees] Student type intl value: ' + studentTypeIntlValue);
              break;
            case 'course_load':  loadId = sel.id; break;
          }
        }

        if (!levelId) {
          console.log('[fees] No level dropdown — static fallback');
          await page.close(); await browser.disconnect();
          return await extractFeesFromStaticPage(renderedHtml, uniName, universityId, feeUrl);
        }

        if (yearId && yearValue) {
          await safeSelect(page, yearId, yearValue);
          console.log('[fees] Selected year, waiting for level options...');
        }
        if (studentTypeId && studentTypeIntlValue) {
          try { await safeSelect(page, studentTypeId, studentTypeIntlValue); } catch(e) {}
        }

        const liveLevels = await waitForOptions(page, levelId, 2, 6000);
        console.log('[fees] Live levels: ' + liveLevels.map(o => '"' + o.label + '"').join(', '));

        const pgLevels = liveLevels
          .map(o => ({ ...o, dbLevel: pgLevel(o.label) }))
          .filter(o => o.dbLevel);

        if (!pgLevels.length) {
          console.log('[fees] No PG levels — static fallback');
          await page.close(); await browser.disconnect();
          return await extractFeesFromStaticPage(renderedHtml, uniName, universityId, feeUrl);
        }
        console.log('[fees] PG levels: ' + pgLevels.map(o => o.label + '(' + o.dbLevel + ')').join(', '));

        const feeRows = [];
        const seen = new Set();

        for (const level of pgLevels) {
          console.log('[fees] ── ' + level.dbLevel + ': ' + level.label + ' ──');

          await page.goto(feeUrl, { waitUntil: 'networkidle2', timeout: 30000 });
          await new Promise(r => setTimeout(r, 800));

          if (yearId && yearValue) {
            await safeSelect(page, yearId, yearValue);
            await new Promise(r => setTimeout(r, 800));
          }
          if (studentTypeId && studentTypeIntlValue) {
            try { await safeSelect(page, studentTypeId, studentTypeIntlValue); await new Promise(r => setTimeout(r, 500)); } catch(e) {}
          }

          await safeSelect(page, levelId, level.value);
          console.log('[fees] Selected level, waiting for faculty...');

          let faculties = [{ value: null, label: 'default' }];
          if (facultyId) {
            const liveFac = await waitForOptions(page, facultyId, 2, 6000);
            if (liveFac.length > 0) {
              faculties = liveFac;
              console.log('[fees] Faculties (' + liveFac.length + '): ' + liveFac.map(f => f.label).slice(0, 8).join(', '));
            }
          }

          for (const faculty of faculties) {
            if (facultyId && faculty.value) {
              await safeSelect(page, facultyId, faculty.value);
              console.log('[fees] Selected faculty "' + faculty.label + '", waiting for discipline...');
            }

            let disciplines = [{ value: null, label: null }];
            if (disciplineId) {
              const liveDisc = await waitForOptions(page, disciplineId, 2, 6000);
              if (liveDisc.length > 0) {
                disciplines = liveDisc;
                console.log('[fees] Disciplines (' + liveDisc.length + '): ' + liveDisc.map(d => d.label).slice(0, 5).join(', '));
              }
            }

            for (const discipline of disciplines) {
              const key = level.dbLevel + '|' + (faculty.value || 'x') + '|' + (discipline.value || 'x');
              if (seen.has(key)) continue;
              seen.add(key);

              if (disciplineId && discipline.value) {
                await safeSelect(page, disciplineId, discipline.value);
                console.log('[fees] Selected discipline "' + discipline.label + '", waiting for load...');
              }

              if (loadId) {
                const liveLoad = await waitForOptions(page, loadId, 1, 6000);
                if (liveLoad.length > 0) {
                  const preferred = liveLoad.find(o => {
                    const t = o.label.toLowerCase();
                    return t.includes('flat') || t.includes('full-time') || t.includes('full time');
                  }) || liveLoad[0];
                  console.log('[fees] Course load: "' + preferred.label + '"');
                  await safeSelect(page, loadId, preferred.value);
                } else {
                  console.warn('[fees] Course load empty after discipline selected');
                }
              }

              console.log('[fees] Clicking Submit...');
              await page.evaluate(() => {
                const btn = document.querySelector("input[type='submit'], button[type='submit'], input[type='button'][value*='Calculate'], a[href*='__doPostBack']");
                if (btn) btn.click();
                else { const form = document.querySelector('form'); if (form) form.submit(); }
              });
              console.log('[fees] Waiting for fee to appear...');
              let feeAppeared = await waitForFee(page, 8000);

              if (!feeAppeared) {
                console.log('[fees] No fee appeared for: ' + level.dbLevel + ' | ' + faculty.label + (discipline.label ? ' | ' + discipline.label : ''));
                continue;
              }

              let result = await readFeeFromDOM(page);

              if (!result) {
                try {
                  const screenshotBuf = await page.screenshot({ type: 'jpeg', quality: 70, fullPage: false });
                  const b64 = screenshotBuf.toString('base64');
                  console.log('[fees] DOM read failed — trying vision fallback');
                  const completion = await callOpenAI({
                    model: 'gpt-4o',
                    messages: [{ role: 'user', content: [
                      { type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + b64 } },
                      { type: 'text', text:
                        'University: ' + uniName + '\nLevel: ' + level.dbLevel + ', Faculty: ' + (faculty.label || 'default') + '\n' +
                        'Extract the international student fee per term shown on screen.\n' +
                        'Return STRICT JSON only: { "fee_per_term": 9720.89, "raw_text": "..." }\n' +
                        'Return { "fee_per_term": null } if not visible.'
                      },
                    ]}],
                    temperature: 0, max_tokens: 150,
                  }, 30000);
                  result = JSON.parse(completion.choices[0].message.content.replace(/```json|```/g, '').trim());
                } catch(e) { console.error('[fees] Vision fallback failed:', e.message); }
              }

              if (!result?.fee_per_term) {
                console.log('[fees] No fee found for: ' + level.dbLevel + ' | ' + faculty.label);
                continue;
              }

              const annualFee = Math.round(result.fee_per_term * termsPerYear * 100) / 100;

              const patternKey = discipline.label
                ? (toPatternKeyword(discipline.label) || discipline.label.toLowerCase().split(/\s/)[0])
                : faculty.label === 'default'
                  ? 'default_' + level.dbLevel
                  : (toPatternKeyword(faculty.label) || faculty.label.toLowerCase().split(/\s/)[0]);

              feeRows.push({
                university_id: universityId,
                program_level: level.dbLevel,
                program_type: null,
                fee_type: 'flat_annual',
                international_fee: annualFee,
                fee_per_instalment: result.fee_per_term,
                instalments_per_year: termsPerYear,
                currency: 'CAD',
                program_name_pattern: patternKey || 'default_' + level.dbLevel,
                faculty_name: faculty.label === 'default' ? null : faculty.label,
                discipline_name: discipline.label || null,
                level_of_study: level.dbLevel,
                academic_year: yearValue || '2025-2026',
                notes: (result.raw_text || '').substring(0, 100),
                fee_page_url: feeUrl,
              });

              console.log('[fees] OK ' + level.dbLevel
                + ' | ' + faculty.label
                + (discipline.label ? ' | ' + discipline.label : '')
                + ' → $' + result.fee_per_term + '/term → $' + annualFee + '/yr');
            }
          }
        }

        await page.close();

        if (!feeRows.length) {
          console.log('[fees] No fees extracted — static fallback');
          return await extractFeesFromStaticPage(renderedHtml, uniName, universityId, feeUrl);
        }

        const { error } = await supabase
          .schema('ingestion')
          .from('university_fee_structure')
          .upsert(feeRows, { onConflict: 'university_id,program_level,program_name_pattern,program_type' });
        if (error) { console.error('[fees] Upsert error:', error.message); return false; }
        console.log('[fees] ' + feeRows.length + ' fee rows upserted for ' + uniName);
        return true;

      } catch (err) {
        console.error('[fees] Fatal: ' + err.message);
        try { await page.close(); } catch(e) {}
        return await extractFeesFromStaticPage(renderedHtml, uniName, universityId, feeUrl);
      } finally {
        await browser.close();
      }
    }

    // Static fallback
  async function extractFeesFromStaticPage(html, uniName, universityId, feeUrl) {
    if (!html) return false;
    const $ = cheerio.load(html);
    $('script,style,nav,footer,header,aside,.menu,.sidebar').remove();
    let feeText = '';
    const el = $('main,article,.content,#content,[role=main]').first().length ? $('main,article,.content,#content,[role=main]').first() : $('body');
    el.find('table').each(function() { $(this).find('tr').each(function() { const c=[]; $(this).find('td,th').each(function(){ c.push($(this).text().trim()); }); feeText += c.join(' | ') + '\n'; }); });
    feeText = (feeText + '\n' + el.clone().find('table').remove().end().text().replace(/\s+/g, ' ').trim()).substring(0, 6000);
    const fees = await extractFeesFromText(feeText, uniName);
    if (!fees?.length) { console.log('[fees5i] Static: nothing found for ' + uniName); return false; }
    const rows = fees.map(f => ({ university_id: universityId, program_level: f.program_level, program_type: f.program_type || null, fee_type: f.fee_type || 'flat_annual', international_fee: f.international_fee, fee_per_instalment: f.international_fee / (f.instalments_per_year || 3), instalments_per_year: f.instalments_per_year || 3, currency: f.currency || 'CAD', program_name_pattern: f.program_name_pattern || 'default_' + f.program_level, notes: f.notes || null, fee_page_url: feeUrl }));
    const { error } = await supabase.schema('ingestion').from('university_fee_structure').upsert(rows, { onConflict: 'university_id,program_level,program_name_pattern,program_type' });
    if (error) { console.error('[fees5i] Static insert error:', error.message); return false; }
    console.log('[fees5i] Static: ' + rows.length + ' rows for ' + uniName);
    return true;
  }

  
async function extractFeesFromText(feeText, universityName) {
  const prompt = `
You are extracting university fee structures from a tuition page.
Return STRICT JSON array only. No markdown, no explanation.
University: ${universityName}

Extract ALL distinct fee entries for INTERNATIONAL students.

FIELDS:
- program_level: "masters" or "doctoral"
- program_type: "research", "professional", or null
- fee_type: "flat_annual" or "per_instalment"
- international_fee: numeric annual fee for a FIRST YEAR student (use the first/lowest progression level only)
- If the table shows multiple progression levels (term 1, term 2, A1, A2, B1 etc.), use ONLY the first level
- Multiply per-term fee by instalments_per_year to get annual total
- instalments_per_year: 1 | 2 | 3
- currency: "CAD", "USD", "GBP", "EUR", "AUD"
- program_name_pattern: "default_masters" or "default_doctoral" for general fees, or lowercase keyword for specific programs
- notes: any relevant notes

RULES:
- Only international student fees
- Graduate year = 3 terms for Canadian, 2 for UK/Australian universities
- If fee shown per term, multiply by instalments_per_year to get annual
- Return [] if no clear fee structure found

Content:
${feeText}
`;
  try {
    const completion = await callOpenAI({
      model: "gpt-4o-mini",
      messages: [{ role: "user", content: prompt }],
      temperature: 0,
    }, 60000);
    return JSON.parse(completion.choices[0].message.content.replace(/```json|```/g, "").trim());
  } catch (err) {
    console.error("[fees] extractFeesFromText failed:", err.message);
    return [];
  }
}

async function getBaseUrlsForUniversity(universityId) {
  const { data: sample } = await supabase
    .schema('ingestion')
    .from('scrape_queue')
    .select('program_url')
    .eq('university_id', universityId)
    .limit(1)
    .single();
  if (!sample?.program_url) return null;
  const parsed = new URL(sample.program_url);
  const baseUrl = parsed.origin;
  const hostParts = parsed.hostname.split('.');
  const rootDomain = hostParts.length > 2
    ? `${parsed.protocol}//${hostParts.slice(-2).join('.')}`
    : baseUrl;
  return [...new Set([baseUrl, rootDomain])];
}

async function scrapeFeeStructure(universityId) {
  const { data: existing } = await supabase
    .schema("ingestion")
    .from("university_fee_structure")
    .select("id")
    .eq("university_id", universityId)
    .limit(1);

  if (existing && existing.length > 0) {
    console.log(`[fees] Fee structure already exists for ${universityId}`);
    return true;
  }

  const { data: uni } = await supabase
    .schema("core")
    .from("universities")
    .select("name")
    .eq("id", universityId)
    .single();

  const baseUrls = await getBaseUrlsForUniversity(universityId);
  if (!baseUrls) {
    console.log(`[fees] No scraped URLs found for ${uni?.name}`);
    return false;
  }

  console.log(`[fees] Trying fee pages for ${uni?.name} at ${baseUrls.join(", ")}`);

  let feeHtml = null;
  let feeUrl = null;

  for (const base of baseUrls) {
    for (const pattern of FEE_PAGE_PATTERNS) {
      const testUrl = base + pattern;
      try {
        let html;
        try {
          const res = await axios.get(testUrl, {
            headers: { "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)" },
            timeout: 15000,
            validateStatus: (s) => s < 404,
          });
          html = res.status === 200 ? res.data : null;
        } catch (e) {
          html = null;
        }

        if (!html || html.length < 3000) {
          html = await Promise.race([
            fetchWithPuppeteer(testUrl),
            new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 30000)),
          ]).catch(() => null);
        }

        if (html && html.length > 3000) {
          const lowerHtml = html.toLowerCase();
          const hasFeeContent = ["tuition", "fee", "international", "domestic", "per credit", "per term", "annual"]
            .some((k) => lowerHtml.includes(k));
          if (hasFeeContent) {
            feeHtml = html;
            feeUrl = testUrl;
            console.log(`[fees] Found fee page at: ${testUrl}`);
            break;
          }
        }
      } catch (e) {
        continue;
      }
    }
    if (feeHtml) break;
  }

  if (!feeHtml) {
    console.log(`[fees] Could not find fee page for ${uni?.name}`);
    return false;
  }

  const $ = cheerio.load(feeHtml);
  $("script, style, nav, footer, header, aside, .menu, .sidebar").remove();

  let feeText = "";
  const mainEl = $("main, article, .content, #content, [role='main']").first();
  const targetEl = mainEl.length ? mainEl : $("body");

  targetEl.find("table").each(function () {
    $(this).find("tr").each(function () {
      const cells = [];
      $(this).find("th, td").each(function () { cells.push($(this).text().trim()); });
      if (cells.length) feeText += cells.join(" | ") + "\n";
    });
    feeText += "\n";
  });

  const nonTableText = targetEl.clone().find("table").remove().end().text().replace(/\s+/g, " ").trim();
  feeText = (feeText + "\n" + nonTableText).substring(0, 8000);

  const fees = await extractFeesFromText(feeText, uni?.name || universityId);
  if (!Array.isArray(fees) || fees.length === 0) {
    console.log(`[fees] No fee entries extracted for ${uni?.name}`);
    return false;
  }

  const feeRows = fees.map((f) => ({
    university_id: universityId,
    program_level: f.program_level,
    program_type: f.program_type || null,
    fee_type: f.fee_type,
    international_fee: f.international_fee,
    instalments_per_year: f.instalments_per_year || 1,
    currency: f.currency || "CAD",
    program_name_pattern: f.program_name_pattern || null,
    notes: f.notes || null,
    fee_page_url: feeUrl,
  }));

  const { error } = await supabase
    .schema("ingestion")
    .from("university_fee_structure")
    .upsert(feeRows, { onConflict: "university_id,program_level,program_name_pattern" });

  if (error) {
    console.error(`[fees] Insert error for ${uni?.name}:`, error.message);
    return false;
  }

  console.log(`[fees] Extracted ${feeRows.length} fee entries for ${uni?.name}`);
  return true;
}

// ============================================================
// WORKER ROUTES
// ============================================================

app.post("/worker/scrape-fees-intelligent/:university_id", async (req, res) => {
  const { university_id } = req.params;
  const { fee_url } = req.body || {};

  try {
    await supabase.schema("ingestion").from("university_fee_structure")
      .delete()
      .eq("university_id", university_id);

    const result = await scrapeFeeStructureIntelligent(university_id, fee_url || null);

    if (!result) {
      return res.status(422).json({
        message: "Fee scraping failed — could not extract fee structure",
        tip: "Try passing a fee_url in the request body: { fee_url: 'https://...' }"
      });
    }

    const { data: fees } = await supabase.schema("ingestion").from("university_fee_structure")
      .select("program_level, program_type, program_name_pattern, international_fee, currency")
      .eq("university_id", university_id);

    res.json({ message: "Fee scraping complete", rows_inserted: fees?.length || 0, fees });
  } catch (err) {
    console.error("[fees-intelligent] Endpoint error:", err.message);
    res.status(500).json({ message: "Fee scraping error", error: err.message });
  }
});

app.post("/worker/add-job", async (req, res) => {
  try {
    const { university_id, directory_urls, crawl_depth = 1 } = req.body;
    if (!university_id) {
      return res.status(400).json({ error: "university_id is required" });
    }

    const { data: uni } = await supabase
      .schema("core")
      .from("universities")
      .select("id, name")
      .eq("id", university_id)
      .single();

    if (!uni) return res.status(404).json({ error: "University not found" });

    const { data, error } = await supabase
      .schema("ingestion")
      .from("university_jobs")
      .insert({
        university_id,
        directory_urls: directory_urls || [],
        crawl_depth,
        status: "queued",
      })
      .select()
      .single();

    if (error) throw error;

    res.json({
      message: `Job queued for ${uni.name}`,
      job_id: data.id,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/worker/jobs", async (req, res) => {
  try {
    const { data } = await supabase
      .schema("ingestion")
      .from("university_jobs")
      .select(
        "id, university_id, status, urls_discovered, urls_scraped, urls_parsed, programs_ready, error_message, created_at, started_at, completed_at",
      )
      .order("created_at", { ascending: false });
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/worker/review/:university_id", async (req, res) => {
  try {
    const { university_id } = req.params;
    const { data: summary } = await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .select("program_type, field_category, degree_level")
      .eq("university_id", university_id)
      .eq("validation_status", "pending")
      .not("field_category", "is", null);

    if (!summary) return res.json({ message: "No programs ready" });

    const grouped = summary.reduce((acc, p) => {
      acc[p.program_type] = (acc[p.program_type] || 0) + 1;
      return acc;
    }, {});

    const { count: nullFields } = await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .select("*", { count: "exact", head: true })
      .eq("university_id", university_id)
      .eq("validation_status", "pending")
      .is("field_category", null);

    res.json({
      total_ready: summary.length,
      by_program_type: grouped,
      null_field_category: nullFields || 0,
      message:
        nullFields > 0
          ? `${nullFields} programs still have null field_category — inspect before migrating`
          : "Ready to migrate",
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/worker/migrate/:university_id", async (req, res) => {
  try {
    const { university_id } = req.params;
    const { data: programs, error } = await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .select("*")
      .eq("university_id", university_id)
      .eq("validation_status", "pending")
      .not("field_category", "is", null);

    if (error) throw error;
    if (!programs || programs.length === 0) {
      return res.json({ message: "No programs to migrate" });
    }

    let success = 0;
    let failed = 0;
    let skipped = 0;

    const { data: feeStructures } = await supabase
      .schema("ingestion")
      .from("university_fee_structure")
      .select("*")
      .eq("university_id", university_id);

    for (const p of programs) {
      try {
        if (!p.duration_years) {
          const name = p.program_name.toLowerCase();
          if (name.includes('certificate') || name.includes('diploma') || name.includes('grad. cert')) {
            p.duration_years = 1;
          }
        }
        let finalTuitionUSD = p.tuition_usd ? Math.round(p.tuition_usd) : null;

        if (!finalTuitionUSD) {
          const tuitionResult = resolveTuition(p.program_name, p.program_type, university_id, feeStructures || []);
          console.log(`[migrate] ${p.program_name} → resolveTuition: ${JSON.stringify(tuitionResult)}`);
          if (tuitionResult) {
            const rate = CURRENCY_TO_USD[tuitionResult.currency] || 1.0;
            finalTuitionUSD = Math.round(tuitionResult.amount * rate);
          }
        }

        // Do NOT skip programs with no resolved tuition — attempt the insert.
        // Programs from listing pages will often have null tuition; the DB constraint
        // (if any) will surface a real error. Hard-skipping here silently drops
        // every listing-page-extracted program and inflates the skipped count.
        if (!finalTuitionUSD) {
          console.log(`[migrate-warn] No tuition resolved for: ${p.program_name} — inserting with null`);
        }

        const { error: insertError } = await supabase.schema("core")
          .from("courses")
          .upsert({
            name: p.program_name,
            university_id: p.university_id,
            degree_level: p.degree_level,
            duration_years: p.duration_years,
            tuition_usd: finalTuitionUSD,
            field_category: p.field_category,
            internship_available: p.internship_available || false,
            gre_required: p.gre_required || false,
            gmat_required: p.gmat_required || false,
            scholarship_available: p.scholarship_available || false,
            scholarship_details: p.scholarship_details || null,
            funding_guaranteed: p.funding_guaranteed || false,
            program_type: p.program_type || null,
            duration_confidence: p.duration_confidence || "high",
            ielts_minimum: p.ielts_minimum || null,
            pte_minimum: p.pte_minimum || null,
            toefl_minimum: p.toefl_minimum || null,
            min_gpa_percentage: p.min_gpa_percentage || null,
            accepts_backlogs: p.accepts_backlogs !== false,
            work_experience_required: p.work_experience_required || 0,
            subjects_required: p.subjects_required || [],
            application_deadline_intl: p.application_deadline_intl || null,
            application_materials: p.application_materials || [],
            source_parsed_id: p.id,
            migrated_at: new Date().toISOString(),
            data_quality: "parsed"
          // ignoreDuplicates removed: conflicts must UPDATE, not silently do nothing.
          // With ignoreDuplicates: true, Supabase returns error: null even when no row
          // was written — every existing program appeared to succeed while being skipped.
          }, { onConflict: "university_id,name,degree_level,program_type" });

        if (insertError) {
          console.error(`Migration failed for ${p.program_name}:`, insertError.message);
          failed++;
          continue;
        }

        await supabase.schema("ingestion").from("parsed_programs")
          .update({ validation_status: "migrated" }).eq("id", p.id);
        success++;
      } catch (err) {
        console.error(`Migration error for ${p.program_name}:`, err.message);
        failed++;
      }
    }

    await supabase
      .schema("ingestion")
      .from("university_jobs")
      .update({ status: "migrated" })
      .eq("university_id", university_id)
      .eq("status", "ready_for_review");

    res.json({
      message: "Migration complete",
      success,
      failed,
      skipped,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/worker/reprocess/:university_id", async (req, res) => {
  const { university_id } = req.params;

  try {
    const { data: uni } = await supabase.schema('core').from('universities')
      .select('name').eq('id', university_id).single();
    if (!uni) return res.status(404).json({ error: 'University not found' });

    console.log(`[reprocess] Starting full reprocess for ${uni.name}`);

    const summary = { university: uni.name };

    const { count: resetCount } = await supabase.schema('ingestion').from('scrape_queue')
      .update({ status: 'pending', error_message: null })
      .eq('university_id', university_id)
      .in('status', ['scraped', 'failed'])
      .select('*', { count: 'exact', head: true });
    summary.queue_reset = resetCount || 0;
    console.log(`[reprocess] Reset ${summary.queue_reset} scrape_queue items to pending`);

    await supabase.schema('ingestion').from('raw_program_pages')
      .update({ parse_status: 'pending' })
      .eq('university_id', university_id)
      .in('parse_status', ['failed', 'processing']);

    let totalScraped = 0, totalParsed = 0;
    const MAX_PIPELINE_ROUNDS = 3;
    for (let round = 1; round <= MAX_PIPELINE_ROUNDS; round++) {
      const scraped = await scrapeQueueForUniversity(university_id);
      totalScraped += scraped;
      console.log(`[reprocess] Round ${round}: scraped ${scraped} pages`);

      const { parsed, seeded } = await parsePagesForUniversity(university_id);
      totalParsed += parsed;
      console.log(`[reprocess] Round ${round}: parsed ${parsed} pages, seeded ${seeded} new URLs from listing pages`);

      if (seeded === 0 || round === MAX_PIPELINE_ROUNDS) break;
      console.log(`[reprocess] Listing pages seeded ${seeded} URLs — running pipeline round ${round + 1}`);
    }
    summary.scraped = totalScraped;
    summary.parsed = totalParsed;

    const fixed = await autoFixFieldCategories(university_id);
    summary.fixed = fixed;
    console.log(`[reprocess] Fixed ${fixed} field categories`);

    const { count: ready } = await supabase.schema('ingestion').from('parsed_programs')
      .select('*', { count: 'exact', head: true })
      .eq('university_id', university_id)
      .eq('validation_status', 'pending');

    const { count: nullFields } = await supabase.schema('ingestion').from('parsed_programs')
      .select('*', { count: 'exact', head: true })
      .eq('university_id', university_id)
      .eq('validation_status', 'pending')
      .is('field_category', null);

    summary.programs_ready = ready || 0;
    summary.null_field_category = nullFields || 0;
    summary.message = nullFields > 0
      ? `${nullFields} programs still have null field_category — inspect before migrating`
      : 'Ready to migrate';

    console.log(`[reprocess] Done — ${ready} programs ready, ${nullFields} null field_category`);
    res.json(summary);

  } catch (err) {
    console.error('[reprocess] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post("/worker/run", async (req, res) => {
  res.json({ message: "Worker triggered" });
  runWorker();
});

app.get("/worker/status", (req, res) => {
  res.json({ running: workerRunning });
});

// ============================================================
// FEE DIAGNOSTIC — Inspect fee page content for any university
// ============================================================
app.get("/worker/diagnose-fees/:university_id", async (req, res) => {
  try {
    const { university_id } = req.params;

    const { data: uni } = await supabase
      .schema("core")
      .from("universities")
      .select("name")
      .eq("id", university_id)
      .single();

    const { data: sampleUrl } = await supabase
      .schema("ingestion")
      .from("scrape_queue")
      .select("program_url")
      .eq("university_id", university_id)
      .limit(1)
      .single();

    if (!sampleUrl)
      return res.json({ error: "No scraped URLs found for this university" });

    const parsedUrl = new URL(sampleUrl.program_url);
    const baseUrl = parsedUrl.origin;

    // Also build root domain (e.g. graduate.carleton.ca → carleton.ca)
    const hostParts = parsedUrl.hostname.split(".");
    const rootDomain =
      hostParts.length > 2
        ? `${parsedUrl.protocol}//${hostParts.slice(-2).join(".")}`
        : baseUrl;

    const baseUrls = [...new Set([baseUrl, rootDomain])];

    const results = [];
    for (const base of baseUrls) {
      for (const pattern of FEE_PAGE_PATTERNS) {
        const testUrl = base + pattern;
        try {
          let html;
          try {
            const r = await axios.get(testUrl, {
              headers: {
                "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)",
              },
              timeout: 10000,
              validateStatus: (s) => s < 404,
            });
            html = r.status === 200 ? r.data : null;
          } catch (e) {
            html = null;
          }

          if (!html || html.length < 3000) {
            html = await Promise.race([
              fetchWithPuppeteer(testUrl),
              new Promise((_, reject) =>
                setTimeout(() => reject(new Error("timeout")), 30000),
              ),
            ]).catch(() => null);
          }

          if (!html) {
            results.push({
              url: testUrl,
              status: "failed",
              reason: "no response",
            });
            continue;
          }

          const $ = cheerio.load(html);
          const hasFeeContent = ["tuition", "fee", "international"].some((k) =>
            html.toLowerCase().includes(k),
          );

          const tables = [];
          $("table").each(function () {
            const rows = [];
            $(this)
              .find("tr")
              .each(function () {
                const cells = [];
                $(this)
                  .find("th, td")
                  .each(function () {
                    cells.push($(this).text().trim());
                  });
                if (cells.length) rows.push(cells.join(" | "));
              });
            if (rows.length) tables.push(rows.slice(0, 5).join("\n"));
          });

          const feeDivs = [];
          $("div, section, article").each(function () {
            const text = $(this).text().replace(/\s+/g, " ").trim();
            if (
              (text.includes("international") ||
                text.includes("International")) &&
              (text.includes("$") || text.includes("CAD")) &&
              text.length > 50 &&
              text.length < 1000
            ) {
              feeDivs.push(text.substring(0, 300));
            }
          });

          const bodyText = $("body")
            .text()
            .replace(/\s+/g, " ")
            .trim()
            .substring(0, 500);

          results.push({
            url: testUrl,
            status: "found",
            html_length: html.length,
            has_fee_content: hasFeeContent,
            tables_found: tables.length,
            table_sample: tables[0] ? tables[0].substring(0, 400) : null,
            fee_divs_found: feeDivs.length,
            fee_div_sample: feeDivs[0] || null,
            body_sample: bodyText,
          });

          if (hasFeeContent) break;
        } catch (e) {
          results.push({ url: testUrl, status: "error", reason: e.message });
        }
      }
    }

    res.json({
      university: uni?.name,
      base_url: baseUrl,
      base_urls: baseUrls,
      results,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// POST /worker/run-pipeline
// Bulk-queue universities for stage-by-stage processing.
// Responds immediately — background worker picks up jobs.
//
// Body: {
//   university_ids: ["uuid1", "uuid2", ...],   // required
//   directory_urls_map: { "uuid1": ["https://..."], ... },  // optional
//   crawl_depth: 1   // optional, default 1
// }
// ============================================================
app.post('/worker/run-pipeline', async (req, res) => {
  const { university_ids, directory_urls_map = {}, crawl_depth = 1 } = req.body;

  if (!university_ids || !Array.isArray(university_ids) || university_ids.length === 0) {
    return res.status(400).json({ error: 'university_ids must be a non-empty array' });
  }

  const { data: unis, error: uniErr } = await supabase
    .schema('core').from('universities')
    .select('id, name').in('id', university_ids);
  if (uniErr) return res.status(500).json({ error: uniErr.message });

  const validIds = (unis || []).map(u => u.id);
  if (validIds.length === 0) return res.status(404).json({ error: 'No valid university IDs found' });

  // Create one pipeline_jobs row per university per stage
  const jobs = [];
  for (const uid of validIds) {
    for (const stage of PIPELINE_STAGES) {
      jobs.push({ university_id: uid, stage, status: 'pending', attempts: 0, max_attempts: 3 });
    }
  }

  const { error: jobErr } = await supabase.schema('ingestion').from('pipeline_jobs')
    .upsert(jobs, { onConflict: 'university_id,stage' });
  if (jobErr) return res.status(500).json({ error: jobErr.message });

  // Create university_jobs entries (for backward-compat with /worker/review and /worker/migrate)
  for (const uid of validIds) {
    await supabase.schema('ingestion').from('university_jobs').insert({
      university_id: uid,
      directory_urls: directory_urls_map[uid] || [],
      crawl_depth,
      status: 'queued',
    });
  }

  res.json({
    queued: validIds.length,
    skipped_not_found: university_ids.length - validIds.length,
    total_stage_jobs: jobs.length,
    universities: (unis || []).map(u => ({ id: u.id, name: u.name })),
  });

  runPipelineWorker();
});

// ============================================================
// GET /worker/pipeline-status
// Returns the current state of all pipeline jobs by stage.
// ============================================================
app.get('/debug/browserless-token', (req, res) => {
  const token = process.env.BROWSERLESS_TOKEN;
  res.json({
    token_set: !!token,
    token_length: token ? token.length : 0,
    token_preview: token ? token.substring(0, 6) + '...' + token.substring(token.length - 4) : 'NOT SET'
  });
});

app.get('/worker/pipeline-status', async (req, res) => {
  const { data: jobs } = await supabase.schema('ingestion').from('pipeline_jobs')
    .select('university_id, stage, status, error_message');

  if (!jobs) return res.status(500).json({ error: 'Failed to fetch pipeline jobs' });

  // Aggregate counts per stage × status
  const byStage = {};
  for (const stage of PIPELINE_STAGES) {
    byStage[stage] = { pending: 0, running: 0, complete: 0, failed: 0, skipped: 0, total: 0 };
  }
  const perUniversity = {};
  for (const job of jobs) {
    if (byStage[job.stage]) {
      byStage[job.stage][job.status] = (byStage[job.stage][job.status] || 0) + 1;
      byStage[job.stage].total++;
    }
    if (!perUniversity[job.university_id]) perUniversity[job.university_id] = {};
    perUniversity[job.university_id][job.stage] = job.status;
  }

  // Determine active stage (lowest stage with pending or running jobs)
  let activeStage = null;
  for (const stage of PIPELINE_STAGES) {
    if ((byStage[stage].pending + byStage[stage].running) > 0) { activeStage = stage; break; }
  }

  // Summarise per-university outcomes
  let readyForReview = 0, universitiesWithFailures = 0;
  for (const stages of Object.values(perUniversity)) {
    if (stages.fix === 'complete') readyForReview++;
    if (Object.values(stages).includes('failed')) universitiesWithFailures++;
  }

  const failures = jobs
    .filter(j => j.status === 'failed')
    .map(j => ({ university_id: j.university_id, stage: j.stage, error: j.error_message }));

  res.json({
    active_stage: activeStage,
    pipeline_worker_running: pipelineWorkerRunning,
    total_universities: Object.keys(perUniversity).length,
    ready_for_review: readyForReview,
    universities_with_failures: universitiesWithFailures,
    by_stage: byStage,
    failures: failures.slice(0, 50),
  });
});

// ============================================================
// START BACKGROUND WORKER — polls every 10 seconds continuously
// ============================================================

// Reset stuck pipeline_jobs every 3 minutes (separate from polling loop)
setInterval(async () => {
  try {
    const { error: pipelineResetErr } = await supabase
      .schema("ingestion")
      .from("pipeline_jobs")
      .update({ status: "pending", error_message: "Reset after 30min timeout" })
      .eq("status", "running")
      .lt("started_at", new Date(Date.now() - 30 * 60 * 1000).toISOString());

    if (pipelineResetErr) {
      console.error("[worker-interval] pipeline_jobs reset error:", pipelineResetErr.message);
    }
  } catch (err) {
    console.error("[worker-interval] Unhandled error in stuck-job reset:", err.message);
  }
}, 3 * 60 * 1000);

console.log("Background worker started — self-scheduling (10s active, 30s idle)");

app.post("/scrape-fees-batch", async (req, res) => {
  const { university_ids } = req.body;
  res.json({ message: "Fee scraping started" });

  for (const uid of university_ids) {
    console.log(`[fees-batch] Processing ${uid}`);
    await scrapeFeeStructure(uid);
    await new Promise((r) => setTimeout(r, 2000));
  }
  console.log("[fees-batch] Complete");
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", async () => {
  console.log(`Server running on port ${PORT} — worker enabled`);
  await supabase.schema('ingestion').from('pipeline_jobs')
    .update({ status: 'pending', attempts: 0 })
    .eq('status', 'running');
  runPipelineWorker();
});
// force
