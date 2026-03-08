require("dotenv").config();
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const cheerio = require("cheerio");
const puppeteer = require("puppeteer-core");

const browserWSEndpoint = `wss://chrome.browserless.io?token=2U5UENwcHnFsfxg065dfe159c7865e9aaf2ae16ccd0f026e2`;

async function fetchWithPuppeteer(url) {
  const browser = await puppeteer.connect({ browserWSEndpoint });
  try {
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    // Accept cookies if present
    try {
      await page.waitForTimeout(2000);
      const cookieSelectors = [
        'button[id*="accept"]',
        'button[class*="accept"]',
        'button[id*="cookie"]',
        'button[class*="cookie"]',
        'a[id*="accept"]',
        "#onetrust-accept-btn-handler",
        ".cookie-accept",
        '[aria-label*="accept"]',
        'button:contains("Accept")',
        'button:contains("Accept All")',
        'button:contains("I agree")',
        'button:contains("Allow")',
      ];

      for (const selector of cookieSelectors) {
        try {
          const btn = await page.$(selector);
          if (btn) {
            await btn.click();
            await page.waitForTimeout(1000);
            console.log(`[puppeteer] Accepted cookies with: ${selector}`);
            break;
          }
        } catch (e) {}
      }
    } catch (e) {}

    // Wait for content to load after cookie acceptance
    await page.waitForTimeout(3000);
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

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

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

app.post("/recommend", async (req, res) => {
  try {
    console.log("======== NEW REQUEST ========");
    console.log("BODY:", req.body);
    const answers = req.body;

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
      .lte("tuition_usd", tBand.max)
      .gte("duration_years", dBand.min)
      .lte("duration_years", dBand.max);

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

    const { data: courses, error: coErr } = await courseQuery;

    if (cErr) console.error("Countries fetch error:", cErr.message);
    if (uErr) console.error("Universities fetch error:", uErr.message);
    if (coErr) console.error("Courses fetch error:", coErr.message);

    if (!countries || !universities || !courses) {
      return res
        .status(500)
        .json({ error: "Failed to fetch core data from the database." });
    }

    const { data: countryData } = await supabase
      .from("country_normalized")
      .select("*");

    const { data: rankingData } = await supabase
      .from("university_composite_ranking")
      .select("id, final_score");

    const rankingMap = {};
    if (rankingData) {
      rankingData.forEach((r) => {
        rankingMap[r.id] = r.final_score;
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

    console.log("Eligible courses count:", eligibleCourses.length);
    console.log("Total courses fetched:", courses ? courses.length : 0);
    console.log(
      "Sample course:",
      courses ? JSON.stringify(courses[0]) : "none",
    );
    console.log(
      "Answers received:",
      JSON.stringify({
        level: answers.level,
        duration: answers.duration,
        tuition_band: answers.tuition_band,
        field: answers.field,
      }),
    );

    if (eligibleCourses.length === 0) {
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
        console.warn(
          "Country not found in countryMap:",
          country.id,
          country.name,
        );
        return 0;
      }

      let costWeight = 1;

      const costBandMap = {
        "$0 - $20K": 20000,
        "$20K - $30K": 25000,
        "More than $30K": 35000,
      };
      const userCostMidpoint = costBandMap[answers.cost_of_living] || 25000;
      const maxCostRange = 35000;
      const costAlignmentScore = clamp(
        1 - (c.cost_score != null ? 1 - c.cost_score : 0.5),
      );

      let pswWeight =
        answers.work_permit_importance === "Very strongly (3 years and above)"
          ? 1
          : answers.work_permit_importance.includes("Wouldn’t mind")
            ? 0.6
            : 0.3;

      let prWeight =
        answers.pr_importance === "Very strongly"
          ? 1
          : answers.pr_importance === "Wouldn’t mind"
            ? 0.6
            : 0.3;

      let govWeight =
        answers.gov_support_importance === "Very strongly"
          ? 1
          : answers.gov_support_importance === "Wouldn’t mind"
            ? 0.6
            : answers.gov_support_importance === "Don’t mind"
              ? 0.3
              : 0.3;

      let englishWeight =
        answers.english_preference === "Yes"
          ? 1
          : answers.english_preference === "Prefer but flexible"
            ? 0.6
            : 0.3;

      let weightedSum =
        costWeight * Math.max(0, Math.min(1, costAlignmentScore)) +
        pswWeight * c.psw_score +
        prWeight * c.pr_pathway_clarity_score +
        govWeight * c.government_support_score +
        englishWeight * c.english_score;

      let totalWeight =
        costWeight + pswWeight + prWeight + govWeight + englishWeight;

      return clamp(weightedSum / totalWeight);
    }

    function computeCourseScore(course, answers) {
      let courseComponents = [];
      let courseWeights = [];

      let internshipWeightMap = {
        "Very strongly": 1,
        "Wouldn’t mind": 0.6,
        "Don’t care": 0.3,
      };
      let internshipWeight =
        internshipWeightMap[answers.internship_importance] || 0;
      courseComponents.push(
        internshipWeight * (course.internship_available ? 1 : 0),
      );
      courseWeights.push(internshipWeight);

      let scholarshipWeightMap = {
        "Very strongly (more than 20% of tuition)": 1,
        "Wouldn’t mind getting one (less than 20% of tuition or none)": 0.6,
        "Don’t care": 0.3,
      };
      let scholarshipWeight =
        scholarshipWeightMap[answers.scholarship_importance] || 0;
      const scholarshipScore = course.scholarship_available ? 0.8 : 0.2;
      courseComponents.push(scholarshipWeight * scholarshipScore);
      courseWeights.push(scholarshipWeight);

      return clamp(
        courseComponents.reduce((a, b) => a + b, 0) /
          (courseWeights.reduce((a, b) => a + b, 0) || 1),
      );
    }

    function computeUniversityScore(university, country, answers, rankingMap) {
      let locationScore =
        answers.location_preference === "Anywhere in the country"
          ? 1
          : university.location_type === answers.location_preference
            ? 1
            : 0;

      const careerScore = university.career_services_score ?? 0.5;
      const admissionScoreRaw = university.admission_speed_score ?? 0.5;

      let careerWeightMap = {
        "Very strongly (placement driven institutions)": 1,
        "Moderately (academics driven institutions)": 0.6,
        "Not that much": 0.3,
      };
      let careerWeight = careerWeightMap[answers.career_importance] || 0;

      let admissionWeightMap = {
        "Very strongly": 1,
        "Not that much": 0.6,
        No: 0.3,
      };
      let admissionWeight =
        admissionWeightMap[answers.admission_speed_importance] || 0;

      const compositeRanking = rankingMap[university.id] ?? 0.5;

      const admissionSpeedScore = university.admission_speed_score ?? 0.5;
      let admissionScore = admissionWeight * admissionSpeedScore;

      let rankingWeight = 0;
      if (
        answers.ranking_importance === "Only want to apply in top institutions"
      )
        rankingWeight = 1;
      if (answers.ranking_importance === "Top and middle institutions are fine")
        rankingWeight = 0.7;
      if (
        answers.ranking_importance === "All institution irrespective of ranking"
      )
        rankingWeight = 0.4;

      let rankingScore = rankingWeight * compositeRanking;

      const uniNumerator =
        locationScore +
        rankingScore +
        careerWeight * careerScore +
        admissionScore;
      const uniDenominator = 1 + rankingWeight + careerWeight + admissionWeight;
      return clamp(uniNumerator / (uniDenominator || 1));
    }

    // 4️⃣ SCORE PATHWAYS
    const pathways = eligibleCourses.map((course) => {
      const university = universities.find(
        (u) => u.id === course.university_id,
      );
      if (!university) return null;
      const country = countries.find((c) => c.id === university.country_id);
      if (!country) return null;

      let countryScore = computeCountryScore(country, answers, countryMap);
      let courseScore = computeCourseScore(course, answers);
      let universityScore = computeUniversityScore(
        university,
        country,
        answers,
        rankingMap,
      );

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
        answers.internship_importance !== "Don’t care"
      ) {
        explanation.push("Includes internship as part of the curriculum");
      }

      if (courseScore >= 0.7)
        explanation.push(
          "Strong course alignment with your academic preferences",
        );
      else if (courseScore >= 0.4)
        explanation.push("Reasonable course fit based on your priorities");

      if (universityScore >= 0.7)
        explanation.push(
          "Institution scores well on ranking, location, and services",
        );
      else if (universityScore >= 0.4)
        explanation.push("Institution meets your core university preferences");

      if (
        answers.location_preference !== "Anywhere in the country" &&
        university.location_type === answers.location_preference
      ) {
        explanation.push(
          "Campus location matches your " +
            answers.location_preference.toLowerCase() +
            " preference",
        );
      }

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
    });

    // 5️⃣ Sort & Return Top 5
    const top5 = pathways
      .filter((p) => p !== null)
      .sort((a, b) => (b.finalScore || 0) - (a.finalScore || 0))
      .slice(0, 5);

    res.json(top5);
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
  "program","degree","graduate","master","phd","doctoral","course",
  "faculty","school","department","msc","mba","med","llm","meng","certificate","diploma",
];

function isListingPage(html, sourceUrl) {
  try {
    const $ = cheerio.load(html);

    $("script, style, nav, footer, header, .menu, .navigation, .breadcrumb").remove();

    const mainSelectors = ["main", "article", ".content", "#content", "[role='main']", ".page-content", ".main-content"];
    let mainText = "";
    for (const sel of mainSelectors) {
      const el = $(sel);
      if (el.length && el.text().trim().length > 200) {
        mainText = el.text().replace(/\s+/g, " ").trim();
        break;
      }
    }
    if (!mainText) mainText = $("body").text().replace(/\s+/g, " ").trim();

    const wordCount = mainText.split(/\s+/).length;

    const programHeadings = [
      "admission", "requirement", "curriculum", "degree requirement",
      "program structure", "course requirement", "application",
      "tuition", "duration", "thesis", "dissertation", "supervisor"
    ];
    const headingText = $("h1, h2, h3").text().toLowerCase();
    const hasProgamHeadings = programHeadings.some(h => headingText.includes(h));

    if (wordCount > 300 || hasProgamHeadings) {
      return false;
    }

    console.log(`[parse] Detected listing page (words: ${wordCount}, no program headings): ${sourceUrl}`);
    return true;
  } catch (e) {
    return false;
  }
}

// ----------------------
// PARSE SINGLE PROGRAM
// ----------------------

async function parseProgramPage(pageId) {
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
      await supabase.schema("ingestion").from("raw_program_pages")
        .update({ parse_status: "skipped" }).eq("id", pageId);
      console.log(`[parse] Skipped listing page: ${raw.source_url}`);
      return { success: true, programs: [], skipped: true };
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

    const completion = await Promise.race([
      openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        temperature: 0,
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("OpenAI timeout after 60s")), 60000),
      ),
    ]);

    const content = completion.choices[0].message.content;
    const parsed = JSON.parse(content.replace(/```json|```/g, "").trim());

    // Handle both single object (legacy) and array (multi-degree pages)
    const programList = Array.isArray(parsed) ? parsed : [parsed];

    const { data: feeStructures } = await supabase
      .schema("ingestion")
      .from("university_fee_structure")
      .select("*")
      .eq("university_id", raw.university_id);

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
      if (!duration_years && program.total_credits_required) {
        const { data: uni } = await supabase
          .schema("core")
          .from("universities")
          .select("credits_per_year")
          .eq("id", raw.university_id)
          .single();
        if (uni?.credits_per_year) {
          duration_years = Math.ceil(
            program.total_credits_required / uni.credits_per_year,
          );
        }
      }

      const CAD_TO_USD = 0.74;

      console.log(
        `[parse-fees] ${program.program_name} | type=${program.program_type} | uni=${raw.university_id} | feeStructures=${feeStructures?.length || 0}`,
      );

      const tuitionCAD = resolveTuition(
        program.program_name,
        program.program_type,
        raw.university_id,
        feeStructures,
      );
      console.log(`[parse-fees] resolveTuition returned: ${tuitionCAD}`);
      const tuition_usd = tuitionCAD
        ? Math.round(tuitionCAD * CAD_TO_USD)
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
  const concurrency = req.body.concurrency || 5;

  res.json({
    message: `Starting parallel parse — limit: ${limit}, concurrency: ${concurrency}`,
  });

  (async () => {
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
  try {
    const limit = req.body.limit || 10;
    const university_id = req.body.university_id;

    let query = supabase
      .schema("ingestion")
      .from("scrape_queue")
      .select("*")
      .eq("status", "pending");

    if (university_id) {
      query = query.eq("university_id", university_id);
    }

    const { data: queueItems, error: qErr } = await query.limit(limit);

    if (qErr) return res.status(500).json({ error: qErr });
    if (!queueItems || queueItems.length === 0) {
      return res.json({ message: "Queue is empty" });
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
          try {
            const browser = await puppeteer.connect({ browserWSEndpoint });
            const page = await browser.newPage();
            await page.goto(item.program_url, {
              waitUntil: "domcontentloaded",
              timeout: 30000,
            });
            await new Promise((r) => setTimeout(r, 3000));
            html = await page.content();
            await browser.disconnect();
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

    res.json({
      message: "Queue processing complete",
      ...results,
    });
  } catch (err) {
    console.error("Queue processing error:", err.message);
    res
      .status(500)
      .json({ error: "Queue processing failed", details: err.message });
  }
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

    const CAD_TO_USD = 0.74;

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
          const tuitionCAD = resolveTuition(
            p.program_name,
            p.program_type,
            p.university_id,
            feeStructures,
          );
          console.log(
            `[migrate-fees] ${p.program_name} → resolveTuition: ${tuitionCAD}`,
          );
          if (tuitionCAD) {
            finalTuitionUSD = Math.round(tuitionCAD * CAD_TO_USD);
          }
        }

        if (!finalTuitionUSD) {
          console.log(`[migrate-skip] No tuition for: ${p.program_name}`);
          skipped++;
          continue;
        }

        const { error: insertError } = await supabase
          .schema("core")
          .from("courses")
          .insert({
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
        const tuitionCAD = resolveTuition(
          course.name,
          course.program_type,
          uniId,
          feeStructures,
        );
        console.log(`[migrate-fees] resolveTuition returned: ${tuitionCAD}`);
        if (tuitionCAD) {
          const tuitionUSD = Math.round(tuitionCAD * CAD_TO_USD);
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

    // ---- STEP 2: SCRAPE ----
    await updateJobStatus(job.id, "scraping");
    const scrapeResult = await scrapeQueueForUniversity(job.university_id);
    await supabase
      .schema("ingestion")
      .from("university_jobs")
      .update({ urls_scraped: scrapeResult })
      .eq("id", job.id);
    console.log(`[worker] Scraped ${scrapeResult} pages`);

    // ---- STEP 3: PARSE ----
    await updateJobStatus(job.id, "parsing");
    const parseResult = await parsePagesForUniversity(job.university_id);
    await supabase
      .schema("ingestion")
      .from("university_jobs")
      .update({ urls_parsed: parseResult })
      .eq("id", job.id);
    console.log(`[worker] Parsed ${parseResult} pages`);

    // ---- STEP 4: SCRAPE FEE STRUCTURE (intelligent — tries form-based then static) ----
    await updateJobStatus(job.id, "fee_scraping");
    const feeResult = await scrapeFeeStructureIntelligent(job.university_id);
    if (!feeResult) {
      console.warn(`[worker] Fee scraping FAILED for ${job.university_id} — manual fee insertion needed`);
      await supabase.schema("ingestion").from("university_jobs")
        .update({ error_message: "Fee scraping failed — insert fees manually into university_fee_structure" })
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
  const update = { status };
  if (status === "crawling") update.started_at = new Date().toISOString();
  if (errorMessage) update.error_message = errorMessage;
  await supabase
    .schema("ingestion")
    .from("university_jobs")
    .update(update)
    .eq("id", jobId);
}

async function crawlDirectory(universityId, dirUrl, depth = 1) {
  const parsedBase = new URL(dirUrl);
  const baseDomain = parsedBase.hostname;

  const PROGRAM_URL_SIGNALS = [
    "program",
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
    const path = url.toLowerCase();
    const hasProgram = PROGRAM_URL_SIGNALS.some((s) => path.includes(s));
    const shouldSkip = SKIP_URL_SIGNALS.some((s) => path.includes(s));
    return hasProgram && !shouldSkip;
  }

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
            setTimeout(() => reject(new Error("Puppeteer timeout")), 45000),
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

      console.log(`[crawl] ${url} → found ${foundOnPage} program URLs`);
      await new Promise((r) => setTimeout(r, 500));
    }
  }

  if (discovered.length === 0) return 0;

  const { data } = await supabase
    .schema("ingestion")
    .from("scrape_queue")
    .upsert(discovered, { onConflict: "program_url" })
    .select();

  return data ? data.length : 0;
}

async function scrapeQueueForUniversity(universityId) {
  const CONCURRENCY = 5;
  let totalScraped = 0;

  while (true) {
    const { data: items } = await supabase
      .schema("ingestion")
      .from("scrape_queue")
      .select("*")
      .eq("university_id", universityId)
      .eq("status", "pending")
      .limit(50);

    if (!items || items.length === 0) break;

    for (let i = 0; i < items.length; i += CONCURRENCY) {
      const chunk = items.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (item) => {
          try {
            await supabase
              .schema("ingestion")
              .from("scrape_queue")
              .update({ status: "processing" })
              .eq("id", item.id);

            let html;
            try {
              const res = await axios.get(item.program_url, {
                headers: {
                  "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)",
                },
                timeout: 30000,
              });
              html = res.data;
            } catch (e) {
              html = null;
            }

            if (!html || html.length < 500) {
              html = await Promise.race([
                fetchWithPuppeteer(item.program_url),
                new Promise((_, reject) =>
                  setTimeout(
                    () => reject(new Error("Puppeteer timeout after 45s")),
                    45000,
                  ),
                ),
              ]);
            }

            if (!html || html.length < 500) {
              await supabase
                .schema("ingestion")
                .from("scrape_queue")
                .update({ status: "failed", error_message: "Page too small" })
                .eq("id", item.id);
              return;
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
              .update({
                status: "scraped",
                scraped_at: new Date().toISOString(),
              })
              .eq("id", item.id);
            totalScraped++;
          } catch (e) {
            await supabase
              .schema("ingestion")
              .from("scrape_queue")
              .update({ status: "failed", error_message: e.message })
              .eq("id", item.id);
          }
        }),
      );
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  return totalScraped;
}

async function parsePagesForUniversity(universityId) {
  const CONCURRENCY = 5;
  let totalParsed = 0;

  while (true) {
    const { data: pages } = await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .select("id")
      .eq("university_id", universityId)
      .eq("parse_status", "pending")
      .limit(50);

    if (!pages || pages.length === 0) break;

    for (let i = 0; i < pages.length; i += CONCURRENCY) {
      const chunk = pages.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (page) => {
          try {
            await parseProgramPage(page.id);
            totalParsed++;
          } catch (e) {
            console.error(`[parse] Failed page ${page.id}:`, e.message);
          }
        }),
      );
    }
  }

  return totalParsed;
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

  let fixed = 0;
  for (const p of programs) {
    const assigned = autoAssignFieldCategory(p.program_name);
    if (assigned) {
      await supabase
        .schema("ingestion")
        .from("parsed_programs")
        .update({ field_category: assigned })
        .eq("id", p.id);
      fixed++;
      console.log(`[fix] "${p.program_name}" → "${assigned}"`);
    } else {
      console.log(`[fix] Could not assign category for: "${p.program_name}"`);
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
      const completion = await Promise.race([
        openai.chat.completions.create({
          model: "gpt-4o-mini",
          messages: [{ role: "user", content: categoryPrompt }],
          temperature: 0,
        }),
        new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 30000)),
      ]);
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

  if (programType) {
    const specificWithType = levelFees.filter(f =>
      f.program_type === programType &&
      f.program_name_pattern &&
      f.program_name_pattern !== `default_${level}` &&
      nameLower.includes(f.program_name_pattern.toLowerCase())
    );
    if (specificWithType.length > 0) {
      specificWithType.sort((a, b) => b.program_name_pattern.length - a.program_name_pattern.length);
      const fee = specificWithType[0];
      return fee.fee_type === 'flat_annual'
        ? fee.international_fee
        : fee.international_fee * (fee.instalments_per_year || 2);
    }
  }

  const specificNoType = levelFees.filter(f =>
    f.program_type === null &&
    f.program_name_pattern &&
    f.program_name_pattern !== `default_${level}` &&
    nameLower.includes(f.program_name_pattern.toLowerCase())
  );
  if (specificNoType.length > 0) {
    specificNoType.sort((a, b) => b.program_name_pattern.length - a.program_name_pattern.length);
    const fee = specificNoType[0];
    return fee.fee_type === 'flat_annual'
      ? fee.international_fee
      : fee.international_fee * (fee.instalments_per_year || 2);
  }

  if (programType) {
    const defaultWithType = levelFees.find(f =>
      f.program_type === programType &&
      f.program_name_pattern === `default_${level}`
    );
    if (defaultWithType) {
      return defaultWithType.fee_type === 'flat_annual'
        ? defaultWithType.international_fee
        : defaultWithType.international_fee * (defaultWithType.instalments_per_year || 2);
    }
  }

  const defaultFee = levelFees.find(f =>
    f.program_name_pattern === `default_${level}` &&
    f.program_type === null
  );
  if (defaultFee) {
    return defaultFee.fee_type === 'flat_annual'
      ? defaultFee.international_fee
      : defaultFee.international_fee * (defaultFee.instalments_per_year || 2);
  }

  return null;
}

async function analyseFeePage(html, url) {
  const $ = cheerio.load(html);
  const hasForms = $("form, select, input[type='radio'], input[type='submit']").length > 0;
  const hasTable = $("table").length > 0;
  const hasDropdowns = $("select").length > 0;

  const formElements = [];
  $("select").each(function () {
    const id = $(this).attr("id") || $(this).attr("name") || "";
    const label = $(`label[for='${id}']`).text().trim() || id;
    const options = [];
    $(this).find("option").each(function () {
      const val = $(this).attr("value") || "";
      const text = $(this).text().trim();
      if (val && text) options.push({ value: val, text });
    });
    if (options.length > 0) formElements.push({ type: "select", id, label, options });
  });

  $("input[type='radio']").each(function () {
    const name = $(this).attr("name") || "";
    const val = $(this).attr("value") || "";
    const label = $(`label[for='${$(this).attr("id")}']`).text().trim() || val;
    formElements.push({ type: "radio", name, value: val, label });
  });

  return { hasForms, hasTable, hasDropdowns, formElements, isFormBased: formElements.length > 0 };
}

async function buildFormFillingPlan(html, url, universityName) {
  const $ = cheerio.load(html);
  $("script, style, nav, footer, header, aside").remove();

  let formHtml = "";
  $("form, select, input, label, button[type='submit'], input[type='submit']").each(function () {
    formHtml += $.html(this) + "\n";
  });
  $("table").each(function () {
    formHtml += $.html(this).substring(0, 2000) + "\n";
  });
  formHtml = formHtml.substring(0, 6000);

  const prompt = `
You are helping scrape international graduate tuition fees from a university fee calculator page.
University: ${universityName}
URL: ${url}

Here is the relevant HTML from the fee page (forms, selects, inputs, tables):
${formHtml}

Your task: Return a JSON plan describing exactly how to use this form to extract international graduate fees.

Return STRICT JSON only (no markdown, no explanation) with this structure:
{
  "page_type": "form_calculator" | "static_table" | "mixed",
  "submit_selector": "CSS selector for the submit button or null if auto-submits",
  "result_selector": "CSS selector where the fee result appears after submission",
  "combinations": [
    {
      "label": "International Masters Research",
      "fields": [
        { "selector": "CSS selector", "type": "select|radio|checkbox", "value": "exact option value to select" }
      ]
    }
  ]
}

RULES:
- Include combinations for: international + masters (research), international + masters (professional), international + doctoral
- If the form has a "faculty" or "program" field, include the most common/default option (e.g. "Arts" or "General")
- If the page is a static table (no form), set page_type to "static_table" and combinations to []
- If you cannot determine the form structure, return { "page_type": "unknown", "combinations": [] }
- selector values must be valid CSS selectors (use #id, [name='x'], or select:nth-of-type(n))
`;

  try {
    const completion = await Promise.race([
      openai.chat.completions.create({
        model: "gpt-4o",
        messages: [{ role: "user", content: prompt }],
        temperature: 0,
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 45000)),
    ]);
    const content = completion.choices[0].message.content.replace(/```json|```/g, "").trim();
    return JSON.parse(content);
  } catch (err) {
    console.error("[fees-intelligent] GPT form plan failed:", err.message);
    return { page_type: "unknown", combinations: [] };
  }
}

async function identifyFormSelectors(html, universityName) {
  const $ = cheerio.load(html);
  const selects = [];
  $("select").each(function(i) {
    const id = $(this).attr("id") || $(this).attr("name") || `select_${i}`;
    const labelEl = $(`label[for='${$(this).attr("id") || ""}']`).text().trim()
      || $(this).closest("div, p, tr").find("label, th, td").first().text().trim()
      || id;
    const options = [];
    $(this).find("option").each(function() {
      const val = $(this).attr("value");
      const text = $(this).text().trim();
      if (val !== undefined && val !== "" && text) options.push({ value: val, text });
    });
    selects.push({ selector: id.startsWith("select_") ? `select:nth-of-type(${i+1})` : `#${id}`, label: labelEl, options });
  });

  let submitSelector = null;
  const submitCandidates = ["input[type='submit']", "button[type='submit']", "button.btn-primary"];
  for (const s of submitCandidates) {
    if ($(s).length) { submitSelector = s; break; }
  }

  let resultSelector = "table";
  if ($(".views-table").length) resultSelector = ".views-table";
  else if ($("#fee-results").length) resultSelector = "#fee-results";

  const prompt = `
University: ${universityName}
You are identifying form fields on a university tuition fee calculator page.

Here are the select dropdowns found on the page:
${JSON.stringify(selects, null, 2)}

Return STRICT JSON only:
{
  "level_selector": "CSS selector for the 'level of study' dropdown",
  "level_masters_values": ["array of option values that mean masters/graduate diploma"],
  "level_doctoral_values": ["array of option values that mean PhD/doctoral"],
  "faculty_selector": "CSS selector for faculty/school dropdown, or null if not present",
  "discipline_selector": "CSS selector for discipline/program dropdown, or null",
  "student_type_selector": "CSS selector for domestic/international selector, or null",
  "student_type_international_value": "option value for international students, or null",
  "submit_selector": "${submitSelector || "input[type='submit']"}",
  "result_selector": "${resultSelector}",
  "cascading": true or false (true if changing faculty updates discipline options)
}

RULES:
- Use the exact CSS selectors from the selects array above
- If no faculty dropdown exists, set faculty_selector to null
- Only include values that clearly mean masters/doctoral — skip blank/placeholder options
`;

  try {
    const completion = await Promise.race([
      openai.chat.completions.create({ model: "gpt-4o", messages: [{ role: "user", content: prompt }], temperature: 0 }),
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 30000)),
    ]);
    return JSON.parse(completion.choices[0].message.content.replace(/```json|```/g, "").trim());
  } catch (err) {
    console.error("[fees-enum] GPT selector identification failed:", err.message);
    return null;
  }
}

async function executeFullEnumeration(feeUrl, selectors, universityName) {
  const browser = await puppeteer.connect({ browserWSEndpoint });
  const page = await browser.newPage();
  const results = [];

  try {
    await page.goto(feeUrl, { waitUntil: "networkidle2", timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));

    if (selectors.student_type_selector && selectors.student_type_international_value) {
      try {
        await page.select(selectors.student_type_selector, selectors.student_type_international_value);
        await new Promise(r => setTimeout(r, 500));
      } catch (e) { console.warn("[fees-enum] Could not set student type:", e.message); }
    }

    let facultyOptions = [{ value: null, text: "default" }];
    if (selectors.faculty_selector) {
      try {
        facultyOptions = await page.$$eval(`${selectors.faculty_selector} option`, opts =>
          opts.filter(o => o.value && o.value !== "").map(o => ({ value: o.value, text: o.textContent.trim() }))
        );
        console.log(`[fees-enum] Found ${facultyOptions.length} faculties for ${universityName}`);
      } catch (e) { console.warn("[fees-enum] Could not read faculty options:", e.message); }
    }

    const levelCombos = [
      { values: selectors.level_masters_values || [], label: "masters" },
      { values: selectors.level_doctoral_values || [], label: "doctoral" },
    ];

    for (const levelCombo of levelCombos) {
      if (!levelCombo.values.length) continue;
      const levelValue = levelCombo.values[0];

      for (const faculty of facultyOptions) {
        try {
          await page.goto(feeUrl, { waitUntil: "networkidle2", timeout: 30000 });
          await new Promise(r => setTimeout(r, 1000));

          if (selectors.student_type_selector && selectors.student_type_international_value) {
            try { await page.select(selectors.student_type_selector, selectors.student_type_international_value); await new Promise(r => setTimeout(r, 500)); } catch (e) {}
          }

          await page.select(selectors.level_selector, levelValue);
          await new Promise(r => setTimeout(r, 800));

          if (selectors.faculty_selector && faculty.value) {
            try {
              await page.select(selectors.faculty_selector, faculty.value);
              await new Promise(r => setTimeout(r, 800));
            } catch (e) { continue; }
          }

          const billingSelectors = ["select[name*='billing']", "select[name*='load']", "select[name*='status']"];
          for (const bs of billingSelectors) {
            try {
              const opts = await page.$$eval(`${bs} option`, o => o.map(x => x.value));
              const ftOpt = opts.find(v => v && (v.includes("full") || v.includes("flat") || v.includes("FT")));
              if (ftOpt) { await page.select(bs, ftOpt); await new Promise(r => setTimeout(r, 300)); break; }
            } catch (e) {}
          }

          try {
            await page.click(selectors.submit_selector);
            await new Promise(r => setTimeout(r, 2000));
          } catch (e) {
            try { await page.keyboard.press("Enter"); await new Promise(r => setTimeout(r, 2000)); } catch (e2) {}
          }

          let feeAmount = null;
          let feeText = "";
          try {
            await page.waitForSelector(selectors.result_selector, { timeout: 5000 });
            feeText = await page.$eval(selectors.result_selector, el => {
              const rows = el.querySelectorAll("tr");
              for (const row of rows) {
                const cells = row.querySelectorAll("td");
                if (cells.length >= 2) {
                  return Array.from(cells).map(c => c.textContent.trim()).join(" | ");
                }
              }
              return el.textContent.replace(/\s+/g, " ").trim().substring(0, 200);
            });

            const feeMatch = feeText.match(/[\$£€]?([\d,]+\.?\d*)/);
            if (feeMatch) feeAmount = parseFloat(feeMatch[1].replace(/,/g, ""));
          } catch (e) {
            console.warn(`[fees-enum] Could not extract result for ${levelCombo.label} / ${faculty.text}`);
            continue;
          }

          if (feeAmount && feeAmount > 100) {
            const label = `${levelCombo.label} | ${faculty.text}`;
            results.push({ label, level: levelCombo.label, faculty: faculty.text, feePerTerm: feeAmount });
            console.log(`[fees-enum] ${label}: ${feeAmount}/term`);
          }

        } catch (comboErr) {
          console.warn(`[fees-enum] Combo failed (${levelCombo.label}/${faculty.text}):`, comboErr.message);
        }
      }
    }

  } finally {
    try { await page.close(); } catch (e) {}
    try { await browser.disconnect(); } catch (e) {}
  }

  return results;
}

function buildFeeRowsFromEnumeration(results, universityId, feeUrl, instalmentsPerYear = 3) {
  const feeRows = [];
  const seen = new Set();

  for (const r of results) {
    const annualFee = Math.round(r.feePerTerm * instalmentsPerYear * 100) / 100;

    const facultyLower = r.faculty.toLowerCase();
    let pattern = null;
    if (facultyLower === "default") {
      pattern = `default_${r.level === "masters" ? "masters" : "doctoral"}`;
    } else {
      pattern = facultyLower
        .replace(/faculty of |school of |telfer |department of /gi, "")
        .replace(/[^a-z0-9 ]/g, "")
        .trim()
        .split(" ")[0];
    }

    const key = `${r.level}|${pattern}`;
    if (seen.has(key)) continue;
    seen.add(key);

    feeRows.push({
      university_id: universityId,
      program_level: r.level === "masters" ? "masters" : "doctoral",
      program_type: null,
      fee_type: "flat_annual",
      international_fee: annualFee,
      instalments_per_year: instalmentsPerYear,
      currency: "CAD",
      program_name_pattern: pattern,
      notes: `Per term: ${r.feePerTerm} CAD × ${instalmentsPerYear} terms`,
      fee_page_url: feeUrl,
    });
  }

  return feeRows;
}

async function parseFormResults(results, universityName) {
  if (results.length === 0) return [];

  const resultText = results.map(r => {
    const $ = cheerio.load(r.html);
    $("script, style").remove();
    return `=== ${r.label} ===\n${$("body").text().replace(/\s+/g, " ").trim().substring(0, 1500)}`;
  }).join("\n\n");

  const prompt = `
You are extracting international graduate tuition fees from university fee calculator results.
University: ${universityName}

Results from the fee calculator for different combinations:
${resultText}

Return STRICT JSON array only. Each entry is one fee row:
[
  {
    "program_level": "masters" | "doctoral",
    "program_type": "research" | "professional" | null,
    "fee_type": "flat_annual" | "per_instalment",
    "international_fee": numeric_amount,
    "instalments_per_year": 1 | 2 | 3,
    "currency": "CAD" | "GBP" | "EUR" | "AUD" | "USD",
    "program_name_pattern": null or specific keyword,
    "notes": "optional notes"
  }
]

RULES:
- international_fee should be the ANNUAL total for a FIRST YEAR student (use the first/lowest progression level only)
- If the table shows multiple progression levels (term 1, term 2, A1, A2, B1 etc.), use ONLY the first level
- Multiply per-term fee by instalments_per_year to get annual total
- If you see "per term" fees, set fee_type to "per_instalment" and set instalments_per_year accordingly
- Graduate year = 3 terms for Canadian universities, 2 for UK/Australian
- program_name_pattern = null for default fees, or lowercase keyword for program-specific (e.g. "mba", "engineering")
- Deduplicate: if masters research and professional have same fee, create one entry with program_type = null
- Return [] if no clear fee amounts found
`;

  try {
    const completion = await Promise.race([
      openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        temperature: 0,
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 45000)),
    ]);
    const content = completion.choices[0].message.content.replace(/```json|```/g, "").trim();
    return JSON.parse(content);
  } catch (err) {
    console.error("[fees-intelligent] GPT result parsing failed:", err.message);
    return [];
  }
}

async function extractFeesFromText(feeText, universityName) {
  const feePrompt = `
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
- program_name_pattern: null for defaults, or lowercase keyword for specific programs
- notes: any relevant notes

RULES:
- Only international student fees
- Graduate year = 3 terms for Canadian, 2 for UK/Australian
- program_name_pattern = "default_masters" or "default_doctoral" for general fees
- Return [] if no clear fee structure found

Content:
${feeText}
`;
  try {
    const completion = await Promise.race([
      openai.chat.completions.create({ model: "gpt-4o-mini", messages: [{ role: "user", content: feePrompt }], temperature: 0 }),
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 60000)),
    ]);
    const content = completion.choices[0].message.content.replace(/```json|```/g, "").trim();
    return JSON.parse(content);
  } catch (err) {
    console.error("[fees] GPT extraction failed:", err.message);
    return [];
  }
}

function findFeePageUrl(baseUrls) {
  return new Promise(async (resolve) => {
    for (const base of baseUrls) {
      for (const pattern of FEE_PAGE_PATTERNS) {
        const testUrl = base + pattern;
        try {
          const res = await axios.get(testUrl, {
            headers: { "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)" },
            timeout: 15000,
            validateStatus: (s) => s < 404,
          });
          if (res.status === 200 && res.data.length > 3000) {
            const hasFee = ["tuition", "fee", "international"].some(k => res.data.toLowerCase().includes(k));
            if (hasFee) return resolve(testUrl);
          }
        } catch (e) { continue; }
      }
    }
    resolve(null);
  });
}

async function getBaseUrlsForUniversity(universityId) {
  const { data: sampleUrl } = await supabase
    .schema("ingestion")
    .from("scrape_queue")
    .select("program_url")
    .eq("university_id", universityId)
    .limit(1)
    .single();
  if (!sampleUrl) return null;
  const parsedUrl = new URL(sampleUrl.program_url);
  const baseUrl = parsedUrl.origin;
  const hostParts = parsedUrl.hostname.split(".");
  const rootDomain = hostParts.length > 2
    ? `${parsedUrl.protocol}//${hostParts.slice(-2).join(".")}`
    : baseUrl;
  return [...new Set([baseUrl, rootDomain])];
}

async function scrapeFeeStructureIntelligent(universityId, manualFeeUrl = null) {
  const { data: uni } = await supabase.schema("core").from("universities").select("name").eq("id", universityId).single();
  const universityName = uni?.name || universityId;

  let feeUrl = manualFeeUrl;
  if (!feeUrl) {
    const baseUrls = await getBaseUrlsForUniversity(universityId);
    if (!baseUrls) {
      console.log(`[fees-intelligent] No URLs found for ${universityName}`);
      return false;
    }
    feeUrl = await findFeePageUrl(baseUrls);
  }

  if (!feeUrl) {
    console.log(`[fees-intelligent] Could not find fee page for ${universityName}`);
    return false;
  }
  console.log(`[fees-intelligent] Using fee URL: ${feeUrl}`);

  let html = null;
  try {
    const res = await axios.get(feeUrl, { headers: { "User-Agent": "Mozilla/5.0" }, timeout: 15000 });
    if (res.status === 200 && res.data.length > 1000) html = res.data;
  } catch (e) {}

  if (!html) {
    html = await Promise.race([
      fetchWithPuppeteer(feeUrl),
      new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 30000)),
    ]).catch(() => null);
  }

  if (!html) {
    console.log(`[fees-intelligent] Could not fetch fee page for ${universityName}`);
    return false;
  }

  const analysis = await analyseFeePage(html, feeUrl);
  console.log(`[fees-intelligent] Page type for ${universityName}: ${analysis.isFormBased ? "form-based" : "static"}, forms=${analysis.hasForms}, tables=${analysis.hasTable}`);

  let fees = [];

  if (analysis.isFormBased) {
    console.log(`[fees-intelligent] Identifying form selectors for ${universityName}...`);
    const selectors = await identifyFormSelectors(html, universityName);

    if (selectors && selectors.level_selector) {
      console.log(`[fees-intelligent] Starting full enumeration for ${universityName}...`);
      const enumResults = await executeFullEnumeration(feeUrl, selectors, universityName);
      console.log(`[fees-intelligent] Enumeration complete: ${enumResults.length} combinations extracted`);
      if (enumResults.length > 0) {
        const instalmentsPerYear = 3;
        const feeRows = buildFeeRowsFromEnumeration(enumResults, universityId, feeUrl, instalmentsPerYear);

        if (feeRows.length > 0) {
          const { error } = await supabase.schema("ingestion").from("university_fee_structure")
            .upsert(feeRows, { onConflict: "university_id,program_level,program_name_pattern,program_type" });
          if (error) { console.error(`[fees-intelligent] Insert error:`, error.message); }
          else {
            console.log(`[fees-intelligent] Inserted ${feeRows.length} fee rows from form enumeration`);
            return true;
          }
        }
      }
    }

    console.log(`[fees-intelligent] Form approach yielded nothing, falling back to static extraction`);
  }

  if (fees.length === 0) {
    const $ = cheerio.load(html);
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

    fees = await extractFeesFromText(feeText, universityName);
  }

  if (!fees || fees.length === 0) {
    console.log(`[fees-intelligent] No fees extracted for ${universityName}`);
    return false;
  }

  const feeRows = fees.map(f => ({
    university_id: universityId,
    program_level: f.program_level,
    program_type: f.program_type || null,
    fee_type: f.fee_type || "flat_annual",
    international_fee: f.international_fee,
    instalments_per_year: f.instalments_per_year || 1,
    currency: f.currency || "CAD",
    program_name_pattern: f.program_name_pattern || `default_${f.program_level}`,
    notes: f.notes || null,
    fee_page_url: feeUrl,
  }));

  const { error } = await supabase.schema("ingestion").from("university_fee_structure")
    .upsert(feeRows, { onConflict: "university_id,program_level,program_name_pattern,program_type" });

  if (error) {
    console.error(`[fees-intelligent] Insert error for ${universityName}:`, error.message);
    return false;
  }
  console.log(`[fees-intelligent] Extracted ${feeRows.length} fee entries for ${universityName}`);
  return true;
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

    const CAD_TO_USD = 0.74;

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
          const tuitionCAD = resolveTuition(p.program_name, p.program_type, university_id, feeStructures || []);
          console.log(`[migrate] ${p.program_name} → tuitionCAD: ${tuitionCAD}`);
          if (tuitionCAD) finalTuitionUSD = Math.round(tuitionCAD * CAD_TO_USD);
        }

        if (!finalTuitionUSD) {
          console.log(`[migrate-skip] No tuition: ${p.program_name}`);
          skipped++;
          continue;
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
          }, { onConflict: "university_id,name,degree_level,program_type", ignoreDuplicates: true });

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
// START BACKGROUND WORKER — runs every 3 minutes
// ============================================================
setInterval(
  async () => {
    // Only reset jobs stuck in active processing states for over 2 hours
    // NEVER reset ready_for_review or migrated jobs
    await supabase
      .schema("ingestion")
      .from("university_jobs")
      .update({ status: "queued", error_message: "Reset after timeout" })
      .in("status", [
        "crawling",
        "scraping",
        "parsing",
        "fixing",
        "fee_scraping",
      ])
      .not("status", "in", '("ready_for_review","migrated","failed")')
      .lt(
        "started_at",
        new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      );

    runWorker();
  },
  3 * 60 * 1000,
);
console.log("Background worker started — polling every 3 minutes");

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
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT} — worker enabled`);
});
// force
