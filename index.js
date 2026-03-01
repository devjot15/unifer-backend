require("dotenv").config();
const express = require("express");
const cors = require("cors");
const axios = require("axios");
const cheerio = require("cheerio");
const { createClient } = require("@supabase/supabase-js");
const OpenAI = require("openai");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

// Connect to Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
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
    .schema("core").from("countries")
    .select("*");

  if (error) {
    return res.status(500).json({ error });
  }

  res.json(data);
});

app.get("/seed", async (req, res) => {
  try {
    // Clear existing data (order matters due to foreign keys)
    await supabase.schema("core").from("courses").delete().neq("id", "00000000-0000-0000-0000-000000000000");
    await supabase.schema("core").from("universities").delete().neq("id", "00000000-0000-0000-0000-000000000000");
    await supabase.schema("core").from("countries").delete().neq("id", "00000000-0000-0000-0000-000000000000");

    const countriesData = [
      { name: "Canada", cost_of_living_band: "20-30K", work_permit_level: 0.9, english_first_language: true, government_support_level: 0.8, pr_opportunity_level: 0.9 },
      { name: "Germany", cost_of_living_band: "0-20K", work_permit_level: 0.6, english_first_language: false, government_support_level: 0.9, pr_opportunity_level: 0.7 },
      { name: "Australia", cost_of_living_band: "30K+", work_permit_level: 0.8, english_first_language: true, government_support_level: 0.7, pr_opportunity_level: 0.6 },
      { name: "UK", cost_of_living_band: "30K+", work_permit_level: 0.7, english_first_language: true, government_support_level: 0.6, pr_opportunity_level: 0.5 },
      { name: "Ireland", cost_of_living_band: "20-30K", work_permit_level: 0.8, english_first_language: true, government_support_level: 0.7, pr_opportunity_level: 0.6 }
    ];

    const { data: countries, error: countriesError } = await supabase.schema("core").from("countries").insert(countriesData).select();
    if (countriesError) throw countriesError;

    for (let country of countries) {
      for (let i = 1; i <= 5; i++) {
        const { data: university, error: uniError } = await supabase
          .schema("core").from("universities")
          .insert({
            name: `${country.name} University ${i}`,
            country_id: country.id,
            location_type: i % 2 === 0 ? "Main city" : "Smaller cities",
            ranking_score: Math.random(),
            career_services_score: 0.5,
            admission_speed_score: 0.5
          })
          .select()
          .single();
        
        if (uniError) throw uniError;
        if (!university) continue;

        for (let j = 1; j <= 10; j++) {
          const { error: courseError } = await supabase.schema("core").from("courses").insert({
            name: `Course ${j}`,
            university_id: university.id,
            level: j % 2 === 0 ? "UG" : "PG",
            duration_category: j % 2 === 0 ? "3 years or less" : "1 year or less",
            internship_available: j % 2 === 0,
            gre_required: false,
            gmat_required: false,
            scholarship_level: Math.random(),
            tuition_band: j % 3 === 0 ? "Less than $12k" : j % 3 === 1 ? "$12k - $25k" : "More than $25K",
            field_category: "engineering & tech"
          });
          if (courseError) throw courseError;
        }
      }
    }

    res.send("Database reset and seeded successfully 🚀");
  } catch (error) {
    console.error(error);
    res.status(500).send("Seeding failed");
  }
});

app.post("/recommend", async (req, res) => {
  try {
    console.log("======== NEW REQUEST ========");
    console.log("BODY:", req.body);
    const answers = req.body;

    // 1️⃣ Fetch all data
    const { data: countries, error: cErr } = await supabase
      .from("countries").select("*");

    const { data: universities, error: uErr } = await supabase
      .from("universities").select("*");

    const { data: courses, error: coErr } = await supabase
      .from("courses").select("*");

    if (cErr) console.error("Countries fetch error:", cErr.message);
    if (uErr) console.error("Universities fetch error:", uErr.message);
    if (coErr) console.error("Courses fetch error:", coErr.message);

    if (!countries || !universities || !courses) {
      return res.status(500).json({ error: "Failed to fetch core data from the database." });
    }

    const { data: countryData } = await supabase
      .from("country_normalized")
      .select("*");

    const { data: rankingData } = await supabase
      .from("university_composite_ranking")
      .select("id, final_score");

    const rankingMap = {};
    if (rankingData) {
      rankingData.forEach(r => {
        rankingMap[r.id] = r.final_score;
      });
    }

    const countryMap = {};
    if (countryData) {
      countryData.forEach(c => {
        countryMap[c.id] = c;
      });
    }

    // 2️⃣ HARD COURSE ELIMINATION
    // Numerical tuition bounds from user selection
    const tuitionBounds = {
      "Less than $12k":   { min: 0,      max: 11999 },
      "$12k - $25k":      { min: 12000,  max: 25000 },
      "More than $25K":   { min: 25001,  max: 999999 }
    };
    const tuitionRange = tuitionBounds[answers.tuition_band] || { min: 0, max: 999999 };

    // Numerical duration bounds from user selection
    const durationBounds = {
      "1 year or less":    { min: 0,   max: 1 },
      "More than 1 year":  { min: 1,   max: 99 },
      "3 years or less":   { min: 0,   max: 3 },
      "More than 3 years": { min: 3,   max: 99 }
    };
    const durationRange = durationBounds[answers.duration] || { min: 0, max: 99 };

    const eligibleCourses = courses.filter(course => {
      // Degree level
      if (course.degree_level !== answers.level) return false;

      // Duration — numerical comparison
      if (course.duration_years == null) return false;
      if (course.duration_years < durationRange.min ||
          course.duration_years > durationRange.max) return false;

      // Tuition — numerical comparison
      if (course.tuition_usd == null) return false;
      if (course.tuition_usd < tuitionRange.min ||
          course.tuition_usd > tuitionRange.max) return false;

      // Field
      if (course.field_category !== answers.field) return false;

      // GRE/GMAT
      if (answers.gre_filter === "Without GRE or GMAT") {
        if (course.gre_required || course.gmat_required) return false;
      }
      if (answers.gre_filter === "Without GRE") {
        if (course.gre_required) return false;
      }
      if (answers.gre_filter === "Without GMAT") {
        if (course.gmat_required) return false;
      }

      return true;
    });

    console.log("Eligible courses count:", eligibleCourses.length);
    console.log("Total courses fetched:", courses ? courses.length : 0);
    console.log("Sample course:", courses ? JSON.stringify(courses[0]) : "none");
    console.log("Answers received:", JSON.stringify({
      level: answers.level,
      duration: answers.duration,
      tuition_band: answers.tuition_band,
      field: answers.field
    }));

    if (eligibleCourses.length === 0) {
      return res.json({
        empty: true,
        message: "No courses matched your exact filters.",
        suggestion: "Try adjusting your tuition band, duration, or GRE filter to see more results."
      });
    }

    // 3️⃣ MACRO WEIGHTS
    function computeMacroWeights(p1, p2, p3) {
      const base = { 1: 0.50, 2: 0.32, 3: 0.18 };

      const normalise = (val) => {
        if (!val) return null;
        const map = {
          "country": "Country",
          "course": "Course",
          "institution": "Institution"
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

    const weights = computeMacroWeights(answers.priority_1, answers.priority_2, answers.priority_3);

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

    const maxCost = Math.max(...countries.map(c => c.avg_cost_of_living_usd));
    const minCost = Math.min(...countries.map(c => c.avg_cost_of_living_usd));
    const maxWorkYears = Math.max(...countries.map(c => c.post_study_work_years));

    function normalizeCost(cost) {
      if (maxCost === minCost) return 1;
      return 1 - ((cost - minCost) / (maxCost - minCost));
    }

    function normalizeWorkYears(years) {
      if (maxWorkYears === 0) return 0;
      return years / maxWorkYears;
    }

    function normalizeRank(rank, maxRank) {
      if (!rank || !maxRank) return null;
      return 1 - ((rank - 1) / (maxRank - 1));
    }

    function computeCountryScore(country, answers, countryMap) {
      const c = countryMap[country.id];
      if (!c) {
        console.warn("Country not found in countryMap:", country.id, country.name);
        return 0;
      }

      let costWeight = 1;

      const costBandMap = {
        "$0 - $20K": 20000,
        "$20K - $30K": 25000,
        "More than $30K": 35000
      };
      const userCostMidpoint = costBandMap[answers.cost_of_living] || 25000;
      const maxCostRange = 35000;
      const costAlignmentScore = clamp(
        1 - (c.cost_score != null ? (1 - c.cost_score) : 0.5)
      );

      let pswWeight = answers.work_permit_importance === "Very strongly (3 years and above)" ? 1 :
                      answers.work_permit_importance.includes("Wouldn’t mind") ? 0.6 : 0.3;

      let prWeight = answers.pr_importance === "Very strongly" ? 1 :
                     answers.pr_importance === "Wouldn’t mind" ? 0.6 : 0.3;

      let govWeight = answers.gov_support_importance === "Very strongly" ? 1 :
                      answers.gov_support_importance === "Wouldn’t mind" ? 0.6 :
                      answers.gov_support_importance === "Don’t mind" ? 0.3 : 0.3;

      let englishWeight = answers.english_preference === "Yes" ? 1 :
                          answers.english_preference === "Prefer but flexible" ? 0.6 : 0.3;

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
        "Don’t care": 0.3
      };
      let internshipWeight = internshipWeightMap[answers.internship_importance] || 0;
      courseComponents.push(internshipWeight * (course.internship_available ? 1 : 0));
      courseWeights.push(internshipWeight);

      let scholarshipWeightMap = {
        "Very strongly (more than 20% of tuition)": 1,
        "Wouldn’t mind getting one (less than 20% of tuition or none)": 0.6,
        "Don’t care": 0.3
      };
      let scholarshipWeight = scholarshipWeightMap[answers.scholarship_importance] || 0;
      const scholarshipScore = course.scholarship_level != null
        ? course.scholarship_level
        : (course.scholarship_available ? 0.8 : 0.2);
      courseComponents.push(scholarshipWeight * scholarshipScore);
      courseWeights.push(scholarshipWeight);

      return clamp(
        courseComponents.reduce((a, b) => a + b, 0) /
        (courseWeights.reduce((a, b) => a + b, 0) || 1)
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
        "Not that much": 0.3
      };
      let careerWeight = careerWeightMap[answers.career_importance] || 0;

      let admissionWeightMap = {
        "Very strongly": 1,
        "Not that much": 0.6,
        "No": 0.3
      };
      let admissionWeight = admissionWeightMap[answers.admission_speed_importance] || 0;

      const compositeRanking = rankingMap[university.id] ?? 0.5;

      const admissionSpeedScore = university.admission_speed_score ?? 0.5;
      let admissionScore = admissionWeight * admissionSpeedScore;

      let rankingWeight = 0;
      if (answers.ranking_importance === "Only want to apply in top institutions") rankingWeight = 1;
      if (answers.ranking_importance === "Top and middle institutions are fine") rankingWeight = 0.7;
      if (answers.ranking_importance === "All institution irrespective of ranking") rankingWeight = 0.4;

      let rankingScore = rankingWeight * compositeRanking;

      const uniNumerator = locationScore + rankingScore + (careerWeight * careerScore) + admissionScore;
      const uniDenominator = 1 + rankingWeight + careerWeight + admissionWeight;
      return clamp(uniNumerator / (uniDenominator || 1));
    }

    // 4️⃣ SCORE PATHWAYS
    const pathways = eligibleCourses.map(course => {
      const university = universities.find(u => u.id === course.university_id);
      if (!university) return null;
      const country = countries.find(c => c.id === university.country_id);
      if (!country) return null;

      let countryScore = computeCountryScore(country, answers, countryMap);
      let courseScore = computeCourseScore(course, answers);
      let universityScore = computeUniversityScore(university, country, answers, rankingMap);

      // FINAL ADDITIVE SCORE
      let finalScore = computeFinalScore(weights, {
        country: countryScore,
        course: courseScore,
        university: universityScore
      });

      if (!isFinite(finalScore)) {
        finalScore = 0;
      }

      const explanation = [];

      if (countryScore >= 0.7) explanation.push("Strong country match based on your living and work preferences");
      else if (countryScore >= 0.4) explanation.push("Moderate country alignment with your preferences");

      if (answers.pr_importance === "Very strongly" && country.pr_pathway_clarity_score >= 0.7) {
        explanation.push("Strong permanent residency pathway available");
      }

      if (answers.english_preference === "Yes" && country.english_primary_language) {
        explanation.push("English-speaking country matches your preference");
      }

      if (answers.work_permit_importance.includes("Very strongly") && country.post_study_work_years >= 3) {
        explanation.push("Post-study work permit of 3+ years available");
      }

      if (course.internship_available && answers.internship_importance !== "Don’t care") {
        explanation.push("Includes internship as part of the curriculum");
      }

      if (courseScore >= 0.7) explanation.push("Strong course alignment with your academic preferences");
      else if (courseScore >= 0.4) explanation.push("Reasonable course fit based on your priorities");

      if (universityScore >= 0.7) explanation.push("Institution scores well on ranking, location, and services");
      else if (universityScore >= 0.4) explanation.push("Institution meets your core university preferences");

      if (answers.location_preference !== "Anywhere in the country" &&
          university.location_type === answers.location_preference) {
        explanation.push("Campus location matches your " + answers.location_preference.toLowerCase() + " preference");
      }

      if (explanation.length === 0) {
        explanation.push("Balanced match across country, course, and institution factors");
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
          university: Math.round(universityScore * 100) / 100
        },
        explanation
      };
    });

    // 5️⃣ Sort & Return Top 5
    const top5 = pathways
      .filter(p => p !== null)
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
        error: "university_id and program_url are required"
      });
    }

    console.log("Scraping:", program_url);

    // Fetch page
    const response = await axios.get(program_url, {
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)"
      },
      timeout: 15000
    });

    const html = response.data;

    if (!html || html.length < 1000) {
      return res.status(400).json({
        error: "Page content too small or invalid"
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
        parse_status: "pending"
      })
      .select();

    if (error) {
      console.error("Insert error:", error);
      return res.status(500).json({ error });
    }

    res.json({
      message: "Program page scraped successfully",
      inserted_id: data[0].id
    });

  } catch (err) {
    console.error("Scrape failed:", err.message);

    await supabase
      .schema("ingestion")
      .from("scrape_logs")
      .insert({
        university_id: req.body.university_id || null,
        status: "failed",
        error_message: err.message
      });

    res.status(500).json({
      error: "Scraping failed",
      details: err.message
    });
  }
});

// ----------------------
// PARSE SINGLE PROGRAM
// ----------------------

app.post("/parse-program", async (req, res) => {
  try {

    const { data: rawPages } = await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .select("*")
      .eq("parse_status", "pending")
      .limit(1);

    if (!rawPages || rawPages.length === 0) {
      return res.json({ message: "No pending pages" });
    }

    const raw = rawPages[0];

    const $ = cheerio.load(raw.raw_html);
    $("script, style, noscript").remove();
    const cleanText = $("body").text().replace(/\s+/g, " ").trim();
    const trimmedText = cleanText.substring(0, 8000);

    const prompt = `
Extract the following fields.

Return STRICT JSON only.

Fields:
- program_name
- degree_level (UG or PG)
Extract ALL of the following if present.

1. official_duration_value (numeric)
2. official_duration_unit ("months" or "years")
3. official_duration_text (exact text from page)
4. total_credits_required (numeric)
5. credit_system ("US", "UK", "ECTS", "AUS", etc.)
6. completion_time_value (numeric)
7. completion_time_unit ("months" or "years")

IMPORTANT:
- Official duration refers to advertised program length.
- Completion time refers to average time students take.
- If something is not clearly stated, return null.
- Do not guess.

- tuition_raw_text (exact fee text from the page, e.g. "$12,500 CAD" or "£9,250")
- field_category (must be exactly one of the following:
    engineering & tech,
    business, management and economics,
    science & applied science,
    medicine, health and life science,
    social science & humanities,
    arts, design & creative studies,
    law, public policy & governance,
    hospitality, tourism & service industry,
    education & teaching,
    agriculture, sustainability & environmental studies)
- internship_available (true or false)
- gre_required (true or false)
- gmat_required (true or false)
- scholarship_available (true or false)

Content:
${trimmedText}
`;

    const completion = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        { role: "system", content: "You extract structured academic program data. Return valid JSON only." },
        { role: "user", content: prompt }
      ],
      temperature: 0
    });

    let aiResponse = completion.choices[0].message.content
      .replace(/```json/g, "")
      .replace(/```/g, "")
      .trim();

    let parsed;

    try {
      parsed = JSON.parse(aiResponse);
    } catch (err) {
      await supabase
        .schema("ingestion")
        .from("raw_program_pages")
        .update({ parse_status: "failed" })
        .eq("id", raw.id);

      return res.status(400).json({ error: "Invalid JSON from AI" });
    }

    let country = {};
    try {
      const { data: university } = await supabase
        .schema("core")
        .from("universities")
        .select("*, countries(*)")
        .eq("id", raw.university_id)
        .single();

      if (university && university.countries) {
        country = university.countries;
      }
    } catch (e) {
      console.error("Country lookup skipped:", e.message);
    }

    // Duration normalization
    function convertToYears(value, unit) {
      if (!value || !unit) return null;

      let years = null;

      if (unit.toLowerCase() === "months") {
        years = value / 12;
      } else if (unit.toLowerCase() === "years") {
        years = value;
      }

      if (!years) return null;

      return Math.round(years * 100) / 100;
    }

    // Deterministic credit system override
    if (parsed.total_credits_required) {

      if (country.name === "Canada") {
        parsed.credit_system = "CAN";
      } else if (country.name === "United States") {
        parsed.credit_system = "US";
      } else if (country.name === "United Kingdom" || country.name === "UK") {
        parsed.credit_system = "UK";
      } else if (country.name === "Germany") {
        parsed.credit_system = "ECTS";
      } else if (country.name === "Australia") {
        parsed.credit_system = "AUS";
      }

    }

    let duration_years = null;
    let duration_confidence = "low";

    if (parsed.official_duration_value) {
      duration_years = convertToYears(
        parsed.official_duration_value,
        parsed.official_duration_unit
      );
      duration_confidence = "high";
    } else if (parsed.total_credits_required && country.credits_per_year) {
      const years =
        parsed.total_credits_required / country.credits_per_year;

      duration_years = Math.round(years * 100) / 100;
      duration_confidence = "high";
    } else if (parsed.completion_time_value) {
      duration_years = convertToYears(
        parsed.completion_time_value,
        parsed.completion_time_unit
      );
      duration_confidence = "medium";
    }

    if (!duration_years) {
      duration_confidence = "low";
    }

    // Tuition parsing from raw text
    const rawFeeText = parsed.tuition_raw_text;

    let tuition_amount = null;
    let tuition_currency = null;

    const match = rawFeeText
      ? rawFeeText.match(/([\$£€]|CAD|USD|AUD|GBP)?\s?([\d,]+(\.\d+)?)/i)
      : null;

    if (match) {
      tuition_amount = parseFloat(match[2].replace(/,/g, ""));

      if (rawFeeText.includes("CAD") || rawFeeText.includes("CA$")) tuition_currency = "CAD";
      else if (rawFeeText.includes("AUD")) tuition_currency = "AUD";
      else if (rawFeeText.includes("USD") || rawFeeText.includes("$")) tuition_currency = "USD";
      else if (rawFeeText.includes("£")) tuition_currency = "GBP";
      else if (rawFeeText.includes("€")) tuition_currency = "EUR";
    }

    // Currency normalization
    const exchangeRates = {
      CAD: 0.74,
      GBP: 1.27,
      AUD: 0.66,
      EUR: 1.08,
      USD: 1
    };

    const rate = exchangeRates[tuition_currency] || 1;
    const tuition_usd = tuition_amount ? Math.round(tuition_amount * rate * 100) / 100 : null;

    // Insert parsed data
    await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .insert({
        university_id: raw.university_id,
        program_name: parsed.program_name,
        degree_level: parsed.degree_level,
        duration_years,
        tuition_usd,
        tuition_raw_text: parsed.tuition_raw_text,
        tuition_amount,
        tuition_currency,
        official_duration_value: parsed.official_duration_value,
        official_duration_unit: parsed.official_duration_unit,
        official_duration_text: parsed.official_duration_text,
        total_credits_required: parsed.total_credits_required,
        credit_system: parsed.credit_system,
        completion_time_value: parsed.completion_time_value,
        completion_time_unit: parsed.completion_time_unit,
        duration_confidence,
        field_category: parsed.field_category,
        internship_available: parsed.internship_available,
        gre_required: parsed.gre_required,
        gmat_required: parsed.gmat_required,
        scholarship_available: parsed.scholarship_available,
        validation_status: "pending"
      });

    await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .update({ parse_status: "parsed" })
      .eq("id", raw.id);

    res.json({ message: "Program parsed successfully" });

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Parsing failed" });
  }
});

// ----------------------
// PARSE BATCH
// ----------------------

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

app.post("/parse-batch", async (req, res) => {
  const limit = req.body.limit || 10;

  res.json({ message: `Starting background parse of up to ${limit} programs` });

  (async () => {
    let success = 0;
    let failed = 0;

    for (let i = 0; i < limit; i++) {
      try {
        const response = await fetch("http://localhost:5000/parse-program", {
          method: "POST"
        });
        const result = await response.json();
        if (result.message === "No pending pages") break;
        success++;
      } catch (err) {
        console.error("Batch parse error:", err.message);
        failed++;
      }
      await delay(2000);
    }

    console.log(`Parse batch complete — success: ${success}, failed: ${failed}`);
  })();
});

// ==============================
// CRAWLER — DISCOVER PROGRAM URLS
// ==============================

app.post("/crawl-university", async (req, res) => {
  try {
    const { university_id, directory_url } = req.body;

    if (!university_id || !directory_url) {
      return res.status(400).json({ error: "university_id and directory_url are required" });
    }

    console.log("Crawling directory:", directory_url);

    const response = await axios.get(directory_url, {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; UNIFERBot/1.0)" },
      timeout: 20000
    });

    const $ = cheerio.load(response.data);
    const discovered = [];
    const seen = new Set();

    $("a[href]").each(function () {
      const href = $(this).attr("href");
      if (!href) return;

      let fullUrl;
      try {
        fullUrl = new URL(href, directory_url).toString();
      } catch (e) {
        return;
      }

      const isUBCProgram =
        fullUrl.includes("grad.ubc.ca/prospective-students/graduate-degree-programs/") &&
        fullUrl !== directory_url &&
        !fullUrl.includes("#") &&
        !seen.has(fullUrl);

      if (isUBCProgram) {
        seen.add(fullUrl);
        discovered.push({
          university_id,
          program_url: fullUrl,
          status: "pending"
        });
      }
    });

    console.log("Discovered URLs:", discovered.length);

    if (discovered.length === 0) {
      return res.json({ message: "No program URLs found", discovered: 0 });
    }

    const { data, error } = await supabase
      .schema("ingestion")
      .from("scrape_queue")
      .upsert(discovered, { onConflict: "program_url" })
      .select();

    if (error) {
      console.error("Queue insert error:", error);
      return res.status(500).json({ error });
    }

    res.json({
      message: "Crawl complete",
      discovered: discovered.length,
      queued: data ? data.length : 0
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

    const { data: queueItems, error: qErr } = await supabase
      .schema("ingestion")
      .from("scrape_queue")
      .select("*")
      .eq("status", "pending")
      .limit(limit);

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
          timeout: 15000
        });

        const html = scrapeResponse.data;

        if (!html || html.length < 500) {
          await supabase.schema("ingestion").from("scrape_queue")
            .update({ status: "failed", error_message: "Page too small" })
            .eq("id", item.id);
          results.failed++;
          continue;
        }

        await supabase
          .schema("ingestion")
          .from("raw_program_pages")
          .upsert({
            university_id: item.university_id,
            source_url: item.program_url,
            raw_html: html,
            parse_status: "pending"
          }, { onConflict: "source_url" });

        await supabase
          .schema("ingestion")
          .from("scrape_queue")
          .update({ status: "scraped", scraped_at: new Date().toISOString() })
          .eq("id", item.id);

        results.success++;
        await delay(1500);

      } catch (err) {
        console.error("Failed to scrape:", item.program_url, err.message);
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
      ...results
    });

  } catch (err) {
    console.error("Queue processing error:", err.message);
    res.status(500).json({ error: "Queue processing failed", details: err.message });
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
