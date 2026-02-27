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
            ranking_level: Math.random(),
            career_services_level: Math.random(),
            admission_speed_level: Math.random()
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
    const { data: countries } = await supabase.schema("core").from("countries").select("*");
    const { data: universities } = await supabase.schema("core").from("universities").select("*");
    const { data: courses } = await supabase.schema("core").from("courses").select("*");

    const { data: rankingSystems } = await supabase
      .from("ranking_systems")
      .select("*");

    const { data: universityRankings } = await supabase
      .schema("rankings").from("university_rankings")
      .select("*");

    const { data: countryData } = await supabase
      .from("country_normalized")
      .select("*");

    const { data: rankingData } = await supabase
      .from("university_composite_ranking")
      .select("id, final_score");

    const rankingMap = {};
    rankingData.forEach(r => {
      rankingMap[r.id] = r.final_score;
    });

    const countryMap = {};
    countryData.forEach(c => {
      countryMap[c.id] = c;
    });

    // 2️⃣ HARD COURSE ELIMINATION
    const eligibleCourses = courses.filter(course => {
      // Level
      if (course.level !== answers.level) return false;

      // Duration
      if (course.duration_category !== answers.duration) return false;

      // Tuition band
      if (course.tuition_band !== answers.tuition_band) return false;

      // Field
      if (course.field_category !== answers.field) return false;

      // GRE/GMAT filter
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

    if (eligibleCourses.length === 0) {
      return res.json({ message: "No eligible courses found." });
    }

    // 3️⃣ MACRO WEIGHTS
    function computeMacroWeights(p1, p2, p3) {
      const base = {
        1: 0.5,
        2: 0.32,
        3: 0.18
      };

      const weights = {};
      weights[p1] = base[1];
      weights[p2] = base[2];
      weights[p3] = base[3];

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
      if (!c) return 0;

      let costWeight = 1;
      let pswWeight = answers.work_permit_importance === "Very strongly (3 years and above)" ? 1 :
                      answers.work_permit_importance.includes("Wouldn’t mind") ? 0.6 : 0.3;

      let prWeight = answers.pr_importance === "Very strongly" ? 1 :
                     answers.pr_importance === "Wouldn’t mind" ? 0.6 : 0.3;

      let govWeight = answers.gov_support_importance === "Very strongly" ? 1 :
                      answers.gov_support_importance === "Wouldn’t mind" ? 0.6 : 0.3;

      let englishWeight = answers.english_preference === "Yes" ? 1 :
                          answers.english_preference === "Prefer but flexible" ? 0.6 : 0.3;

      let weightedSum =
        costWeight * c.cost_score +
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
      courseComponents.push(scholarshipWeight * course.scholarship_level);
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

      let rankingPenalty = compositeRanking * 0.2;
      // max 20% slowdown for top-ranked universities

      let admissionSpeedScore =
        country.admission_speed_baseline * (1 - rankingPenalty);

      let admissionScore = admissionWeight * admissionSpeedScore;

      let rankingWeight = 0;
      if (answers.ranking_importance === "Only want to apply in top institutions") rankingWeight = 1;
      if (answers.ranking_importance === "Top and middle institutions are fine") rankingWeight = 0.7;
      if (answers.ranking_importance === "All institution irrespective of ranking") rankingWeight = 0.4;

      let rankingScore = rankingWeight * compositeRanking;

      return clamp(
        (locationScore + rankingScore + careerWeight * careerScore + admissionScore) / 4
      );
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

      // COUNTRY EXPLANATION (relative logic)

      if (answers.pr_importance === "Very strongly") {
        explanation.push("Permanent residency opportunity aligns with your priority");
      }

      if (answers.work_permit_importance.includes("Very strongly")) {
        explanation.push("Post-study work duration supports your long-term plan");
      }

      if (answers.english_preference === "Yes" && country.english_first_language) {
        explanation.push("English-speaking country preference satisfied");
      }

      // COURSE EXPLANATION

      if (course.internship_available && answers.internship_importance !== "Don’t care") {
        explanation.push("Includes internship component as preferred");
      }

      if (answers.scholarship_importance.includes("Very strongly")) {
        explanation.push("Scholarship availability considered in scoring");
      }

      // UNIVERSITY EXPLANATION

      if (answers.ranking_importance.includes("top")) {
        explanation.push("Institution ranking aligned with your preference");
      }

      if (answers.career_importance !== "Not that much") {
        explanation.push("Career services strength influenced ranking");
      }

      if (answers.location_preference === university.location_type) {
        explanation.push("Campus location matches your preference");
      }

      // SAFETY FALLBACK
      if (explanation.length === 0) {
        explanation.push("Balanced match across country, course, and institution factors");
      }

      return {
        country: country.name,
        university: university.name,
        course: course.name,
        finalScore,   // 🔥 ADD THIS
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

    // Fetch one pending page
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

    // Clean HTML
    const $ = cheerio.load(raw.raw_html);
    $("script, style, noscript").remove();
    const cleanText = $("body").text().replace(/\s+/g, " ").trim();
    const trimmedText = cleanText.substring(0, 8000);

    const prompt = `
Extract the following fields from the content.

Return STRICT JSON only. No markdown. No explanation.

Fields:
- program_name
- degree_level (UG or PG)
- duration_years (numeric in years)
- tuition_usd (numeric in USD)
- field_category
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

    let aiResponse = completion.choices[0].message.content;

    // Clean markdown if present
    aiResponse = aiResponse
      .replace(/```json/g, "")
      .replace(/```/g, "")
      .trim();

    let parsed;

    try {
      parsed = JSON.parse(aiResponse);
    } catch (err) {
      console.error("JSON parse error:", err);

      await supabase
        .schema("ingestion")
        .from("raw_program_pages")
        .update({ parse_status: "failed" })
        .eq("id", raw.id);

      return res.status(400).json({ error: "Invalid JSON from AI" });
    }

    // Basic validation
    if (!parsed.program_name || !parsed.duration_years || !parsed.tuition_usd) {
      await supabase
        .schema("ingestion")
        .from("raw_program_pages")
        .update({ parse_status: "failed" })
        .eq("id", raw.id);

      return res.status(400).json({ error: "Missing critical fields" });
    }

    // Confidence score
    let confidence = 1;
    if (!parsed.field_category) confidence -= 0.1;
    if (!parsed.internship_available) confidence -= 0.05;
    if (!parsed.scholarship_available) confidence -= 0.05;
    confidence = Math.max(confidence, 0.6);

    // Insert parsed data
    await supabase
      .schema("ingestion")
      .from("parsed_programs")
      .insert({
        university_id: raw.university_id,
        ...parsed,
        validation_status: "pending"
      });

    // Update raw status
    await supabase
      .schema("ingestion")
      .from("raw_program_pages")
      .update({ parse_status: "parsed", confidence_score: confidence })
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
  const limit = req.body.limit || 5;

  for (let i = 0; i < limit; i++) {
    await fetch("http://localhost:5000/parse-program", {
      method: "POST"
    });
    await delay(2000);
  }

  res.json({ message: `${limit} programs processed` });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
