require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static("public"));

// Connect to Supabase
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

app.get("/", (req, res) => {
  res.send("Study Abroad Engine API running 🚀");
});

// Test route to fetch countries
app.get("/countries", async (req, res) => {
  const { data, error } = await supabase
    .from("countries")
    .select("*");

  if (error) {
    return res.status(500).json({ error });
  }

  res.json(data);
});

app.get("/seed", async (req, res) => {
  try {
    // Clear existing data (order matters due to foreign keys)
    await supabase.from("courses").delete().neq("id", "00000000-0000-0000-0000-000000000000");
    await supabase.from("universities").delete().neq("id", "00000000-0000-0000-0000-000000000000");
    await supabase.from("countries").delete().neq("id", "00000000-0000-0000-0000-000000000000");

    const countriesData = [
      { name: "Canada", cost_of_living_band: "20-30K", work_permit_level: 0.9, english_first_language: true, government_support_level: 0.8, pr_opportunity_level: 0.9 },
      { name: "Germany", cost_of_living_band: "0-20K", work_permit_level: 0.6, english_first_language: false, government_support_level: 0.9, pr_opportunity_level: 0.7 },
      { name: "Australia", cost_of_living_band: "30K+", work_permit_level: 0.8, english_first_language: true, government_support_level: 0.7, pr_opportunity_level: 0.6 },
      { name: "UK", cost_of_living_band: "30K+", work_permit_level: 0.7, english_first_language: true, government_support_level: 0.6, pr_opportunity_level: 0.5 },
      { name: "Ireland", cost_of_living_band: "20-30K", work_permit_level: 0.8, english_first_language: true, government_support_level: 0.7, pr_opportunity_level: 0.6 }
    ];

    const { data: countries, error: countriesError } = await supabase.from("countries").insert(countriesData).select();
    if (countriesError) throw countriesError;

    for (let country of countries) {
      for (let i = 1; i <= 5; i++) {
        const { data: university, error: uniError } = await supabase
          .from("universities")
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
          const { error: courseError } = await supabase.from("courses").insert({
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
    const { data: countries } = await supabase.from("countries").select("*");
    const { data: universities } = await supabase.from("universities").select("*");
    const { data: courses } = await supabase.from("courses").select("*");

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

    if (eligibleCourses.length === 0) {
      return res.json({ message: "No eligible courses found." });
    }

    // 3️⃣ MACRO WEIGHTS
    const weights = { Country: 0, Course: 0, Institution: 0 };

    weights[answers.priority_1] = 0.50;
    weights[answers.priority_2] = 0.32;
    weights[answers.priority_3] = 0.18;

    // 4️⃣ SCORE PATHWAYS
    const pathways = eligibleCourses.map(course => {
      const university = universities.find(u => u.id === course.university_id);
      const country = countries.find(c => c.id === university.country_id);

      // COUNTRY SCORE (real intensity logic)

      let countryComponents = [];
      let countryWeights = [];

      let costMatch = answers.cost_of_living === country.cost_of_living_band ? 1 : 0;
      countryComponents.push(costMatch);
      countryWeights.push(1);

      if (answers.english_preference === "Yes") {
        countryComponents.push(country.english_first_language ? 1 : 0);
        countryWeights.push(1);
      }
      if (answers.english_preference === "Prefer but flexible") {
        countryComponents.push(country.english_first_language ? 1 : 0.6);
        countryWeights.push(1);
      }

      let workWeightMap = {
        "Very strongly (3 years and above)": 1,
        "Wouldn’t mind (less than 3 years and more than 1 year)": 0.6,
        "Not really (1 year or less)": 0.3
      };
      let workWeight = workWeightMap[answers.work_permit_importance] || 0;
      countryComponents.push(workWeight * country.work_permit_level);
      countryWeights.push(workWeight);

      let govWeightMap = {
        "Very strongly": 1,
        "Wouldn’t mind": 0.6,
        "Don’t mind": 0.3
      };
      let govWeight = govWeightMap[answers.gov_support_importance] || 0;
      countryComponents.push(govWeight * country.government_support_level);
      countryWeights.push(govWeight);

      let prWeightMap = {
        "Very strongly": 1,
        "Wouldn’t mind": 0.6,
        "Don’t care": 0.3
      };
      let prWeight = prWeightMap[answers.pr_importance] || 0;
      countryComponents.push(prWeight * country.pr_opportunity_level);
      countryWeights.push(prWeight);

      let countryScore =
        countryComponents.reduce((a, b) => a + b, 0) /
        (countryWeights.reduce((a, b) => a + b, 0) || 1);

      // COURSE SCORE (real intensity logic)

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

      let courseScore =
        courseComponents.reduce((a, b) => a + b, 0) /
        (courseWeights.reduce((a, b) => a + b, 0) || 1);

      // UNIVERSITY SCORE (real ranking intensity)

      let locationScore = 1;
      if (answers.location_preference === "Main city") {
        locationScore = university.location_type === "Main city" ? 1 : 0;
      }
      if (answers.location_preference === "Smaller cities") {
        locationScore = university.location_type === "Smaller cities" ? 1 : 0;
      }

      let rankingWeight = 0;
      if (answers.ranking_importance === "Only want to apply in top institutions") rankingWeight = 1;
      if (answers.ranking_importance === "Top and middle institutions are fine") rankingWeight = 0.7;
      if (answers.ranking_importance === "All institution irrespective of ranking") rankingWeight = 0.4;

      let careerWeight = 0;
      if (answers.career_importance === "Very strongly (placement driven institutions)") careerWeight = 1;
      if (answers.career_importance === "Moderately (academics driven institutions)") careerWeight = 0.6;
      if (answers.career_importance === "Not that much") careerWeight = 0.3;

      let admissionWeight = 0;
      if (answers.admission_speed_importance === "Very strongly") admissionWeight = 1;
      if (answers.admission_speed_importance === "Not that much") admissionWeight = 0.6;
      if (answers.admission_speed_importance === "No") admissionWeight = 0.3;

      let admissionScore = admissionWeight * university.admission_speed_level;

      let universityScore =
        (locationScore +
         rankingWeight * university.ranking_level +
         careerWeight * university.career_services_level +
         admissionScore) / 4;

      // FINAL ADDITIVE SCORE
      let finalScore =
        (weights.Country * countryScore) +
        (weights.Course * courseScore) +
        (weights.Institution * universityScore);

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
      .sort((a, b) => (b.finalScore || 0) - (a.finalScore || 0))
      .slice(0, 5);

    res.json(top5);

  } catch (error) {
    console.error(error);
    res.status(500).send("Recommendation failed");
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
