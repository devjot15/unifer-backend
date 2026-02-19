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
    const answers = req.body;

    // 1️⃣ Fetch all data
    const { data: countries } = await supabase.from("countries").select("*");
    const { data: universities } = await supabase.from("universities").select("*");
    const { data: courses } = await supabase.from("courses").select("*");

    // 2️⃣ HARD COURSE ELIMINATION
    const eligibleCourses = courses.filter(course => {
      if (course.level !== answers.level) return false;
      if (course.tuition_band !== answers.tuition_band) return false;
      if (course.field_category !== answers.field) return false;

      if (answers.gre_filter !== "No filter") {
        if (course.gre_required || course.gmat_required) return false;
      }

      if (answers.level === "UG" && course.duration_category !== answers.duration) return false;
      if (answers.level === "PG" && course.duration_category !== answers.duration) return false;

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

      // COUNTRY SCORE
      let countryScore = (
        country.work_permit_level +
        country.government_support_level +
        country.pr_opportunity_level
      ) / 3;

      // COURSE SCORE
      let internshipScore = course.internship_available ? 1 : 0;
      let scholarshipScore = course.scholarship_level;

      let courseScore = (internshipScore + scholarshipScore) / 2;

      // UNIVERSITY SCORE
      let universityScore = (
        university.ranking_level +
        university.career_services_level +
        university.admission_speed_level
      ) / 3;

      // FINAL ADDITIVE SCORE
      let finalScore =
        (weights.Country * countryScore) +
        (weights.Course * courseScore) +
        (weights.Institution * universityScore);

      return {
        country: country.name,
        university: university.name,
        course: course.name,
        finalScore
      };
    });

    // 5️⃣ Sort & Return Top 5
    const top5 = pathways
      .sort((a, b) => b.finalScore - a.finalScore)
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
