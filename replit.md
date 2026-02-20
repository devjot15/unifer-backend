# Study Abroad Recommendation Engine

## Overview

This is a **Study Abroad Recommendation Engine** — a web application that helps students find the best study abroad pathways (country + university + course combinations) based on their preferences. Users answer a questionnaire covering program level, duration, tuition budget, field of study, GRE/GMAT requirements, internship preferences, scholarships, and priority weightings (country vs. course vs. institution). The engine scores and ranks eligible pathways using a weighted scoring algorithm.

The app is built as a Node.js/Express backend with a static HTML/CSS/JS frontend served from the `public/` directory, using **Supabase** (hosted PostgreSQL) as the database.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend
- **Runtime:** Node.js with Express 5
- **Entry point:** `index.js` — single file containing all routes and logic
- **Static file serving:** Express serves the `public/` directory for the frontend
- **CORS:** Enabled globally via the `cors` middleware
- **Environment variables:** Managed with `dotenv` (`.env` file expected with `SUPABASE_URL` and `SUPABASE_KEY`)

### Frontend
- **Location:** `public/` directory, served as static files
- **Architecture:** Single-page HTML application (`public/index.html`) with inline CSS and JavaScript
- **No build step or framework** — plain HTML, CSS, and vanilla JS
- **UI:** Multi-step questionnaire form that collects user preferences and posts to the `/recommend` endpoint

### Database (Supabase / PostgreSQL)
- **Provider:** Supabase (external hosted PostgreSQL with REST API)
- **Connection:** Via `@supabase/supabase-js` client library using `SUPABASE_URL` and `SUPABASE_KEY` environment variables
- **Three main tables with foreign key relationships:**

  1. **`countries`** — Stores country-level attributes
     - `id` (UUID, primary key)
     - `name`, `cost_of_living_band`, `work_permit_level` (0-1 float), `english_first_language` (boolean), `government_support_level` (0-1 float), `pr_opportunity_level` (0-1 float)

  2. **`universities`** — Stores university-level attributes, linked to countries
     - `id` (UUID, primary key)
     - `name`, `country_id` (FK → countries), `location_type` ("Main city" | "Smaller cities"), `ranking_level` (0-1 float), `career_services_level` (0-1 float), `admission_speed_level` (0-1 float)

  3. **`courses`** — Stores course-level attributes, linked to universities
     - `id` (UUID, primary key)
     - `name`, `university_id` (FK → universities), `level` ("UG" | "PG"), `duration_category`, `internship_available` (boolean), `gre_required` (boolean), `gmat_required` (boolean), `scholarship_level` (0-1 float), `tuition_band`, `field_category`

- **Foreign key chain:** courses → universities → countries
- **Seeding:** The `/seed` GET endpoint clears all data and re-inserts 5 countries, 25 universities (5 per country), and 250 courses (10 per university)

### API Routes
- `GET /` — Health check
- `GET /countries` — Returns all countries from the database
- `GET /seed` — Clears and re-seeds the database with sample data
- `POST /recommend` — Core recommendation endpoint that:
  1. Fetches all countries, universities, and courses
  2. Applies hard filters (level, tuition band, field, GRE/GMAT, duration)
  3. Assigns macro weights based on user's priority ranking (50% / 32% / 18%)
  4. Scores each eligible course pathway (country score + course score + institution score)
  5. Returns ranked recommendations

### Recommendation Algorithm
- **Hard elimination:** Courses are filtered out if they don't match the user's required level, tuition band, field, duration, or GRE/GMAT preferences
- **Weighted scoring:** Three scoring dimensions (Country, Course, Institution) are each calculated as averages of their respective attribute levels (0-1 floats), then combined using user-specified priority weights (50%, 32%, 18%)
- **Country score:** Average of work_permit_level, government_support_level, pr_opportunity_level
- **Course score:** Factors include internship availability and scholarship level
- **Institution score:** Factors include ranking_level, career_services_level, admission_speed_level

## External Dependencies

### Services
- **Supabase** — PostgreSQL database and REST API. Requires `SUPABASE_URL` and `SUPABASE_KEY` environment variables to be set. The database schema (tables for countries, universities, courses) must be created in Supabase before the app can seed or query data.

### NPM Packages
- `express` (v5) — Web server framework
- `@supabase/supabase-js` (v2) — Supabase client for database operations
- `cors` — Cross-origin resource sharing middleware
- `dotenv` — Environment variable loading from `.env` file
- `@types/node` — Node.js type definitions (likely residual, not used for TypeScript)

### Environment Variables Required
| Variable | Description |
|---|---|
| `SUPABASE_URL` | The URL of your Supabase project |
| `SUPABASE_KEY` | The anon/service key for your Supabase project |