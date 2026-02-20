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
- **Validation:** 
  - All fields are required before submission
  - Unique priorities (Country, Course, Institution) must be selected in decreasing order
  - Submit button is disabled until all required fields are filled
- **UX Features:**
  - Sticky progress bar tracking overall page scroll
  - Performance-optimized scroll tracking using `requestAnimationFrame`
  - Professional UNIFER branding with teal color scheme (#0F7C78)
  - Result highlighting for "Best Overall Match"
  - "Start Again" button to reset the search process

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
  4. Scoring dimensions: Country (50%), Course (32%), Institution (18%)
  5. Returns ranked recommendations

### Recent Changes
- **2026-02-20 (latest):**
  - Converted single-page form into a **4-step wizard** with Next/Back navigation buttons
  - Step 1: Define Your Priority, Step 2: Country Selection, Step 3: Course Selection, Step 4: University Selection
  - Progress bar now tracks step completion (25% per step) instead of scroll position
  - Per-step validation: users must complete all fields before advancing
  - Priority uniqueness validation on Step 1 (prevents duplicate priorities)
  - Submit button ("Run Decision Analysis") only appears on Step 4 and is gated to that step
  - Rewrote JavaScript to use string concatenation instead of template literals to avoid nested backtick issues in inline scripts
  - CSS: `.form-step` visibility toggling, `.nav-buttons` layout, styled `.prev-btn`, `.next-btn`, `.analyze-btn`
  - Stale validation messages auto-clear when user changes selections
- **2026-02-20 (earlier):**
  - Implemented strict dropdown pattern with `required` attribute and explicit `value` attributes
  - Added real-time form validation to enable/disable the submit button
  - Added a "Start Again" button and a professional footer with UNIFER branding
  - Refined CSS for disabled button states and invalid select placeholders
