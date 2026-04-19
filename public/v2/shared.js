/* ===== UNIFER v2 — app shell =====
   Hash-based routing between the quiz and results views.
   Global state lives on window.UNIFER. */

(function () {
  'use strict';

  // ----- Global state -----
  window.UNIFER = {
    answers: {},
    session_id: null,
    results: [],
    tweakCount: 0,
    eligiblePool: null,   // cached pool for the tweak loop (Stage 5)
    _currentView: null,
  };

  // Stage 3 additions — ML session tracking + form time
  window.UNIFER.sessionId = null;
  window.UNIFER.formStartTime = Date.now();
  window.UNIFER.lastCourseCount = null;  // populated by the quiz at submit time

  const ROOT = document.getElementById('app-root');

  // ----- sessionStorage persistence -----
  // Keep results + answers across a hard refresh so landing on #/results reuses
  // the student's last compute instead of falling back to mocks.
  const STORAGE_KEY = 'unifer_session_v1';
  const STORAGE_TTL_MS = 3600000;  // 1 hour

  function _persistResults(results) {
    try {
      const payload = {
        results: results,
        answers: window.UNIFER.answers || {},
        sessionId: window.UNIFER.sessionId || null,
        lastCourseCount: window.UNIFER.lastCourseCount || null,
        tweakCount: window.UNIFER.tweakCount || 0,
        timestamp: Date.now()
      };
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
    } catch (e) {
      console.log('[unifer] sessionStorage write failed', e);
    }
  }
  window.UNIFER._persistResults = _persistResults;

  function _hydrateFromStorage() {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      if (!raw) return false;
      const payload = JSON.parse(raw);
      if (!payload || Date.now() - (payload.timestamp || 0) > STORAGE_TTL_MS) {
        sessionStorage.removeItem(STORAGE_KEY);
        return false;
      }
      if (Array.isArray(payload.results) && payload.results.length > 0) {
        window.UNIFER.results = payload.results;
        window.UNIFER.eligiblePool = payload.results;
        window.UNIFER.answers = payload.answers || {};
        window.UNIFER.sessionId = payload.sessionId || null;
        window.UNIFER.lastCourseCount = payload.lastCourseCount || null;
        window.UNIFER.tweakCount = payload.tweakCount || 0;
        return true;
      }
    } catch (e) {
      console.log('[unifer] sessionStorage hydrate failed', e);
    }
    return false;
  }
  _hydrateFromStorage();

  // ----- View registry -----
  const VIEWS = {
    'quiz':    '/v2/quiz.html',
    'results': '/v2/results.html',
  };

  // ----- Routing -----
  function parseHash() {
    const h = (location.hash || '').replace(/^#\/?/, '');
    if (h === 'results') return 'results';
    return 'quiz';  // default
  }

  async function renderView(view) {
    if (window.UNIFER._currentView === view) return;
    window.UNIFER._currentView = view;

    // Show a small loading state during fetch
    ROOT.innerHTML = '<div class="boot-loading">Loading…</div>';

    try {
      const url = VIEWS[view];
      const resp = await fetch(url, { cache: 'no-store' });
      if (!resp.ok) throw new Error('Failed to load ' + url + ' — status ' + resp.status);
      const html = await resp.text();

      // Inject into the shell. The view file should be a self-contained fragment:
      // <style>...</style> <div>...</div> <script>...</script>
      ROOT.innerHTML = html;
      ROOT.classList.remove('view-swap');
      // Force reflow so the animation restarts on re-entry
      void ROOT.offsetWidth;
      ROOT.classList.add('view-swap');

      // Re-execute any <script> tags in the injected content
      // (innerHTML does NOT execute scripts by default)
      const scripts = ROOT.querySelectorAll('script');
      scripts.forEach((oldScript) => {
        const s = document.createElement('script');
        for (const attr of oldScript.attributes) s.setAttribute(attr.name, attr.value);
        s.text = oldScript.textContent;
        oldScript.parentNode.replaceChild(s, oldScript);
      });

      // Notify the view it has been mounted
      window.dispatchEvent(new CustomEvent('unifer:viewmounted', { detail: { view } }));
    } catch (err) {
      console.error('[unifer] renderView failed:', err);
      ROOT.innerHTML = '<div class="boot-loading">Something went wrong. Please refresh.</div>';
    }
  }

  // ----- Public navigate() -----
  window.UNIFER.navigate = function (view) {
    const target = view === 'results' ? '#/results' : '#/quiz';
    if (location.hash === target) {
      renderView(view);
    } else {
      location.hash = target;  // triggers hashchange → renderView
    }
  };

  // ----- ML session bootstrap -----
  async function ensureSession() {
    if (window.UNIFER.sessionId) return window.UNIFER.sessionId;
    try {
      const res = await fetch('/ml/session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const data = await res.json();
      window.UNIFER.sessionId = data.session_id || null;
    } catch (e) {
      console.log('[unifer] session init failed', e);
    }
    return window.UNIFER.sessionId;
  }

  // Create the ML session as soon as the quiz view first mounts.
  window.addEventListener('unifer:viewmounted', (e) => {
    if (e.detail && e.detail.view === 'quiz') ensureSession();
  });

  // ----- GPA conversion -----
  function computeGpaPercentage(a) {
    // If the quiz already stored a profile_gpa_percentage (computed during input), use it
    if (a.profile_gpa_percentage) return a.profile_gpa_percentage;

    // Otherwise compute from profile_gpa_numeric + profile_grading_system
    const raw = parseFloat(a.profile_gpa_numeric);
    if (isNaN(raw)) return '';
    const sys = a.profile_grading_system || 'INDIA_PCT';
    let pct;
    if (sys === 'INDIA_PCT') pct = raw;
    else if (sys === 'INDIA_CGPA_10') pct = raw * 10;
    else if (sys === 'INDIA_CGPA_4') pct = (raw / 4) * 100;
    else return '';
    return pct.toFixed(1);
  }

  // ----- /recommend response → UI shape -----
  function transformRecommendResponse(rawArray) {
    if (!Array.isArray(rawArray)) return [];

    return rawArray.map((item, idx) => {
      // Real backend fields:
      //   course, university, country, duration, tuition_usd,
      //   scores.{country,course,university}, explanation[]
      // UI-expected fields:
      //   id, name, country, city, course, duration, tuition,
      //   scores.{country,course,institution} (0-100 ints),
      //   confidence, why, chips, dims, rankings, subject, stats, employ, cost

      const scores = item.scores || {};
      return {
        id: 'u' + (idx + 1),
        name: item.university || 'Unknown',
        country: item.country || '',
        city: item.city || '',  // backend may or may not provide
        course: item.course || '',
        duration: typeof item.duration === 'number'
          ? (item.duration === 1 ? '1 year' : item.duration + ' years')
          : (item.duration || ''),
        tuition: item.tuition_usd || null,

        // Backend sends scores as 0-1 floats; UI expects 0-100 ints. Map "university" → "institution".
        scores: {
          country: scores.country !== null && scores.country !== undefined ? Math.round(scores.country * 100) : null,
          course: scores.course !== null && scores.course !== undefined ? Math.round(scores.course * 100) : null,
          institution: scores.university !== null && scores.university !== undefined ? Math.round(scores.university * 100) : null,
        },

        // "Why this aligns" — backend sends an array of strings; UI shows a paragraph.
        // Join them into a single string. UI can format as bullet list internally too.
        why: Array.isArray(item.explanation) ? item.explanation.join(' ') : (item.explanation || ''),
        whyList: Array.isArray(item.explanation) ? item.explanation : [],

        // Confidence: backend doesn't currently return this. Default to true for top 3, false otherwise (placeholder).
        confidence: idx < 3,

        // Tappable chips for the tweak loop. Use the user's actual answers as the source so chips reflect what they picked.
        chips: buildChipsFromAnswers(window.UNIFER.answers, item),

        // The following fields aren't returned by the backend yet — Stage 6 may surface real data.
        rankings: item.rankings || null,
        subject: item.subject || null,
        stats: item.stats || null,
        employ: item.employ || null,
        cost: item.cost || null,
        dims: item.dims || null,

        // Preserve the scoring components so the frontend can re-rank locally
        // when the student tweaks soft filters (see window.UNIFER.rerank).
        _components: item._components || null,
      };
    });
  }

  const CANONICAL_FIELDS = [
    { label: 'Engineering & Technology',         value: 'engineering & technology' },
    { label: 'Computer Science & Data',          value: 'computer science & data technology' },
    { label: 'Natural Sciences',                 value: 'natural sciences' },
    { label: 'Life Sciences & Biotechnology',    value: 'life sciences & biotechnology' },
    { label: 'Medicine & Clinical Health',       value: 'medicine & clinical health' },
    { label: 'Public Health & Allied Health',    value: 'public health & allied health' },
    { label: 'Business & Management',            value: 'business & management' },
    { label: 'Economics, Finance & Accounting',  value: 'economics, finance & accounting' },
    { label: 'Social Sciences',                  value: 'social sciences' },
    { label: 'Humanities & Languages',           value: 'humanities & languages' },
    { label: 'Arts & Design',                    value: 'arts, design & creative studies' },
    { label: 'Law, Politics & Governance',       value: 'law, politics & governance' },
    { label: 'Education & Teaching',             value: 'education & teaching' },
    { label: 'Environment & Sustainability',     value: 'environment, sustainability & agriculture' },
    { label: 'Hospitality & Tourism',            value: 'hospitality, tourism & service industry' }
  ];

  function fieldValueToLabel(value) {
    const match = CANONICAL_FIELDS.find(f => f.value === value);
    return match ? match.label : value;
  }

  window.UNIFER.CANONICAL_FIELDS = CANONICAL_FIELDS;
  window.UNIFER.fieldValueToLabel = fieldValueToLabel;
  window.UNIFER.fieldLabelToValue = function (label) {
    const match = CANONICAL_FIELDS.find(f => f.label === label);
    return match ? match.value : label;
  };

  function buildChipsFromAnswers(answers, item) {
    // Build 3-4 chips from the student's actual quiz answers — these are the inputs they can tweak.
    // Each chip: { k: 'shortKey', v: 'displayValue', q: 'modal question', opts: [...] }
    const a = answers || {};
    const chips = [];

    // Field
    if (a.field) {
      chips.push({
        k: 'field',
        v: fieldValueToLabel(a.field),
        q: 'Change your field?',
        opts: CANONICAL_FIELDS.map(f => f.label)
      });
    }

    // Budget
    if (a.tuition_band) {
      const budgetLabels = {
        'Up to $5k': 'up to $5k', 'Up to $15k': 'up to $15k',
        'Up to $30k': 'up to $30k', 'Up to $50k': 'up to $50k',
        'No limit': 'no upper limit'
      };
      chips.push({
        k: 'budget',
        v: budgetLabels[a.tuition_band] || a.tuition_band,
        q: 'Change your budget?',
        opts: ['Up to $5k', 'Up to $15k', 'Up to $30k', 'Up to $50k', 'No upper limit']
      });
    }

    // Research vs industry
    if (a.research_importance) {
      const focusLabels = { high: 'Research-focused', medium: 'Balanced', low: 'Industry-focused' };
      chips.push({
        k: 'focus',
        v: focusLabels[a.research_importance] || 'Balanced',
        q: 'Change your focus?',
        opts: ['Research-focused', 'Balanced', 'Industry-focused']
      });
    }

    // Ranking importance
    if (a.ranking_importance) {
      const rankLabels = { '0.75': 'Top institutions only', '0.50': 'Top + middle', '0.25': 'All institutions' };
      chips.push({
        k: 'tier',
        v: rankLabels[a.ranking_importance] || 'Top + middle',
        q: 'How picky on institution tier?',
        opts: ['Top institutions only', 'Top and middle are fine', 'All institutions']
      });
    }

    return chips;
  }

  function titleCaseFromValue(v) {
    // Convert "computer science & data technology" → "Computer Science & Data Technology"
    return String(v).replace(/\b\w/g, c => c.toUpperCase());
  }

  // ----- Local re-rank: recomputes finalScore per eligible pool item using fresh answers -----
  // Mirrors the backend's blend: university = alpha*composite + beta*blendedSub, then admit-modulated;
  // finalScore = w_country*countryScore + w_course*courseScore + w_inst*uniScore.
  // Only soft-filter answers affect this math (dimension importance, ranking_importance,
  // subject_ranking_importance, priorities). Hard filters would shrink the eligible pool and
  // require a real /recommend call — see Stage 5B-3.
  window.UNIFER.rerank = function (newAnswers) {
    const pool = window.UNIFER.eligiblePool;
    if (!pool || !pool.length) return [];

    const reranked = pool.map(item => {
      const c = item._components;
      if (!c) {
        // No components — keep item with existing finalScore. Cannot re-rank.
        return Object.assign({}, item, { finalScore: item.finalScore || 0 });
      }

      // Recompute alpha and delta from new answers
      const alpha = parseFloat(newAnswers.ranking_importance);
      const alphaEff = isNaN(alpha) ? (c.alpha || 0) : alpha;
      const beta = 1 - alphaEff;
      const delta = ({ high: 0.60, medium: 0.35, low: 0.10 })[newAnswers.subject_ranking_importance] || c.delta || 0.10;

      // Recompute subScore from the per-dimension breakdown using new importance answers
      let newSubScore = c.subScore;
      if (c.subScoreBreakdown) {
        const DIM_WEIGHT = { high: 0.25, medium: 0.15, low: 0.05 };
        const dimImportance = {
          employability: newAnswers.career_importance || 'low',
          teaching: newAnswers.teaching_importance || 'low',
          research: newAnswers.research_importance || 'low',
          student_experience: newAnswers.teaching_importance || 'low', // derived from teaching
          international: newAnswers.international_importance || 'low',
          selectivity: newAnswers.selectivity_importance || 'low',
          prestige: newAnswers.prestige_importance || 'low',
        };
        let num = 0, den = 0;
        for (const [dim, score] of Object.entries(c.subScoreBreakdown)) {
          if (score == null) continue;
          const importance = dimImportance[dim] || 'low';
          const w = DIM_WEIGHT[importance] || DIM_WEIGHT.low;
          num += w * score;
          den += w;
        }
        if (den > 0) newSubScore = num / den;
      }

      // Recompute blended sub-score with new delta
      const newBlendedSub = (c.subjectSubScore !== null && c.subjectSubScore !== undefined)
        ? (1 - delta) * newSubScore + delta * c.subjectSubScore
        : newSubScore;

      // Recompute universityScore with new alpha/beta
      const newUniRaw = (c.compositeScore !== null && c.compositeScore !== undefined)
        ? alphaEff * c.compositeScore + beta * newBlendedSub
        : 0.70 * newBlendedSub;

      // Re-apply admit modulation if it was applied originally
      const newUniModulated = (c.pAdmit !== null && c.pAdmit !== undefined)
        ? newUniRaw * (0.25 + 0.75 * c.pAdmit)
        : newUniRaw;

      // Recompute macro weights from new priority order
      const newWeights = computeMacroWeightsFromAnswers(newAnswers, c.weights);

      // Recompute finalScore
      const cs = c.countryScoreRaw;  // null when country pre-selected
      const cos = c.courseScoreRaw;
      let newFinal;
      if (cs === null || cs === undefined) {
        // 2-entity mode: only course + institution
        const total = newWeights.course + newWeights.institution || 1;
        newFinal = (newWeights.course * cos + newWeights.institution * newUniModulated) / total;
      } else {
        newFinal = newWeights.country * cs + newWeights.course * cos + newWeights.institution * newUniModulated;
      }

      return Object.assign({}, item, {
        finalScore: newFinal,
        scores: {
          country: cs != null ? Math.round(cs * 100) : null,
          course: Math.round(cos * 100),
          institution: Math.round(newUniModulated * 100),
        },
      });
    });

    // Sort by new finalScore, take top 5
    reranked.sort((a, b) => (b.finalScore || 0) - (a.finalScore || 0));
    return reranked.slice(0, 5);
  };

  // Helper: recompute macro weights from priorities (matches backend computeMacroWeights)
  function computeMacroWeightsFromAnswers(answers, fallback) {
    // Stage 6A-fix-4: prefer typed array, fall back to individual fields.
    const arr = Array.isArray(answers.priorities) ? answers.priorities : null;
    const p1 = arr ? arr[0] : (answers.priorities_1 || answers.priority_1);
    const p2 = arr ? arr[1] : (answers.priorities_2 || answers.priority_2);
    const p3 = arr ? (arr[2] || '') : (answers.priorities_3 || answers.priority_3);

    // 2-entity mode (country pre-selected, no priority_3)
    const is2Entity = !p3 || p3 === '';

    const baseTriple = { 1: 0.50, 2: 0.32, 3: 0.18 };
    const baseDouble = { 1: 0.50 / 0.82, 2: 0.32 / 0.82 };  // ≈ { 1: 0.6098, 2: 0.3902 }
    const base = is2Entity ? baseDouble : baseTriple;

    const weights = { country: 0, course: 0, institution: 0 };
    const map = { Country: 'country', Course: 'course', Institution: 'institution' };

    [p1, p2, p3].forEach((p, i) => {
      if (!p) return;
      const key = map[p];
      if (!key) return;
      weights[key] = base[i + 1] || 0;
    });

    // Fallback to provided weights if computation produced all zeros
    const sum = weights.country + weights.course + weights.institution;
    if (sum === 0 && fallback) return fallback;

    return weights;
  }

  // ----- Computing-state overlay -----
  function showComputingLoader() {
    const existing = document.getElementById('unifer-computing-overlay');
    if (existing) existing.remove();
    const root = document.getElementById('app-root');
    if (root) root.classList.add('unifer-computing-blur');

    const overlay = document.createElement('div');
    overlay.id = 'unifer-computing-overlay';
    overlay.style.cssText = 'position:fixed; inset:0; background:rgba(255,255,255,0.94); display:flex; flex-direction:column; align-items:center; justify-content:center; z-index:1000; gap:20px; font-family:var(--font);';

    const count = window.UNIFER.lastCourseCount;
    const headline = count
      ? `Ranking your top matches from ${count.toLocaleString()} courses.`
      : 'Ranking your top matches.';
    const subText = count
      ? `Cross-referencing rankings, acceptance data, and your stated priorities across ${count.toLocaleString()} eligible courses.`
      : 'Cross-referencing rankings, acceptance data, and your stated priorities.';

    overlay.innerHTML = `
      <div style="width:40px; height:40px; border-radius:50%; border:3px solid var(--line); border-top-color:var(--teal); animation:unifer-spin 0.9s linear infinite;"></div>
      <div style="font-size:18px; color:var(--ink); font-weight:600; max-width:380px; text-align:center; padding:0 24px;">${headline}</div>
      <div style="font-size:13px; color:var(--ink-3); max-width:420px; text-align:center; line-height:1.5; padding:0 24px;">${subText}</div>
      <style>@keyframes unifer-spin { to { transform: rotate(360deg); } }</style>
    `;
    document.body.appendChild(overlay);

    // Stage 6B: if the loader is still spinning (no error shown) after 20s, treat as stuck.
    setTimeout(() => {
      const o = document.getElementById('unifer-computing-overlay');
      if (o && !o.querySelector('.submit-error')) {
        showSubmitError('This is taking longer than expected. Your quiz is saved — try again.');
      }
    }, 20000);
  }

  // Stage 6B: retry UI rendered inside the computing overlay on /recommend failure or timeout.
  function showSubmitError(errorMsg) {
    let overlay = document.getElementById('unifer-computing-overlay');
    if (!overlay) {
      const root = document.getElementById('app-root');
      if (root) root.classList.add('unifer-computing-blur');
      overlay = document.createElement('div');
      overlay.id = 'unifer-computing-overlay';
      overlay.style.cssText = 'position:fixed; inset:0; background:rgba(255,255,255,0.94); display:flex; flex-direction:column; align-items:center; justify-content:center; z-index:1000; gap:20px; font-family:var(--font);';
      document.body.appendChild(overlay);
    }

    overlay.innerHTML = `
      <div class="submit-error">
        <div class="submit-error-icon">!</div>
        <div class="submit-error-title">Something went wrong</div>
        <div class="submit-error-sub">${errorMsg || 'We could not reach the ranking engine.'}</div>
        <button type="button" class="submit-error-retry" id="submitRetryBtn">Try again</button>
        <button type="button" class="submit-error-back" id="submitBackBtn">Back to quiz</button>
      </div>
      <style>
        .submit-error { display:flex; flex-direction:column; align-items:center; gap:14px; max-width:380px; text-align:center; font-family:var(--font); padding:0 24px; }
        .submit-error-icon { width:44px; height:44px; border-radius:50%; background:var(--amber-soft, #fff7e6); color:var(--amber-ink, #7a4a00); display:flex; align-items:center; justify-content:center; font-size:22px; font-weight:700; }
        .submit-error-title { font-size:18px; font-weight:600; color:var(--ink); }
        .submit-error-sub { font-size:14px; color:var(--ink-2); line-height:1.5; }
        .submit-error-retry, .submit-error-back { padding:10px 20px; border-radius:999px; font-size:14px; cursor:pointer; }
        .submit-error-retry { background:var(--teal); color:white; border:none; }
        .submit-error-back { background:transparent; color:var(--ink-3); border:1px solid var(--line); }
      </style>
    `;

    const retryBtn = document.getElementById('submitRetryBtn');
    const backBtn = document.getElementById('submitBackBtn');
    if (retryBtn) {
      retryBtn.addEventListener('click', () => {
        window.UNIFER.hideComputingLoader();
        showComputingLoader();
        window.UNIFER.submitQuiz();
      });
    }
    if (backBtn) {
      backBtn.addEventListener('click', () => {
        window.UNIFER.hideComputingLoader();
        window.UNIFER.navigate('quiz');
      });
    }
  }
  window.UNIFER.showSubmitError = showSubmitError;

  // Public cleanup so other callers (submitQuiz, preview returns, etc.) can force-clear
  // the blur + overlay without depending on the viewmounted event alone.
  // Strips the blur class from EVERY element that carries it — previous versions
  // only targeted #app-root and missed blur set on .matches-wrap by showRequeryLoader.
  window.UNIFER.hideComputingLoader = function () {
    document.querySelectorAll('.unifer-computing-blur').forEach(el => {
      el.classList.remove('unifer-computing-blur');
      el.style.filter = '';
      el.style.backdropFilter = '';
    });
    const o = document.getElementById('unifer-computing-overlay');
    if (o) o.remove();
    // Also clear any stuck requery pill that outlived its request.
    const pill = document.getElementById('requery-loader');
    if (pill) pill.remove();
  };

  // ----- Build /recommend payload from a UNIFER.answers-shaped object -----
  function buildRecommendPayload(a) {
    a = a || {};

    // Stage 6A-fix-4: prefer the typed `priorities` array when present, fall
    // back to individual priority_1/2/3 fields for stale sessions and v1.
    const prioritiesArr = Array.isArray(a.priorities) ? a.priorities : null;
    const priority_1 = prioritiesArr ? (prioritiesArr[0] || '') : (a.priorities_1 || '');
    const priority_2 = prioritiesArr ? (prioritiesArr[1] || '') : (a.priorities_2 || '');
    const priority_3 = prioritiesArr ? (prioritiesArr[2] || '') : (a.priorities_3 || '');

    return {
      priorities: prioritiesArr ? prioritiesArr.slice() : undefined,
      priority_1: priority_1,
      priority_2: priority_2,
      priority_3: priority_3,

      work_permit_importance: a.work_permit_importance || '',
      english_preference: a.english_preference || '',
      pr_importance: a.pr_importance || '',

      level: a.level || '',
      duration: a.duration || '',
      tuition_band: a.tuition_band || '',
      field: a.field || '',
      sub_field: a.sub_field || '',
      internship_importance: a.internship_importance || '',
      scholarship_importance: a.scholarship_importance || '',

      ranking_importance: a.ranking_importance || '',
      career_importance: a.career_importance || '',
      career_type: a.career_type || '',
      teaching_importance: a.teaching_importance || '',
      research_importance: a.research_importance || '',
      student_experience_importance: '',  // backend derives this from teaching_importance
      international_importance: a.international_importance || '',
      selectivity_importance: a.selectivity_importance || '',
      prestige_importance: a.prestige_importance || '',
      subject_ranking_importance: a.subject_ranking_importance || '',

      profile_degree_completed: a.profile_degree_completed || '',
      profile_gpa_percentage: computeGpaPercentage(a) || '',
      profile_backlogs: a.profile_backlogs || '',
      profile_english_test: a.profile_english_test || '',
      profile_english_scores: a.profile_english_scores ? JSON.stringify(a.profile_english_scores) : '',
      profile_exam_type: a.profile_exam_type || '',
      profile_gre_scores: (a.profile_exam_type === 'GRE' && a.profile_exam_scores) ? JSON.stringify(a.profile_exam_scores) : '',
      profile_gmat_scores: (a.profile_exam_type === 'GMAT' && a.profile_exam_scores) ? JSON.stringify(a.profile_exam_scores) : '',
      profile_work_experience: a.profile_work_experience || '',
      profile_grading_system: a.profile_grading_system || 'INDIA_PCT',
      profile_institution_id: a.profile_institution_id || '',
      profile_institution_tier: a.profile_institution_tier || '',
      profile_institution_name: a.profile_institution_name || '',
      profile_institution_anabin: a.profile_institution_anabin || '',

      selected_country: (a.country_decided === 'Yes' && a.selected_country) ? a.selected_country : null
    };
  }

  // ----- Re-run /recommend with current (or overridden) answers -----
  // Called by hard-filter chip taps, what-if previews, and scenarios sliders.
  // Unlike submitQuiz, does NOT fire /ml/save-profile, does NOT show the
  // computing loader, and does NOT navigate. Returns the transformed top-5.
  //
  // Options:
  //   transient: true  → do not mutate window.UNIFER.results/eligiblePool; just return the data
  //                      (used by the scenarios tab for pure exploration)
  //
  // In-flight requests are superseded by newer calls via AbortController so we
  // never render stale results from a request that loses its race.
  let _activeRecommendController = null;

  window.UNIFER.requeryRecommend = async function (overrideAnswers, options) {
    options = options || {};
    // Cancel any pending /recommend request before starting a new one.
    if (_activeRecommendController) _activeRecommendController.abort();
    const controller = new AbortController();
    _activeRecommendController = controller;
    const signal = controller.signal;

    const a = overrideAnswers || window.UNIFER.answers || {};
    const recommendPayload = buildRecommendPayload(a);

    try {
      const res = await fetch('/recommend', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(recommendPayload),
        signal: signal
      });

      if (!res.ok) {
        // Stage 6A-fix-3: log the error body so we can see WHAT the backend rejected.
        let errBody = '';
        try { errBody = await res.text(); } catch(e) {}
        console.error('[unifer] /recommend failed:', res.status, errBody);
        if (!options.transient) {
          window.UNIFER.recommendError = `Server error ${res.status}`;
        }
        return [];
      }

      const data = await res.json();
      if (signal.aborted) return [];

      if (Array.isArray(data)) {
        const transformed = transformRecommendResponse(data);
        if (!options.transient) {
          window.UNIFER.results = transformed;
          window.UNIFER.eligiblePool = transformed;
          window.UNIFER.recommendError = null;
          _persistResults(transformed);
        }
        return transformed;
      } else {
        if (!options.transient) {
          window.UNIFER.recommendError = data && (data.message || 'No matches found');
        }
        return [];
      }
    } catch (err) {
      if (err && err.name === 'AbortError') {
        console.log('[unifer] /recommend request superseded');
        return [];
      }
      console.error('[unifer] requeryRecommend failed', err);
      if (!options.transient) window.UNIFER.recommendError = 'Network error';
      return [];
    } finally {
      if (_activeRecommendController === controller) _activeRecommendController = null;
    }
  };

  // ----- Submit: call /recommend + fire-and-forget /ml/save-profile -----
  window.UNIFER.submitQuiz = async function () {
    const a = window.UNIFER.answers || {};
    const sessionId = window.UNIFER.sessionId;

    // ─── Construct /recommend payload — matches production exactly ───
    const recommendPayload = buildRecommendPayload(a);

    // ─── Fire /ml/save-profile in parallel (non-blocking) ───
    if (sessionId) {
      const profilePayload = Object.assign({}, recommendPayload, {
        session_id: sessionId,
        degree_completed: recommendPayload.profile_degree_completed,
        gpa_percentage: recommendPayload.profile_gpa_percentage,
        backlogs: recommendPayload.profile_backlogs,
        english_test: recommendPayload.profile_english_test,
        english_scores: recommendPayload.profile_english_scores,
        gre_scores: recommendPayload.profile_gre_scores,
        gmat_scores: recommendPayload.profile_gmat_scores,
        work_experience: recommendPayload.profile_work_experience,
        total_form_time_seconds: (Date.now() - window.UNIFER.formStartTime) / 1000,
        completed: true
      });
      fetch('/ml/save-profile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(profilePayload)
      }).catch(() => {});
    }

    // ─── Show loading state ───
    showComputingLoader();

    // ─── Call /recommend and wait ───
    try {
      const res = await fetch('/recommend', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(recommendPayload)
      });
      if (!res.ok) {
        showSubmitError(`Server responded ${res.status}. Your quiz is saved — try again.`);
        return;
      }
      const data = await res.json();

      if (!Array.isArray(data)) {
        // Backend returned an error-shaped object or an explicit empty payload.
        if (data && data.empty) {
          window.UNIFER.results = [];
          window.UNIFER.recommendError = null;
          _persistResults([]);
        } else {
          showSubmitError((data && data.message) || 'No matches returned.');
          return;
        }
      } else {
        const transformed = transformRecommendResponse(data);
        window.UNIFER.results = transformed;
        window.UNIFER.eligiblePool = transformed;  // cache for Stage 5 tweak loop
        window.UNIFER.recommendError = null;
        _persistResults(transformed);
      }
    } catch (err) {
      console.error('[unifer] /recommend failed', err);
      showSubmitError('Network error. Check your connection and retry.');
      return;
    }

    // ─── Navigate to results ───
    window.UNIFER.navigate('results');
    // Wait for the results view to mount its initial cards, then unblur so the
    // transition reads as "computing → sharpen new". If the viewmounted listener
    // below already cleaned up, this is a harmless no-op.
    setTimeout(() => window.UNIFER.hideComputingLoader(), 600);
  };

  // ----- Init -----
  window.addEventListener('hashchange', () => renderView(parseHash()));
  document.addEventListener('DOMContentLoaded', () => renderView(parseHash()));
  if (document.readyState === 'interactive' || document.readyState === 'complete') {
    renderView(parseHash());
  }

  // Safety net: every time a view mounts, schedule a loader/blur cleanup so a
  // stuck computing overlay can never persist across navigation.
  window.addEventListener('unifer:viewmounted', () => {
    setTimeout(() => {
      if (window.UNIFER && typeof window.UNIFER.hideComputingLoader === 'function') {
        window.UNIFER.hideComputingLoader();
      }
    }, 700);
  });

  window.UNIFER.fetchEligibleCount = async function (partialAnswers) {
    const a = partialAnswers || window.UNIFER.answers || {};
    const payload = {};
    if (a.level) payload.level = a.level;
    if (a.field) payload.field = a.field;
    if (a.sub_field) payload.sub_field = a.sub_field;
    if (a.program_type_preference) payload.program_type_preference = a.program_type_preference;
    if (a.tuition_band) payload.tuition_band = a.tuition_band;
    if (a.duration) payload.duration = a.duration;
    if (a.country_decided === 'Yes' && a.selected_country) {
      payload.selected_country = a.selected_country;
    }
    const gpaPct = computeGpaPercentage ? computeGpaPercentage(a) : a.profile_gpa_percentage;
    if (gpaPct) payload.profile_gpa_percentage = String(gpaPct);
    if (a.profile_backlogs) payload.profile_backlogs = String(a.profile_backlogs);

    try {
      const res = await fetch('/eligible-count', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) return null;
      const data = await res.json();
      return typeof data.count === 'number' ? data.count : null;
    } catch (err) {
      console.log('[unifer] eligible-count fetch failed', err);
      return null;
    }
  };

  /* ═══════════════════════════════════════════════════════
     Stage 6C — PDF export of results page
     Lazy-loads html2canvas + jsPDF on first use.
     ═══════════════════════════════════════════════════════ */

  let _pdfLibsPromise = null;
  function _loadPdfLibs() {
    if (_pdfLibsPromise) return _pdfLibsPromise;
    _pdfLibsPromise = new Promise((resolve, reject) => {
      // html-to-image supports oklch() and other modern CSS color functions.
      // It exposes itself as window.htmlToImage (UMD build).
      const s1 = document.createElement('script');
      s1.src = 'https://cdn.jsdelivr.net/npm/html-to-image@1.11.11/dist/html-to-image.min.js';
      s1.onload = () => {
        const s2 = document.createElement('script');
        s2.src = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
        s2.onload = () => {
          if (window.htmlToImage && (window.jspdf || window.jsPDF)) resolve();
          else reject(new Error('PDF libs failed to initialise'));
        };
        s2.onerror = () => reject(new Error('Failed to load jsPDF'));
        document.head.appendChild(s2);
      };
      s1.onerror = () => reject(new Error('Failed to load html-to-image'));
      document.head.appendChild(s1);
    });
    return _pdfLibsPromise;
  }

  function _formatFilterSummary(answers) {
    const a = answers || {};
    const parts = [];

    const levelLabel = {
      'PG': "Masters",
      'PhD': "PhD"
    }[a.level] || "Masters";

    const fieldLabel = a.field
      ? a.field.replace(/&/g, 'and').replace(/\b\w/g, c => c.toUpperCase())
      : "any field";

    parts.push(`${levelLabel} programmes in ${fieldLabel}`);

    if (a.sub_field && a.sub_field !== '') {
      parts.push(`specifically ${a.sub_field.toLowerCase()}`);
    }

    if (a.country_decided === 'Yes' && a.selected_country) {
      parts.push(`in ${a.selected_country}`);
    } else {
      parts.push(`open to any country`);
    }

    if (a.tuition_band && a.tuition_band !== 'No limit') {
      parts.push(`with tuition ${a.tuition_band.toLowerCase()}/year`);
    } else {
      parts.push(`with no budget ceiling`);
    }

    if (a.duration && a.duration !== 'More than 3 years') {
      parts.push(`of ${a.duration.toLowerCase()}`);
    }

    if (a.ranking_importance === '0.75') {
      parts.push(`prioritising highly-ranked institutions`);
    } else if (a.ranking_importance === '0.25') {
      parts.push(`prioritising fit over prestige`);
    } else {
      parts.push(`balancing ranking and course fit`);
    }

    const arr = Array.isArray(a.priorities)
      ? a.priorities
      : [a.priorities_1, a.priorities_2, a.priorities_3].filter(Boolean);
    if (arr.length > 0) {
      parts.push(`ranked by ${arr.join(' > ').toLowerCase()}`);
    }

    return parts.join(', ') + '.';
  }

  function _slugify(s) {
    return (s || 'student').toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 30) || 'student';
  }

  function _formatDate(d) {
    const opts = { year: 'numeric', month: 'long', day: 'numeric' };
    return new Intl.DateTimeFormat('en-US', opts).format(d);
  }

  function _isoDate(d) {
    return d.toISOString().slice(0, 10);
  }

  /* Render an off-screen wrapper around a cloned content node with a page
     header + footer, capture to canvas, return canvas. Cleans up afterwards. */
  async function _capturePageAsCanvas({ contentNode, firstName, filterSummary, pageNum, totalPages, isPage1 }) {
    const wrapper = document.createElement('div');
    wrapper.style.cssText = `
      position: fixed; top: -10000px; left: 0;
      width: 820px; background: white;
      padding: 48px 44px 56px;
      font-family: var(--font, 'Inter', system-ui, sans-serif);
      color: var(--ink, #1a2a2a);
      z-index: -9999;
      box-sizing: border-box;
    `;

    const wordmarkImg = document.querySelector('.wordmark-img');
    const wordmarkSrc = wordmarkImg ? wordmarkImg.src : '';

    const title = firstName
      ? `${firstName}'s UNIFER shortlist`
      : `Your UNIFER shortlist`;
    const subtitle = `Personalised study abroad recommendations · ${_formatDate(new Date())}`;

    const header = document.createElement('div');
    header.style.cssText = `display:flex; align-items:center; gap:14px; margin-bottom: 18px; padding-bottom: 14px; border-bottom: 1px solid #e0e6e6;`;
    header.innerHTML = `
      <img src="${wordmarkSrc}" alt="UNIFER" style="height:28px; width:auto; flex:0 0 auto;" />
      <div style="flex:1; min-width:0;">
        <div style="font-size:20px; font-weight:600; color:#1a2a2a; line-height:1.2;">${title}</div>
        <div style="font-size:12.5px; color:#4a5a5a; margin-top:4px;">${subtitle}</div>
      </div>
    `;
    wrapper.appendChild(header);

    if (isPage1 && filterSummary) {
      const summary = document.createElement('div');
      summary.style.cssText = `font-size:13px; color:#4a5a5a; line-height:1.6; margin-bottom:22px; font-style:italic;`;
      summary.textContent = filterSummary;
      wrapper.appendChild(summary);
    }

    const clone = contentNode.cloneNode(true);
    clone.querySelectorAll('button, input, .chip, .whatif, .preview-banner, #low-results-banner, .skip-btn').forEach(el => {
      if (el.classList.contains('chip')) {
        el.style.pointerEvents = 'none';
        el.style.cursor = 'default';
      } else if (el.tagName === 'BUTTON' || el.tagName === 'INPUT') {
        el.remove();
      }
    });
    clone.querySelectorAll('#whatifRow, .preview-chrome').forEach(el => el.remove());
    wrapper.appendChild(clone);

    if (wordmarkSrc) {
      const watermark = document.createElement('div');
      watermark.style.cssText = `
        position: absolute; top: 50%; left: 50%;
        transform: translate(-50%, -50%) rotate(-20deg);
        opacity: 0.05;
        pointer-events: none;
        width: 60%;
      `;
      watermark.innerHTML = `<img src="${wordmarkSrc}" alt="" style="width:100%;" />`;
      wrapper.appendChild(watermark);
    }

    const footer = document.createElement('div');
    footer.style.cssText = `
      margin-top: 32px; padding-top: 14px; border-top: 1px solid #e0e6e6;
      display:flex; justify-content:space-between;
      font-size:11px; color:#7a8a8a;
    `;
    footer.innerHTML = `
      <span>Page ${pageNum} of ${totalPages}</span>
      <span>Generated by UNIFER · unifer.app</span>
    `;
    wrapper.appendChild(footer);

    document.body.appendChild(wrapper);

    // Wait for images (wordmark) to load. Errors are non-fatal — we proceed anyway.
    await new Promise(resolve => {
      const imgs = wrapper.querySelectorAll('img');
      if (imgs.length === 0) return resolve();
      let remaining = imgs.length;
      const done = () => {
        remaining--;
        if (remaining <= 0) resolve();
      };
      imgs.forEach(img => {
        if (img.complete && img.naturalWidth > 0) {
          done();
        } else if (img.complete && img.naturalWidth === 0) {
          console.warn('[unifer] PDF: image failed to load, continuing without it', img.src.slice(0, 80));
          done();
        } else {
          img.addEventListener('load', done, { once: true });
          img.addEventListener('error', () => {
            console.warn('[unifer] PDF: image errored, continuing without it', img.src.slice(0, 80));
            done();
          }, { once: true });
        }
      });
      setTimeout(resolve, 2500);
    });

    // Capture using html-to-image (supports oklch, lab, color() functions).
    // htmlToImage.toCanvas returns a Promise<HTMLCanvasElement>.
    const canvas = await window.htmlToImage.toCanvas(wrapper, {
      pixelRatio: 2,
      backgroundColor: '#ffffff',
      cacheBust: true,
      width: 820,
      // Skip remote stylesheet inlining (Google Fonts CSS blocked by CORS).
      // The page's already-loaded fonts will be used from the browser's cache
      // via font-family fallbacks. Inter renders if available, else system sans.
      skipFonts: true,
      // Filter out elements that shouldn't be captured (interactive controls)
      filter: (node) => {
        if (!node.classList) return true;
        if (node.classList.contains('low-results-banner')) return false;
        if (node.classList.contains('whatif')) return false;
        if (node.classList.contains('preview-chrome')) return false;
        return true;
      },
      style: {
        // Ensure the captured wrapper renders at its intended width even though
        // it's positioned off-screen.
        transform: 'none'
      }
    });

    wrapper.remove();

    return canvas;
  }

  window.UNIFER.downloadPdf = async function() {
    const results = (window.UNIFER.results || []).filter(Boolean);
    if (results.length === 0) {
      console.warn('[unifer] downloadPdf: no results to export');
      return;
    }

    const overlay = document.createElement('div');
    overlay.id = 'pdf-generating-overlay';
    overlay.style.cssText = `
      position: fixed; inset: 0; z-index: 6500;
      display: flex; align-items: center; justify-content: center;
      background: rgba(255,255,255,0.9); backdrop-filter: blur(4px);
      font-family: var(--font, system-ui); font-size: 14px; color: var(--ink, #1a2a2a);
    `;
    overlay.innerHTML = `
      <div style="display:flex; flex-direction:column; align-items:center; gap:14px;">
        <div style="width:36px; height:36px; border:3px solid #e0e6e6; border-top-color: var(--teal, #0a8a7a); border-radius:50%; animation: pdfSpin 900ms linear infinite;"></div>
        <div>Generating your PDF...</div>
      </div>
      <style>@keyframes pdfSpin { to { transform: rotate(360deg); } }</style>
    `;
    document.body.appendChild(overlay);

    try {
      await _loadPdfLibs();

      const answers = window.UNIFER.answers || {};
      const firstName = (answers.first_name || '').trim();
      const filterSummary = _formatFilterSummary(answers);

      const body = document.body;
      const originalView = body.getAttribute('data-view') || 'ranked';
      const originalTab = body.getAttribute('data-tab') || 'matches';

      if (originalTab !== 'matches') {
        body.setAttribute('data-tab', 'matches');
      }

      body.setAttribute('data-view', 'ranked');
      await new Promise(r => setTimeout(r, 120));
      const rankedNode = document.querySelector('#rankedStack, .ranked-stack, [data-view-content="ranked"]');
      if (!rankedNode) throw new Error('Ranked view DOM not found');

      const canvas1 = await _capturePageAsCanvas({
        contentNode: rankedNode,
        firstName,
        filterSummary,
        pageNum: 1,
        totalPages: 2,
        isPage1: true
      });

      body.setAttribute('data-view', 'compare');
      await new Promise(r => setTimeout(r, 200));

      const compareStack = document.createElement('div');
      compareStack.style.cssText = `display:flex; flex-direction:column; gap:28px;`;

      const tableEl = document.querySelector('#compareTable, .compare-table, [data-compare-sub="table"]');
      const barsEl = document.querySelector('#compareBars, .compare-bars, [data-compare-sub="bars"]');
      const stripsEl = document.querySelector('#compareStrips, .compare-strips, [data-compare-sub="strips"]');

      [tableEl, barsEl, stripsEl].forEach(el => {
        if (!el) return;
        const sub = el.cloneNode(true);
        sub.style.display = 'block';
        sub.style.visibility = 'visible';
        compareStack.appendChild(sub);
      });

      if (compareStack.children.length === 0) {
        const compareWrap = document.querySelector('.compare-wrap, [data-view-content="compare"]');
        if (compareWrap) compareStack.appendChild(compareWrap.cloneNode(true));
      }

      const canvas2 = await _capturePageAsCanvas({
        contentNode: compareStack,
        firstName,
        filterSummary,
        pageNum: 2,
        totalPages: 2,
        isPage1: false
      });

      body.setAttribute('data-view', originalView);
      body.setAttribute('data-tab', originalTab);

      const jsPDFCtor = (window.jspdf && window.jspdf.jsPDF) || window.jsPDF;
      if (!jsPDFCtor) throw new Error('jsPDF constructor not found');

      const pdf = new jsPDFCtor({
        orientation: 'portrait',
        unit: 'mm',
        format: 'a4'
      });

      const pdfW = pdf.internal.pageSize.getWidth();
      const pdfH = pdf.internal.pageSize.getHeight();

      function addCanvasToPage(canvas, isFirstPage) {
        if (!isFirstPage) pdf.addPage();
        const imgData = canvas.toDataURL('image/jpeg', 0.92);
        const ratio = canvas.width / canvas.height;
        const targetW = pdfW - 8;
        const targetH = targetW / ratio;
        let finalW = targetW;
        let finalH = targetH;
        if (targetH > pdfH - 8) {
          finalH = pdfH - 8;
          finalW = finalH * ratio;
        }
        const x = (pdfW - finalW) / 2;
        const y = (pdfH - finalH) / 2;
        pdf.addImage(imgData, 'JPEG', x, y, finalW, finalH);
      }

      addCanvasToPage(canvas1, true);
      addCanvasToPage(canvas2, false);

      const slug = _slugify(firstName);
      const date = _isoDate(new Date());
      const filename = `unifer-shortlist-${slug}-${date}.pdf`;
      pdf.save(filename);
    } catch (err) {
      console.error('[unifer] downloadPdf failed', err);
      alert('Sorry, PDF generation failed. Please try again or contact support.');
    } finally {
      overlay.remove();
    }
  };
})();
