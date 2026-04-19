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

  function buildChipsFromAnswers(answers, item) {
    // Build 3-4 chips from the student's actual quiz answers — these are the inputs they can tweak.
    // Each chip: { k: 'shortKey', v: 'displayValue', q: 'modal question', opts: [...] }
    const a = answers || {};
    const chips = [];

    // Field
    if (a.field) {
      chips.push({
        k: 'field',
        v: titleCaseFromValue(a.field),
        q: 'Change your field?',
        opts: ['Computer Science & Data', 'Business & Management', 'Engineering & Technology', 'Economics, Finance & Accounting', 'Life Sciences & Biotechnology']
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
    const p1 = answers.priorities_1 || answers.priority_1;
    const p2 = answers.priorities_2 || answers.priority_2;
    const p3 = answers.priorities_3 || answers.priority_3;

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

    // Auto-remove on next view mount
    const remove = () => {
      const o = document.getElementById('unifer-computing-overlay');
      if (o) o.remove();
    };
    window.addEventListener('unifer:viewmounted', remove, { once: true });
    // Safety timeout
    setTimeout(remove, 15000);
  }

  // ----- Build /recommend payload from a UNIFER.answers-shaped object -----
  function buildRecommendPayload(a) {
    a = a || {};
    return {
      priority_1: a.priorities_1 || '',
      priority_2: a.priorities_2 || '',
      priority_3: a.priorities_3 || '',

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
  window.UNIFER.requeryRecommend = async function (overrideAnswers, options) {
    options = options || {};
    const a = overrideAnswers || window.UNIFER.answers || {};
    const recommendPayload = buildRecommendPayload(a);

    try {
      const res = await fetch('/recommend', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(recommendPayload)
      });
      const data = await res.json();
      if (Array.isArray(data)) {
        const transformed = transformRecommendResponse(data);
        if (!options.transient) {
          window.UNIFER.results = transformed;
          window.UNIFER.eligiblePool = transformed;
          window.UNIFER.recommendError = null;
        }
        return transformed;
      } else {
        if (!options.transient) {
          window.UNIFER.recommendError = data && (data.message || 'No matches found');
        }
        return [];
      }
    } catch (err) {
      console.error('[unifer] requeryRecommend failed', err);
      if (!options.transient) window.UNIFER.recommendError = 'Network error';
      return [];
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
      const data = await res.json();

      if (!Array.isArray(data)) {
        // Backend returned an error-shaped object: { message, suggestion } or similar
        window.UNIFER.results = [];
        window.UNIFER.recommendError = data && (data.message || 'No matches found');
      } else {
        const transformed = transformRecommendResponse(data);
        window.UNIFER.results = transformed;
        window.UNIFER.eligiblePool = transformed;  // cache for Stage 5 tweak loop
        window.UNIFER.recommendError = null;
      }
    } catch (err) {
      console.error('[unifer] /recommend failed', err);
      window.UNIFER.results = [];
      window.UNIFER.recommendError = 'Network error — please try again.';
    }

    // ─── Navigate to results ───
    window.UNIFER.navigate('results');
  };

  // ----- Init -----
  window.addEventListener('hashchange', () => renderView(parseHash()));
  document.addEventListener('DOMContentLoaded', () => renderView(parseHash()));
  if (document.readyState === 'interactive' || document.readyState === 'complete') {
    renderView(parseHash());
  }
})();
