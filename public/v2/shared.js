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

  // ----- Computing-state overlay -----
  function showComputingLoader() {
    const existing = document.getElementById('unifer-computing-overlay');
    if (existing) existing.remove();
    const overlay = document.createElement('div');
    overlay.id = 'unifer-computing-overlay';
    overlay.style.cssText = 'position:fixed; inset:0; background:rgba(255,255,255,0.94); display:flex; flex-direction:column; align-items:center; justify-content:center; z-index:1000; gap:20px; font-family:var(--font);';
    overlay.innerHTML = `
      <div style="width:40px; height:40px; border-radius:50%; border:3px solid var(--line); border-top-color:var(--teal); animation:unifer-spin 0.9s linear infinite;"></div>
      <div style="font-size:16px; color:var(--ink); font-weight:500;">Computing your matches…</div>
      <div style="font-size:13px; color:var(--ink-3);">Running your profile through 1,068 universities</div>
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

  // ----- Submit: call /recommend + fire-and-forget /ml/save-profile -----
  window.UNIFER.submitQuiz = async function () {
    const a = window.UNIFER.answers || {};
    const sessionId = window.UNIFER.sessionId;

    // ─── Construct /recommend payload — matches production exactly ───
    const recommendPayload = {
      // Priorities (priorities_1/2/3 from the rank question)
      priority_1: a.priorities_1 || '',
      priority_2: a.priorities_2 || '',
      priority_3: a.priorities_3 || '',

      // Section 3 — country prefs (empty string when country pre-selected)
      work_permit_importance: a.work_permit_importance || '',
      english_preference: a.english_preference || '',
      pr_importance: a.pr_importance || '',

      // Section 4 — course & field
      level: a.level || '',
      duration: a.duration || '',
      tuition_band: a.tuition_band || '',
      field: a.field || '',
      sub_field: a.sub_field || '',
      internship_importance: a.internship_importance || '',
      scholarship_importance: a.scholarship_importance || '',

      // Section 5 — university selection
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

      // Section 1 — profile
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

      // Selected country — only when country_decided === "Yes"
      selected_country: (a.country_decided === 'Yes' && a.selected_country) ? a.selected_country : null
    };

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
        window.UNIFER.results = data;
        window.UNIFER.eligiblePool = data;  // cache for Stage 5 tweak loop
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
