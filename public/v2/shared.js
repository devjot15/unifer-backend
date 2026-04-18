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

  // ----- Placeholder backend calls (wired in Stage 3) -----
  window.UNIFER.submitQuiz = async function () {
    // Stage 1: stub that just navigates to results. Real /recommend call comes in Stage 3.
    console.log('[unifer] submitQuiz stub — answers:', window.UNIFER.answers);
    window.UNIFER.navigate('results');
  };

  // ----- Init -----
  window.addEventListener('hashchange', () => renderView(parseHash()));
  document.addEventListener('DOMContentLoaded', () => renderView(parseHash()));
  if (document.readyState === 'interactive' || document.readyState === 'complete') {
    renderView(parseHash());
  }
})();
