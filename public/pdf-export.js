/* ═══════════════════════════════════════════════════════════════════
   UNIFER PDF Export — Stage 6C-fix-5
   Purpose-built print layout. Target 2 pages, flows to 3 when dense.
   Exposes: window.UNIFER.downloadPdf
   ═══════════════════════════════════════════════════════════════════ */
(function() {
  'use strict';
  if (!window.UNIFER) window.UNIFER = {};

  let _pdfLibsPromise = null;
  function _loadPdfLibs() {
    if (_pdfLibsPromise) return _pdfLibsPromise;
    _pdfLibsPromise = new Promise((resolve, reject) => {
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
  let _logoDataUrl = null;
  let _logoPromise = null;
  function _loadLogo() {
    if (_logoDataUrl) return Promise.resolve(_logoDataUrl);
    if (_logoPromise) return _logoPromise;
    _logoPromise = new Promise((resolve) => {
      const img = new Image();
      img.crossOrigin = 'anonymous';
      img.onload = () => {
        try {
          const canvas = document.createElement('canvas');
          canvas.width = img.naturalWidth;
          canvas.height = img.naturalHeight;
          const ctx = canvas.getContext('2d');
          ctx.drawImage(img, 0, 0);
          _logoDataUrl = canvas.toDataURL('image/png');
          resolve(_logoDataUrl);
        } catch (e) {
          console.warn('[unifer] logo conversion failed', e);
          resolve(null);
        }
      };
      img.onerror = () => { console.warn('[unifer] logo fetch failed'); resolve(null); };
      img.src = '/logo-new.png';
    });
    return _logoPromise;
  }


  function _formatFilterSummary(a) {
    a = a || {};
    const parts = [];
    const lvl = { 'PG': 'Masters', 'PhD': 'PhD' }[a.level] || 'Masters';
    const fld = a.field ? a.field.replace(/&/g, 'and').replace(/\b\w/g, c => c.toUpperCase()) : 'any field';
    parts.push(lvl + ' programmes in ' + fld);
    if (a.sub_field) parts.push('specifically ' + a.sub_field.toLowerCase());
    if (a.country_decided === 'Yes' && a.selected_country) parts.push('in ' + a.selected_country);
    else parts.push('open to any country');
    if (a.tuition_band && a.tuition_band !== 'No limit') parts.push('with tuition ' + a.tuition_band.toLowerCase() + '/year');
    else parts.push('with no budget ceiling');
    if (a.duration && a.duration !== 'More than 3 years') parts.push('of ' + a.duration.toLowerCase());
    if (a.ranking_importance === '0.75') parts.push('prioritising highly-ranked institutions');
    else if (a.ranking_importance === '0.25') parts.push('prioritising fit over prestige');
    else parts.push('balancing ranking and course fit');
    const arr = Array.isArray(a.priorities) ? a.priorities : [a.priorities_1, a.priorities_2, a.priorities_3].filter(Boolean);
    if (arr.length) parts.push('ranked by ' + arr.join(' > ').toLowerCase());
    return parts.join(', ') + '.';
  }

  function _slugify(s) { return (s || 'student').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 30) || 'student'; }
  function _formatDate(d) { return new Intl.DateTimeFormat('en-US', { year: 'numeric', month: 'long', day: 'numeric' }).format(d); }
  function _isoDate(d) { return d.toISOString().slice(0, 10); }
  function _fmt$(n) { if (n == null) return '—'; return '$' + Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 }); }
  function _avgRank(r) {
    if (!r) return '—';
    const vals = ['QS', 'THE', 'ARWU'].map(k => r[k]).filter(v => v != null && !isNaN(v));
    if (!vals.length) return '—';
    return '#' + Math.round(vals.reduce((a, b) => a + b, 0) / vals.length);
  }
  function _workRights(c) {
    const m = { 'United Kingdom': '2 years', 'Canada': 'up to 3 years', 'Australia': '2–4 years', 'Germany': '18 months', 'Ireland': '2 years', 'Netherlands': '1 year', 'France': '1 year', 'Singapore': 'employer-sponsored', 'New Zealand': '3 years', 'United States': '1 year (STEM 3)' };
    return m[c] || '—';
  }
  function _rankingsLine(r) {
    if (!r) return '';
    const p = [];
    ['QS', 'THE', 'ARWU', 'CUG', 'Guardian', 'GUG'].forEach(k => { if (r[k] != null) p.push(k + ' #' + r[k]); });
    return p.join(' · ');
  }

  const _COLORS = ['#0a8a7a', '#d97706', '#6366f1', '#db2777', '#059669'];

  function _buildRankedHtml(results, startIdx, endIdx, showLegend) {
    startIdx = startIdx != null ? startIdx : 0;
    endIdx = endIdx != null ? endIdx : 5;
    showLegend = showLegend !== false;
    const slice = results.slice(startIdx, endIdx);
    const cards = slice.map((u, localIdx) => {
      const globalIdx = startIdx + localIdx;
      const rank = globalIdx + 1;
      const color = _COLORS[globalIdx] || '#0a8a7a';
      const sc = u.scores || {};
      const moreRows = [];
      if (u.stats && u.stats.medianGPA) moreRows.push(['Median admitted GPA', String(u.stats.medianGPA)]);
      if (u.stats && u.stats.gre) moreRows.push(['GRE (Q) typical', String(u.stats.gre)]);
      if (u.stats && u.stats.acceptance) moreRows.push(['Acceptance rate', u.stats.acceptance + '%']);
      if (u.employ && u.employ.rate) moreRows.push(['Employment rate', u.employ.rate + '%']);
      if (u.employ && u.employ.salary) moreRows.push(['Median starting salary', _fmt$(u.employ.salary)]);
      if (u.cost && u.cost.living) moreRows.push(['Estimated living' + (u.city ? ' (' + u.city + ')' : ''), _fmt$(u.cost.living) + '/yr']);
      const moreHtml = moreRows.length ? '<div style="margin-top:8px;padding-top:8px;border-top:1px dashed #e0e6e6;display:flex;flex-wrap:wrap;gap:4px 16px;font-size:10.5px;">' + moreRows.map(r => '<div style="display:flex;gap:5px;"><span style="color:#7a8a8a;">' + r[0] + ':</span><span style="color:#1a2a2a;font-weight:500;">' + r[1] + '</span></div>').join('') + '</div>' : '';
      const scoreRow = (label, key) => {
        const v = sc[key] != null ? sc[key] : 0;
        return '<div style="display:flex;align-items:center;gap:10px;margin-bottom:7px;"><div style="width:108px;font-size:12.5px;color:#4a5a5a;">' + label + '</div><div style="flex:1;height:10px;background:#d5dcdc;border-radius:5px;overflow:hidden;"><div style="height:100%;width:' + v + '%;background:' + color + ';border-radius:5px;"></div></div><div style="width:44px;text-align:right;font-size:13px;font-weight:700;color:#1a2a2a;">' + v + '%</div></div>';
      };
      // Stage 7.8 — keep tick glued to last word of uni name (prevents drop to next line)
      const _rawName = u.name || '—';
      const _lastSpaceIdx = _rawName.lastIndexOf(' ');
      const _leading = _lastSpaceIdx > 0 ? _rawName.slice(0, _lastSpaceIdx + 1) : '';
      const _lastWord = _lastSpaceIdx > 0 ? _rawName.slice(_lastSpaceIdx + 1) : _rawName;
      const nameBlock = u.confidence
        ? _leading + '<span style="white-space:nowrap;">' + _lastWord + '\u00A0<span style="color:#0a8a7a;font-size:14px;font-weight:600;">✓</span></span>'
        : _rawName;
      const uniRankLine = (typeof _rankingsLine === 'function') ? _rankingsLine(u.rankings || {}) : '';
      // Stage 7.7 — subject shape is { name, QS?, ARWU?, CUG?, Guardian? } with real published ranks
      const subjName = (u.subject && u.subject.name) ? u.subject.name : '';
      const subjRanks = [];
      if (u.subject) {
        ['QS','THE','ARWU','CUG','Guardian'].forEach(function(k) {
          if (u.subject[k] != null) subjRanks.push(k + ' #' + u.subject[k]);
        });
      }
      const subjLine = subjRanks.join(' · ');
      const hasUniRank = uniRankLine && uniRankLine !== '—' && uniRankLine.length > 0;
      const hasSubjRank = subjRanks.length > 0;
      let rankingsHtml = '';
      if (hasUniRank) {
        rankingsHtml += '<div style="font-size:10px;font-weight:600;letter-spacing:0.07em;color:#7a8a8a;text-transform:uppercase;margin-bottom:4px;">University rankings</div>';
        rankingsHtml += '<div style="font-size:11.5px;color:#1a2a2a;line-height:1.5;' + (hasSubjRank ? 'margin-bottom:10px;' : '') + '">' + uniRankLine + '</div>';
      }
      if (hasSubjRank) {
        rankingsHtml += '<div style="font-size:10px;font-weight:600;letter-spacing:0.07em;color:#7a8a8a;text-transform:uppercase;margin-bottom:4px;">Subject' + (subjName ? ' · ' + subjName : '') + '</div>';
        rankingsHtml += '<div style="font-size:11.5px;color:#1a2a2a;line-height:1.5;">' + subjLine + '</div>';
      }
      if (!hasUniRank && !hasSubjRank) {
        rankingsHtml = '<div style="font-size:11px;color:#7a8a8a;font-style:italic;">Ranking data unavailable</div>';
      }
      const whyHtml = '<div style="margin-top:10px;padding-top:9px;border-top:1px dashed #e0e6e6;"><div style="font-size:10px;font-weight:600;letter-spacing:0.07em;color:#7a8a8a;text-transform:uppercase;margin-bottom:5px;">Why this aligns</div><div style="font-size:12px;color:#4a5a5a;line-height:1.55;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:3;overflow:hidden;max-height:5em;">' + (u.why || '') + '</div></div>';
      return '<div style="border:1px solid #e0e6e6;border-radius:2px;padding:14px 22px;margin-bottom:10px;background:white;page-break-inside:avoid;box-shadow:0 1px 3px rgba(0,0,0,0.04);"><div style="display:flex;gap:18px;align-items:flex-start;"><div style="flex:0 0 230px;"><div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;"><div style="width:26px;height:26px;border-radius:7px;background:' + color + ';color:white;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;flex:0 0 auto;">#' + rank + '</div><div style="font-size:16px;font-weight:600;color:#1a2a2a;line-height:1.2;">' + nameBlock + '</div></div><div style="font-size:12.5px;color:#1a2a2a;margin-left:36px;margin-bottom:3px;line-height:1.3;">' + (u.course || '—') + '</div><div style="font-size:11px;color:#7a8a8a;margin-left:36px;">' + (u.country || '—') + ' · ' + (u.duration || '—') + ' · ' + _fmt$(u.tuition) + '/yr</div></div><div style="flex:0 0 260px;">' + (sc.country != null ? scoreRow('Country match', 'country') : '') + scoreRow('Course match', 'course') + scoreRow('Institution match', 'institution') + '</div><div style="flex:1;min-width:0;">' + rankingsHtml + '</div></div>' + whyHtml + moreHtml + '</div>';
    }).join('');
    const hasAnyConfidence = results.slice(0, 5).some(function(u) { return u.confidence; });
    const confidenceLegend = (showLegend && hasAnyConfidence) ? '<div style="margin-top:10px;font-size:10px;color:#7a8a8a;font-style:italic;text-align:left;"><span style="color:#0a8a7a;font-style:normal;font-weight:600;">✓</span> = appears in all major ranking frameworks</div>' : '';
    const frameworkLegend = showLegend ? '<div style="margin-top:8px;font-size:9px;color:#7a8a8a;line-height:1.6;">QS = QS World University Rankings &nbsp;·&nbsp; THE = Times Higher Education &nbsp;·&nbsp; ARWU = Academic Ranking of World Universities &nbsp;·&nbsp; CUG = Complete University Guide &nbsp;·&nbsp; Guardian = Guardian University Guide</div>' : '';
    return '<div>' + cards + confidenceLegend + frameworkLegend + '</div>';
  }

  function _buildCompareAB(results) {
    const actives = results.slice(0, 5);
    if (!actives.length) return '<div style="padding:20px;color:#7a8a8a;font-size:12px;">No universities to compare.</div>';
    const headerCols = '<div style="display:flex;gap:10px;margin-bottom:26px;padding:0 4px;">' + actives.map((u, i) => '<div style="flex:1;min-width:0;border-top:3px solid ' + _COLORS[i] + ';padding-top:12px;min-height:48px;"><div style="font-size:12px;font-weight:600;color:#1a2a2a;line-height:1.3;overflow:hidden;text-overflow:ellipsis;">' + (u.name || '—') + '</div><div style="font-size:10.5px;color:#7a8a8a;margin-top:3px;line-height:1.25;">' + (u.course || '') + '</div></div>').join('') + '</div>';
    const tableRows = [
      ['Tuition / yr', u => u && u.tuition != null ? _fmt$(u.tuition) : '—', u => u && u.tuition != null],
      ['Duration', u => u && u.duration ? u.duration : '—', u => u && u.duration],
      ['Country', u => u && u.country ? u.country : '—', u => u && u.country],
      ['Overall ranking (avg QS·THE·ARWU)', u => u && u.rankings ? _avgRank(u.rankings) : '—', u => u && u.rankings],
      ['Subject rank (QS)', u => (u && u.subject && u.subject.QS != null) ? '#' + u.subject.QS : '—', u => u && u.subject && u.subject.QS != null],
      ['Median admitted GPA', u => (u && u.stats && u.stats.medianGPA) ? u.stats.medianGPA : '—', u => u && u.stats && u.stats.medianGPA],
      ['Employment rate', u => (u && u.employ && u.employ.rate != null) ? u.employ.rate + '%' : '—', u => u && u.employ && u.employ.rate != null],
      ['Post-study work', u => (u && u.country) ? _workRights(u.country) : '—', u => u && u.country && _workRights(u.country) !== '—'],
      ['Acceptance rate', u => (u && u.stats && u.stats.acceptance) ? u.stats.acceptance + '%' : '—', u => u && u.stats && u.stats.acceptance != null]
    ];
    const vis = tableRows.filter(r => actives.some(u => r[2](u)));
    const tableHtml = vis.length ? '<div style="margin-bottom:24px;"><div style="font-size:11px;font-weight:700;letter-spacing:0.08em;color:#7a8a8a;text-transform:uppercase;margin-bottom:14px;">A · Side-by-side facts</div><table style="width:100%;border-collapse:collapse;font-size:12px;">' + vis.map(r => '<tr style="border-bottom:1px solid #eef2f2;"><td style="padding:18px 12px;color:#4a5a5a;width:32%;font-weight:500;">' + r[0] + '</td>' + actives.map(u => '<td style="padding:18px 12px;color:#1a2a2a;">' + r[1](u) + '</td>').join('') + '</tr>').join('') + '</table></div>' : '';
    const groups = [['Country match', 'country'], ['Course match', 'course'], ['Institution match', 'institution']];
    const breakdownHtml = '<div><div style="font-size:11px;font-weight:700;letter-spacing:0.08em;color:#7a8a8a;text-transform:uppercase;margin-bottom:12px;">B · Score breakdown</div>' + groups.map(g => {
      const rows = actives.map((u, i) => {
        const v = (u.scores && u.scores[g[1]] != null) ? u.scores[g[1]] : 0;
        return '<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;"><div style="width:32%;font-size:12.5px;color:#4a5a5a;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + (u.name || '—') + '</div><div style="flex:1;height:6px;background:#eef2f2;border-radius:3px;overflow:hidden;"><div style="height:100%;width:' + v + '%;background:' + _COLORS[i] + ';"></div></div><div style="width:32px;text-align:right;font-size:11.5px;font-weight:600;color:#1a2a2a;">' + v + '</div></div>';
      }).join('');
      return '<div style="margin-bottom:28px;"><div style="font-size:13.5px;font-weight:600;color:#1a2a2a;margin-bottom:16px;">' + g[0] + '</div>' + rows + '</div>';
    }).join('') + '</div>';
    const hint = '<div style="font-size:11px;color:#7a8a8a;margin-bottom:18px;font-style:italic;">All three visualisations show the same 5 universities. Colors are consistent across sections.</div>';
    return hint + headerCols + tableHtml + breakdownHtml;
  }

  function _buildCompareC(results) {
    const actives = results.slice(0, 5);
    if (!actives.length) return '<div style="padding:20px;color:#7a8a8a;font-size:12px;">No universities to compare.</div>';
    const headerCols = '<div style="display:flex;gap:10px;margin-bottom:26px;padding:0 4px;">' + actives.map((u, i) => '<div style="flex:1;min-width:0;border-top:3px solid ' + _COLORS[i] + ';padding-top:12px;min-height:48px;"><div style="font-size:12px;font-weight:600;color:#1a2a2a;line-height:1.3;overflow:hidden;text-overflow:ellipsis;">' + (u.name || '—') + '</div><div style="font-size:10.5px;color:#7a8a8a;margin-top:3px;line-height:1.25;">' + (u.course || '') + '</div></div>').join('') + '</div>';
    const dims = [['Research strength', 'research'], ['Teaching quality', 'teaching'], ['Employability', 'employability'], ['International diversity', 'diversity'], ['Prestige', 'prestige'], ['Selectivity', 'selectivity']];
    function _getDimVal(u, k) {
      if (!u) return null;
      if (u.dims && u.dims[k] != null) return Number(u.dims[k]);
      const comp = u._components && u._components.subScoreBreakdown;
      if (!comp) return null;
      const aliased = k === 'diversity' ? 'international' : k;
      return comp[aliased] != null ? Math.round(comp[aliased] * 100) : null;
    }
    const stripsArr = dims.map(d => {
      const points = actives.map((u, i) => { const v = _getDimVal(u, d[1]); return v == null ? null : { u: u, i: i, x: v }; }).filter(Boolean);
      return points.length === 0 ? null : { name: d[0], points: points };
    }).filter(Boolean);
    const stripsHtml = stripsArr.length ? '<div><div style="font-size:11px;font-weight:700;letter-spacing:0.08em;color:#7a8a8a;text-transform:uppercase;margin-bottom:14px;">C · Dimensional strips</div>' + stripsArr.map(s => '<div style="margin-bottom:50px;"><div style="font-size:14px;font-weight:600;color:#1a2a2a;margin-bottom:14px;">' + s.name + '</div><div style="display:flex;align-items:center;gap:10px;"><span style="font-size:10px;color:#7a8a8a;font-style:italic;flex:0 0 auto;">weak</span><div style="flex:1;position:relative;height:34px;background:linear-gradient(to right,#f3f5f5,#e0e6e6);border-radius:4px;">' + s.points.map(p => '<div style="position:absolute;left:' + Math.max(0, Math.min(100, p.x)) + '%;top:50%;transform:translate(-50%,-50%);width:14px;height:14px;background:' + _COLORS[p.i] + ';border:2px solid white;border-radius:50%;"></div>').join('') + '</div><span style="font-size:10px;color:#7a8a8a;font-style:italic;flex:0 0 auto;">strong</span></div></div>').join('') + '</div>' : '';
    const hint = '<div style="font-size:11px;color:#7a8a8a;margin-bottom:18px;font-style:italic;">Each strip plots all 5 universities on one axis from weak to strong. Useful for spotting clusters and outliers at a glance.</div>';
    return hint + headerCols + stripsHtml;
  }

  function _buildCompareHtml(results) {
    return _buildCompareAB(results) + _buildCompareC(results);
  }

  function _buildPrintDom(results, pageType) {
    const w = document.createElement('div');
    w.className = 'unifer-pdf-print';
    w.style.cssText = "position:fixed;top:0;left:0;width:780px;background:white;font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1a2a2a;padding:0;box-sizing:border-box;z-index:-1;pointer-events:none;";
    w.innerHTML = pageType === 'ranked' ? _buildRankedHtml(results, 0, 5, true)
      : pageType === 'compareAB' ? _buildCompareAB(results)
      : pageType === 'compareC' ? _buildCompareC(results)
      : _buildCompareHtml(results);
    document.body.appendChild(w);
    return w;
  }

  async function _captureWrapperAsCanvas(w) {
    await new Promise(r => setTimeout(r, 150));
    return window.htmlToImage.toCanvas(w, { pixelRatio: 1.5, backgroundColor: '#ffffff', cacheBust: true, skipFonts: true });
  }

  window.UNIFER.downloadPdf = async function() {
    const results = (window.UNIFER.results || []).filter(Boolean);
    if (!results.length) { console.warn('[unifer] downloadPdf: no results'); return; }

    const overlay = document.createElement('div');
    overlay.id = 'pdf-generating-overlay';
    overlay.style.cssText = 'position:fixed;inset:0;z-index:6500;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,0.9);backdrop-filter:blur(4px);font-family:var(--font,system-ui);font-size:14px;color:var(--ink,#1a2a2a);';
    overlay.innerHTML = '<div style="display:flex;flex-direction:column;align-items:center;gap:14px;"><div style="width:36px;height:36px;border:3px solid #e0e6e6;border-top-color:#0a8a7a;border-radius:50%;animation:pdfSpin 900ms linear infinite;"></div><div>Generating your PDF...</div></div><style>@keyframes pdfSpin{to{transform:rotate(360deg);}}</style>';
    document.body.appendChild(overlay);

    try {
      await _loadPdfLibs();
      const logoDataUrl = await _loadLogo();
      const answers = window.UNIFER.answers || {};
      const firstName = (answers.first_name || '').trim();
      const filterSummary = _formatFilterSummary(answers);

      const rWrap = _buildPrintDom(results, 'ranked');
      const rCanvas = await _captureWrapperAsCanvas(rWrap);
      rWrap.remove();

      const cAbWrap = _buildPrintDom(results, 'compareAB');
      const cAbCanvas = await _captureWrapperAsCanvas(cAbWrap);
      cAbWrap.remove();

      const cCWrap = _buildPrintDom(results, 'compareC');
      const cCCanvas = await _captureWrapperAsCanvas(cCWrap);
      cCWrap.remove();

      const jsPDFCtor = (window.jspdf && window.jspdf.jsPDF) || window.jsPDF;
      if (!jsPDFCtor) throw new Error('jsPDF not found');
      const pdf = new jsPDFCtor({ orientation: 'portrait', unit: 'mm', format: 'a4' });
      const pdfW = pdf.internal.pageSize.getWidth();
      const pdfH = pdf.internal.pageSize.getHeight();
      const margin = 12, contentW = pdfW - margin * 2, footerH = 10;
      const title = firstName ? firstName + "'s UNIFER shortlist" : 'Your UNIFER shortlist';
      const subtitle = 'Personalised study abroad recommendations · ' + _formatDate(new Date());

      // Stage 7.10 — adaptive scale-to-fit. Always returns count:1 (one page per section).
      // If content fits naturally at full width: render at full width, natural height.
      // If content is too tall: scale down by height, center horizontally. Never clipped, never split.
      function plan(canvas, sumFirst) {
        const top = sumFirst ? margin + 41 : margin + 23;
        const available = pdfH - top - footerH - 4;
        const scaleByWidth = contentW / canvas.width;
        const naturalHeight = canvas.height * scaleByWidth;
        if (naturalHeight <= available) {
          return { count: 1, x: margin, top: top, width: contentW, height: naturalHeight };
        }
        const scaleByHeight = available / canvas.height;
        const scaledWidth = canvas.width * scaleByHeight;
        return { count: 1, x: margin + (contentW - scaledWidth) / 2, top: top, width: scaledWidth, height: available };
      }
      const rPlan = plan(rCanvas, true);
      const cAbPlan = plan(cAbCanvas, false);
      const cCPlan = plan(cCCanvas, false);
      const totalPages = rPlan.count + cAbPlan.count + cCPlan.count;

      function chrome(pn, drawSum) {

        if (logoDataUrl) {
          pdf.addImage(logoDataUrl, 'PNG', margin, margin, 27, 6);
        } else {
          pdf.setFillColor(10, 138, 122);
          pdf.roundedRect(margin, margin, 7, 7, 1.5, 1.5, 'F');
          pdf.setTextColor(255, 255, 255);
          pdf.setFont('helvetica', 'bold');
          pdf.setFontSize(11);
          pdf.text('U', margin + 3.5, margin + 4.8, { align: 'center' });
          pdf.setTextColor(10, 138, 122);
          pdf.setFontSize(14);
          pdf.text('unifer', margin + 9, margin + 5.5);
        }
        pdf.setTextColor(30, 42, 42);
        pdf.setFont('helvetica', 'bold');
        pdf.setFontSize(15);
        pdf.text(title, margin, margin + 13);
        pdf.setTextColor(74, 90, 90);
        pdf.setFont('helvetica', 'normal');
        pdf.setFontSize(9.5);
        pdf.text(subtitle, margin, margin + 18);
        if (drawSum && filterSummary) {
          pdf.setTextColor(122, 138, 138);
          pdf.setFont('helvetica', 'bold');
          pdf.setFontSize(8);
          pdf.text('PREFERENCE SUMMARY', margin, margin + 23);
          pdf.setTextColor(74, 90, 90);
          pdf.setFont('helvetica', 'italic');
          pdf.setFontSize(9);
          pdf.text(pdf.splitTextToSize(filterSummary, contentW), margin, margin + 28);
          pdf.setDrawColor(224, 230, 230);
          pdf.setLineWidth(0.2);
          pdf.line(margin, margin + 37, pdfW - margin, margin + 37);
        } else {
          pdf.setDrawColor(224, 230, 230);
          pdf.setLineWidth(0.2);
          pdf.line(margin, margin + 20, pdfW - margin, margin + 20);
        }
        pdf.setDrawColor(224, 230, 230);
        pdf.setLineWidth(0.2);
        pdf.line(margin, pdfH - footerH, pdfW - margin, pdfH - footerH);
        pdf.setTextColor(122, 138, 138);
        pdf.setFont('helvetica', 'normal');
        pdf.setFontSize(8);
        pdf.text('Page ' + pn + ' of ' + totalPages, margin, pdfH - footerH + 5);
        pdf.text('Generated by UNIFER · unifer.app', pdfW - margin, pdfH - footerH + 5, { align: 'right' });
      }

      // Stage 7.10 — simple render, no slicing. plan() guarantees count:1 and proper scale.
      function paged(canvas, p, start, sumFirst) {
        const pn = start;
        if (pn > 1) pdf.addPage();
        chrome(pn, sumFirst);
        pdf.addImage(canvas.toDataURL('image/jpeg', 0.85), 'JPEG', p.x, p.top, p.width, p.height);
      }

      paged(rCanvas, rPlan, 1, true);
      paged(cAbCanvas, cAbPlan, rPlan.count + 1, false);
      paged(cCCanvas, cCPlan, rPlan.count + cAbPlan.count + 1, false);
      pdf.save('unifer-shortlist-' + _slugify(firstName) + '-' + _isoDate(new Date()) + '.pdf');
    } catch (err) {
      console.error('[unifer] downloadPdf failed', err);
      alert('Sorry, PDF generation failed. Please try again or contact support.');
    } finally {
      overlay.remove();
    }
  };
})();
