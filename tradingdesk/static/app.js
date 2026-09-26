/* Trading Desk UI: start runs, follow them live over server-sent events, read the reports. */
(() => {
  'use strict';

  const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
  const CRYPTO_SUFFIXES = ['-USD', '-USDT', '-USDC', '-BTC', '-ETH'];
  const TOKEN_KEY = 'tradingdesk.token';
  const CUSTOM = '__custom__';

  const state = {
    options: null,
    health: null,
    runs: [],
    selected: null,   // id of the run on screen
    run: null,        // its latest snapshot, kept current by the event stream
    stream: null,     // AbortController of the open event stream
    activeTab: null,
    tabPinned: false, // the user picked a tab, so new sections stop stealing focus
  };

  const $ = (id) => document.getElementById(id);

  // ---- small helpers -------------------------------------------------------

  function esc(text) {
    return String(text ?? '').replace(/[&<>"']/g, (c) => (
      {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c]
    ));
  }

  function markdown(text) {
    if (window.marked && window.DOMPurify) {
      return DOMPurify.sanitize(marked.parse(text || ''));
    }
    return `<pre>${esc(text)}</pre>`;
  }

  function getToken() {
    try { return localStorage.getItem(TOKEN_KEY) || ''; } catch { return ''; }
  }

  function setToken(value) {
    try {
      if (value) localStorage.setItem(TOKEN_KEY, value);
      else localStorage.removeItem(TOKEN_KEY);
    } catch { /* private mode: the token lives for this page only */ }
  }

  function authHeaders() {
    const token = getToken();
    return token ? {Authorization: `Bearer ${token}`} : {};
  }

  function describeError(detail) {
    if (!detail) return '';
    if (typeof detail === 'string') return detail;
    if (Array.isArray(detail)) {
      return detail.map((d) => {
        const where = (d.loc || []).filter((p) => p !== 'body').join('.');
        return where ? `${where}: ${d.msg}` : d.msg;
      }).join('; ');
    }
    return JSON.stringify(detail);
  }

  async function api(path, options = {}, retry = true) {
    const headers = Object.assign({}, authHeaders(), options.headers || {});
    let init = Object.assign({}, options, {headers});
    if (options.body !== undefined) {
      headers['Content-Type'] = 'application/json';
      init = Object.assign(init, {body: JSON.stringify(options.body)});
    }
    const res = await fetch(path, init);
    if (res.status === 401 && retry) {
      const token = window.prompt('This server requires an API token (TRADINGDESK_API_TOKEN):', getToken());
      if (token === null) throw new Error('An API token is required.');
      setToken(token.trim());
      return api(path, options, false);
    }
    if (!res.ok) {
      let detail = `${res.status} ${res.statusText}`;
      try { detail = describeError((await res.json()).detail) || detail; } catch { /* not JSON */ }
      throw new Error(detail);
    }
    return res.status === 204 ? null : res.json();
  }

  function fmtClock(iso) {
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? '' : d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
  }

  function fmtElapsed(run) {
    if (!run || !run.started_at) return '';
    const end = run.finished_at ? new Date(run.finished_at) : new Date();
    const secs = Math.max(0, Math.floor((end - new Date(run.started_at)) / 1000));
    return `${String(Math.floor(secs / 60)).padStart(2, '0')}:${String(secs % 60).padStart(2, '0')}`;
  }

  function fmtTokens(n) {
    return n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n);
  }

  function signalClass(signal) {
    const s = String(signal || '').toLowerCase();
    if (s === 'buy' || s === 'overweight') return 'up';
    if (s === 'sell' || s === 'underweight') return 'down';
    if (s === 'hold') return 'flat';
    return 'review';
  }

  function showFormError(message) {
    const box = $('form-error');
    box.textContent = message;
    box.classList.toggle('hidden', !message);
  }

  // ---- server state --------------------------------------------------------

  async function loadHealth() {
    const pill = $('server-status');
    try {
      const res = await fetch('/api/health');
      state.health = await res.json();
      pill.textContent = `v${state.health.version} · ${state.health.auth_required ? 'token required' : 'open'}`;
      pill.className = 'pill pill-ok';
      $('token-btn').classList.toggle('hidden', !state.health.auth_required);
    } catch {
      pill.textContent = 'server unreachable';
      pill.className = 'pill pill-bad';
    }
  }

  async function loadOptions() {
    state.options = await api('/api/options');
    buildForm();
  }

  async function loadRuns() {
    state.runs = await api('/api/runs');
    renderRunList();
  }

  // ---- the form ------------------------------------------------------------

  function currentProvider() {
    return state.options.providers.find((p) => p.key === $('f-provider').value);
  }

  function buildForm() {
    const o = state.options;
    const d = o.defaults;
    $('f-ticker').placeholder = o.ticker_examples;
    $('f-date').value = d.analysis_date;
    $('f-date').max = d.analysis_date;
    $('f-analysts').innerHTML = o.analysts.map((a) => (
      `<label class="check"><input type="checkbox" name="analysts" value="${esc(a.key)}" checked><span>${esc(a.label)}</span></label>`
    )).join('');
    $('f-depth').innerHTML = o.research_depths.map((r) => (
      `<option value="${r.value}" title="${esc(r.description)}">${esc(r.label)} (${r.value} round${r.value > 1 ? 's' : ''})</option>`
    )).join('');
    const depths = o.research_depths.map((r) => r.value);
    $('f-depth').value = String(depths.includes(d.research_depth) ? d.research_depth : depths[0]);
    $('f-provider').innerHTML = o.providers.map((p) => (
      `<option value="${esc(p.key)}">${esc(p.label)}${p.key_required && p.key_configured === false ? ' (no key on server)' : ''}</option>`
    )).join('');
    $('f-provider').value = o.providers.some((p) => p.key === d.llm_provider) ? d.llm_provider : o.providers[0].key;
    $('f-language').innerHTML = o.languages.map((l) => `<option value="${esc(l)}">${esc(l)}</option>`).join('')
      + `<option value="${CUSTOM}">Custom…</option>`;
    if (o.languages.includes(d.output_language)) {
      $('f-language').value = d.output_language;
    } else {
      $('f-language').value = CUSTOM;
      $('f-language-custom').value = d.output_language;
    }
    $('f-language-custom').classList.toggle('hidden', $('f-language').value !== CUSTOM);
    $('f-checkpoint').checked = Boolean(d.checkpoint_enabled);
    onProviderChange(true);
  }

  function onProviderChange(initial) {
    const p = currentProvider();
    if (!p) return;
    const d = state.options.defaults;
    const isDefault = p.key === d.llm_provider;

    const warn = $('key-warning');
    if (p.key_required && p.key_configured === false) {
      warn.textContent = `${p.api_key_env} is not set on the server; runs with this provider will be refused.`;
      warn.classList.remove('hidden');
    } else {
      warn.classList.add('hidden');
    }

    const showUrl = p.needs_backend_url || p.key === 'ollama';
    $('backend-field').classList.toggle('hidden', !showUrl);
    $('f-backend-url').value = (isDefault && d.backend_url) ? d.backend_url : (p.default_url || '');
    $('f-backend-url').required = Boolean(p.needs_backend_url);

    for (const mode of ['quick', 'deep']) {
      const select = $(`f-${mode}`);
      const custom = $(`f-${mode}-custom`);
      const preferred = initial && isDefault ? d[`${mode}_think_llm`] : null;
      if (p.models) {
        select.classList.remove('hidden');
        select.innerHTML = p.models[mode].map((m) => `<option value="${esc(m.value)}">${esc(m.label)}</option>`).join('')
          + `<option value="${CUSTOM}">Custom model ID…</option>`;
        if (preferred && p.models[mode].some((m) => m.value === preferred)) {
          select.value = preferred;
        } else if (preferred) {
          select.value = CUSTOM;
          custom.value = preferred;
        }
        custom.classList.toggle('hidden', select.value !== CUSTOM);
      } else {
        select.classList.add('hidden');
        select.innerHTML = '';
        custom.classList.remove('hidden');
        custom.value = preferred || '';
        custom.placeholder = p.key === 'azure' ? 'deployment name' : 'model id, e.g. openai/gpt-6-luna';
      }
    }

    const t = p.thinking;
    $('thinking-field').classList.toggle('hidden', !t);
    if (t) {
      $('thinking-label').textContent = t.label;
      $('f-thinking').innerHTML = '<option value="">Provider default</option>'
        + t.choices.map((c) => `<option value="${esc(c.value)}">${esc(c.label)}</option>`).join('');
      const preset = d[t.config_key];
      $('f-thinking').value = preset && t.choices.some((c) => c.value === preset) ? preset : '';
    }
  }

  function onTickerInput() {
    const value = $('f-ticker').value.trim().toUpperCase();
    const crypto = CRYPTO_SUFFIXES.some((s) => value.endsWith(s));
    const fundamentals = document.querySelector('#f-analysts input[value="fundamentals"]');
    if (fundamentals) {
      fundamentals.disabled = crypto;
      if (crypto) fundamentals.checked = false;
    }
    $('ticker-note').textContent = crypto ? 'Crypto: the fundamentals analyst does not run.' : '';
    $('ticker-note').classList.toggle('hidden', !crypto);
  }

  function readForm() {
    const p = currentProvider();
    const modelValue = (mode) => {
      const select = $(`f-${mode}`);
      const custom = $(`f-${mode}-custom`);
      return (p.models && select.value !== CUSTOM) ? select.value : custom.value.trim();
    };
    const body = {
      ticker: $('f-ticker').value.trim(),
      analysis_date: $('f-date').value,
      analysts: [...document.querySelectorAll('#f-analysts input:checked')].map((i) => i.value),
      research_depth: Number($('f-depth').value),
      llm_provider: p.key,
      backend_url: $('backend-field').classList.contains('hidden') ? null : ($('f-backend-url').value.trim() || null),
      quick_think_llm: modelValue('quick'),
      deep_think_llm: modelValue('deep'),
      output_language: $('f-language').value === CUSTOM ? $('f-language-custom').value.trim() : $('f-language').value,
      checkpoint: $('f-checkpoint').checked,
      portfolio: null,
    };
    if (p.thinking) body[p.thinking.config_key] = $('f-thinking').value || null;
    const portfolioText = $('f-portfolio').value.trim();
    if (portfolioText) body.portfolio = JSON.parse(portfolioText);
    return body;
  }

  async function submitRun(event) {
    event.preventDefault();
    showFormError('');
    const button = $('f-submit');
    button.disabled = true;
    try {
      const body = readForm();
      if (!body.analysts.length) throw new Error('Select at least one analyst.');
      const run = await api('/api/runs', {method: 'POST', body});
      await loadRuns();
      await selectRun(run.id);
    } catch (err) {
      showFormError(err instanceof SyntaxError ? `Portfolio is not valid JSON: ${err.message}` : err.message);
    } finally {
      button.disabled = false;
    }
  }

  // ---- the run list --------------------------------------------------------

  function renderRunList() {
    $('run-list').innerHTML = state.runs.map((r) => `
      <li class="run-item ${r.id === state.selected ? 'active' : ''}" data-id="${esc(r.id)}">
        <div class="run-item-top">
          <span class="ticker">${esc(r.ticker)}</span>
          <span class="pill pill-${esc(r.status)}">${esc(r.status.replace('_', ' '))}</span>
        </div>
        <div class="run-item-bottom muted">
          <span>${esc(r.analysis_date)}</span>
          <span>${esc(r.llm_provider || '')}</span>
          ${r.signal ? `<span class="signal signal-${signalClass(r.review ? 'review' : r.signal)} small">${esc(r.review ? 'REVIEW' : r.signal)}</span>` : ''}
        </div>
      </li>`).join('');
    $('run-list-empty').classList.toggle('hidden', state.runs.length > 0);
  }

  // ---- the selected run ----------------------------------------------------

  async function selectRun(id) {
    closeStream();
    state.selected = id;
    state.tabPinned = false;
    state.activeTab = null;
    renderRunList();
    state.run = await api(`/api/runs/${id}`);
    renderRun();
    if (!TERMINAL.has(state.run.status)) openStream(state.run);
  }

  async function refreshRun(id) {
    if (state.selected !== id) return;
    state.run = await api(`/api/runs/${id}`);
    renderRun();
  }

  function closeStream() {
    if (state.stream) {
      state.stream.abort();
      state.stream = null;
    }
  }

  async function openStream(run) {
    const controller = new AbortController();
    state.stream = controller;
    let response;
    try {
      response = await fetch(`/api/runs/${run.id}/events`, {
        headers: Object.assign({Accept: 'text/event-stream', 'Last-Event-ID': String(run.last_seq || 0)}, authHeaders()),
        signal: controller.signal,
      });
    } catch {
      return;
    }
    if (!response.ok || !response.body) return;
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    try {
      for (;;) {
        const {value, done} = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, {stream: true});
        let split;
        while ((split = buffer.indexOf('\n\n')) >= 0) {
          const block = buffer.slice(0, split);
          buffer = buffer.slice(split + 2);
          handleBlock(block, run.id);
        }
      }
    } catch {
      if (controller.signal.aborted) return;
    } finally {
      if (state.stream === controller) state.stream = null;
    }
    // The stream ends when the run does: pick up the final snapshot (report availability, timings).
    if (state.selected === run.id) {
      try { await refreshRun(run.id); await loadRuns(); } catch { /* shown on the next refresh */ }
    }
  }

  function handleBlock(block, runId) {
    let kind = 'message';
    const data = [];
    for (const line of block.split('\n')) {
      if (line.startsWith('event:')) kind = line.slice(6).trim();
      else if (line.startsWith('data:')) data.push(line.slice(5).trim());
    }
    if (!data.length || state.selected !== runId || !state.run) return;
    let payload;
    try { payload = JSON.parse(data.join('\n')); } catch { return; }
    applyEvent(kind, payload);
  }

  function applyEvent(kind, d) {
    const r = state.run;
    switch (kind) {
      case 'status':
        Object.assign(r, d);
        renderHeader();
        if (TERMINAL.has(r.status)) loadRuns().catch(() => {});
        break;
      case 'agents':
        r.agent_status = d.agents;
        renderAgents();
        break;
      case 'section':
        r.sections[d.key] = d.content;
        if (!state.tabPinned) state.activeTab = d.key;
        renderTabs();
        renderReport();
        break;
      case 'message':
        r.messages.push(d);
        if (r.messages.length > 200) r.messages.shift();
        renderFeed();
        break;
      case 'stats':
        r.stats = d;
        renderStats();
        break;
      default:
        break;
    }
  }

  function renderRun() {
    if (!state.run) return;
    $('empty-state').classList.add('hidden');
    $('run-panel').classList.remove('hidden');
    renderHeader();
    renderAgents();
    renderFeed();
    renderTabs();
    renderReport();
    renderStats();
  }

  function renderHeader() {
    const r = state.run;
    const s = r.settings || {};
    $('rp-title').textContent = `${r.ticker} · ${r.analysis_date}${r.asset_type !== 'stock' ? ` · ${r.asset_type}` : ''}`;
    $('rp-meta').textContent = [
      s.llm_provider,
      s.quick_think_llm && s.deep_think_llm ? `${s.quick_think_llm} / ${s.deep_think_llm}` : null,
      s.research_depth ? `depth ${s.research_depth}` : null,
      s.thinking ? `thinking ${s.thinking}` : null,
      s.output_language,
      s.portfolio ? 'with portfolio' : null,
    ].filter(Boolean).join(' · ');
    const status = $('rp-status');
    status.textContent = r.status.replace('_', ' ');
    status.className = `pill pill-${r.status}`;
    const signal = $('rp-signal');
    if (r.signal) {
      signal.textContent = r.review ? 'REVIEW' : r.signal;
      signal.className = `signal signal-${signalClass(r.review ? 'review' : r.signal)}`;
      signal.title = r.review ? 'No rating could be read from the final decision' : 'Portfolio manager rating';
    } else {
      signal.className = 'signal hidden';
    }
    $('rp-elapsed').textContent = fmtElapsed(r);
    $('rp-cancel').classList.toggle('hidden', TERMINAL.has(r.status));
    $('rp-download').classList.toggle('hidden', !r.report_available);
    const error = $('rp-error');
    error.textContent = r.error || '';
    error.classList.toggle('hidden', !r.error);
  }

  function renderAgents() {
    const r = state.run;
    const rows = [];
    for (const team of state.options.teams) {
      const agents = team.agents.filter((a) => a in r.agent_status);
      agents.forEach((agent, index) => {
        const status = r.agent_status[agent];
        rows.push(`<tr><td>${index === 0 ? esc(team.name) : ''}</td><td>${esc(agent)}</td>`
          + `<td><span class="pill pill-${esc(status)}">${esc(status.replace('_', ' '))}</span></td></tr>`);
      });
    }
    $('rp-agents').innerHTML = rows.join('') || '<tr><td colspan="3" class="muted">Waiting to start…</td></tr>';
  }

  function renderFeed() {
    const items = (state.run.messages || []).slice(-40).reverse();
    $('rp-feed').innerHTML = items.map((m) => (
      `<li><span class="mono muted">${esc(fmtClock(m.at))}</span>`
      + `<span class="kind kind-${esc(String(m.kind).toLowerCase())}">${esc(m.kind)}</span>`
      + `<span class="text">${esc(m.content)}</span></li>`
    )).join('') || '<li class="muted">Nothing yet.</li>';
  }

  function renderTabs() {
    const r = state.run;
    const available = state.options.sections.filter((s) => r.sections[s.key]);
    if (!available.length) {
      $('rp-tabs').innerHTML = '';
      state.activeTab = null;
      return;
    }
    if (!state.activeTab || !r.sections[state.activeTab]) state.activeTab = available[available.length - 1].key;
    let group = null;
    const parts = [];
    for (const s of available) {
      if (s.group !== group) {
        group = s.group;
        parts.push(`<span class="tab-group">${esc(group)}</span>`);
      }
      parts.push(`<button type="button" class="tab ${s.key === state.activeTab ? 'active' : ''}" data-key="${esc(s.key)}">${esc(s.title)}</button>`);
    }
    $('rp-tabs').innerHTML = parts.join('');
  }

  function renderReport() {
    const text = state.activeTab ? state.run.sections[state.activeTab] : '';
    $('rp-report').innerHTML = text ? markdown(text) : '<p class="muted">Waiting for the first report…</p>';
  }

  function renderStats() {
    const s = state.run.stats || {};
    if (!Object.keys(s).length) {
      $('rp-stats').innerHTML = '';
      return;
    }
    $('rp-stats').innerHTML = `<span>LLM calls <b>${Number(s.llm_calls || 0)}</b></span>`
      + `<span>Tool calls <b>${Number(s.tool_calls || 0)}</b></span>`
      + `<span>Tokens <b>${fmtTokens(Number(s.tokens_in || 0))}↑ ${fmtTokens(Number(s.tokens_out || 0))}↓</b></span>`;
  }

  async function cancelRun() {
    if (!state.selected || !window.confirm('Stop this run after the current agent step?')) return;
    try {
      await api(`/api/runs/${state.selected}/cancel`, {method: 'POST'});
    } catch (err) {
      window.alert(err.message);
    }
  }

  async function downloadReport() {
    const r = state.run;
    if (!r) return;
    try {
      const res = await fetch(`/api/runs/${r.id}/report`, {headers: authHeaders()});
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      const url = URL.createObjectURL(await res.blob());
      const link = document.createElement('a');
      link.href = url;
      link.download = `${r.ticker}_${r.analysis_date}_report.md`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      window.alert(`Could not download the report: ${err.message}`);
    }
  }

  // ---- wiring --------------------------------------------------------------

  async function init() {
    $('run-form').addEventListener('submit', submitRun);
    $('f-provider').addEventListener('change', () => onProviderChange(false));
    for (const mode of ['quick', 'deep']) {
      $(`f-${mode}`).addEventListener('change', () => {
        $(`f-${mode}-custom`).classList.toggle('hidden', $(`f-${mode}`).value !== CUSTOM);
      });
    }
    $('f-language').addEventListener('change', () => {
      $('f-language-custom').classList.toggle('hidden', $('f-language').value !== CUSTOM);
    });
    $('f-ticker').addEventListener('input', onTickerInput);
    $('run-list').addEventListener('click', (event) => {
      const item = event.target.closest('.run-item');
      if (item) selectRun(item.dataset.id).catch((err) => showFormError(err.message));
    });
    $('rp-tabs').addEventListener('click', (event) => {
      const tab = event.target.closest('.tab');
      if (!tab) return;
      state.activeTab = tab.dataset.key;
      state.tabPinned = true;
      renderTabs();
      renderReport();
    });
    $('rp-cancel').addEventListener('click', cancelRun);
    $('rp-download').addEventListener('click', downloadReport);
    $('token-btn').addEventListener('click', () => {
      const token = window.prompt('API token (leave empty to forget it):', getToken());
      if (token === null) return;
      setToken(token.trim());
      loadRuns().catch((err) => showFormError(err.message));
    });
    setInterval(() => {
      if (state.run && state.run.status === 'running') $('rp-elapsed').textContent = fmtElapsed(state.run);
    }, 1000);
    // Queued runs have no stream of their own; a periodic refresh keeps their statuses honest.
    setInterval(() => loadRuns().catch(() => {}), 15000);

    await loadHealth();
    try {
      await loadOptions();
      await loadRuns();
    } catch (err) {
      showFormError(err.message);
    }
  }

  init();
})();
