// localRunViewer.js — Local pipeline run progress viewer (webview side).
// Receives live events from LocalRunPanel via postMessage and updates the UI.
// Shows dependency arrows between activities and a popover for output/details.

(function () {
    'use strict';

    const vscode = acquireVsCodeApi();

    const BOX_W = 190, BOX_H = 68, H_GAP = 80, V_GAP = 20;

    let pendingArrowRaf = null; // handle for the two-pass arrow-injection rAF

    // ─── State ────────────────────────────────────────────────────────────────
    const state = {
        pipelineName:       window.PIPELINE_NAME,
        runId:              window.RUN_ID,
        pipelineActivities: window.PIPELINE_ACTIVITIES || [],  // static structure for layout
        pipelineStatus:     'Running',
        activities:    {},   // { name: { name, type, status, output, error, startTime, endTime, durationMs } }
        activityOrder: [],
        selectedName:  null,
        startTime:     new Date(),
        endTime:       null,
        layout:        {},   // { name: { x, y } } — computed once from pipelineActivities
    };

    // ─── Config ───────────────────────────────────────────────────────────────
    const ACTIVITY_CONFIG = {
        Wait:                    { icon: '⏱', color: '#607d8b' },
        Fail:                    { icon: '✗',  color: '#f44336' },
        SetVariable:             { icon: '✎',  color: '#9c27b0' },
        AppendVariable:          { icon: '+',  color: '#9c27b0' },
        Filter:                  { icon: '⊙',  color: '#00897b' },
        ForEach:                 { icon: '↻',  color: '#0288d1' },
        Until:                   { icon: '↺',  color: '#0288d1' },
        IfCondition:             { icon: '⑂',  color: '#ff8f00' },
        Switch:                  { icon: '⎔',  color: '#ff8f00' },
        ExecutePipeline:         { icon: '▶',  color: '#1565c0' },
        WebActivity:             { icon: '🌐', color: '#00838f' },
        SynapseNotebook:         { icon: '📓', color: '#7b1fa2' },
        SparkJob:                { icon: '⚡',  color: '#e65100' },
        Script:                  { icon: '⌨',  color: '#455a64' },
        SqlServerStoredProcedure:{ icon: '🗄',  color: '#1565c0' },
        Copy:                    { icon: '⇒',  color: '#0277bd' },
        Lookup:                  { icon: '🔍', color: '#558b2f' },
        Delete:                  { icon: '🗑',  color: '#c62828' },
        GetMetadata:             { icon: 'ℹ',  color: '#00695c' },
        Validation:              { icon: '✔',  color: '#2e7d32' },
        WebHook:                 { icon: '↗',  color: '#6a1b9a' },
    };

    function getActivityConf(type) { return ACTIVITY_CONFIG[type] || { icon: '◈', color: '#616161' }; }

    // ─── Layout ───────────────────────────────────────────────────────────────
    function computeLayout(activities) {
        if (!activities || activities.length === 0) return {};
        const col = {};
        for (const a of activities) col[a.name] = 0;
        // Iterative topological depth (handle multi-level deps)
        let changed = true;
        for (let pass = 0; pass < activities.length && changed; pass++) {
            changed = false;
            for (const a of activities) {
                for (const dep of (a.dependsOn || [])) {
                    const d = (col[dep.activity] ?? 0) + 1;
                    if (d > (col[a.name] ?? 0)) { col[a.name] = d; changed = true; }
                }
            }
        }
        // Group by column
        const maxCol = Math.max(0, ...Object.values(col));
        const columns = Array.from({ length: maxCol + 1 }, () => []);
        for (const a of activities) columns[col[a.name] ?? 0].push(a.name);
        // Assign positions
        const positions = {};
        for (let ci = 0; ci <= maxCol; ci++) {
            columns[ci].forEach((name, ri) => {
                positions[name] = { x: 20 + ci * (BOX_W + H_GAP), y: 20 + ri * (BOX_H + V_GAP) };
            });
        }
        return positions;
    }

    // Returns a fallback position for activities not in the original layout (ForEach children etc.)
    function ensurePosition(name) {
        if (!state.layout[name]) {
            const existing = Object.values(state.layout);
            const maxX = existing.length ? Math.max(...existing.map(p => p.x)) : 20;
            const col   = existing.filter(p => p.x === maxX);
            const maxY  = col.length ? Math.max(...col.map(p => p.y)) : 20;
            state.layout[name] = { x: maxX + BOX_W + H_GAP, y: maxY };
        }
        return state.layout[name];
    }

    // ─── Init ─────────────────────────────────────────────────────────────────
    function init() {
        state.layout = computeLayout(state.pipelineActivities);
        render();
        vscode.postMessage({ command: 'ready' });

        window.addEventListener('message', (event) => {
            const msg = event.data;
            switch (msg.command) {
                case 'activityUpdate': handleActivityUpdate(msg); break;
                case 'pipelineEnd':    handlePipelineEnd(msg);    break;
            }
        });
    }

    function handleActivityUpdate(msg) {
        const { name, status, output, error } = msg;
        const now = new Date();
        if (!state.activities[name]) {
            state.activityOrder.push(name);
            state.activities[name] = { name, type: null, status: 'Queued', startTime: now, endTime: null, durationMs: null, output: null, error: null };
            ensurePosition(name);
        }
        const a = state.activities[name];
        if (status === 'Running' && !a.startTime) a.startTime = now;
        if (['Succeeded','Failed','Skipped','Cancelled'].includes(status)) {
            a.endTime   = now;
            a.durationMs = a.startTime ? (now - new Date(a.startTime)) : null;
        }
        a.status = status;
        a.output = output;
        a.error  = error;
        render();
    }

    function handlePipelineEnd(msg) {
        state.pipelineStatus = msg.status;
        state.endTime        = new Date();
        if (Array.isArray(msg.activityRuns)) {
            for (const rec of msg.activityRuns) {
                if (!state.activities[rec.activityName]) {
                    state.activityOrder.push(rec.activityName);
                    state.activities[rec.activityName] = { name: rec.activityName, type: rec.activityType, status: rec.status, output: rec.output, error: rec.error?.message ?? null, startTime: rec.activityRunStart, endTime: rec.activityRunEnd, durationMs: rec.durationInMs };
                    ensurePosition(rec.activityName);
                } else {
                    const a = state.activities[rec.activityName];
                    a.type = rec.activityType;
                    if (rec.status) a.status = rec.status;
                    if (rec.output !== undefined) a.output = rec.output;
                    if (rec.error)   a.error = rec.error.message ?? null;
                    if (rec.activityRunStart) a.startTime = rec.activityRunStart;
                    if (rec.activityRunEnd)   { a.endTime = rec.activityRunEnd; a.durationMs = rec.durationInMs; }
                }
            }
        }
        render();
    }

    // ─── Render ───────────────────────────────────────────────────────────────
    function render() {
        const app       = document.getElementById('app');
        const isRunning = state.pipelineStatus === 'Running';
        const elapsed   = state.endTime
            ? formatDuration(state.endTime - state.startTime)
            : formatDuration(Date.now() - state.startTime);

        app.innerHTML = `
            <div class="header">
                <h1>▶ ${esc(state.pipelineName)}</h1>
                <span class="status-badge ${state.pipelineStatus.toLowerCase()}">${state.pipelineStatus}</span>
                ${isRunning ? `<button class="btn-cancel" id="btn-cancel">◼ Cancel</button>` : ''}
                <span class="run-id-label">Run: ${state.runId.slice(0, 8)}</span>
            </div>
            <div class="run-summary">
                <div class="run-summary-item"><span class="run-summary-label">Activities:</span><span>${state.activityOrder.length}</span></div>
                <div class="run-summary-item"><span class="run-summary-label">Elapsed:</span><span>${elapsed}</span></div>
                ${!isRunning ? `<div class="run-summary-item"><span class="run-summary-label">Completed:</span><span>${state.endTime ? new Date(state.endTime).toLocaleTimeString() : '-'}</span></div>` : ''}
            </div>
            <div class="canvas-wrapper">
                ${renderCanvas()}
            </div>
            ${renderDetailsPanel(state.selectedName ? state.activities[state.selectedName] : null)}
        `;

        if (isRunning) {
            document.getElementById('btn-cancel')?.addEventListener('click', () => vscode.postMessage({ command: 'cancel' }));
        }
        document.querySelectorAll('.activity-box').forEach(el => {
            el.addEventListener('click', () => { state.selectedName = el.dataset.name; render(); });
        });
        document.getElementById('config-collapse-btn')?.addEventListener('click', () => {
            document.querySelector('.config-panel')?.classList.toggle('minimized');
        });

        // Popover triggers on detail panel buttons
        document.getElementById('btn-view-output')?.addEventListener('click',  () => showPopover('Output',  state.selectedName));
        document.getElementById('btn-view-error')?.addEventListener('click',   () => showPopover('Error',   state.selectedName));
        document.getElementById('btn-view-details')?.addEventListener('click', () => showPopover('Details', state.selectedName));

        // Phase 2: inject SVG arrows after browser has laid out the boxes
        if (pendingArrowRaf) cancelAnimationFrame(pendingArrowRaf);
        pendingArrowRaf = requestAnimationFrame(insertDependencyArrows);
    }

    // ─── Canvas (boxes only — arrows injected post-render via DOM measurement) ──
    function renderCanvas() {
        // Compute container minimum size from the full pre-computed layout
        // (includes ALL activities, not just those that have reported in yet)
        let minW = 400, minH = 160;
        for (const pos of Object.values(state.layout)) {
            minW = Math.max(minW, pos.x + BOX_W + 60);
            minH = Math.max(minH, pos.y + BOX_H + 60);
        }

        // Show ALL activities from pipelineActivities; supplement with live data
        const allNames = state.pipelineActivities.length > 0
            ? state.pipelineActivities.map(a => a.name)
            : state.activityOrder;

        if (allNames.length === 0) {
            return `<div class="canvas-container canvas-empty" id="canvas-container">Waiting for activities…</div>`;
        }

        const boxes = allNames.map(name => {
            const live  = state.activities[name];
            const paDef = state.pipelineActivities.find(a => a.name === name);
            const type  = live?.type ?? paDef?.type ?? null;
            return renderActivityBox({ name, type, ...(live ?? {}) });
        }).join('');

        // SVG is NOT included here — injected by insertDependencyArrows() after layout
        return `<div class="canvas-container" id="canvas-container"
             style="min-width:${minW}px;min-height:${minH}px;">
            ${boxes}
        </div>`;
    }

    function renderActivityBox(a) {
        const conf      = getActivityConf(a.type);
        const hasLive   = !!state.activities[a.name];
        const statusRaw = hasLive ? (a.status || 'Queued') : 'waiting';
        const statusCss = statusRaw.toLowerCase().replace(/ /g, '');
        const isSelected  = state.selectedName === a.name;
        const spinnerHtml = a.status === 'Running' ? `<span class="spinner"></span>` : '';
        const statusLabel = hasLive ? (a.status || 'Queued') : '—';
        const pos = state.layout[a.name] || { x: 20, y: 20 };

        return `<div class="activity-box status-${statusCss} ${isSelected ? 'selected' : ''}"
                     style="left:${pos.x}px;top:${pos.y}px;"
                     data-name="${esc(a.name)}">
            <div class="activity-header">
                <span class="activity-type-label">${esc(a.type || '…')}</span>
            </div>
            <div class="activity-body">
                <div class="activity-icon-large" style="color:${conf.color}">${conf.icon}</div>
                <div class="activity-label" title="${esc(a.name)}">${esc(a.name)}</div>
                <span class="activity-status-indicator ${statusCss}">${spinnerHtml}${esc(statusLabel)}</span>
            </div>
        </div>`;
    }

    // Condition → stroke colour
    const DEP_COLORS = { Succeeded: '#107c10', Failed: '#d13438', Skipped: '#ffa500', Completed: '#0078d4' };

    /**
     * Phase-2 of rendering: called from requestAnimationFrame after boxes are
     * in the DOM so we can read their actual offsetTop/offsetHeight.
     * Builds an SVG overlay using real measured centres instead of the BOX_H constant.
     */
    function insertDependencyArrows() {
        pendingArrowRaf = null;
        const container = document.getElementById('canvas-container');
        if (!container || !state.pipelineActivities.length) return;

        // Remove any previously injected SVG
        container.querySelectorAll('.dep-svg').forEach(el => el.remove());

        // Measure actual rendered box positions
        const measured = {};
        container.querySelectorAll('.activity-box[data-name]').forEach(el => {
            measured[el.dataset.name] = {
                l: el.offsetLeft,
                t: el.offsetTop,
                w: el.offsetWidth,
                h: el.offsetHeight,
            };
        });

        const paths = [];
        const defs  = Object.entries(DEP_COLORS).map(([cond, color]) =>
            `<marker id="arrow-${cond}" viewBox="0 0 10 10" refX="9" refY="5"
                     markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill="${color}"/>
            </marker>`
        ).join('');

        for (const a of state.pipelineActivities) {
            const tm = measured[a.name];
            if (!tm) continue;
            for (const dep of (a.dependsOn || [])) {
                const fm = measured[dep.activity];
                if (!fm) continue;
                const cond     = dep.dependencyConditions?.[0] ?? 'Succeeded';
                const color    = DEP_COLORS[cond] || DEP_COLORS.Succeeded;
                // Connect right-centre of source → left-centre of target
                const x1 = fm.l + fm.w, y1 = fm.t + fm.h / 2;
                const x2 = tm.l,        y2 = tm.t + tm.h / 2;
                const cpX = (x1 + x2) / 2;
                paths.push(
                    `<path d="M${x1},${y1} C${cpX},${y1} ${cpX},${y2} ${x2},${y2}"
                        stroke="${color}" stroke-width="1.5" fill="none"
                        marker-end="url(#arrow-${cond})" opacity="0.85"/>`
                );
                if (cond !== 'Succeeded') {
                    paths.push(
                        `<text x="${(x1+x2)/2}" y="${(y1+y2)/2 - 6}" text-anchor="middle"
                             font-size="9" fill="${color}">${esc(cond)}</text>`
                    );
                }
            }
        }

        if (!paths.length) return;

        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.classList.add('dep-svg');
        svg.setAttribute('width',  container.offsetWidth);
        svg.setAttribute('height', container.offsetHeight);
        svg.innerHTML = `<defs>${defs}</defs>${paths.join('')}`;
        container.insertBefore(svg, container.firstChild);
    }

    // ─── Details panel ────────────────────────────────────────────────────────
    function renderDetailsPanel(a) {
        const content = a ? renderActivityDetails(a) : `<div class="empty-state">Click an activity to view details.</div>`;
        return `<div class="config-panel">
            <div class="config-tabs">
                <span class="config-header-title">${a ? esc(a.name) : 'Details'}</span>
                <button class="config-collapse-btn" id="config-collapse-btn">⌃</button>
            </div>
            <div class="config-content">${content}</div>
        </div>`;
    }

    function renderActivityDetails(a) {
        const dur = a.durationMs != null ? formatDuration(a.durationMs) : (a.status === 'Running' ? 'Running…' : '—');
        const rows = [
            ['Type',     a.type    || '—'],
            ['Status',   a.status  || '—'],
            ['Started',  a.startTime ? new Date(a.startTime).toLocaleTimeString() : '—'],
            ['Ended',    a.endTime   ? new Date(a.endTime).toLocaleTimeString()   : '—'],
            ['Duration', dur],
        ];
        let html = `<div class="property-grid">
            ${rows.map(([k, v]) => `<div class="property-label">${esc(k)}</div><div class="property-value">${esc(v)}</div>`).join('')}
        </div>`;
        if (a.error) {
            html += `<div class="error-section"><div class="error-title">Error</div>
                <div style="font-size:12px;word-break:break-word">${esc(a.error)}</div></div>`;
        }
        html += `<div class="action-buttons">
            ${a.output != null ? `<button class="btn btn-secondary" id="btn-view-output">Output ↑</button>` : ''}
            ${a.error          ? `<button class="btn btn-secondary" id="btn-view-error">Error ↑</button>`   : ''}
            <button class="btn btn-secondary" id="btn-view-details">Details ↑</button>
        </div>`;
        return html;
    }

    // ─── Popover (inline JSON viewer expanding upward over the canvas) ─────────
    function showPopover(tab, activityName) {
        const a = state.activities[activityName];
        if (!a) return;

        const tabs = [];
        if (a.output != null) tabs.push({ label: 'Output',  data: a.output });
        tabs.push({                       label: 'Details', data: buildDetailsObject(a) });
        if (a.error)          tabs.push({ label: 'Error',   data: a.error });

        let activeIdx = tabs.findIndex(t => t.label === tab);
        if (activeIdx < 0) activeIdx = 0;

        renderPopover(activityName, tabs, activeIdx);
    }

    function buildDetailsObject(a) {
        return {
            name:        a.name,
            type:        a.type,
            status:      a.status,
            startTime:   a.startTime ? new Date(a.startTime).toISOString() : null,
            endTime:     a.endTime   ? new Date(a.endTime).toISOString()   : null,
            durationMs:  a.durationMs,
            pipeline:    state.pipelineName,
            runId:       state.runId,
        };
    }

    function renderPopover(activityName, tabs, activeIdx) {
        const root = document.getElementById('popover-root');
        if (!root) return;

        const tabButtonsHtml = tabs.map((t, i) => `
            <button class="popover-tab ${i === activeIdx ? 'active' : ''}" data-tab="${i}">${esc(t.label)}</button>`
        ).join('');

        const data    = tabs[activeIdx].data;
        const jsonStr = data === null || data === undefined
            ? 'null'
            : (typeof data === 'string' ? data : JSON.stringify(data, null, 2));

        root.innerHTML = `
            <div class="popover-overlay active" id="popover-overlay">
                <div class="popover-backdrop" id="popover-backdrop"></div>
                <div class="popover-card" id="popover-card">
                    <div class="popover-header">
                        <span class="popover-title">${esc(activityName)}</span>
                        <button class="popover-close" id="popover-close">×</button>
                    </div>
                    ${tabs.length > 1 ? `<div class="popover-tabs">${tabButtonsHtml}</div>` : ''}
                    <div class="popover-body">
                        <pre class="popover-json">${esc(jsonStr)}</pre>
                    </div>
                </div>
            </div>`;

        document.getElementById('popover-close')?.addEventListener('click', closePopover);
        document.getElementById('popover-backdrop')?.addEventListener('click', closePopover);
        root.querySelectorAll('.popover-tab').forEach(btn => {
            btn.addEventListener('click', () => renderPopover(activityName, tabs, parseInt(btn.dataset.tab)));
        });
    }

    function closePopover() {
        const root = document.getElementById('popover-root');
        if (root) root.innerHTML = '';
    }

    // ─── Utilities ────────────────────────────────────────────────────────────
    function formatDuration(ms) {
        if (ms < 1000)  return `${Math.round(ms)} ms`;
        if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
        return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
    }

    function esc(s) {
        return String(s === null || s === undefined ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    init();
})();

(function () {
    'use strict';

    const vscode = acquireVsCodeApi();

    // ─── State ────────────────────────────────────────────────────────────────
    const state = {
        pipelineName:  window.PIPELINE_NAME,
        runId:         window.RUN_ID,
        pipelineStatus:'Running',
        activities:    {},   // { name: { name, type, status, output, error, startTime, endTime, durationMs } }
        activityOrder: [],   // names in arrival order
        selectedName:  null,
        startTime:     new Date(),
        endTime:       null,
    };

    // ─── Config: activity-type → icon & color (mirrors pipelineEditorV2.js) ──
    const ACTIVITY_CONFIG = {
        Wait:                    { icon: '⏱', color: '#607d8b' },
        Fail:                    { icon: '✗',  color: '#f44336' },
        SetVariable:             { icon: '✎',  color: '#9c27b0' },
        AppendVariable:          { icon: '+',  color: '#9c27b0' },
        Filter:                  { icon: '⊙',  color: '#00897b' },
        ForEach:                 { icon: '↻',  color: '#0288d1' },
        Until:                   { icon: '↺',  color: '#0288d1' },
        IfCondition:             { icon: '⑂',  color: '#ff8f00' },
        Switch:                  { icon: '⎔',  color: '#ff8f00' },
        ExecutePipeline:         { icon: '▶',  color: '#1565c0' },
        WebActivity:             { icon: '🌐', color: '#00838f' },
        SynapseNotebook:         { icon: '📓', color: '#7b1fa2' },
        SparkJob:                { icon: '⚡',  color: '#e65100' },
        Script:                  { icon: '⌨',  color: '#455a64' },
        SqlServerStoredProcedure:{ icon: '🗄',  color: '#1565c0' },
        Copy:                    { icon: '⇒',  color: '#0277bd' },
        Lookup:                  { icon: '🔍', color: '#558b2f' },
        Delete:                  { icon: '🗑',  color: '#c62828' },
        GetMetadata:             { icon: 'ℹ',  color: '#00695c' },
        Validation:              { icon: '✔',  color: '#2e7d32' },
        WebHook:                 { icon: '↗',  color: '#6a1b9a' },
    };

    function getActivityConf(type) {
        return ACTIVITY_CONFIG[type] || { icon: '◈', color: '#616161' };
    }

    // ─── Init ─────────────────────────────────────────────────────────────────
    function init() {
        render();

        // Signal host that the webview script is loaded and ready to receive messages
        vscode.postMessage({ command: 'ready' });

        window.addEventListener('message', (event) => {
            const msg = event.data;
            switch (msg.command) {
                case 'activityUpdate':
                    handleActivityUpdate(msg);
                    break;
                case 'pipelineEnd':
                    handlePipelineEnd(msg);
                    break;
            }
        });
    }

    function handleActivityUpdate(msg) {
        const { name, status, output, error } = msg;
        const now = new Date();
        if (!state.activities[name]) {
            state.activityOrder.push(name);
            state.activities[name] = { name, type: null, status: 'Queued', startTime: now, endTime: null, durationMs: null, output: null, error: null };
        }
        const a = state.activities[name];
        if (status === 'Running' && !a.startTime) a.startTime = now;
        if (['Succeeded','Failed','Skipped','Cancelled'].includes(status)) {
            a.endTime = now;
            a.durationMs = a.startTime ? (now - new Date(a.startTime)) : null;
        }
        a.status = status;
        a.output = output;
        a.error  = error;
        render();
    }

    function handlePipelineEnd(msg) {
        state.pipelineStatus = msg.status;
        state.endTime        = new Date();
        // Merge any final activityRuns data
        if (Array.isArray(msg.activityRuns)) {
            for (const rec of msg.activityRuns) {
                if (!state.activities[rec.activityName]) {
                    state.activityOrder.push(rec.activityName);
                    state.activities[rec.activityName] = { name: rec.activityName, type: rec.activityType, status: rec.status, output: rec.output, error: rec.error?.message ?? null, startTime: rec.activityRunStart, endTime: rec.activityRunEnd, durationMs: rec.durationInMs };
                } else {
                    const a = state.activities[rec.activityName];
                    a.type = rec.activityType;
                    if (rec.status) a.status = rec.status;
                    if (rec.output !== undefined) a.output = rec.output;
                    if (rec.error)   a.error  = rec.error.message ?? null;
                    if (rec.activityRunStart) a.startTime = rec.activityRunStart;
                    if (rec.activityRunEnd)   { a.endTime = rec.activityRunEnd; a.durationMs = rec.durationInMs; }
                }
            }
        }
        render();
    }

    // ─── Render ───────────────────────────────────────────────────────────────
    function render() {
        const app = document.getElementById('app');
        const isRunning = state.pipelineStatus === 'Running';
        const elapsed   = state.endTime
            ? formatDuration(state.endTime - state.startTime)
            : formatDuration(Date.now() - state.startTime);

        app.innerHTML = `
            <div class="header">
                <h1>▶ ${esc(state.pipelineName)}</h1>
                <span class="status-badge ${state.pipelineStatus.toLowerCase()}">${state.pipelineStatus}</span>
                ${isRunning ? `<button class="btn-cancel" id="btn-cancel">◼ Cancel</button>` : ''}
                <span class="run-id-label">Run: ${state.runId.slice(0, 8)}</span>
            </div>

            <div class="run-summary">
                <div class="run-summary-item">
                    <span class="run-summary-label">Activities:</span>
                    <span>${state.activityOrder.length}</span>
                </div>
                <div class="run-summary-item">
                    <span class="run-summary-label">Elapsed:</span>
                    <span>${elapsed}</span>
                </div>
                ${!isRunning ? `<div class="run-summary-item">
                    <span class="run-summary-label">Completed:</span>
                    <span>${state.endTime ? new Date(state.endTime).toLocaleTimeString() : '-'}</span>
                </div>` : ''}
            </div>

            <div class="canvas-wrapper">
                <div class="canvas-container">
                    ${state.activityOrder.length === 0
                        ? `<div style="color:var(--vscode-descriptionForeground);font-size:13px;padding:20px">Waiting for activities…</div>`
                        : state.activityOrder.map(name => renderActivityBox(state.activities[name])).join('')}
                </div>
            </div>

            ${renderConfigPanel(state.selectedName ? state.activities[state.selectedName] : null)}
        `;

        if (isRunning) {
            document.getElementById('btn-cancel')?.addEventListener('click', () => {
                vscode.postMessage({ command: 'cancel' });
            });
        }

        document.querySelectorAll('.activity-box').forEach(el => {
            el.addEventListener('click', () => {
                state.selectedName = el.dataset.name;
                render();
            });
        });

        document.getElementById('config-collapse-btn')?.addEventListener('click', () => {
            document.querySelector('.config-panel')?.classList.toggle('minimized');
        });

        document.getElementById('btn-view-output')?.addEventListener('click', () => {
            const a = state.activities[state.selectedName];
            vscode.postMessage({ command: 'showOutput', activity: a });
        });

        document.getElementById('btn-view-details')?.addEventListener('click', () => {
            const a = state.activities[state.selectedName];
            vscode.postMessage({ command: 'showDetails', activity: a });
        });

        document.getElementById('btn-view-error')?.addEventListener('click', () => {
            const a = state.activities[state.selectedName];
            vscode.postMessage({ command: 'showError', activity: a });
        });
    }

    function renderActivityBox(a) {
        const conf    = getActivityConf(a.type);
        const status  = (a.status || 'Queued').toLowerCase().replace(/ /g, '');
        const isSelected = state.selectedName === a.name;
        const spinnerHtml = a.status === 'Running' ? `<span class="spinner"></span>` : '';
        const statusLabel = a.status || 'Queued';

        return `<div class="activity-box status-${status} ${isSelected ? 'selected' : ''}"
                     data-name="${esc(a.name)}">
            <div class="activity-header">
                <span class="activity-type-label">${esc(a.type || '…')}</span>
            </div>
            <div class="activity-body">
                <div class="activity-icon-large" style="color:${conf.color}">${conf.icon}</div>
                <div class="activity-label" title="${esc(a.name)}">${esc(a.name)}</div>
                <span class="activity-status-indicator ${status}">${spinnerHtml}${esc(statusLabel)}</span>
            </div>
        </div>`;
    }

    function renderConfigPanel(a) {
        const header = a
            ? `${esc(a.name)} — ${esc(a.type || '')} — ${esc(a.status || '')}`
            : 'Select an activity to view details';

        const content = a ? renderActivityDetails(a) : `<div class="empty-state">Click an activity box to see its details.</div>`;

        return `<div class="config-panel">
            <div class="config-tabs">
                <span class="config-header-title">${a ? esc(a.name) : 'Details'}</span>
                <button class="config-collapse-btn" id="config-collapse-btn">⌃</button>
            </div>
            <div class="config-content">${content}</div>
        </div>`;
    }

    function renderActivityDetails(a) {
        const dur = a.durationMs != null ? formatDuration(a.durationMs) : (a.status === 'Running' ? 'Running…' : '—');
        const startLabel = a.startTime ? new Date(a.startTime).toLocaleTimeString() : '—';
        const endLabel   = a.endTime   ? new Date(a.endTime).toLocaleTimeString()   : '—';

        const rows = [
            ['Type',     a.type || '—'],
            ['Status',   a.status || '—'],
            ['Started',  startLabel],
            ['Ended',    endLabel],
            ['Duration', dur],
        ];

        let html = `<div class="property-grid">
            ${rows.map(([k, v]) => `<div class="property-label">${esc(k)}</div><div class="property-value">${esc(v)}</div>`).join('')}
        </div>`;

        if (a.error) {
            html += `<div class="error-section">
                <div class="error-title">Error</div>
                <div style="font-size:12px;word-break:break-word">${esc(a.error)}</div>
            </div>`;
        }

        html += `<div class="action-buttons">
            ${a.output != null ? `<button class="btn btn-secondary" id="btn-view-output">View Output</button>` : ''}
            ${a.error          ? `<button class="btn btn-secondary" id="btn-view-error">View Error</button>` : ''}
            <button class="btn btn-secondary" id="btn-view-details">View Full Details</button>
        </div>`;

        return html;
    }

    // ─── Utilities ────────────────────────────────────────────────────────────
    function formatDuration(ms) {
        if (ms < 1000)         return `${Math.round(ms)} ms`;
        if (ms < 60000)        return `${(ms / 1000).toFixed(1)} s`;
        const m = Math.floor(ms / 60000);
        const s = Math.floor((ms % 60000) / 1000);
        return `${m}m ${s}s`;
    }

    function esc(s) {
        return String(s === null || s === undefined ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    init();
})();
