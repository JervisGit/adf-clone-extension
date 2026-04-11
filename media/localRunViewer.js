// localRunViewer.js — Local pipeline run progress viewer (webview side).
// Receives live events from LocalRunPanel via postMessage and updates the UI.

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
