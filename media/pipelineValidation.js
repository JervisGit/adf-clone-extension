// pipelineValidation.js — Validation results panel webview script.
// Receives INITIAL_DATA from the host page and 'updateResults' messages via the VS Code API.

(function () {
    'use strict';

    const vscode = acquireVsCodeApi();
    let currentData = null;

    // ── Bootstrap ─────────────────────────────────────────────────────────────
    function init() {
        currentData = window.INITIAL_DATA;
        render(currentData);

        window.addEventListener('message', (event) => {
            const msg = event.data;
            if (msg.command === 'updateResults') {
                currentData = msg;
                render(currentData);
            }
        });
    }

    // ── Render ────────────────────────────────────────────────────────────────
    function render(data) {
        const { result, totalErrors, pipelineName, timestamp } = data;
        const app = document.getElementById('app');

        const tsLabel = timestamp
            ? new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
            : '';

        const pipeErr  = result.pipelineErrors ?? [];
        const actErr   = result.activityErrors ?? {};
        const actNames = Object.keys(actErr);
        const totalActivityErrs = actNames.reduce((s, k) => s + actErr[k].length, 0);

        app.innerHTML = `
            <div class="header">
                <div class="header-title">Validate: ${esc(pipelineName)}</div>
                ${tsLabel ? `<div class="header-meta">Last validated: ${tsLabel}</div>` : ''}
                <button class="btn-revalidate" id="btn-revalidate">↺ Re-validate</button>
            </div>

            <div class="summary-bar ${totalErrors === 0 ? 'valid' : 'has-errors'}">
                <div class="summary-stat">
                    <span class="badge ${pipeErr.length > 0 ? 'error' : 'ok'}">${pipeErr.length}</span>
                    Pipeline-level error${pipeErr.length !== 1 ? 's' : ''}
                </div>
                <div class="summary-stat">
                    <span class="badge ${totalActivityErrs > 0 ? 'error' : 'ok'}">${totalActivityErrs}</span>
                    Activity error${totalActivityErrs !== 1 ? 's' : ''}
                    ${actNames.length > 0 ? `across ${actNames.length} activity${actNames.length !== 1 ? ' groups' : ''}` : ''}
                </div>
                ${totalErrors === 0
                    ? `<div class="summary-stat" style="color:#4caf50; font-weight:600;">✔ All checks passed</div>`
                    : `<div class="summary-stat"><span class="badge error">${totalErrors}</span> total</div>`}
            </div>

            <div class="content">${totalErrors === 0 ? renderValid() : renderErrors(pipeErr, actErr, actNames)}</div>
        `;

        document.getElementById('btn-revalidate').addEventListener('click', () => {
            vscode.postMessage({ command: 'revalidate' });
        });

        // Collapsible groups
        document.querySelectorAll('.error-group-header').forEach(header => {
            header.addEventListener('click', () => {
                header.classList.toggle('collapsed');
                const body = header.nextElementSibling;
                if (body) body.classList.toggle('collapsed');
            });
        });
    }

    function renderValid() {
        return `<div class="valid-state">
            <div class="icon">✔</div>
            <h2>No errors found</h2>
            <p>The pipeline passed all local validation checks.<br>
            Activity names are unique, references are intact, and all required fields are set.</p>
        </div>`;
    }

    function renderErrors(pipeErr, actErr, actNames) {
        let html = '';

        // Pipeline-level errors
        if (pipeErr.length > 0) {
            html += `<div class="error-group error-group-pipeline">
                <div class="error-group-header section-pipeline">
                    <span style="color:var(--vscode-errorForeground)">⚠</span>
                    Pipeline — ${pipeErr.length} error${pipeErr.length !== 1 ? 's' : ''}
                    <span class="chevron">▼</span>
                </div>
                <div class="error-group-body">
                    ${pipeErr.map(msg => `<div class="error-row">
                        <span class="error-bullet">●</span>
                        <span class="error-text">${esc(msg)}</span>
                    </div>`).join('')}
                </div>
            </div>`;
        }

        // Per-activity errors
        for (const name of actNames) {
            const errs = actErr[name];
            html += `<div class="error-group">
                <div class="error-group-header">
                    <span style="color:var(--vscode-errorForeground)">⬡</span>
                    ${esc(name)} —
                    <span style="color:var(--vscode-descriptionForeground);font-weight:400">
                        ${errs.length} error${errs.length !== 1 ? 's' : ''}
                    </span>
                    <span class="chevron">▼</span>
                </div>
                <div class="error-group-body">
                    ${errs.map(msg => `<div class="error-row">
                        <span class="error-bullet">●</span>
                        <span class="error-text">${esc(msg)}</span>
                    </div>`).join('')}
                </div>
            </div>`;
        }

        return html;
    }

    function esc(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    init();
})();
