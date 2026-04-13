/* global acquireVsCodeApi */
const vscode = acquireVsCodeApi();

// Signal ready so the extension host delivers the snapshot data
vscode.postMessage({ command: 'ready' });

window.addEventListener('message', (event) => {
    const msg = event.data;
    if (msg.command !== 'loadSnapshot') return;
    document.getElementById('app').innerHTML = renderSnapshot(msg);
});

function esc(s) {
    return String(s ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function renderSnapshot(data) {
    const { notebookName, generatedAt, evaluatedParams = {}, cells = [] } = data;

    // ── Parameters section ─────────────────────────────────────────────────────
    const paramEntries = Object.entries(evaluatedParams);
    const paramSection = paramEntries.length > 0
        ? `<div class="cell param-cell">
            <div class="cell-label">Parameters</div>
            <pre class="cell-source">${paramEntries.map(([k, v]) => `${esc(k)} = ${esc(JSON.stringify(v))}`).join('\n')}</pre>
           </div>`
        : '';

    // ── Cells ──────────────────────────────────────────────────────────────────
    let codeIdx = 0;
    const cellsHtml = cells.map((cell) => {
        if (cell.cellType === 'markdown') {
            return `<div class="cell md-cell">
                <div class="cell-label">Markdown</div>
                <pre class="cell-source md">${esc(cell.source)}</pre>
            </div>`;
        }

        codeIdx++;
        const out = cell.output;
        let outSection = '';

        if (out) {
            const htmlContent = out.data?.['text/html'];
            const rawText     = out.text ?? out.data?.['text/plain'];
            const textStr     = Array.isArray(rawText) ? rawText.join('') : String(rawText ?? '');

            if (htmlContent) {
                const htmlStr = Array.isArray(htmlContent) ? htmlContent.join('') : htmlContent;
                outSection = `<div class="cell-label out-label">Out [${codeIdx}]</div>
                    <div class="cell-output-html">${htmlStr}</div>`;
            } else if (textStr.trim()) {
                outSection = `<div class="cell-label out-label">Out [${codeIdx}]</div>
                    <pre class="cell-output">${esc(textStr)}</pre>`;
            } else if (out.status === 'error') {
                const tb = (out.traceback ?? []).join('\n');
                outSection = `<div class="cell-label out-label">Out [${codeIdx}]</div>
                    <pre class="cell-output error">${esc(out.evalue ?? '')}\n${esc(tb)}</pre>`;
            }
        }

        return `<div class="cell code-cell">
            <div class="cell-label">In [${codeIdx}]</div>
            <pre class="cell-source">${esc(cell.source)}</pre>
            ${outSection}
        </div>`;
    }).join('');

    // ── Header ─────────────────────────────────────────────────────────────────
    const dateStr = generatedAt
        ? new Date(generatedAt).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' })
        : '';

    return `
        <div class="nb-header">
            <div class="nb-title">\uD83D\uDCD3 ${esc(notebookName)}</div>
            <div class="nb-meta">Snapshot generated ${esc(dateStr)}</div>
        </div>
        ${paramSection}
        ${cellsHtml}
    `;
}
