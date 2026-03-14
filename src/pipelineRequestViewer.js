const vscode = require('vscode');

class PipelineRequestViewerProvider {
    static panels = new Map(); // Map<requestId, panel>

    constructor(context) {
        this.context = context;
    }

    async openRequest(storageAccountName, containerName, req) {
        const column = vscode.window.activeTextEditor
            ? vscode.window.activeTextEditor.viewColumn
            : undefined;

        const panelKey = req.requestId;

        if (PipelineRequestViewerProvider.panels.has(panelKey)) {
            PipelineRequestViewerProvider.panels.get(panelKey).reveal(column);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'pipelineRequestViewer',
            `Request: ${req.pipelineName}`,
            column || vscode.ViewColumn.One,
            { enableScripts: true, retainContextWhenHidden: true }
        );

        PipelineRequestViewerProvider.panels.set(panelKey, panel);

        panel.onDidDispose(() => {
            PipelineRequestViewerProvider.panels.delete(panelKey);
        }, null, this.context.subscriptions);

        panel.webview.html = this._getHtml(req);
    }

    _getHtml(req) {
        const statusColors = {
            pending: '#f0ad4e',
            running: '#5bc0de',
            succeeded: '#5cb85c',
            failed: '#d9534f'
        };
        const statusIcons = {
            pending: '⏳',
            running: '🔄',
            succeeded: '✅',
            failed: '❌'
        };

        const color = statusColors[req.status] || '#888';
        const icon = statusIcons[req.status] || '○';

        const fmt = (isoStr) => {
            if (!isoStr) return '—';
            const utc = new Date(isoStr);
            const sgt = new Date(utc.getTime() + 8 * 60 * 60 * 1000);
            const d = String(sgt.getUTCDate()).padStart(2, '0');
            const h = String(sgt.getUTCHours()).padStart(2, '0');
            const mi = String(sgt.getUTCMinutes()).padStart(2, '0');
            const s = String(sgt.getUTCSeconds()).padStart(2, '0');
            const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
            return `${monthNames[sgt.getUTCMonth()]} ${d}, ${sgt.getUTCFullYear()} ${h}:${mi}:${s} SGT`;
        };

        const escHtml = (str) => String(str)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');

        const paramStr = Object.keys(req.parameters || {}).length
            ? escHtml(JSON.stringify(req.parameters, null, 2))
            : '(none)';

        const rows = [
            ['Request ID', escHtml(req.requestId)],
            ['Pipeline', escHtml(req.pipelineName)],
            ['Requested By', escHtml(req.requestedBy || 'unknown')],
            ['Requested At', fmt(req.requestedAt)],
            ['Status', `<span class="status-badge" style="background:${color}">${icon} ${escHtml(req.status)}</span>`],
            ['Status Updated At', fmt(req.statusUpdatedAt)],
            ['Run ID', req.runId ? escHtml(req.runId) : '—'],
            ['Run Started At', fmt(req.runStartedAt)],
            ['Run Completed At', fmt(req.runCompletedAt)],
            ['Run Status', req.runStatus ? escHtml(req.runStatus) : '—'],
            ['Error', req.errorMessage ? `<span style="color:#d9534f">${escHtml(req.errorMessage)}</span>` : '—']
        ];

        const tableRows = rows.map(([k, v]) =>
            `<tr><td class="label">${k}</td><td>${v}</td></tr>`
        ).join('\n');

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Run Request</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
            color: var(--vscode-foreground);
            background: var(--vscode-editor-background);
            padding: 24px;
        }
        h1 {
            font-size: 1.3rem;
            margin-bottom: 20px;
            color: var(--vscode-foreground);
            border-bottom: 1px solid var(--vscode-panel-border);
            padding-bottom: 10px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            max-width: 700px;
        }
        tr { border-bottom: 1px solid var(--vscode-panel-border); }
        td {
            padding: 10px 12px;
            vertical-align: top;
            font-size: 0.92rem;
        }
        td.label {
            font-weight: 600;
            color: var(--vscode-descriptionForeground);
            white-space: nowrap;
            width: 160px;
        }
        .status-badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 12px;
            color: #fff;
            font-weight: 600;
            font-size: 0.85rem;
        }
        h2 {
            font-size: 1rem;
            margin: 24px 0 8px;
            color: var(--vscode-foreground);
        }
        pre {
            background: var(--vscode-textBlockQuote-background);
            border: 1px solid var(--vscode-panel-border);
            border-radius: 4px;
            padding: 12px;
            font-size: 0.85rem;
            white-space: pre-wrap;
            word-break: break-all;
            max-width: 700px;
        }
    </style>
</head>
<body>
    <h1>Pipeline Run Request — ${escHtml(req.pipelineName)}</h1>
    <table>
        ${tableRows}
    </table>
    <h2>Parameters</h2>
    <pre>${paramStr}</pre>
</body>
</html>`;
    }
}

module.exports = { PipelineRequestViewerProvider };
