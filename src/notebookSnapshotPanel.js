'use strict';
// notebookSnapshotPanel.js — Opens a VS Code webview panel showing notebook
// cell inputs and Synapse-returned outputs in a notebook-style view.

const vscode = require('vscode');

class NotebookSnapshotPanel {
    /**
     * Open a new notebook snapshot panel.
     * @param {vscode.ExtensionContext} context
     * @param {object} snapshotData  { notebookName, generatedAt, evaluatedParams, cells }
     */
    static show(context, snapshotData) {
        const panel = vscode.window.createWebviewPanel(
            'notebookSnapshot',
            `\uD83D\uDCD3 ${snapshotData.notebookName}`,
            vscode.ViewColumn.Beside,
            { enableScripts: true, retainContextWhenHidden: true }
        );

        const mediaUri = (name) =>
            panel.webview.asWebviewUri(vscode.Uri.joinPath(context.extensionUri, 'media', name));

        const cssUri = mediaUri('notebookSnapshotViewer.css');
        const jsUri  = mediaUri('notebookSnapshotViewer.js');
        const csp    = panel.webview.cspSource;

        panel.webview.html = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy"
          content="default-src 'none'; style-src ${csp} 'unsafe-inline'; script-src ${csp} 'unsafe-inline';">
    <link rel="stylesheet" href="${cssUri}">
    <title>\uD83D\uDCD3 ${_esc(snapshotData.notebookName)}</title>
</head>
<body>
    <div id="app"><div class="loading">Loading notebook snapshot\u2026</div></div>
    <script src="${jsUri}"></script>
</body>
</html>`;

        panel.webview.onDidReceiveMessage((msg) => {
            if (msg.command === 'ready') {
                panel.webview.postMessage({ command: 'loadSnapshot', ...snapshotData });
            }
        });
    }
}

function _esc(s) {
    return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

module.exports = { NotebookSnapshotPanel };
