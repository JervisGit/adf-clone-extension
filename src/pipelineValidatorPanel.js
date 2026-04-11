'use strict';
// pipelineValidatorPanel.js — VS Code command handler for "Validate Pipeline".
// Opens a WebView panel beside the active editor showing validation results.
// One panel per pipeline name (re-validates in place on subsequent calls).

const vscode = require('vscode');
const path   = require('path');
const fs     = require('fs');
const { validatePipeline } = require('./activityEngine/engine');

class PipelineValidatorPanel {
    static panels = new Map(); // Map<pipelineName, vscode.WebviewPanel>

    constructor(context) {
        this.context = context;
    }

    /**
     * Called from the command handler.
     * item may be: a FileItem from the tree (has .filePath), or undefined (use active editor file).
     */
    async validate(item) {
        let filePath = item?.filePath;
        if (!filePath) {
            filePath = vscode.window.activeTextEditor?.document?.uri?.fsPath;
        }
        if (!filePath || !filePath.endsWith('.json')) {
            vscode.window.showErrorMessage('Please select or open a pipeline JSON file to validate.');
            return;
        }

        let pipelineJson;
        try {
            pipelineJson = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        } catch (err) {
            vscode.window.showErrorMessage(`Failed to parse pipeline file: ${err.message}`);
            return;
        }

        const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri?.fsPath;
        const result = validatePipeline(pipelineJson, workspaceRoot);
        const pipelineName = pipelineJson?.name ?? path.basename(filePath, '.json');
        const totalErrors = result.pipelineErrors.length +
            Object.values(result.activityErrors).reduce((s, e) => s + e.length, 0);

        this._openOrUpdatePanel(pipelineName, filePath, result, totalErrors);

        if (totalErrors === 0) {
            vscode.window.showInformationMessage(`Pipeline "${pipelineName}" is valid — no errors found.`);
        } else {
            vscode.window.showWarningMessage(
                `Pipeline "${pipelineName}" has ${totalErrors} validation error${totalErrors === 1 ? '' : 's'}.`
            );
        }
    }

    _openOrUpdatePanel(pipelineName, filePath, result, totalErrors) {
        const existingPanel = PipelineValidatorPanel.panels.get(pipelineName);
        if (existingPanel) {
            existingPanel.reveal(vscode.ViewColumn.Beside);
            existingPanel.webview.postMessage({ command: 'updateResults', result, totalErrors, timestamp: new Date().toISOString() });
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'pipelineValidator',
            `Validate: ${pipelineName}`,
            vscode.ViewColumn.Beside,
            { enableScripts: true, retainContextWhenHidden: true }
        );
        PipelineValidatorPanel.panels.set(pipelineName, panel);

        panel.onDidDispose(() => {
            PipelineValidatorPanel.panels.delete(pipelineName);
        }, null, this.context.subscriptions);

        panel.webview.html = this._getHtml(panel.webview, pipelineName, result, totalErrors);

        panel.webview.onDidReceiveMessage(async (msg) => {
            if (msg.command === 'revalidate') {
                let pipelineJson;
                try { pipelineJson = JSON.parse(fs.readFileSync(filePath, 'utf8')); }
                catch (err) { vscode.window.showErrorMessage(`Failed to re-read file: ${err.message}`); return; }
                const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri?.fsPath;
                const newResult = validatePipeline(pipelineJson, workspaceRoot);
                const newErrors = newResult.pipelineErrors.length +
                    Object.values(newResult.activityErrors).reduce((s, e) => s + e.length, 0);
                panel.webview.postMessage({ command: 'updateResults', result: newResult, totalErrors: newErrors, timestamp: new Date().toISOString() });
            }
        }, undefined, this.context.subscriptions);
    }

    _getHtml(webview, pipelineName, result, totalErrors) {
        const mediaUri = (name) => webview.asWebviewUri(
            vscode.Uri.joinPath(this.context.extensionUri, 'media', name)
        );
        const cssUri = mediaUri('pipelineValidation.css');
        const jsUri  = mediaUri('pipelineValidation.js');
        const csp    = webview.cspSource;
        const initialData = JSON.stringify({ result, totalErrors, pipelineName, timestamp: new Date().toISOString() });

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy"
          content="default-src 'none'; style-src ${csp} 'unsafe-inline'; script-src ${csp} 'unsafe-inline';">
    <link rel="stylesheet" href="${cssUri}">
    <title>Validate: ${_escHtml(pipelineName)}</title>
</head>
<body>
    <script>var INITIAL_DATA = ${initialData};<\/script>
    <div id="app"></div>
    <script src="${jsUri}"></script>
</body>
</html>`;
    }
}

function _escHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

module.exports = { PipelineValidatorPanel };
