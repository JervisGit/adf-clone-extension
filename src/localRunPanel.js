'use strict';
// localRunPanel.js — VS Code command handler for "Run Pipeline Locally".
// Prompts user for parameter values, starts LocalPipelineRunner, and opens
// a live-updating run viewer WebView (beside the editor) that reuses the
// same visual style as the existing PipelineRunViewer.

const vscode = require('vscode');
const path   = require('path');
const fs     = require('fs');
const { LocalPipelineRunner } = require('./activityEngine/localRunner');
const { validatePipeline }    = require('./activityEngine/engine');

class LocalRunPanel {
    static panels = new Map(); // Map<runId, vscode.WebviewPanel>

    constructor(context) {
        this.context = context;
    }

    /**
     * Called from the command handler.
     * item may be a FileItem from the tree (has .filePath), or undefined (active editor).
     */
    async runPipeline(item) {
        let filePath = item?.filePath;
        if (!filePath) {
            filePath = vscode.window.activeTextEditor?.document?.uri?.fsPath;
        }
        if (!filePath || !filePath.endsWith('.json')) {
            vscode.window.showErrorMessage('Please select or open a pipeline JSON file to run.');
            return;
        }

        let pipelineJson;
        try {
            pipelineJson = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        } catch (err) {
            vscode.window.showErrorMessage(`Failed to parse pipeline file: ${err.message}`);
            return;
        }

        const pipelineName = pipelineJson?.name ?? path.basename(filePath, '.json');
        const paramDefs    = pipelineJson?.properties?.parameters ?? {};
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri?.fsPath;

        // ── Validation gate ────────────────────────────────────────────────────
        const valResult = validatePipeline(pipelineJson, workspaceRoot);
        const pipelineErrorCount  = valResult.pipelineErrors?.length ?? 0;
        const activityErrorCount  = Object.values(valResult.activityErrors ?? {})
            .reduce((n, errs) => n + errs.length, 0);
        const totalErrors = pipelineErrorCount + activityErrorCount;

        if (totalErrors > 0) {
            const choice = await vscode.window.showWarningMessage(
                `"${pipelineName}" has ${totalErrors} validation error${totalErrors !== 1 ? 's' : ''}. Running it may produce unexpected results.`,
                { modal: true },
                'Run Anyway',
                'Cancel'
            );
            if (choice !== 'Run Anyway') return;
        }
        // ── End validation gate ────────────────────────────────────────────────

        // Prompt user for parameters
        const parameters = await promptParameters(pipelineName, paramDefs);
        if (parameters === null) return; // user cancelled

        const runner = new LocalPipelineRunner(pipelineJson, parameters, workspaceRoot);

        // Extract a lightweight activity summary for the webview canvas layout
        const pipelineActivities = (pipelineJson?.properties?.activities ?? []).map(a => ({
            name:      a.name,
            type:      a.type,
            dependsOn: (a.dependsOn ?? []).map(d => ({ activity: d.activity })),
        }));

        this._openPanel(runner, pipelineName, pipelineActivities);
        // runner.run() is called by the panel once the webview sends 'ready'
    }

    // ─── WebView panel ────────────────────────────────────────────────────────

    _openPanel(runner, pipelineName, pipelineActivities) {
        const panel = vscode.window.createWebviewPanel(
            'localRunViewer',
            `▶ ${pipelineName}`,
            vscode.ViewColumn.Beside,
            { enableScripts: true, retainContextWhenHidden: true }
        );

        LocalRunPanel.panels.set(runner.runId, panel);
        panel.onDidDispose(() => {
            LocalRunPanel.panels.delete(runner.runId);
            runner.cancel();
        }, null, this.context.subscriptions);

        // Deliver initial (empty) HTML immediately so the panel is visible
        panel.webview.html = this._getHtml(panel.webview, pipelineName, runner.runId, pipelineActivities);

        // Forward runner events to the webview
        runner.on('activityUpdate', (update) => {
            panel.webview.postMessage({ command: 'activityUpdate', ...update });
        });
        runner.on('pipelineEnd', (event) => {
            panel.webview.postMessage({ command: 'pipelineEnd', ...event });
        });

        // Handle messages from webview (cancel, show details, and ready signal)
        panel.webview.onDidReceiveMessage(async (msg) => {
            switch(msg.command) {
                case 'ready':
                    // Webview script is loaded — safe to start the runner now
                    runner.run().catch(() => {});
                    break;
                case 'cancel':
                    runner.cancel();
                    break;
            }
        }, undefined, this.context.subscriptions);
    }

    _getHtml(webview, pipelineName, runId, pipelineActivities) {
        const mediaUri = (name) => webview.asWebviewUri(
            vscode.Uri.joinPath(this.context.extensionUri, 'media', name)
        );
        const cssUri = mediaUri('localRunViewer.css');
        const jsUri  = mediaUri('localRunViewer.js');
        const csp    = webview.cspSource;

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy"
          content="default-src 'none'; style-src ${csp} 'unsafe-inline'; script-src ${csp} 'unsafe-inline';">
    <link rel="stylesheet" href="${cssUri}">
    <title>▶ ${_escHtml(pipelineName)}</title>
</head>
<body>
    <script>
        var PIPELINE_NAME       = ${JSON.stringify(pipelineName)};
        var RUN_ID              = ${JSON.stringify(runId)};
        var PIPELINE_ACTIVITIES = ${JSON.stringify(pipelineActivities)};
    </script>
    <div id="app"></div>
    <div id="popover-root"></div>
    <script src="${jsUri}"></script>
</body>
</html>`;
    }
}

// ─── Parameter prompt ─────────────────────────────────────────────────────────
// Reuses the same pattern as pipelineRequestProvider._promptParameters.
// Returns { paramName: value } or null if user cancelled.

async function promptParameters(pipelineName, paramDefs) {
    const paramNames = Object.keys(paramDefs);
    const parameters = {};

    for (const name of paramNames) {
        const def  = paramDefs[name];
        const type = (def.type || 'String').toLowerCase();
        const defaultValue = def.defaultValue !== undefined ? String(def.defaultValue) : '';

        if (type === 'bool') {
            const picked = await vscode.window.showQuickPick(
                [
                    { label: 'true',  description: 'Boolean true' },
                    { label: 'false', description: 'Boolean false' },
                ],
                { placeHolder: `${pipelineName} › ${name} (Bool)${defaultValue ? ` — default: ${defaultValue}` : ''}` }
            );
            if (picked === undefined) return null;
            parameters[name] = picked.label === 'true';
        } else {
            const value = await vscode.window.showInputBox({
                prompt: `${pipelineName} › ${name} (${def.type || 'String'})`,
                placeHolder: defaultValue || `Enter value for ${name}`,
                value: defaultValue,
                validateInput: (val) => {
                    if (!val.trim()) return null; // allow empty (use default)
                    if (type === 'int'    && isNaN(parseInt(val, 10)))  return 'Must be an integer';
                    if (type === 'float'  && isNaN(parseFloat(val)))    return 'Must be a number';
                    if ((type === 'array' || type === 'object') && val.trim()) {
                        try { JSON.parse(val); } catch { return 'Must be valid JSON'; }
                    }
                    return null;
                }
            });
            if (value === undefined) return null; // user pressed Escape

            if (type === 'int') {
                parameters[name] = value.trim() ? parseInt(value, 10)  : (def.defaultValue ?? 0);
            } else if (type === 'float') {
                parameters[name] = value.trim() ? parseFloat(value)     : (def.defaultValue ?? 0.0);
            } else if (type === 'array' || type === 'object') {
                parameters[name] = value.trim() ? JSON.parse(value) : (def.defaultValue ?? (type === 'array' ? [] : {}));
            } else {
                parameters[name] = value;
            }
        }
    }
    return parameters;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function _escHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

module.exports = { LocalRunPanel };
