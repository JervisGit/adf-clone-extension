'use strict';
// localRunPanel.js — VS Code command handler for "Run Pipeline Locally".
// Prompts user for parameter values, starts LocalPipelineRunner, and opens
// a live-updating run viewer WebView (beside the editor) that reuses the
// same visual style as the existing PipelineRunViewer.

const vscode = require('vscode');
const path   = require('path');
const fs     = require('fs');
const { LocalPipelineRunner }    = require('./activityEngine/localRunner');
const { validatePipeline }       = require('./activityEngine/engine');
const { NotebookSnapshotPanel }  = require('./notebookSnapshotPanel');

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
            await vscode.window.showErrorMessage(
                `Cannot run "${pipelineName}": ${totalErrors} validation error${totalErrors !== 1 ? 's' : ''} found.` +
                ` Fix all errors first (click the ✓ icon on the pipeline to see details).`,
                { modal: true }
            );
            return;
        }
        // ── End validation gate ────────────────────────────────────────────────

        // ── Copy format pre-run check (warn on unsupported formats before starting) ──
        const copyWarnings = _checkCopyFormatSupport(pipelineJson, workspaceRoot);
        if (copyWarnings.length > 0) {
            const detail = copyWarnings.map(w => `• ${w.activityName}: ${w.reason}`).join('\n');
            const choice = await vscode.window.showWarningMessage(
                `"${pipelineName}" has Copy ${copyWarnings.length === 1 ? 'activity' : 'activities'} that ` +
                `cannot run locally and will show as Failed:\n\n${detail}`,
                { modal: true },
                'Run Anyway', 'Cancel'
            );
            if (choice !== 'Run Anyway') return;
        }
        // ── End Copy format check ─────────────────────────────────────────────

        // Prompt user for parameters
        const parameters = await promptParameters(pipelineName, paramDefs);
        if (parameters === null) return; // user cancelled

        const runner = new LocalPipelineRunner(pipelineJson, parameters, workspaceRoot, this.context.extensionUri.fsPath);

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
            {
                enableScripts: true,
                retainContextWhenHidden: true,
                localResourceRoots: [
                    vscode.Uri.joinPath(this.context.extensionUri, 'media')
                ]
            }
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

        // Per-activity snapshot store so the user can re-open snapshots after they close
        const snapshots = new Map(); // activityName → snapshotData

        runner.on('notebookSnapshot', (snapshotData) => {
            snapshots.set(snapshotData.activityName, snapshotData);
            NotebookSnapshotPanel.show(this.context, snapshotData);
            panel.webview.postMessage({ command: 'snapshotAvailable', activityName: snapshotData.activityName });
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
                case 'openSnapshot': {
                    const snap = snapshots.get(msg.activityName);
                    if (snap) NotebookSnapshotPanel.show(this.context, snap);
                    break;
                }
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

        // Build activity icon URI map (same icon set as pipelineEditorV2)
        const iconTypes = [
            ['SynapseNotebook', 'notebook.png'],
            ['Copy', 'copy.png'],
            ['AppendVariable', 'append_var.png'],
            ['Delete', 'delete.png'],
            ['ExecutePipeline', 'execute_pipeline.png'],
            ['Fail', 'error2.png'],
            ['GetMetadata', 'get_metadata.png'],
            ['Lookup', 'lookup.png'],
            ['SqlServerStoredProcedure', 'stored_proc.png'],
            ['Script', 'script1.png'],
            ['SetVariable', 'set_var.png'],
            ['Validation', 'validation.png'],
            ['WebActivity', 'web.png'],
            ['WebHook', 'webhook.png'],
            ['Wait', 'wait.png'],
            ['Filter', 'filter.png'],
            ['IfCondition', 'if2.png'],
            ['ExecuteDataFlow', 'AzureDataFactoryDataFlowsCircle.svg'],
            ['SparkJob', 'sparkjob.png'],
        ];
        const activityIconsMap = {};
        for (const [type, file] of iconTypes) {
            const iconPath = vscode.Uri.joinPath(this.context.extensionUri, 'media', 'icons', file);
            if (fs.existsSync(iconPath.fsPath)) {
                activityIconsMap[type] = webview.asWebviewUri(iconPath).toString();
            }
        }

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy"
          content="default-src 'none'; style-src ${csp} 'unsafe-inline'; script-src ${csp} 'unsafe-inline'; img-src ${csp};">
    <link rel="stylesheet" href="${cssUri}">
    <title>▶ ${_escHtml(pipelineName)}</title>
</head>
<body>
    <script>
        var PIPELINE_NAME       = ${JSON.stringify(pipelineName)};
        var RUN_ID              = ${JSON.stringify(runId)};
        var PIPELINE_ACTIVITIES = ${JSON.stringify(pipelineActivities)};
        var ACTIVITY_ICONS      = ${JSON.stringify(activityIconsMap)};
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

/**
 * Scans all Copy activities in a pipeline (including nested in IfCondition/ForEach/etc.)
 * and returns warnings for source formats that cannot run locally.
 * @returns {{ activityName: string, reason: string }[]}
 */
function _checkCopyFormatSupport(pipelineJson, workspaceRoot) {
    if (!workspaceRoot) return [];
    const SUPPORTED_FOR_SQL  = new Set(['DelimitedText', 'Json', 'Parquet', 'Excel', 'Xml', '']);
    // SQL dataset types are also supported (SQL→SQL copy)
    const SQL_SINK_TYPES     = new Set(['AzureSqlTable', 'AzureSqlDWTable', 'AzureSqlMITable']);
    const SQL_SRC_TYPES      = new Set(['AzureSqlTable', 'AzureSqlDatabaseTable', 'AzureSQLDWTable', 'SqlServerTable',
                                         'AzureSqlDWTable', 'AzureSqlMITable']);
    const warnings = [];

    function scanActivities(activities) {
        for (const act of (activities ?? [])) {
            if (act.type === 'Copy') {
                const tp      = act.typeProperties ?? {};
                const srcName = tp.source?.dataset?.referenceName ?? act.inputs?.[0]?.referenceName;
                const sinkName = tp.sink?.dataset?.referenceName  ?? act.outputs?.[0]?.referenceName;
                if (srcName && sinkName) {
                    try {
                        const srcDsFile  = path.join(workspaceRoot, 'dataset', `${srcName}.json`);
                        const sinkDsFile = path.join(workspaceRoot, 'dataset', `${sinkName}.json`);
                        const srcType    = fs.existsSync(srcDsFile)
                            ? (JSON.parse(fs.readFileSync(srcDsFile,  'utf8'))?.properties?.type ?? '') : '';
                        const sinkType   = fs.existsSync(sinkDsFile)
                            ? (JSON.parse(fs.readFileSync(sinkDsFile, 'utf8'))?.properties?.type ?? '') : '';
                        if (SQL_SINK_TYPES.has(sinkType) && !SUPPORTED_FOR_SQL.has(srcType) && !SQL_SRC_TYPES.has(srcType)) {
                            warnings.push({ activityName: act.name,
                                reason: `"${srcType}" → SQL — format not supported in local run (needs Spark engine)` });
                        }
                    } catch { /* ignore read errors */ }
                }
            }
            // Recurse into container activities
            scanActivities(act.typeProperties?.activities);
            scanActivities(act.typeProperties?.ifTrueActivities);
            scanActivities(act.typeProperties?.ifFalseActivities);
            scanActivities(act.typeProperties?.defaultActivities);
            for (const c of (act.typeProperties?.cases ?? [])) scanActivities(c.activities);
        }
    }
    scanActivities(pipelineJson?.properties?.activities);
    return warnings;
}

module.exports = { LocalRunPanel };
