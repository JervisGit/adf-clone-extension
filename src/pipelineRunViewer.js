const vscode = require('vscode');
const { ADLSRestClient } = require('./adlsRestClient');

class PipelineRunViewerProvider {
    static panels = new Map(); // Map<runFolder, panel>

    constructor(context) {
        this.context = context;
    }

    async openPipelineRun(storageAccountName, containerName, runFolder, runInfo) {
        const column = vscode.window.activeTextEditor
            ? vscode.window.activeTextEditor.viewColumn
            : undefined;

        // Check if panel already exists for this run
        if (PipelineRunViewerProvider.panels.has(runFolder)) {
            PipelineRunViewerProvider.panels.get(runFolder).reveal(column);
            return;
        }

        // Create a new panel
        const panel = vscode.window.createWebviewPanel(
            'pipelineRunViewer',
            `Pipeline Run: ${runInfo.pipelineName}`,
            column || vscode.ViewColumn.One,
            {
                enableScripts: true,
                retainContextWhenHidden: true
            }
        );

        // Store panel
        PipelineRunViewerProvider.panels.set(runFolder, panel);

        // Handle panel disposal
        panel.onDidDispose(() => {
            PipelineRunViewerProvider.panels.delete(runFolder);
        }, null, this.context.subscriptions);

        // Load activity runs from ADLS
        try {
            const client = new ADLSRestClient(storageAccountName);
            const activityRunsPath = `pipeline-runs/${runFolder}/activity_runs.json`;
            const content = await client.readFile(containerName, activityRunsPath);
            const activityRuns = JSON.parse(content);

            // Extract activities array
            const activities = activityRuns.value || activityRuns;

            // Set webview content
            panel.webview.html = this.getHtmlContent(panel.webview, activities, runInfo);

            // Handle messages from webview
            panel.webview.onDidReceiveMessage(
                message => {
                    switch (message.command) {
                        case 'showDetails':
                            this.showActivityDetails(message.activity, 'details');
                            break;
                        case 'showInput':
                            this.showActivityDetails(message.activity, 'input');
                            break;
                        case 'showOutput':
                            this.showActivityDetails(message.activity, 'output');
                            break;
                    }
                },
                undefined,
                this.context.subscriptions
            );

        } catch (error) {
            vscode.window.showErrorMessage(`Failed to load pipeline run: ${error.message}`);
            panel.dispose();
        }
    }

    async showActivityDetails(activity, section) {
        let content = '';
        let title = '';

        if (section === 'input') {
            title = `${activity.activityName} - Input`;
            content = JSON.stringify(activity.input || {}, null, 2);
        } else if (section === 'output') {
            title = `${activity.activityName} - Output`;
            content = JSON.stringify(activity.output || {}, null, 2);
        } else {
            title = `${activity.activityName} - Full Details`;
            content = JSON.stringify(activity, null, 2);
        }

        const doc = await vscode.workspace.openTextDocument({
            content: content,
            language: 'json'
        });
        await vscode.window.showTextDocument(doc, { preview: false });
    }

    getHtmlContent(webview, activities, runInfo) {
        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Run Viewer</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: var(--vscode-font-family);
            color: var(--vscode-foreground);
            background-color: var(--vscode-editor-background);
            padding: 20px;
            overflow-x: auto;
        }

        .header {
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 1px solid var(--vscode-panel-border);
        }

        .header h1 {
            font-size: 20px;
            margin-bottom: 10px;
        }

        .header-info {
            display: flex;
            gap: 30px;
            color: var(--vscode-descriptionForeground);
            font-size: 13px;
        }

        .header-info-item {
            display: flex;
            gap: 5px;
        }

        .header-info-label {
            font-weight: 600;
        }

        .canvas-container {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
            padding: 20px;
            background-color: var(--vscode-editor-background);
            border: 1px solid var(--vscode-panel-border);
            border-radius: 4px;
            min-height: 200px;
            overflow-x: auto;
        }

        .activity-box {
            background-color: var(--vscode-input-background);
            border: 2px solid var(--vscode-input-border);
            border-radius: 6px;
            padding: 12px;
            min-width: 180px;
            max-width: 220px;
            cursor: pointer;
            transition: all 0.2s;
            flex-shrink: 0;
        }

        .activity-box:hover {
            border-color: var(--vscode-focusBorder);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
        }

        .activity-box.selected {
            border-color: var(--vscode-focusBorder);
            background-color: var(--vscode-list-activeSelectionBackground);
        }

        .activity-box.status-succeeded {
            border-left: 4px solid #4caf50;
        }

        .activity-box.status-failed {
            border-left: 4px solid #f44336;
        }

        .activity-box.status-inprogress,
        .activity-box.status-queued {
            border-left: 4px solid #2196f3;
        }

        .activity-icon {
            font-size: 24px;
            margin-bottom: 8px;
            text-align: center;
        }

        .activity-name {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 4px;
            word-wrap: break-word;
        }

        .activity-type {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
            margin-bottom: 4px;
        }

        .activity-status {
            font-size: 11px;
            margin-top: 6px;
            padding: 2px 6px;
            border-radius: 3px;
            display: inline-block;
        }

        .activity-status.succeeded {
            background-color: rgba(76, 175, 80, 0.2);
            color: #4caf50;
        }

        .activity-status.failed {
            background-color: rgba(244, 67, 54, 0.2);
            color: #f44336;
        }

        .activity-status.inprogress,
        .activity-status.queued {
            background-color: rgba(33, 150, 243, 0.2);
            color: #2196f3;
        }

        .bottom-panel {
            background-color: var(--vscode-input-background);
            border: 1px solid var(--vscode-panel-border);
            border-radius: 4px;
            padding: 15px;
        }

        .panel-title {
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 15px;
        }

        .activity-details {
            display: none;
        }

        .activity-details.active {
            display: block;
        }

        .details-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }

        .detail-item {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .detail-label {
            font-size: 12px;
            color: var(--vscode-descriptionForeground);
            font-weight: 600;
        }

        .detail-value {
            font-size: 13px;
            padding: 6px 8px;
            background-color: var(--vscode-editor-background);
            border: 1px solid var(--vscode-input-border);
            border-radius: 3px;
            word-break: break-all;
        }

        .action-buttons {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }

        .btn {
            padding: 8px 16px;
            font-size: 13px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            background-color: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
            transition: background-color 0.2s;
        }

        .btn:hover {
            background-color: var(--vscode-button-hoverBackground);
        }

        .btn-secondary {
            background-color: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
        }

        .btn-secondary:hover {
            background-color: var(--vscode-button-secondaryHoverBackground);
        }

        .error-info {
            margin-top: 15px;
            padding: 12px;
            background-color: rgba(244, 67, 54, 0.1);
            border-left: 4px solid #f44336;
            border-radius: 3px;
        }

        .error-title {
            font-weight: 600;
            margin-bottom: 8px;
            color: #f44336;
        }

        .error-message {
            font-size: 13px;
            color: var(--vscode-foreground);
            white-space: pre-wrap;
        }

        .empty-state {
            text-align: center;
            padding: 40px;
            color: var(--vscode-descriptionForeground);
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>${runInfo.pipelineName}</h1>
        <div class="header-info">
            <div class="header-info-item">
                <span class="header-info-label">Run ID:</span>
                <span>${runInfo.runId}</span>
            </div>
            <div class="header-info-item">
                <span class="header-info-label">Timestamp:</span>
                <span>${this.formatTimestamp(runInfo.timestamp)}</span>
            </div>
            <div class="header-info-item">
                <span class="header-info-label">Activities:</span>
                <span>${activities.length}</span>
            </div>
        </div>
    </div>

    <div class="canvas-container">
        ${activities.length === 0 
            ? '<div class="empty-state">No activities found in this pipeline run</div>'
            : activities.map((activity, index) => this.renderActivityBox(activity, index)).join('')
        }
    </div>

    <div class="bottom-panel">
        <div class="panel-title">Activity Details</div>
        ${activities.map((activity, index) => this.renderActivityDetails(activity, index)).join('')}
        ${activities.length === 0 
            ? '<div class="empty-state">Select an activity to view details</div>'
            : ''
        }
    </div>

    <script>
        const vscode = acquireVsCodeApi();
        let selectedActivityIndex = null;

        function selectActivity(index) {
            // Remove previous selection
            document.querySelectorAll('.activity-box').forEach(box => {
                box.classList.remove('selected');
            });
            document.querySelectorAll('.activity-details').forEach(panel => {
                panel.classList.remove('active');
            });

            // Add new selection
            const box = document.querySelector(\`.activity-box[data-index="\${index}"]\`);
            const details = document.querySelector(\`.activity-details[data-index="\${index}"]\`);
            
            if (box && details) {
                box.classList.add('selected');
                details.classList.add('active');
                selectedActivityIndex = index;
            }
        }

        function showInput(index) {
            const activity = ${JSON.stringify(activities)}[index];
            vscode.postMessage({
                command: 'showInput',
                activity: activity
            });
        }

        function showOutput(index) {
            const activity = ${JSON.stringify(activities)}[index];
            vscode.postMessage({
                command: 'showOutput',
                activity: activity
            });
        }

        function showDetails(index) {
            const activity = ${JSON.stringify(activities)}[index];
            vscode.postMessage({
                command: 'showDetails',
                activity: activity
            });
        }

        // Select first activity by default
        if (${activities.length} > 0) {
            selectActivity(0);
        }
    </script>
</body>
</html>`;
    }

    renderActivityBox(activity, index) {
        const statusClass = activity.status.toLowerCase().replace(/\s+/g, '');
        const icon = this.getActivityIcon(activity.activityType);
        
        return `
        <div class="activity-box status-${statusClass}" data-index="${index}" onclick="selectActivity(${index})">
            <div class="activity-icon">${icon}</div>
            <div class="activity-name">${activity.activityName}</div>
            <div class="activity-type">${activity.activityType}</div>
            <div class="activity-status ${statusClass}">${activity.status}</div>
        </div>`;
    }

    renderActivityDetails(activity, index) {
        const hasError = activity.status === 'Failed' && activity.error && activity.error.message;
        const durationSec = activity.durationInMs ? (activity.durationInMs / 1000).toFixed(2) : 'N/A';

        return `
        <div class="activity-details" data-index="${index}">
            <div class="details-grid">
                <div class="detail-item">
                    <div class="detail-label">Activity Name</div>
                    <div class="detail-value">${activity.activityName}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Activity Type</div>
                    <div class="detail-value">${activity.activityType}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Status</div>
                    <div class="detail-value">${activity.status}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Run Start</div>
                    <div class="detail-value">${this.formatTimestamp(new Date(activity.activityRunStart))}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Run End</div>
                    <div class="detail-value">${this.formatTimestamp(new Date(activity.activityRunEnd))}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Duration</div>
                    <div class="detail-value">${durationSec} seconds</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Activity Run ID</div>
                    <div class="detail-value">${activity.activityRunId}</div>
                </div>
            </div>

            ${hasError ? `
            <div class="error-info">
                <div class="error-title">❌ Error Details</div>
                <div class="error-message"><strong>Error Code:</strong> ${activity.error.errorCode || 'N/A'}</div>
                <div class="error-message"><strong>Message:</strong> ${activity.error.message || 'N/A'}</div>
                <div class="error-message"><strong>Failure Type:</strong> ${activity.error.failureType || 'N/A'}</div>
            </div>
            ` : ''}

            <div class="action-buttons">
                <button class="btn" onclick="showInput(${index})">📥 View Input</button>
                <button class="btn" onclick="showOutput(${index})">📤 View Output</button>
                <button class="btn btn-secondary" onclick="showDetails(${index})">📋 View Full Details</button>
            </div>
        </div>`;
    }

    getActivityIcon(activityType) {
        const icons = {
            'Copy': '📄',
            'SynapseNotebook': '📓',
            'Notebook': '📓',
            'ExecutePipeline': '🔄',
            'IfCondition': '🔀',
            'ForEach': '🔁',
            'Wait': '⏳',
            'Until': '🔁',
            'Switch': '🔀',
            'Lookup': '🔍',
            'GetMetadata': 'ℹ️',
            'Delete': '🗑️',
            'SqlServerStoredProcedure': '⚙️',
            'Script': '📝',
            'WebActivity': '🌐',
            'Validation': '✅',
            'Filter': '🔽',
            'Fail': '❌',
            'SetVariable': '📌',
            'AppendVariable': '➕'
        };
        
        return icons[activityType] || '⚡';
    }

    formatTimestamp(date) {
        if (!date || !(date instanceof Date)) {
            return 'N/A';
        }
        
        // Convert to Singapore time (UTC+8)
        const sgtTime = new Date(date.getTime() + (8 * 60 * 60 * 1000));
        
        const year = sgtTime.getUTCFullYear();
        const month = String(sgtTime.getUTCMonth() + 1).padStart(2, '0');
        const day = String(sgtTime.getUTCDate()).padStart(2, '0');
        const hours = String(sgtTime.getUTCHours()).padStart(2, '0');
        const minutes = String(sgtTime.getUTCMinutes()).padStart(2, '0');
        const seconds = String(sgtTime.getUTCSeconds()).padStart(2, '0');
        
        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} SGT`;
    }
}

module.exports = { PipelineRunViewerProvider };
