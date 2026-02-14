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
                        case 'viewNotebook':
                            this.openNotebookSnapshot(storageAccountName, containerName, runFolder, message.activity);
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

        if (section === 'input') {
            content = JSON.stringify(activity.input || {}, null, 2);
        } else if (section === 'output') {
            content = JSON.stringify(activity.output || {}, null, 2);
        } else {
            content = JSON.stringify(activity, null, 2);
        }

        const doc = await vscode.workspace.openTextDocument({
            content: content,
            language: 'json'
        });
        await vscode.window.showTextDocument(doc, { preview: false });
    }

    async openNotebookSnapshot(storageAccountName, containerName, runFolder, activity) {
        try {
            const client = new ADLSRestClient(storageAccountName);
            
            // Show progress
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: `Loading notebook snapshot for ${activity.activityName}...`,
                cancellable: false
            }, async (progress) => {
                progress.report({ increment: 30, message: 'Fetching from ADLS...' });
                
                const snapshot = await client.getNotebookSnapshot(containerName, runFolder, activity.activityRunId);
                
                progress.report({ increment: 30, message: 'Processing notebook...' });
                
                // Extract notebook content from snapshot
                const notebookContent = snapshot.result?.snapshot?.notebookContent?.properties;
                if (!notebookContent || !notebookContent.cells) {
                    throw new Error('Invalid notebook snapshot format - missing cells');
                }

                // Process cells to improve display
                const processedCells = notebookContent.cells.map(cell => {
                    const processedCell = { ...cell };
                    
                    // Process outputs to show execution timing
                    if (processedCell.outputs && Array.isArray(processedCell.outputs)) {
                        // Check if cell has error output
                        const hasError = processedCell.outputs.some(output => output.output_type === 'error');
                        
                        processedCell.outputs = processedCell.outputs.map(output => {
                            // Look for statement metadata
                            if (output.output_type === 'display_data' && 
                                output.data?.['application/vnd.livy.statement-meta+json']) {
                                
                                const meta = output.data['application/vnd.livy.statement-meta+json'];
                                const startTime = meta.execution_start_time;
                                const finishTime = meta.execution_finish_time;
                                
                                // Calculate duration
                                let durationText = '';
                                if (startTime && finishTime) {
                                    const start = new Date(startTime);
                                    const finish = new Date(finishTime);
                                    const durationMs = finish - start;
                                    
                                    if (durationMs < 1000) {
                                        durationText = `${durationMs} ms`;
                                    } else {
                                        durationText = `${(durationMs / 1000).toFixed(2)} s`;
                                    }
                                }
                                
                                // Create readable status message based on whether cell has error
                                const statusText = hasError ? 'Command failed' : 'Command executed';
                                const timing = durationText ? ` in ${durationText}` : '';
                                
                                // Replace the text/plain output
                                return {
                                    ...output,
                                    data: {
                                        ...output.data,
                                        'text/plain': `${statusText}${timing}`
                                    }
                                };
                            }
                            
                            return output;
                        });
                        
                        // Add parameter indicator as output (not modifying source)
                        if (cell.metadata?.tags?.includes('parameters') || 
                            cell.metadata?.tags?.includes('parameters_overwritten')) {
                            const paramType = cell.metadata.tags.includes('parameters') ? 'Parameters' : 'Pipeline Parameters';
                            
                            // Insert parameter indicator at the beginning of outputs
                            processedCell.outputs.unshift({
                                output_type: 'display_data',
                                data: {
                                    'text/plain': `[${paramType}]`,
                                    'text/html': `<div style="color: #888; font-size: 0.9em; font-style: italic; margin-bottom: 4px;">${paramType}</div>`
                                },
                                metadata: {}
                            });
                        }
                    } else {
                        // If no outputs but has parameter tag, create outputs array with indicator
                        if (cell.metadata?.tags?.includes('parameters') || 
                            cell.metadata?.tags?.includes('parameters_overwritten')) {
                            const paramType = cell.metadata.tags.includes('parameters') ? 'Parameters' : 'Pipeline Parameters';
                            
                            processedCell.outputs = [{
                                output_type: 'display_data',
                                data: {
                                    'text/plain': `[${paramType}]`,
                                    'text/html': `<div style="color: #888; font-size: 0.9em; font-style: italic; margin-bottom: 4px;">${paramType}</div>`
                                },
                                metadata: {}
                            }];
                        }
                    }
                    
                    return processedCell;
                });

                // Create .ipynb format
                const notebook = {
                    cells: processedCells,
                    metadata: notebookContent.metadata || {
                        language_info: {
                            name: 'python'
                        }
                    },
                    nbformat: notebookContent.nbformat || 4,
                    nbformat_minor: notebookContent.nbformat_minor || 2
                };

                progress.report({ increment: 30, message: 'Opening notebook...' });

                // Create a unique filename
                const notebookName = snapshot.result?.snapshot?.notebook || activity.activityName;
                const timestamp = new Date().getTime();
                
                // Use workspace folder or temp directory
                const os = require('os');
                const path = require('path');
                const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
                const baseDir = workspaceFolder || os.tmpdir();
                const fileName = `${notebookName}_${activity.activityRunId.substring(0, 8)}_${timestamp}.ipynb`;
                const tempPath = vscode.Uri.file(path.join(baseDir, fileName));

                // Write notebook to a temporary file
                // eslint-disable-next-line no-undef
                const notebookData = Buffer.from(JSON.stringify(notebook, null, 2));
                await vscode.workspace.fs.writeFile(tempPath, notebookData);
                
                // Open as notebook
                const notebookDoc = await vscode.workspace.openNotebookDocument(tempPath);
                await vscode.window.showNotebookDocument(notebookDoc, { preview: false });
                
                progress.report({ increment: 10, message: 'Done!' });
                
                // Show info message
                vscode.window.showInformationMessage(
                    `Notebook snapshot loaded: ${notebookName}. Note: This is a read-only snapshot from the pipeline run.`
                );
            });

        } catch (error) {
            const errorMsg = error.message || 'Unknown error';
            
            // Provide helpful error messages
            if (errorMsg.includes('Notebooks folder not found')) {
                vscode.window.showErrorMessage(
                    `Notebook snapshot not available: The notebooks folder was not found for this pipeline run. ` +
                    `This usually means the notebook activity didn't complete successfully or snapshots weren't captured.`,
                    'OK'
                );
            } else if (errorMsg.includes('PathNotFound') || errorMsg.includes('does not exist')) {
                vscode.window.showErrorMessage(
                    `Notebook snapshot file not found: ${activity.activityRunId}.json. ` +
                    `The snapshot may not have been saved for this activity run.`,
                    'OK'
                );
            } else {
                vscode.window.showErrorMessage(`Failed to load notebook snapshot: ${errorMsg}`, 'OK');
            }
        }
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
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
            color: var(--vscode-foreground);
            background-color: var(--vscode-editor-background);
            display: flex;
            flex-direction: column;
            height: 100vh;
            overflow: hidden;
        }

        .header {
            padding: 16px 20px;
            border-bottom: 1px solid var(--vscode-panel-border);
            background: var(--vscode-sideBar-background);
        }

        .header h1 {
            font-size: 18px;
            margin-bottom: 8px;
            font-weight: 600;
        }

        .header-info {
            display: flex;
            gap: 24px;
            color: var(--vscode-descriptionForeground);
            font-size: 12px;
        }

        .header-info-item {
            display: flex;
            gap: 6px;
        }

        .header-info-label {
            font-weight: 600;
        }

        .canvas-wrapper {
            flex: 1;
            overflow: auto;
            background: var(--vscode-editor-background);
            border-bottom: 1px solid var(--vscode-panel-border);
            padding: 20px;
        }

        .canvas-container {
            display: flex;
            gap: 16px;
        }

        /* Activity Box - Matching pipeline editor style */
        .activity-box {
            width: 180px;
            background: #f0f0f0;
            border: 1px solid #c8c8c8;
            border-radius: 3px;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
            cursor: pointer;
            flex-shrink: 0;
            user-select: none;
            transition: all 0.2s;
        }

        .activity-box:hover {
            background: #e8e8e8;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.12);
        }

        .activity-box.selected {
            background: #ffffff;
            border: 1px solid #0078d4;
            box-shadow: 0 4px 12px rgba(0, 120, 212, 0.2);
        }

        .activity-box.status-failed {
            border-left: 3px solid #f44336;
        }

        .activity-box.status-succeeded {
            border-left: 3px solid #4caf50;
        }

        .activity-header {
            padding: 4px 8px;
            background: rgba(0, 0, 0, 0.05);
            border-bottom: 1px solid rgba(0, 0, 0, 0.08);
            border-radius: 3px 3px 0 0;
        }

        .activity-box.selected .activity-header {
            background: #0078d4;
            border-bottom: none;
        }

        .activity-type-label {
            font-size: 11px;
            color: #605e5c;
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        .activity-box.selected .activity-type-label {
            color: #ffffff;
        }

        .activity-body {
            display: flex;
            align-items: center;
            padding: 6px 8px;
            gap: 8px;
        }

        .activity-icon-large {
            display: flex;
            align-items: center;
            justify-content: center;
            width: 28px;
            height: 28px;
            flex-shrink: 0;
            font-size: 20px;
            color: var(--activity-color, #0078d4);
        }

        .activity-label {
            font-size: 13px;
            font-weight: 400;
            color: #323130;
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        .activity-status-badge {
            display: none;
        }

        .activity-box.selected .activity-status-badge {
            display: block;
            padding: 4px 8px;
            border-top: 1px solid #edebe9;
            font-size: 11px;
            line-height: 1.2;
            text-align: center;
        }

        .activity-status-badge.succeeded {
            background: rgba(76, 175, 80, 0.1);
            color: #4caf50;
        }

        .activity-status-badge.failed {
            background: rgba(244, 67, 54, 0.1);
            color: #f44336;
        }

        /* Configuration Panel (Bottom) - Matching pipeline editor */
        .config-panel {
            height: 300px;
            min-height: 300px;
            max-height: 300px;
            background: var(--vscode-panel-background);
            border-top: 1px solid var(--vscode-panel-border);
            display: flex;
            flex-direction: column;
            overflow: hidden;
            flex-shrink: 0;
            transition: height 0.2s ease;
        }

        .config-panel.minimized {
            height: 40px !important;
            min-height: 40px !important;
            max-height: 40px !important;
        }

        .config-panel.minimized .config-content {
            display: none;
        }

        .config-tabs {
            display: flex;
            background: var(--vscode-sideBar-background);
            border-bottom: 1px solid var(--vscode-panel-border);
            padding: 0 16px;
            gap: 4px;
            height: 40px;
            align-items: center;
        }

        .config-header-title {
            font-size: 13px;
            font-weight: 600;
            color: var(--vscode-foreground);
        }

        .config-collapse-btn {
            background: transparent;
            border: none;
            cursor: pointer;
            font-size: 14px;
            color: var(--vscode-foreground);
            padding: 4px 8px;
            margin-left: auto;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.2s ease;
            border-radius: 4px;
        }

        .config-collapse-btn:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .config-content {
            flex: 1;
            overflow-y: auto;
            padding: 16px;
            background: var(--vscode-editor-background);
        }

        .config-tab-pane {
            display: none;
        }

        .config-tab-pane.active {
            display: block;
        }

        .property-grid {
            display: grid;
            grid-template-columns: 150px 1fr;
            gap: 12px 16px;
            align-items: center;
        }

        .property-label {
            font-size: 12px;
            font-weight: 600;
            color: var(--vscode-descriptionForeground);
        }

        .property-value {
            font-size: 13px;
            padding: 6px 8px;
            background: var(--vscode-input-background);
            border: 1px solid var(--vscode-input-border);
            border-radius: 3px;
            color: var(--vscode-input-foreground);
            word-break: break-all;
        }

        .action-buttons {
            display: flex;
            gap: 8px;
            margin-top: 16px;
            padding-top: 16px;
            border-top: 1px solid var(--vscode-panel-border);
        }

        .btn {
            padding: 6px 14px;
            font-size: 13px;
            border: none;
            border-radius: 2px;
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

        .error-section {
            margin-top: 16px;
            padding: 12px;
            background: rgba(244, 67, 54, 0.1);
            border-left: 3px solid #f44336;
            border-radius: 3px;
        }

        .error-title {
            font-weight: 600;
            margin-bottom: 8px;
            color: #f44336;
            font-size: 13px;
        }

        .error-detail {
            font-size: 12px;
            margin-bottom: 4px;
        }

        .error-detail strong {
            font-weight: 600;
        }

        .empty-state {
            text-align: center;
            padding: 40px;
            color: var(--vscode-descriptionForeground);
            font-size: 13px;
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

    <div class="canvas-wrapper">
        <div class="canvas-container">
            ${activities.length === 0 
                ? '<div class="empty-state">No activities found in this pipeline run</div>'
                : activities.map((activity, index) => this.renderActivityBox(activity, index)).join('')
            }
        </div>
    </div>

    <div class="config-panel">
        <div class="config-tabs">
            <span class="config-header-title">Activity Details</span>
            <button class="config-collapse-btn" id="configCollapseBtn" onclick="toggleConfig()" title="Collapse Configuration Panel">»</button>
        </div>
        <div class="config-content">
            ${activities.map((activity, index) => this.renderActivityDetails(activity, index)).join('')}
            ${activities.length === 0 
                ? '<div class="empty-state">Select an activity to view details</div>'
                : ''
            }
        </div>
    </div>

    <script>
        const vscode = acquireVsCodeApi();
        let selectedActivityIndex = null;

        function toggleConfig() {
            const panel = document.querySelector('.config-panel');
            const btn = document.getElementById('configCollapseBtn');
            panel.classList.toggle('minimized');
            btn.textContent = panel.classList.contains('minimized') ? '«' : '»';
        }

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

        function viewNotebook(index) {
            const activity = ${JSON.stringify(activities)}[index];
            vscode.postMessage({
                command: 'viewNotebook',
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
        const color = this.getActivityColor(activity.activityType);
        
        return `
        <div class="activity-box status-${statusClass}" data-index="${index}" onclick="selectActivity(${index})" style="--activity-color: ${color}">
            <div class="activity-header">
                <span class="activity-type-label">${activity.activityType}</span>
            </div>
            <div class="activity-body">
                <div class="activity-icon-large">${icon}</div>
                <div class="activity-label">${activity.activityName}</div>
            </div>
            <div class="activity-status-badge ${statusClass}">${activity.status}</div>
        </div>`;
    }

    renderActivityDetails(activity, index) {
        const hasError =activity.status === 'Failed' && activity.error && activity.error.message;
        const durationSec = activity.durationInMs ? (activity.durationInMs / 1000).toFixed(2) : 'N/A';
        const isNotebook = activity.activityType === 'SynapseNotebook' || activity.activityType === 'Notebook';

        return `
        <div class="config-tab-pane activity-details" data-index="${index}">
            <div class="property-grid">
                <div class="property-label">Activity Name</div>
                <div class="property-value">${activity.activityName}</div>
                
                <div class="property-label">Activity Type</div>
                <div class="property-value">${activity.activityType}</div>
                
                <div class="property-label">Status</div>
                <div class="property-value">${activity.status}</div>
                
                <div class="property-label">Run Start</div>
                <div class="property-value">${this.formatTimestamp(new Date(activity.activityRunStart))}</div>
                
                <div class="property-label">Run End</div>
                <div class="property-value">${this.formatTimestamp(new Date(activity.activityRunEnd))}</div>
                
                <div class="property-label">Duration</div>
                <div class="property-value">${durationSec} seconds</div>
                
                <div class="property-label">Activity Run ID</div>
                <div class="property-value">${activity.activityRunId}</div>
            </div>

            ${hasError ? `
            <div class="error-section">
                <div class="error-title">Error Details</div>
                <div class="error-detail"><strong>Error Code:</strong> ${activity.error.errorCode || 'N/A'}</div>
                <div class="error-detail"><strong>Message:</strong> ${activity.error.message || 'N/A'}</div>
                <div class="error-detail"><strong>Failure Type:</strong> ${activity.error.failureType || 'N/A'}</div>
            </div>
            ` : ''}

            <div class="action-buttons">
                <button class="btn" onclick="showInput(${index})">View Input</button>
                <button class="btn" onclick="showOutput(${index})">View Output</button>
                <button class="btn btn-secondary" onclick="showDetails(${index})">View Full JSON</button>
                ${isNotebook ? `<button class="btn" onclick="viewNotebook(${index})">View Notebook</button>` : ''}
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

    getActivityColor(activityType) {
        const colors = {
            'Copy': '#0078d4',
            'SynapseNotebook': '#7719aa',
            'Notebook': '#7719aa',
            'ExecutePipeline': '#00b294',
            'IfCondition': '#ff8c00',
            'ForEach': '#d83b01',
            'Wait': '#00bcf2',
            'Until': '#d83b01',
            'Switch': '#ff8c00',
            'Lookup': '#847545',
            'GetMetadata': '#e3008c',
            'Delete': '#d13438',
            'SqlServerStoredProcedure': '#0078d4',
            'Script': '#00bcf2',
            'WebActivity': '#8661c5',
            'Validation': '#10893e',
            'Filter': '#e3008c',
            'Fail': '#d13438',
            'SetVariable': '#0078d4',
            'AppendVariable': '#0078d4',
            'WebHook': '#9b59b6',
            'StoredProcedure': '#847545'
        };
        
        return colors[activityType] || '#0078d4';
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
