const vscode = require('vscode');
const activitiesConfig = require('./activities-config-verified.json');
const activitySchemas = require('./activity-schemas.json');
const datasetSchemas = require('./dataset-schemas.json');

class PipelineEditorProvider {
	static panels = new Map(); // Map<filePath, panel>

	constructor(context) {
		this.context = context;
	}

	createOrShow(filePath = null) {
		const column = vscode.window.activeTextEditor
			? vscode.window.activeTextEditor.viewColumn
			: undefined;

		// If opening a specific file, check if panel already exists
		if (filePath && PipelineEditorProvider.panels.has(filePath)) {
			PipelineEditorProvider.panels.get(filePath).reveal(column);
			return PipelineEditorProvider.panels.get(filePath);
		}

		// Create title from filename if provided
		const path = require('path');
		const title = filePath 
			? path.basename(filePath, '.json')
			: 'Synapse Pipeline Editor';

		// Otherwise, create a new panel
		const panel = vscode.window.createWebviewPanel(
			'adfPipelineEditor',
			title,
			column || vscode.ViewColumn.One,
			{
				enableScripts: true,
				retainContextWhenHidden: true,
				localResourceRoots: [
					vscode.Uri.joinPath(this.context.extensionUri, 'media')
				]
			}
		);

		// Store panel with associated file path
		if (filePath) {
			PipelineEditorProvider.panels.set(filePath, panel);
		}

		// Set the webview's initial html content
		panel.webview.html = this.getHtmlContent(panel.webview);

		// Send dataset schemas and list to webview after a short delay to ensure it's loaded
		setImmediate(() => {
			const fs = require('fs');
			const path = require('path');
			let datasetList = [];
			let datasetContents = {};
			
			if (vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0) {
				const datasetPath = path.join(vscode.workspace.workspaceFolders[0].uri.fsPath, 'dataset');
				if (fs.existsSync(datasetPath)) {
					const files = fs.readdirSync(datasetPath).filter(f => f.endsWith('.json'));
					files.forEach(file => {
						const name = file.replace('.json', '');
						datasetList.push(name);
						try {
							const filePath = path.join(datasetPath, file);
							const content = fs.readFileSync(filePath, 'utf8');
							datasetContents[name] = JSON.parse(content);
						} catch (err) {
							console.error(`Error reading dataset ${file}:`, err);
						}
					});
				}
			}
			
			panel.webview.postMessage({
				type: 'initSchemas',
				datasetSchemas: datasetSchemas,
				datasetList: datasetList,
				datasetContents: datasetContents
			});
		});

		// Handle messages from the webview
		panel.webview.onDidReceiveMessage(
			async message => {
				switch (message.type) {
					case 'alert':
						vscode.window.showInformationMessage(message.text);
						break;
					case 'save':
						console.log('[Extension] Received save message:', message);
						// Use filePath from message if available, otherwise use closure filePath
						const saveFilePath = message.filePath || filePath;
						await this.savePipelineToWorkspace(message.data, saveFilePath);
						break;
					case 'log':
						console.log('Webview:', message.text);
						break;
				}
			},
			undefined,
			this.context.subscriptions
		);

		// Reset when the current panel is closed
		panel.onDidDispose(
			() => {
				// Remove panel from map
				if (filePath) {
					PipelineEditorProvider.panels.delete(filePath);
				}
				this.pendingPipelineFile = null;
			},
			null,
			this.context.subscriptions
		);

		// Load pending pipeline file if any
		if (this.pendingPipelineFile) {
			const fileToLoad = this.pendingPipelineFile;
			this.pendingPipelineFile = null;
			this.loadPipelineFile(fileToLoad);
		}
		
		return panel;
	}

	addActivity(activityType) {
		// Find the most recently used panel
		const panels = Array.from(PipelineEditorProvider.panels.values());
		const panel = panels.length > 0 ? panels[panels.length - 1] : null;
		
		if (panel) {
			panel.webview.postMessage({
				type: 'addActivity',
				activityType: activityType
			});
		} else {
			vscode.window.showWarningMessage('Please open the pipeline editor first');
		}
	}

	loadPipelineFile(filePath) {
		// Create or show panel for this file
		const panel = this.createOrShow(filePath);
		
		const fs = require('fs');
		try {
			const content = fs.readFileSync(filePath, 'utf8');
			const pipelineJson = JSON.parse(content);
			
			// Send to webview
			panel.webview.postMessage({
				type: 'loadPipeline',
				data: pipelineJson,
				filePath: filePath
			});
			
			vscode.window.showInformationMessage(`Loaded pipeline: ${require('path').basename(filePath)}`);
		} catch (error) {
			vscode.window.showErrorMessage(`Failed to load pipeline: ${error.message}`);
		}
	}

	async savePipelineToWorkspace(pipelineData, filePath) {
		const fs = require('fs');
		const path = require('path');
		
		console.log('[Extension] savePipelineToWorkspace called');
		console.log('[Extension] Pipeline data:', JSON.stringify(pipelineData, null, 2));
		console.log('[Extension] File path:', filePath);
		
		try {
			// Get workspace folder
			if (!vscode.workspace.workspaceFolders) {
				vscode.window.showErrorMessage('No workspace folder open');
				return;
			}
			
			const workspaceRoot = vscode.workspace.workspaceFolders[0].uri.fsPath;
			const pipelineDir = path.join(workspaceRoot, 'pipeline');
			
			// Create pipeline directory if it doesn't exist
			if (!fs.existsSync(pipelineDir)) {
				fs.mkdirSync(pipelineDir, { recursive: true });
			}
			
			console.log('[Extension] Converting to Synapse format...');
			console.log('[Extension] Number of activities:', pipelineData.activities?.length || 0);
			
			// Convert to Synapse format
			const synapseJson = {
				name: pipelineData.name || "pipeline1",
				properties: {
					activities: (pipelineData.activities || []).map(a => {
						console.log('[Extension] Processing activity:', a.name, 'Type:', a.type);
						
						const activity = {
							name: a.name,
							type: a.type
						};
						
						if (a.description) activity.description = a.description;
						if (a.state) activity.state = a.state;
						if (a.onInactiveMarkAs) activity.onInactiveMarkAs = a.onInactiveMarkAs;
						if (a.dependsOn) activity.dependsOn = a.dependsOn;
						if (a.userProperties) activity.userProperties = a.userProperties;
						
						// Add policy
						const policy = {};
						if (a.timeout !== undefined) policy.timeout = a.timeout;
						if (a.retry !== undefined) policy.retry = a.retry;
						if (a.retryIntervalInSeconds !== undefined) policy.retryIntervalInSeconds = a.retryIntervalInSeconds;
						if (a.secureOutput !== undefined) policy.secureOutput = a.secureOutput;
						if (a.secureInput !== undefined) policy.secureInput = a.secureInput;
						if (Object.keys(policy).length > 0) activity.policy = policy;
						
						// Collect typeProperties
						const typeProperties = {};
						const commonProps = ['id', 'type', 'x', 'y', 'width', 'height', 'name', 'description', 'color', 'container', 'element', 
											 'timeout', 'retry', 'retryIntervalInSeconds', 'secureOutput', 'secureInput', 'userProperties', 'state', 'onInactiveMarkAs',
											 'dynamicAllocation', 'minExecutors', 'maxExecutors', 'dependsOn',
											 'sourceDataset', 'sinkDataset', 'recursive', 'modifiedDatetimeStart', 'modifiedDatetimeEnd',
											 'wildcardFolderPath', 'wildcardFileName', 'enablePartitionDiscovery',
											 'writeBatchSize', 'writeBatchTimeout', 'preCopyScript', 'maxConcurrentConnections', 'writeBehavior', 
											 'sqlWriterUseTableLock', 'disableMetricsCollection', '_sourceObject', '_sinkObject'];
						
						for (const key in a) {
							if (!commonProps.includes(key) && a.hasOwnProperty(key) && typeof a[key] !== 'function') {
								typeProperties[key] = a[key];
							}
						}
						
						// For SynapseNotebook, convert dynamicAllocation fields back to conf object
						if (a.type === 'SynapseNotebook') {
							if (a.dynamicAllocation !== undefined || a.minExecutors || a.maxExecutors) {
								typeProperties.conf = {};
								if (a.dynamicAllocation !== undefined) {
									typeProperties.conf['spark.dynamicAllocation.enabled'] = a.dynamicAllocation;
								}
								if (a.minExecutors !== undefined) {
									typeProperties.conf['spark.dynamicAllocation.minExecutors'] = a.minExecutors;
								}
								if (a.maxExecutors !== undefined) {
									typeProperties.conf['spark.dynamicAllocation.maxExecutors'] = a.maxExecutors;
								}
							}
						}
						
						// For Copy activity, reconstruct nested source/sink structures and inputs/outputs
						if (a.type === 'Copy') {
							console.log('[Extension] Copy activity - reconstructing source/sink');
							console.log('[Extension] Source dataset:', a.sourceDataset);
							console.log('[Extension] Sink dataset:', a.sinkDataset);
							console.log('[Extension] Inputs from activity:', a.inputs);
							console.log('[Extension] Outputs from activity:', a.outputs);
							
							// Reconstruct source object or create default based on dataset type
							if (a._sourceObject) {
								typeProperties.source = JSON.parse(JSON.stringify(a._sourceObject));
							} else if (a._sourceDatasetType) {
								// Create basic source structure based on dataset type
								typeProperties.source = {
									type: a._sourceDatasetType + 'Source',
									storeSettings: {
										type: a._sourceDatasetType.includes('Sql') ? 'AzureSqlDatabaseReadSettings' : 'AzureBlobStorageReadSettings'
									}
								};
							}
							
							// Update with any changed values
							if (typeProperties.source) {
								console.log('[Extension] Updating source with field values');
								if (!typeProperties.source.storeSettings) typeProperties.source.storeSettings = {};
								
								if (a.recursive !== undefined) typeProperties.source.storeSettings.recursive = a.recursive;
								if (a.modifiedDatetimeStart !== undefined && a.modifiedDatetimeStart !== '') typeProperties.source.storeSettings.modifiedDatetimeStart = a.modifiedDatetimeStart;
								if (a.modifiedDatetimeEnd !== undefined && a.modifiedDatetimeEnd !== '') typeProperties.source.storeSettings.modifiedDatetimeEnd = a.modifiedDatetimeEnd;
								if (a.wildcardFolderPath !== undefined && a.wildcardFolderPath !== '') typeProperties.source.storeSettings.wildcardFolderPath = a.wildcardFolderPath;
								if (a.wildcardFileName !== undefined && a.wildcardFileName !== '') typeProperties.source.storeSettings.wildcardFileName = a.wildcardFileName;
								if (a.enablePartitionDiscovery !== undefined) typeProperties.source.storeSettings.enablePartitionDiscovery = a.enablePartitionDiscovery;
								if (a.maxConcurrentConnections !== undefined) typeProperties.source.storeSettings.maxConcurrentConnections = a.maxConcurrentConnections;
							}
							
							// Reconstruct sink object or create default based on dataset type
							if (a._sinkObject) {
								typeProperties.sink = JSON.parse(JSON.stringify(a._sinkObject));
							} else if (a._sinkDatasetType) {
								// Create basic sink structure based on dataset type
								typeProperties.sink = {
									type: a._sinkDatasetType + 'Sink',
									writeBehavior: 'insert'
								};
							}
							
							// Update with any changed values
							if (typeProperties.sink) {
								console.log('[Extension] Updating sink with field values');
								if (a.writeBatchSize !== undefined && a.writeBatchSize !== '') typeProperties.sink.writeBatchSize = a.writeBatchSize;
								if (a.writeBatchTimeout !== undefined && a.writeBatchTimeout !== '') typeProperties.sink.writeBatchTimeout = a.writeBatchTimeout;
								if (a.preCopyScript !== undefined && a.preCopyScript !== '') typeProperties.sink.preCopyScript = a.preCopyScript;
								if (a.maxConcurrentConnections !== undefined) typeProperties.sink.maxConcurrentConnections = a.maxConcurrentConnections;
								if (a.writeBehavior !== undefined && a.writeBehavior !== '') typeProperties.sink.writeBehavior = a.writeBehavior;
								if (a.sqlWriterUseTableLock !== undefined) typeProperties.sink.sqlWriterUseTableLock = a.sqlWriterUseTableLock;
								if (a.disableMetricsCollection !== undefined) typeProperties.sink.disableMetricsCollection = a.disableMetricsCollection;
								console.log('[Extension] Reconstructed sink:', typeProperties.sink);
							}
							
							// Add inputs/outputs for Copy activity (at activity level, not typeProperties)
							// Check both sourceDataset property and inputs array
							if (a.sourceDataset || (a.inputs && a.inputs.length > 0)) {
								const sourceRef = a.sourceDataset || (a.inputs[0].referenceName || a.inputs[0]);
								activity.inputs = [{
									referenceName: sourceRef,
									type: 'DatasetReference'
								}];
								console.log('[Extension] Added inputs:', activity.inputs);
							}
							if (a.sinkDataset || (a.outputs && a.outputs.length > 0)) {
								const sinkRef = a.sinkDataset || (a.outputs[0].referenceName || a.outputs[0]);
								activity.outputs = [{
									referenceName: sinkRef,
									type: 'DatasetReference'
								}];
								console.log('[Extension] Added outputs:', activity.outputs);
							}
						}
						
						activity.typeProperties = typeProperties;
						console.log('[Extension] Final activity object:', JSON.stringify(activity, null, 2));
						
						return activity;
					}),
					annotations: [],
					lastPublishTime: new Date().toISOString()
				}
			};
			
			// Determine file path
			if (!filePath) {
				// Create new file with unique name
				let fileName = `${synapseJson.name}.json`;
				filePath = path.join(pipelineDir, fileName);
				
				// Check if file exists, add number if needed
				let counter = 1;
				while (fs.existsSync(filePath)) {
					fileName = `${synapseJson.name}_${counter}.json`;
					filePath = path.join(pipelineDir, fileName);
					counter++;
				}
			}
			
			// Write file
			console.log('[Extension] Writing to file:', filePath);
			console.log('[Extension] Final Synapse format:', JSON.stringify(synapseJson, null, 2));
			fs.writeFileSync(filePath, JSON.stringify(synapseJson, null, 2));
			console.log('[Extension] File written successfully');
			vscode.window.showInformationMessage(`Pipeline saved: ${path.basename(filePath)}`);
			
		} catch (error) {
			console.error('[Extension] Save error:', error);
			vscode.window.showErrorMessage(`Failed to save pipeline: ${error.message}`);
		}
	}

	savePipeline(data) {
		vscode.window.showInformationMessage('Pipeline saved!');
		console.log('Pipeline data:', data);
	}

	getHtmlContent() {
		return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline';">
    <title>Pipeline Editor</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            overflow: hidden;
            background: var(--vscode-editor-background);
            color: var(--vscode-editor-foreground);
        }

        .container {
            display: flex;
            flex-direction: column;
            height: 100vh;
        }

        .main-content {
            display: flex;
            flex: 1;
            overflow: hidden;
            min-height: 0;
        }

        /* Sidebar */
        .sidebar {
            width: 250px;
            min-width: 250px;
            max-width: 250px;
            background: var(--vscode-sideBar-background);
            border-right: 1px solid var(--vscode-panel-border);
            display: flex !important;
            flex-direction: column;
            overflow-y: auto !important;
            overflow-x: hidden;
            flex-shrink: 0;
        }

        .sidebar-header {
            padding: 16px;
            border-bottom: 1px solid var(--vscode-panel-border);
            font-size: 14px;
            font-weight: 600;
        }

        .activity-group {
            border-bottom: 1px solid var(--vscode-panel-border);
        }

        .activity-group-title {
            display: flex;
            align-items: center;
            padding: 10px 12px;
            cursor: pointer;
            font-size: 13px;
            color: var(--vscode-foreground);
            user-select: none;
            font-weight: 400;
        }

        .activity-group-title:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .category-arrow {
            margin-right: 8px;
            font-size: 12px;
            font-weight: bold;
            transition: transform 0.2s ease;
            display: inline-block;
        }

        .activity-group.collapsed .category-arrow {
            transform: rotate(-90deg);
        }

        .activity-group:not(.collapsed) .category-arrow {
            transform: rotate(0deg);
        }

        .activity-group-content {
            background: var(--vscode-sideBar-background);
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-in-out;
            display: block;
        }

        .activity-group:not(.collapsed) .activity-group-content {
            max-height: 300px;
            overflow-y: auto;
            overflow-x: hidden;
            display: block;
        }

        .activity-item {
            padding: 8px 12px 8px 36px;
            cursor: move;
            font-size: 13px;
            color: var(--vscode-foreground);
            transition: background 0.15s;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .activity-item:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .activity-icon {
            width: 20px;
            height: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
        }

        /* Canvas Area */
        .canvas-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            position: relative;
            overflow: hidden;
        }

        .toolbar {
            height: 48px;
            background: var(--vscode-editorGroupHeader-tabsBackground);
            border-bottom: 1px solid var(--vscode-panel-border);
            display: flex;
            align-items: center;
            padding: 0 16px;
            gap: 8px;
        }

        .toolbar-spacer {
            flex: 1;
        }

        .expand-properties-btn {
            padding: 6px 12px;
            border: 1px solid var(--vscode-button-border);
            background: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
        }

        .expand-properties-btn:hover {
            background: var(--vscode-button-secondaryHoverBackground);
        }

        body:not(.properties-visible) .expand-properties-btn {
            display: block;
        }

        body.properties-visible .expand-properties-btn {
            display: none;
        }

        .toolbar-button {
            padding: 6px 12px;
            border: 1px solid var(--vscode-button-border);
            background: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
        }

        .toolbar-button:hover {
            background: var(--vscode-button-secondaryHoverBackground);
        }

        .canvas-wrapper {
            flex: 1;
            position: relative;
            overflow: auto;
            background: var(--vscode-editor-background);
            border: 1px solid var(--vscode-panel-border);
        }

        #canvas {
            position: absolute;
            top: 0;
            left: 0;
            pointer-events: none;
            z-index: 1;
        }

        /* Activity Box - DOM-based */
        .activity-box {
            position: absolute;
            width: 180px;
            min-height: 56px;
            background: #f0f0f0;
            border: 1px solid #c8c8c8;
            border-radius: 3px;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
            cursor: pointer;
            z-index: 10;
            user-select: none;
            will-change: transform;
        }

        .activity-box.dragging {
            cursor: move;
            opacity: 0.8;
            z-index: 100;
        }

        .activity-box:hover {
            background: #e8e8e8;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.12);
        }

        .activity-box.selected {
            background: #ffffff;
            border: 1px solid #0078d4;
            box-shadow: 0 4px 12px rgba(0, 120, 212, 0.2);
            min-height: 88px;
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
            padding: 8px;
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

        /* Connection Points */
        .connection-point {
            position: absolute;
            width: 8px;
            height: 8px;
            background: #c8c8c8;
            border: 2px solid #ffffff;
            border-radius: 50%;
            opacity: 0;
            transition: opacity 0.2s ease;
            z-index: 15;
            cursor: crosshair;
        }

        .activity-box:hover .connection-point,
        .activity-box.selected .connection-point {
            opacity: 1;
        }

        .connection-point:hover {
            background: var(--activity-color, #0078d4);
            transform: scale(1.3);
        }

        .connection-point.top {
            top: -5px;
            left: 50%;
            transform: translateX(-50%);
        }

        .connection-point.right {
            right: -5px;
            top: 50%;
            transform: translateY(-50%);
        }

        .connection-point.bottom {
            bottom: -5px;
            left: 50%;
            transform: translateX(-50%);
        }

        .connection-point.left {
            left: -5px;
            top: 50%;
            transform: translateY(-50%);
        }

        .connection-point.top:hover {
            transform: translateX(-50%) scale(1.3);
        }

        .connection-point.right:hover {
            transform: translateY(-50%) scale(1.3);
        }

        .connection-point.bottom:hover {
            transform: translateX(-50%) scale(1.3);
        }

        .connection-point.left:hover {
            transform: translateY(-50%) scale(1.3);
        }

        /* Activity Actions */
        .activity-actions {
            display: none;
            align-items: center;
            padding: 4px 8px;
            gap: 4px;
            border-top: 1px solid #edebe9;
        }

        .activity-box.selected .activity-actions {
            display: flex;
        }

        .action-icon-btn {
            width: 24px;
            height: 24px;
            border: none;
            background: transparent;
            border-radius: 2px;
            cursor: pointer;
            font-size: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.1s ease;
            color: #605e5c;
        }

        .action-icon-btn:hover {
            background: rgba(0, 0, 0, 0.05);
        }

        .action-icon-btn.info {
            margin-left: auto;
            color: #0078d4;
            font-weight: bold;
        }

        /* Properties Panel (Right Sidebar) */
        .properties-panel {
            width: 300px;
            min-width: 300px;
            max-width: 300px;
            background: var(--vscode-sideBar-background);
            border-left: 1px solid var(--vscode-panel-border);
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            overflow-x: hidden;
            flex-shrink: 0;
            transition: width 0.3s ease, min-width 0.3s ease, max-width 0.3s ease;
        }

        .properties-panel.collapsed {
            width: 0;
            min-width: 0;
            max-width: 0;
            border-left: none;
            overflow: hidden;
            padding: 0;
        }

        .properties-collapse-btn {
            background: transparent;
            border: none;
            cursor: pointer;
            font-size: 18px;
            color: var(--vscode-foreground);
            padding: 4px 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.2s ease;
            border-radius: 4px;
        }

        .properties-collapse-btn:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .properties-header {
            padding: 16px;
            border-bottom: 1px solid var(--vscode-panel-border);
            font-size: 14px;
            font-weight: 600;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .properties-content {
            padding: 16px;
        }

        /* Configuration Panel (Bottom) */
        .config-panel {
            height: 250px !important;
            min-height: 250px !important;
            max-height: 250px !important;
            background: var(--vscode-panel-background) !important;
            border-top: 1px solid var(--vscode-panel-border) !important;
            display: flex !important;
            flex-direction: column !important;
            overflow: visible !important;
            flex-shrink: 0 !important;
            z-index: 1000 !important;
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

        .config-tabs {
            display: flex !important;
            background: var(--vscode-sideBar-background) !important;
            border-bottom: 1px solid var(--vscode-panel-border);
            padding: 0 16px;
            gap: 4px;
            height: 40px !important;
            align-items: center;
        }

        .config-tab {
            padding: 8px 16px;
            border: none;
            background: transparent;
            color: var(--vscode-tab-inactiveForeground);
            cursor: pointer;
            font-size: 13px;
            border-bottom: 2px solid transparent;
            transition: all 0.2s;
        }

        .config-tab:hover {
            color: var(--vscode-tab-activeForeground);
        }

        .config-tab.active {
            color: var(--vscode-tab-activeForeground);
            border-bottom-color: var(--vscode-focusBorder);
        }

        .config-content {
            flex: 1 !important;
            overflow-y: auto !important;
            padding: 16px;
            background: var(--vscode-editor-background) !important;
            display: block !important;
        }

        .config-tab-pane {
            display: none;
        }

        .config-tab-pane.active {
            display: block;
        }

        .property-group {
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .property-label {
            font-size: 12px;
            font-weight: 600;
            color: var(--vscode-descriptionForeground);
            min-width: 150px;
            flex-shrink: 0;
        }

        .property-input {
            flex: 1;
            padding: 6px 8px;
            background: var(--vscode-input-background);
            color: var(--vscode-input-foreground);
            border: 1px solid var(--vscode-input-border);
            border-radius: 3px;
            font-size: 13px;
        }

        .property-input:focus {
            outline: none;
            border-color: var(--vscode-focusBorder);
        }

        .empty-state {
            padding: 24px;
            text-align: center;
            color: var(--vscode-descriptionForeground);
            font-size: 13px;
        }

        /* Context Menu */
        .context-menu {
            position: absolute;
            background: var(--vscode-menu-background);
            border: 1px solid var(--vscode-menu-border);
            border-radius: 4px;
            padding: 4px 0;
            min-width: 150px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
            z-index: 1000;
            display: none;
        }

        .context-menu-item {
            padding: 6px 12px;
            cursor: pointer;
            font-size: 13px;
        }

        .context-menu-item:hover {
            background: var(--vscode-menu-selectionBackground);
            color: var(--vscode-menu-selectionForeground);
        }

        .context-menu-separator {
            height: 1px;
            background: var(--vscode-menu-separatorBackground);
            margin: 4px 0;
        }
    </style>
</head>
<body>
    <div class="container">
    <div class="main-content">
        <!-- Sidebar with Activities -->
        <div class="sidebar">
            <div class="sidebar-header">Activities</div>
            ${activitiesConfig.categories.map(category => `
            <div class="activity-group collapsed">
                <div class="activity-group-title" onclick="toggleCategory(this)">
                    <span class="category-arrow">▼</span> ${category.name}
                </div>
                <div class="activity-group-content">
                    ${category.activities.map(activity => `
                    <div class="activity-item" draggable="true" data-type="${activity.type}">
                        <div class="activity-icon">${activity.icon}</div>
                        <span>${activity.name}</span>
                    </div>`).join('')}
                </div>
            </div>`).join('')}
        </div>

        <!-- Canvas Area -->
        <div class="canvas-container">
            <div class="toolbar">
                <button class="toolbar-button" id="saveBtn">Save</button>
                <button class="toolbar-button" id="clearBtn">Clear</button>
                <button class="toolbar-button" id="zoomInBtn">Zoom In</button>
                <button class="toolbar-button" id="zoomOutBtn">Zoom Out</button>
                <button class="toolbar-button" id="fitBtn">Fit to Screen</button>
                <div class="toolbar-spacer"></div>
                <button class="expand-properties-btn" id="expandPropertiesBtn" onclick="toggleProperties()">Properties</button>
            </div>
            <div class="canvas-wrapper" id="canvasWrapper">
                <canvas id="canvas"></canvas>
            </div>
        </div>

        <!-- Properties Panel (Right Sidebar) -->
        <div class="properties-panel">
            <div class="properties-header">
                <span>Pipeline Properties</span>
                <button class="properties-collapse-btn" onclick="toggleProperties()" title="Collapse Properties Panel">»</button>
            </div>
            <div id="propertiesContent" class="properties-content">
                <div class="empty-state">Pipeline properties and settings</div>
            </div>
        </div>
    </div>
    </div>

    <!-- Configuration Panel (Bottom) -->
    <div class="config-panel" style="position: fixed; bottom: 0; left: 0; right: 0; height: 250px; background: var(--vscode-panel-background); border-top: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; z-index: 100;">
        <div class="config-tabs" id="configTabs">
            <!-- Pipeline-level tabs (shown when no activity selected) -->
            <button class="config-tab pipeline-tab active" data-tab="parameters" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-activeForeground); border-bottom: 2px solid var(--vscode-focusBorder);">Parameters</button>
            <button class="config-tab pipeline-tab" data-tab="pipeline-variables" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Variables</button>
            <button class="config-tab pipeline-tab" data-tab="pipeline-settings" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Settings</button>
            <button class="config-tab pipeline-tab" data-tab="output" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Output</button>
            
            <!-- Activity-level tabs (shown when activity selected, dynamically generated) -->
            <div id="activityTabsContainer"></div>
            
            <button class="config-collapse-btn" id="configCollapseBtn" onclick="toggleConfig()" title="Collapse Configuration Panel">»</button>
        </div>
        <div class="config-content" id="configContent" style="flex: 1; overflow-y: auto; padding: 16px; background: var(--vscode-editor-background);">
            <!-- Pipeline-level tab panes -->
            <div class="config-tab-pane pipeline-pane active" id="tab-parameters">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Parameters</div>
                <div class="empty-state">No parameters defined. Click + to add a parameter.</div>
            </div>
            <div class="config-tab-pane pipeline-pane" id="tab-pipeline-variables">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Variables</div>
                <div class="empty-state">No variables defined. Click + to add a variable.</div>
            </div>
            <div class="config-tab-pane pipeline-pane" id="tab-pipeline-settings">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Settings</div>
                <div class="property-group">
                    <div class="property-label">Annotations</div>
                    <textarea class="property-input" rows="3" placeholder="Add annotations..."></textarea>
                </div>
            </div>
            <div class="config-tab-pane pipeline-pane" id="tab-output">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Output</div>
                <div class="empty-state">Pipeline execution output will appear here</div>
            </div>
            
            <!-- Activity-level tab panes (dynamically generated) -->
            <div id="activityPanesContainer"></div>
        </div>
    </div>
    </div>

    <!-- Context Menu -->
    <div class="context-menu" id="contextMenu">
        <div class="context-menu-item" data-action="delete">Delete</div>
        <div class="context-menu-separator"></div>
        <div class="context-menu-item" data-action="copy">Copy</div>
        <div class="context-menu-item" data-action="paste">Paste</div>
    </div>

    <script>
        // Toggle category function for collapsible categories
        function toggleCategory(element) {
            const activityGroup = element.closest('.activity-group');
            activityGroup.classList.toggle('collapsed');
        }

        console.log('=== Pipeline Editor Script Starting ===');
        const vscode = acquireVsCodeApi();
        console.log('vscode API acquired');
        
        // Dataset schemas and list will be sent via message
        let datasetSchemas = {};
        let datasetList = [];
        let datasetContents = {};
        
        // Toggle properties panel
        function toggleProperties() {
            const panel = document.querySelector('.properties-panel');
            panel.classList.toggle('collapsed');
            // Toggle body class for button visibility
            if (panel.classList.contains('collapsed')) {
                document.body.classList.remove('properties-visible');
            } else {
                document.body.classList.add('properties-visible');
            }
        }

        // Initialize properties state
        document.body.classList.add('properties-visible');
        
        // Toggle config panel function
        function toggleConfig() {
            const panel = document.querySelector('.config-panel');
            const btn = document.getElementById('configCollapseBtn');
            panel.classList.toggle('minimized');
            // Change button icon
            btn.textContent = panel.classList.contains('minimized') ? '«' : '»';
        }
        
        // Canvas state
        let canvas = document.getElementById('canvas');
        let ctx = canvas.getContext('2d');
        console.log('Canvas:', canvas);
        console.log('Canvas context:', ctx);
        let activities = [];
        let connections = [];
        let selectedActivity = null;
        let currentFilePath = null; // Track the current file path
        let draggedActivity = null;
        let connectionStart = null;
        let isDragging = false;
        let dragOffset = { x: 0, y: 0 };
        let scale = 1;
        let panOffset = { x: 0, y: 0 };
        let isPanning = false;
        let panStart = { x: 0, y: 0 };
        let animationFrameId = null;
        let needsRedraw = false;

        // Check if elements exist
        console.log('Sidebar elements:', document.querySelectorAll('.activity-item').length);
        console.log('Config panel:', document.getElementById('configContent'));
        console.log('General content:', document.getElementById('generalContent'));
        console.log('Config tabs:', document.querySelectorAll('.config-tab').length);

        // Initialize canvas
        function resizeCanvas() {
            const wrapper = document.getElementById('canvasWrapper');
            canvas.width = Math.max(wrapper.clientWidth, 2000);
            canvas.height = Math.max(wrapper.clientHeight, 2000);
            draw();
        }

        resizeCanvas();
        window.addEventListener('resize', resizeCanvas);

        // Activity class
        class Activity {
            constructor(type, x, y, container) {
                this.id = Date.now() + Math.random();
                this.type = type;
                this.x = x;
                this.y = y;
                this.width = 180;
                this.height = 56;
                this.name = type;
                this.description = '';
                this.color = this.getColorForType(type);
                this.container = container;
                this.element = null;
                this.createDOMElement();
            }

            getColorForType(type) {
                const colors = {
                    'Copy': '#0078d4',
                    'Delete': '#d13438',
                    'Dataflow': '#00a4ef',
                    'Notebook': '#f2c811',
                    'ForEach': '#7fba00',
                    'IfCondition': '#ff8c00',
                    'Wait': '#00bcf2',
                    'WebActivity': '#8661c5',
                    'StoredProcedure': '#847545'
                };
                return colors[type] || '#0078d4';
            }

            createDOMElement() {
                // Create the main activity box element
                this.element = document.createElement('div');
                this.element.className = 'activity-box';
                this.element.style.left = this.x + 'px';
                this.element.style.top = this.y + 'px';
                this.element.style.setProperty('--activity-color', this.color);
                this.element.dataset.activityId = this.id;
                
                // Create header
                const header = document.createElement('div');
                header.className = 'activity-header';
                const typeLabel = document.createElement('span');
                typeLabel.className = 'activity-type-label';
                typeLabel.textContent = this.getTypeLabel();
                header.appendChild(typeLabel);
                
                // Create body
                const body = document.createElement('div');
                body.className = 'activity-body';
                
                const icon = document.createElement('div');
                icon.className = 'activity-icon-large';
                icon.textContent = this.getIcon();
                
                const label = document.createElement('div');
                label.className = 'activity-label';
                label.textContent = this.name;
                
                body.appendChild(icon);
                body.appendChild(label);
                
                // Create action buttons section (hidden by default, shown when selected)
                const actions = document.createElement('div');
                actions.className = 'activity-actions';
                
                const deleteBtn = document.createElement('button');
                deleteBtn.className = 'action-icon-btn';
                deleteBtn.innerHTML = '×';
                deleteBtn.title = 'Delete';
                deleteBtn.onclick = (e) => {
                    e.stopPropagation();
                    this.handleDelete();
                };
                
                const editBtn = document.createElement('button');
                editBtn.className = 'action-icon-btn';
                editBtn.innerHTML = '{}';
                editBtn.title = 'Edit JSON';
                editBtn.onclick = (e) => e.stopPropagation();
                
                const copyBtn = document.createElement('button');
                copyBtn.className = 'action-icon-btn';
                copyBtn.innerHTML = '⎘';
                copyBtn.title = 'Copy';
                copyBtn.onclick = (e) => e.stopPropagation();
                
                const infoBtn = document.createElement('button');
                infoBtn.className = 'action-icon-btn info';
                infoBtn.innerHTML = 'i';
                infoBtn.title = 'Info';
                infoBtn.onclick = (e) => e.stopPropagation();
                
                actions.appendChild(deleteBtn);
                actions.appendChild(editBtn);
                actions.appendChild(copyBtn);
                actions.appendChild(infoBtn);
                
                // Add connection points
                const positions = ['top', 'right', 'bottom', 'left'];
                positions.forEach(pos => {
                    const point = document.createElement('div');
                    point.className = 'connection-point ' + pos;
                    point.dataset.position = pos;
                    point.dataset.activityId = this.id;
                    this.element.appendChild(point);
                });
                
                // Assemble element
                this.element.appendChild(header);
                this.element.appendChild(body);
                this.element.appendChild(actions);
                
                // Add to container
                this.container.appendChild(this.element);
                
                // Set up event listeners
                this.setupEventListeners();
            }
            
            handleDelete() {
                if (confirm(\`Delete activity "\${this.name}"?\`)) {
                    activities = activities.filter(a => a !== this);
                    connections = connections.filter(c => c.from !== this && c.to !== this);
                    this.remove();
                    selectedActivity = null;
                    showProperties(null);
                    draw();
                }
            }
            
            setupEventListeners() {
                // Click to select
                this.element.addEventListener('mousedown', (e) => {
                    // Don't handle if clicking on connection point
                    if (e.target.classList.contains('connection-point')) {
                        return;
                    }
                    e.stopPropagation();
                    this.handleMouseDown(e);
                });
                
                // Right-click context menu
                this.element.addEventListener('contextmenu', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    selectedActivity = this;
                    showContextMenu(e.clientX, e.clientY);
                });
                
                // Connection point handlers
                this.element.querySelectorAll('.connection-point').forEach(point => {
                    point.addEventListener('mousedown', (e) => {
                        e.stopPropagation();
                        this.handleConnectionStart(e, point);
                    });
                });
            }
            
            handleMouseDown(e) {
                selectedActivity = this;
                draggedActivity = this;
                isDragging = true;
                const rect = this.element.getBoundingClientRect();
                const wrapperRect = this.container.getBoundingClientRect();
                dragOffset.x = e.clientX - rect.left;
                dragOffset.y = e.clientY - rect.top;
                this.element.classList.add('dragging');
                this.element.style.cursor = 'move';
                this.setSelected(true);
                showProperties(selectedActivity);
                
                // Redraw connections
                draw();
            }
            
            handleConnectionStart(e, point) {
                const position = point.dataset.position;
                const connPoint = this.getConnectionPoint(position);
                connectionStart = connPoint;
                connectionStart.activity = this;
                canvas.style.cursor = 'crosshair';
                draw();
            }

            contains(x, y) {
                return x >= this.x && x <= this.x + this.width &&
                       y >= this.y && y <= this.y + this.height;
            }
            
            updatePosition(x, y) {
                this.x = x;
                this.y = y;
                if (this.element) {
                    this.element.style.left = x + 'px';
                    this.element.style.top = y + 'px';
                }
            }
            
            updateName(name) {
                this.name = name;
                if (this.element) {
                    const label = this.element.querySelector('.activity-label');
                    if (label) {
                        label.textContent = name;
                    }
                }
            }
            
            setSelected(selected) {
                if (this.element) {
                    if (selected) {
                        this.element.classList.add('selected');
                        // Deselect all other activities
                        activities.forEach(a => {
                            if (a !== this && a.element) {
                                a.element.classList.remove('selected');
                            }
                        });
                    } else {
                        this.element.classList.remove('selected');
                    }
                }
            }
            
            remove() {
                if (this.element && this.element.parentNode) {
                    this.element.parentNode.removeChild(this.element);
                }
                this.element = null;
            }



            getTypeLabel() {
                const labels = {
                    'Copy': 'Copy data',
                    'Delete': 'Delete',
                    'Dataflow': 'Data flow',
                    'Notebook': 'Notebook',
                    'ForEach': 'ForEach',
                    'IfCondition': 'If Condition',
                    'Wait': 'Wait',
                    'WebActivity': 'Web Activity',
                    'StoredProcedure': 'Stored Procedure'
                };
                return labels[this.type] || this.type;
            }

            getIcon() {
                const icons = {
                    'Copy': '📋',
                    'Delete': '🗑️',
                    'Dataflow': '🌊',
                    'Notebook': '📓',
                    'ForEach': '🔁',
                    'IfCondition': '❓',
                    'Wait': '⏱️',
                    'WebActivity': '🌐',
                    'StoredProcedure': '💾'
                };
                return icons[this.type] || '📦';
            }



            getConnectionPoint(position) {
                const headerHeight = 18;
                switch (position) {
                    case 'top': return { x: this.x + this.width / 2, y: this.y };
                    case 'right': return { x: this.x + this.width, y: this.y + this.height / 2 };
                    case 'bottom': return { x: this.x + this.width / 2, y: this.y + this.height };
                    case 'left': return { x: this.x, y: this.y + this.height / 2 };
                    default: return { x: this.x + this.width / 2, y: this.y + this.height };
                }
            }
        }

        // Connection class
        class Connection {
            constructor(fromActivity, toActivity, condition = 'Succeeded') {
                this.id = Date.now() + Math.random();
                this.from = fromActivity;
                this.to = toActivity;
                this.condition = condition; // Succeeded, Failed, Skipped, Completed
            }

            draw(ctx) {
                // Smart routing based on activity positions
                const fromCenter = { x: this.from.x + this.from.width / 2, y: this.from.y + this.from.height / 2 };
                const toCenter = { x: this.to.x + this.to.width / 2, y: this.to.y + this.to.height / 2 };
                const dx = toCenter.x - fromCenter.x;
                const dy = toCenter.y - fromCenter.y;
                
                let start, end;
                
                // Determine best connection points based on relative position
                if (Math.abs(dx) > Math.abs(dy)) {
                    // Horizontal layout
                    if (dx > 0) {
                        start = this.from.getConnectionPoint('right');
                        end = this.to.getConnectionPoint('left');
                    } else {
                        start = this.from.getConnectionPoint('left');
                        end = this.to.getConnectionPoint('right');
                    }
                } else {
                    // Vertical layout
                    if (dy > 0) {
                        start = this.from.getConnectionPoint('bottom');
                        end = this.to.getConnectionPoint('top');
                    } else {
                        start = this.from.getConnectionPoint('top');
                        end = this.to.getConnectionPoint('bottom');
                    }
                }

                // Color based on condition - ADF style
                const colors = {
                    'Succeeded': '#107c10',
                    'Failed': '#d13438',
                    'Skipped': '#ffa500',
                    'Completed': '#0078d4'
                };
                
                const color = colors[this.condition] || '#107c10';
                
                // Snap to pixel grid for crisp lines
                const snapToPixel = (val) => Math.floor(val) + 0.5;
                const startX = snapToPixel(start.x);
                const startY = snapToPixel(start.y);
                const endX = snapToPixel(end.x);
                const endY = snapToPixel(end.y);
                
                // Draw orthogonal (elbowed) connection line - ADF style
                ctx.strokeStyle = color;
                ctx.lineWidth = 1.5;
                ctx.lineCap = 'butt';
                ctx.lineJoin = 'miter';
                ctx.beginPath();
                
                // Calculate midpoint for elbow
                if (Math.abs(dx) > Math.abs(dy)) {
                    // Horizontal connection with elbow
                    const midX = startX + (endX - startX) / 2;
                    ctx.moveTo(startX, startY);
                    ctx.lineTo(midX, startY);
                    ctx.lineTo(midX, endY);
                    ctx.lineTo(endX, endY);
                } else {
                    // Vertical connection with elbow
                    const midY = startY + (endY - startY) / 2;
                    ctx.moveTo(startX, startY);
                    ctx.lineTo(startX, midY);
                    ctx.lineTo(endX, midY);
                    ctx.lineTo(endX, endY);
                }
                
                ctx.stroke();

                // Draw clean arrow head pointing in the direction of the line
                const arrowSize = 7;
                
                // Determine arrow direction based on the last segment
                let arrowAngle;
                if (Math.abs(dx) > Math.abs(dy)) {
                    // Horizontal approach
                    arrowAngle = dx > 0 ? 0 : Math.PI;
                } else {
                    // Vertical approach
                    arrowAngle = dy > 0 ? Math.PI / 2 : -Math.PI / 2;
                }
                
                ctx.fillStyle = color;
                ctx.beginPath();
                ctx.moveTo(endX, endY);
                ctx.lineTo(
                    endX - arrowSize * Math.cos(arrowAngle - Math.PI / 6),
                    endY - arrowSize * Math.sin(arrowAngle - Math.PI / 6)
                );
                ctx.lineTo(
                    endX - arrowSize * Math.cos(arrowAngle + Math.PI / 6),
                    endY - arrowSize * Math.sin(arrowAngle + Math.PI / 6)
                );
                ctx.closePath();
                ctx.fill();
            }
        }

        // Draw everything
        function draw() {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            
            // Draw grid
            drawGrid();

            // Draw connections only (activities are now DOM elements)
            connections.forEach(conn => conn.draw(ctx));
        }

        // Optimized draw with requestAnimationFrame
        function requestDraw() {
            if (!needsRedraw) {
                needsRedraw = true;
                if (animationFrameId) {
                    cancelAnimationFrame(animationFrameId);
                }
                animationFrameId = requestAnimationFrame(() => {
                    draw();
                    needsRedraw = false;
                    animationFrameId = null;
                });
            }
        }

        function drawGrid() {
            const gridSize = 20;
            ctx.strokeStyle = 'rgba(128, 128, 128, 0.1)';
            ctx.lineWidth = 1;

            for (let x = 0; x < canvas.width; x += gridSize) {
                ctx.beginPath();
                ctx.moveTo(x, 0);
                ctx.lineTo(x, canvas.height);
                ctx.stroke();
            }

            for (let y = 0; y < canvas.height; y += gridSize) {
                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(canvas.width, y);
                ctx.stroke();
            }
        }

        // Event handlers
        document.querySelectorAll('.activity-item').forEach(item => {
            item.addEventListener('dragstart', (e) => {
                e.dataTransfer.setData('activityType', item.getAttribute('data-type'));
                console.log('Drag started:', item.getAttribute('data-type'));
            });
            
            // Also add click to add at center as alternative
            item.addEventListener('dblclick', (e) => {
                const activityType = item.getAttribute('data-type');
                console.log('Double-click add:', activityType);
                const canvasWrapper = document.getElementById('canvasWrapper');
                const centerX = canvas.width / 2;
                const centerY = canvas.height / 2;
                const activity = new Activity(activityType, centerX - 60, centerY - 40 + activities.length * 20, canvasWrapper);
                activities.push(activity);
                console.log('Activities count:', activities.length);
                draw();
            });
        });

        document.getElementById('canvasWrapper').addEventListener('dragover', (e) => {
            e.preventDefault();
            console.log('Drag over canvas');
        });

        document.getElementById('canvasWrapper').addEventListener('drop', (e) => {
            e.preventDefault();
            const activityType = e.dataTransfer.getData('activityType');
            const canvasWrapper = document.getElementById('canvasWrapper');
            const wrapperRect = canvasWrapper.getBoundingClientRect();
            const x = e.clientX - wrapperRect.left + canvasWrapper.scrollLeft;
            const y = e.clientY - wrapperRect.top + canvasWrapper.scrollTop;
            
            console.log('Dropping activity:', activityType, 'at', x, y);
            const activity = new Activity(activityType, x - 90, y - 28, canvasWrapper);
            activities.push(activity);
            console.log('Activities count:', activities.length);
            draw();
        });

        // Canvas mousedown - deselect when clicking empty space
        document.getElementById('canvasWrapper').addEventListener('mousedown', (e) => {
            // Only handle if clicking directly on the wrapper (not on an activity or canvas)
            if (e.target.id === 'canvasWrapper') {
                selectedActivity = null;
                // Deselect all activities
                activities.forEach(a => a.setSelected(false));
                showProperties(null);
                draw();
            }
        });

        document.addEventListener('mousemove', (e) => {
            if (isDragging && draggedActivity) {
                const canvasWrapper = document.getElementById('canvasWrapper');
                const wrapperRect = canvasWrapper.getBoundingClientRect();
                const x = e.clientX - wrapperRect.left + canvasWrapper.scrollLeft - dragOffset.x;
                const y = e.clientY - wrapperRect.top + canvasWrapper.scrollTop - dragOffset.y;
                draggedActivity.updatePosition(x, y);
                requestDraw(); // Use optimized draw
            } else if (connectionStart) {
                const canvasWrapper = document.getElementById('canvasWrapper');
                const wrapperRect = canvasWrapper.getBoundingClientRect();
                const mouseX = e.clientX - wrapperRect.left + canvasWrapper.scrollLeft;
                const mouseY = e.clientY - wrapperRect.top + canvasWrapper.scrollTop;
                
                draw();
                // Draw temporary connection line
                ctx.strokeStyle = '#0078d4';
                ctx.lineWidth = 2;
                ctx.setLineDash([5, 5]);
                ctx.beginPath();
                ctx.moveTo(connectionStart.x, connectionStart.y);
                ctx.lineTo(mouseX, mouseY);
                ctx.stroke();
                ctx.setLineDash([]);
            }
        });

        document.addEventListener('mouseup', (e) => {
            if (connectionStart) {
                // Check if mouse is over an activity element
                const targetElement = document.elementFromPoint(e.clientX, e.clientY);
                if (targetElement) {
                    const activityBox = targetElement.closest('.activity-box');
                    if (activityBox && activityBox.dataset.activityId) {
                        const targetId = parseFloat(activityBox.dataset.activityId);
                        const targetActivity = activities.find(a => a.id === targetId);
                        if (targetActivity && targetActivity !== connectionStart.activity) {
                            // Show condition selector
                            showConnectionConditionDialog(connectionStart.activity, targetActivity, e.clientX, e.clientY);
                        }
                    }
                }
                
                connectionStart = null;
                canvas.style.cursor = 'default';
            }
            
            if (isDragging) {
                if (draggedActivity && draggedActivity.element) {
                    draggedActivity.element.classList.remove('dragging');
                    draggedActivity.element.style.cursor = 'pointer';
                }
            }
            
            isDragging = false;
            draggedActivity = null;
            draw();
        });



        function getMousePos(e) {
            const rect = canvas.getBoundingClientRect();
            return {
                x: e.clientX - rect.left,
                y: e.clientY - rect.top
            };
        }

        // Context menu
        function showContextMenu(x, y) {
            const menu = document.getElementById('contextMenu');
            menu.style.left = x + 'px';
            menu.style.top = y + 'px';
            menu.style.display = 'block';
        }

        document.addEventListener('click', () => {
            document.getElementById('contextMenu').style.display = 'none';
        });

        document.querySelectorAll('.context-menu-item').forEach(item => {
            item.addEventListener('click', (e) => {
                const action = item.getAttribute('data-action');
                
                if (action === 'delete' && selectedActivity) {
                    // Remove from DOM
                    selectedActivity.remove();
                    // Remove from arrays
                    activities = activities.filter(a => a !== selectedActivity);
                    connections = connections.filter(c => 
                        c.from !== selectedActivity && c.to !== selectedActivity
                    );
                    selectedActivity = null;
                    showProperties(null);
                    draw();
                }
            });
        });

        // Connection condition dialog
        function showConnectionConditionDialog(fromActivity, toActivity, x, y) {
            const dialog = document.createElement('div');
            dialog.style.cssText = \`
                position: fixed;
                left: \${x}px;
                top: \${y}px;
                background: var(--vscode-menu-background);
                border: 1px solid var(--vscode-menu-border);
                border-radius: 4px;
                padding: 8px;
                z-index: 10000;
                box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                min-width: 150px;
            \`;
            
            dialog.innerHTML = \`
                <div style="font-size: 12px; font-weight: 600; margin-bottom: 8px; color: var(--vscode-foreground);">Dependency Condition</div>
                <button class="toolbar-button" data-condition="Succeeded" style="width: 100%; margin: 2px 0; background: #00a86b; color: white;">✓ Succeeded</button>
                <button class="toolbar-button" data-condition="Failed" style="width: 100%; margin: 2px 0; background: #d13438; color: white;">✗ Failed</button>
                <button class="toolbar-button" data-condition="Completed" style="width: 100%; margin: 2px 0; background: #0078d4; color: white;">⊙ Completed</button>
                <button class="toolbar-button" data-condition="Skipped" style="width: 100%; margin: 2px 0; background: #ffa500; color: white;">⊘ Skipped</button>
            \`;
            
            document.body.appendChild(dialog);
            
            dialog.querySelectorAll('button').forEach(btn => {
                btn.addEventListener('click', () => {
                    // Check if connection already exists
                    const duplicate = connections.find(c => 
                        c.from.id === fromActivity.id && c.to.id === toActivity.id
                    );
                    
                    if (duplicate) {
                        alert('A dependency already exists between these activities');
                        document.body.removeChild(dialog);
                        return;
                    }
                    
                    const condition = btn.getAttribute('data-condition');
                    const conn = new Connection(fromActivity, toActivity, condition);
                    connections.push(conn);
                    document.body.removeChild(dialog);
                    draw();
                });
            });
            
            // Close on click outside
            setTimeout(() => {
                const closeHandler = (e) => {
                    if (!dialog.contains(e.target)) {
                        document.body.removeChild(dialog);
                        document.removeEventListener('click', closeHandler);
                    }
                };
                document.addEventListener('click', closeHandler);
            }, 100);
        }

        // Configuration panel
        function showProperties(activity, activeTabId = null) {
            const rightPanel = document.getElementById('propertiesContent');
            const bottomPanel = document.getElementById('generalContent');
            
            // Toggle between pipeline-level and activity-level tabs
            const pipelineTabs = document.querySelectorAll('.pipeline-tab');
            const activityTabs = document.querySelectorAll('.activity-tab');
            const pipelinePanes = document.querySelectorAll('.pipeline-pane');
            const activityPanes = document.querySelectorAll('.activity-pane');
            
            if (!activity) {
                // Show pipeline-level tabs, hide activity-level tabs
                pipelineTabs.forEach(tab => tab.style.display = '');
                document.getElementById('activityTabsContainer').innerHTML = '';
                document.getElementById('activityPanesContainer').innerHTML = '';
                pipelinePanes.forEach(pane => pane.style.display = '');
                
                // Activate first pipeline tab
                document.querySelectorAll('.config-tab').forEach(t => {
                    t.classList.remove('active');
                    t.style.borderBottom = 'none';
                });
                document.querySelectorAll('.config-tab-pane').forEach(p => p.classList.remove('active'));
                const firstPipelineTab = document.querySelector('.pipeline-tab');
                if (firstPipelineTab) {
                    firstPipelineTab.classList.add('active');
                    firstPipelineTab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
                    firstPipelineTab.style.color = 'var(--vscode-tab-activeForeground)';
                    document.getElementById('tab-parameters').classList.add('active');
                }
                
                rightPanel.innerHTML = '<div class="empty-state">Select an activity to view its properties</div>';
                return;
            }
            
            // Get schema for activity
            const schema = ${JSON.stringify(activitySchemas)}[activity.type];
            const tabs = schema?.tabs || ['General', 'Settings', 'User Properties'];
            
            // Hide pipeline-level tabs
            pipelineTabs.forEach(tab => tab.style.display = 'none');
            pipelinePanes.forEach(pane => pane.style.display = 'none');
            
            // Helper function to generate form fields
            function generateFormField(key, prop, activity) {
                let value = activity[key] || prop.default || '';
                
                // Handle reference objects (e.g., {referenceName: "...", type: "..."})
                if (prop.type === 'reference' && typeof value === 'object' && value !== null) {
                    value = value.referenceName || JSON.stringify(value);
                }
                
                const required = prop.required ? ' *' : '';
                
                let fieldHtml = \`<div class="property-group">\`;
                fieldHtml += \`<div class="property-label">\${prop.label}\${required}</div>\`;
                
                switch (prop.type) {
                    case 'string':
                        if (prop.multiline) {
                            fieldHtml += \`<textarea class="property-input" data-key="\${key}" rows="3" placeholder="\${prop.label}...">\${value}</textarea>\`;
                        } else {
                            fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="\${prop.label}">\`;
                        }
                        break;
                    case 'text':
                        const readonly = prop.readonly ? 'readonly' : '';
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="\${prop.placeholder || prop.label}" \${readonly}>\`;
                        break;
                    case 'number':
                        const min = prop.min !== undefined ? \`min="\${prop.min}"\` : '';
                        const max = prop.max !== undefined ? \`max="\${prop.max}"\` : '';
                        fieldHtml += \`<input type="number" class="property-input" data-key="\${key}" value="\${value}" \${min} \${max}>\`;
                        break;
                    case 'boolean':
                        const checked = value ? 'checked' : '';
                        fieldHtml += \`<input type="checkbox" data-key="\${key}" \${checked} style="width: auto;">\`;
                        break;
                    case 'select':
                        fieldHtml += \`<select class="property-input" data-key="\${key}">\`;
                        prop.options.forEach(opt => {
                            const selected = opt === value ? 'selected' : '';
                            fieldHtml += \`<option value="\${opt}" \${selected}>\${opt}</option>\`;
                        });
                        fieldHtml += \`</select>\`;
                        break;
                    case 'radio':
                        fieldHtml += \`<div style="display: flex; gap: 16px; flex: 1; align-items: center;">\`;
                        prop.options.forEach(opt => {
                            const checked = opt === value ? 'checked' : '';
                            const displayName = opt === 'storedProcedure' ? 'Stored procedure' : opt.charAt(0).toUpperCase() + opt.slice(1);
                            fieldHtml += \`<label style="display: flex; align-items: center; gap: 6px; cursor: pointer;">\`;
                            fieldHtml += \`<input type="radio" name="\${key}" data-key="\${key}" value="\${opt}" \${checked} style="margin: 0;">\`;
                            fieldHtml += \`<span>\${displayName}</span>\`;
                            fieldHtml += \`</label>\`;
                        });
                        fieldHtml += \`</div>\`;
                        break;
                    case 'keyvalue':
                        fieldHtml += \`<div style="flex: 1;"><div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Key-value pairs</div>\`;
                        fieldHtml += \`<button class="add-kv-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add</button>\`;
                        fieldHtml += \`<div class="kv-list" data-key="\${key}"></div></div>\`;
                        break;
                    case 'dataset':
                        console.log('[GenerateField] Dataset field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        fieldHtml += \`<select class="property-input dataset-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">Select dataset...</option>\`;
                        if (datasetList && datasetList.length > 0) {
                            datasetList.forEach(ds => {
                                const selected = ds === value ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected dataset:', ds, 'matches value:', value);
                                fieldHtml += \`<option value="\${ds}" \${selected}>\${ds}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'reference':
                        fieldHtml += \`<div style="display: flex; gap: 8px; flex: 1;">\`;
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="Select \${prop.label}..." readonly>\`;
                        fieldHtml += \`<button style="padding: 6px 12px; background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Browse</button>\`;
                        fieldHtml += \`</div>\`;
                        break;
                    case 'expression':
                        fieldHtml += \`<div style="display: flex; gap: 8px; flex: 1;">\`;
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="Enter expression...">\`;
                        fieldHtml += \`<button style="padding: 6px 12px; background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">fx</button>\`;
                        fieldHtml += \`</div>\`;
                        break;
                    case 'object':
                    case 'array':
                        fieldHtml += \`<div style="padding: 8px; background: var(--vscode-input-background); border: 1px solid var(--vscode-input-border); border-radius: 2px; font-family: monospace; font-size: 12px; color: var(--vscode-descriptionForeground); flex: 1; cursor: pointer;">\`;
                        fieldHtml += \`Click to configure \${prop.label}...\`;
                        fieldHtml += \`</div>\`;
                        break;
                    default:
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}">\`;
                }
                
                fieldHtml += \`</div>\`;
                return fieldHtml;
            }
            
            // Build content for each tab
            let generalContent = '';
            if (schema && schema.commonProperties) {
                for (const [key, prop] of Object.entries(schema.commonProperties)) {
                    if (prop.section === 'policy') continue;
                    generalContent += generateFormField(key, prop, activity);
                }
                
                const policyProps = Object.entries(schema.commonProperties).filter(([k, p]) => p.section === 'policy');
                if (policyProps.length > 0) {
                    generalContent += '<div style="margin-top: 24px; margin-bottom: 12px; font-weight: 600; font-size: 13px; color: var(--vscode-foreground);">Policy</div>';
                    policyProps.forEach(([key, prop]) => {
                        generalContent += generateFormField(key, prop, activity);
                    });
                }
            }
            
            let settingsContent = '';
            if (schema && schema.typeProperties) {
                for (const [key, prop] of Object.entries(schema.typeProperties)) {
                    settingsContent += generateFormField(key, prop, activity);
                }
            }
            if (!settingsContent) {
                settingsContent = '<div style="color: var(--vscode-descriptionForeground); padding: 20px; text-align: center;">No activity-specific settings available</div>';
            }
            console.log('Settings content length:', settingsContent.length);
            
            // Build Source tab content
            let sourceContent = '';
            if (schema && schema.sourceProperties) {
                for (const [key, prop] of Object.entries(schema.sourceProperties)) {
                    sourceContent += generateFormField(key, prop, activity);
                }
                
                // Ensure _sourceDatasetType is set if we have a sourceDataset
                if (activity.sourceDataset && !activity._sourceDatasetType && datasetContents[activity.sourceDataset]) {
                    activity._sourceDatasetType = datasetContents[activity.sourceDataset].properties?.type;
                    console.log('[ShowProps] Auto-detected source dataset type:', activity._sourceDatasetType);
                }
                
                // If sourceDataset is selected, dynamically load dataset-specific fields
                if (activity.sourceDataset && activity._sourceDatasetType) {
                    const datasetType = activity._sourceDatasetType;
                    console.log('Adding source fields for dataset type:', datasetType);
                    if (datasetSchemas[datasetType] && datasetSchemas[datasetType].sourceFields) {
                        sourceContent += '<div style="border-top: 1px solid var(--vscode-panel-border); margin: 16px 0; padding-top: 16px;"></div>';
                        sourceContent += '<div style="font-size: 13px; font-weight: bold; color: var(--vscode-foreground); margin-bottom: 12px;">Source Settings (' + datasetSchemas[datasetType].name + ')</div>';
                        for (const [key, prop] of Object.entries(datasetSchemas[datasetType].sourceFields)) {
                            sourceContent += generateFormField(key, prop, activity);
                        }
                        console.log('Added', Object.keys(datasetSchemas[datasetType].sourceFields).length, 'source fields');
                    }
                }
            }
            
            // Build Sink tab content
            let sinkContent = '';
            if (schema && schema.sinkProperties) {
                for (const [key, prop] of Object.entries(schema.sinkProperties)) {
                    sinkContent += generateFormField(key, prop, activity);
                }
                
                // Ensure _sinkDatasetType is set if we have a sinkDataset
                if (activity.sinkDataset && !activity._sinkDatasetType && datasetContents[activity.sinkDataset]) {
                    activity._sinkDatasetType = datasetContents[activity.sinkDataset].properties?.type;
                    console.log('[ShowProps] Auto-detected sink dataset type:', activity._sinkDatasetType);
                }
                
                // If sinkDataset is selected, dynamically load dataset-specific fields
                if (activity.sinkDataset && activity._sinkDatasetType) {
                    const datasetType = activity._sinkDatasetType;
                    console.log('Adding sink fields for dataset type:', datasetType);
                    if (datasetSchemas[datasetType] && datasetSchemas[datasetType].sinkFields) {
                        sinkContent += '<div style="border-top: 1px solid var(--vscode-panel-border); margin: 16px 0; padding-top: 16px;"></div>';
                        sinkContent += '<div style="font-size: 13px; font-weight: bold; color: var(--vscode-foreground); margin-bottom: 12px;">Sink Settings (' + datasetSchemas[datasetType].name + ')</div>';
                        for (const [key, prop] of Object.entries(datasetSchemas[datasetType].sinkFields)) {
                            sinkContent += generateFormField(key, prop, activity);
                        }
                        console.log('Added', Object.keys(datasetSchemas[datasetType].sinkFields).length, 'sink fields');
                    }
                }
            }
            
            // Build Mapping tab content
            let mappingContent = '<div style="color: var(--vscode-descriptionForeground); padding: 20px; text-align: center;">Mapping configuration coming soon</div>';
            
            activity.userProperties = activity.userProperties || [];
            let userPropsContent = '<div style="margin-bottom: 12px;">';
            userPropsContent += '<button id="addUserPropBtn" style="padding: 6px 12px; background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; cursor: pointer; border-radius: 2px; font-size: 12px;">+ Add User Property</button>';
            userPropsContent += '</div>';
            userPropsContent += '<div id="userPropsList">';
            activity.userProperties.forEach((prop, idx) => {
                userPropsContent += \`
                    <div class="property-group" style="margin-bottom: 12px;">
                        <input type="text" class="property-input" data-idx="\${idx}" data-field="name" value="\${prop.name}" placeholder="Property name" style="flex: 1;">
                        <input type="text" class="property-input" data-idx="\${idx}" data-field="value" value="\${prop.value}" placeholder="Property value" style="flex: 1;">
                        <button class="remove-user-prop" data-idx="\${idx}" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px;">Remove</button>
                    </div>
                \`;
            });
            userPropsContent += '</div>';
            console.log('User props content length:', userPropsContent.length);
            console.log('Schema:', schema);
            console.log('Activity type:', activity.type);
            
            // Generate activity-level tabs with content already included
            const tabsContainer = document.getElementById('activityTabsContainer');
            const panesContainer = document.getElementById('activityPanesContainer');
            
            let tabsHtml = '';
            let panesHtml = '';
            
            tabs.forEach((tabName, idx) => {
                const tabId = tabName.toLowerCase().split(' ').join('-');
                const isActive = activeTabId ? (tabId === activeTabId) : (idx === 0);
                const activeClass = isActive ? ' active' : '';
                const activeStyle = isActive ? 'color: var(--vscode-tab-activeForeground); border-bottom: 2px solid var(--vscode-focusBorder);' : 'color: var(--vscode-tab-inactiveForeground);';
                
                tabsHtml += \`<button class="config-tab activity-tab\${activeClass}" data-tab="\${tabId}" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; \${activeStyle}">\${tabName}</button>\`;
                
                // Get the content for this tab
                let tabContent = '';
                if (tabId === 'general') tabContent = generalContent;
                else if (tabId === 'settings') tabContent = settingsContent;
                else if (tabId === 'source') tabContent = sourceContent;
                else if (tabId === 'sink') tabContent = sinkContent;
                else if (tabId === 'mapping') tabContent = mappingContent;
                else if (tabId === 'user-properties') tabContent = userPropsContent;
                
                console.log(\`Tab \${tabName} (id: \${tabId}) content length: \${tabContent.length}\`);
                
                const displayStyle = isActive ? 'display: block;' : 'display: none;';
                panesHtml += \`<div class="config-tab-pane activity-pane\${activeClass}" id="tab-\${tabId}" style="\${displayStyle}">\${tabContent}</div>\`;
            });
            
            tabsContainer.innerHTML = tabsHtml;
            panesContainer.innerHTML = panesHtml;

            // Right sidebar - basic properties
            rightPanel.innerHTML = \`
                <div class="property-group">
                    <div class="property-label">Name</div>
                    <input type="text" class="property-input" id="propName" value="\${activity.name}">
                </div>
                <div class="property-group">
                    <div class="property-label">Type</div>
                    <input type="text" class="property-input" value="\${activity.type}" readonly>
                </div>
                <div class="property-group">
                    <div class="property-label">Description</div>
                    <textarea class="property-input" id="propDescription" rows="3">\${activity.description}</textarea>
                </div>
                <div class="property-group">
                    <div class="property-label">Position</div>
                    <div style="display: flex; gap: 8px; flex: 1;">
                        <input type="number" class="property-input" id="propX" value="\${Math.round(activity.x)}" placeholder="X">
                        <input type="number" class="property-input" id="propY" value="\${Math.round(activity.y)}" placeholder="Y">
                    </div>
                </div>
            \`;
            
            // Add event listeners for user properties
            const addUserPropBtn = document.getElementById('addUserPropBtn');
            if (addUserPropBtn) {
                addUserPropBtn.addEventListener('click', () => {
                    activity.userProperties.push({ name: '', value: '' });
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            }
            
            document.querySelectorAll('.remove-user-prop').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const idx = parseInt(e.target.getAttribute('data-idx'));
                    activity.userProperties.splice(idx, 1);
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            document.querySelectorAll('#userPropsList input').forEach(input => {
                input.addEventListener('input', (e) => {
                    const idx = parseInt(e.target.getAttribute('data-idx'));
                    const field = e.target.getAttribute('data-field');
                    activity.userProperties[idx][field] = e.target.value;
                });
            });
            
            // Add event listeners to all config panel inputs to update activity object in real-time
            document.querySelectorAll('#configContent .property-input').forEach(input => {
                const key = input.getAttribute('data-key');
                if (!key) return;
                
                if (input.type === 'checkbox') {
                    input.addEventListener('change', (e) => {
                        activity[key] = e.target.checked;
                        console.log('Updated ' + key + ':', activity[key]);
                    });
                } else {
                    input.addEventListener('input', (e) => {
                        const value = e.target.value;
                        // Convert to appropriate type
                        if (input.type === 'number') {
                            activity[key] = parseFloat(value) || 0;
                        } else {
                            activity[key] = value;
                        }
                        console.log('Updated ' + key + ':', activity[key]);
                    });
                }
            });
            
            // Add event listeners for dataset dropdowns to trigger dynamic field loading
            document.querySelectorAll('#configContent .dataset-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const key = select.getAttribute('data-key');
                    const datasetName = e.target.value;
                    activity[key] = datasetName;
                    console.log('Updated ' + key + ':', activity[key]);
                    
                    // Get dataset type and store it
                    if (datasetName && datasetContents[datasetName]) {
                        const datasetType = datasetContents[datasetName].properties?.type;
                        console.log('Dataset selected:', datasetName, 'Type:', datasetType);
                        
                        // Store dataset type in activity for later use
                        if (key === 'sourceDataset') {
                            activity._sourceDatasetType = datasetType;
                        } else if (key === 'sinkDataset') {
                            activity._sinkDatasetType = datasetType;
                        }
                        
                        // Re-render the current tab to show dataset-specific fields
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            // Add event listeners for radio buttons
            document.querySelectorAll('#configContent input[type="radio"]').forEach(radio => {
                radio.addEventListener('change', (e) => {
                    if (e.target.checked) {
                        const key = e.target.getAttribute('data-key');
                        activity[key] = e.target.value;
                        console.log('Updated ' + key + ':', activity[key]);
                    }
                });
            });
            
            // Add event listeners for keyvalue add buttons
            document.querySelectorAll('.add-kv-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const key = e.target.getAttribute('data-key');
                    const kvList = document.querySelector(\`.kv-list[data-key="\${key}"]\`);
                    if (kvList) {
                        const kvPair = document.createElement('div');
                        kvPair.className = 'property-group';
                        kvPair.style.marginBottom = '8px';
                        kvPair.innerHTML = \`
                            <input type="text" class="property-input" placeholder="Key" style="flex: 1; margin-right: 8px;">
                            <input type="text" class="property-input" placeholder="Value" style="flex: 1; margin-right: 8px;">
                            <button class="remove-kv-btn" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px;">Remove</button>
                        \`;
                        kvList.appendChild(kvPair);
                        
                        // Add remove listener
                        kvPair.querySelector('.remove-kv-btn').addEventListener('click', () => {
                            kvPair.remove();
                        });
                    }
                });
            });
            
            // Add tab click handlers
            document.querySelectorAll('.activity-tab').forEach(tab => {
                tab.addEventListener('click', () => {
                    document.querySelectorAll('.config-tab').forEach(t => {
                        t.classList.remove('active');
                        t.style.color = 'var(--vscode-tab-inactiveForeground)';
                        t.style.borderBottom = 'none';
                    });
                    document.querySelectorAll('.activity-pane').forEach(p => {
                        p.classList.remove('active');
                        p.style.display = 'none';
                    });
                    
                    tab.classList.add('active');
                    tab.style.color = 'var(--vscode-tab-activeForeground)';
                    tab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
                    const tabName = tab.getAttribute('data-tab');
                    const pane = document.getElementById(\`tab-\${tabName}\`);
                    if (pane) {
                        pane.classList.add('active');
                        pane.style.display = '';
                    }
                });
            });

            // Right sidebar - basic properties
            rightPanel.innerHTML = \`
                <div class="property-group">
                    <div class="property-label">Name</div>
                    <input type="text" class="property-input" id="propName" value="\${activity.name}">
                </div>
                <div class="property-group">
                    <div class="property-label">Type</div>
                    <input type="text" class="property-input" value="\${activity.type}" readonly>
                </div>
                <div class="property-group">
                    <div class="property-label">Description</div>
                    <textarea class="property-input" id="propDescription" rows="3">\${activity.description}</textarea>
                </div>
                <div class="property-group">
                    <div class="property-label">Position</div>
                    <div style="display: flex; gap: 8px; flex: 1;">
                        <input type="number" class="property-input" id="propX" value="\${Math.round(activity.x)}" placeholder="X">
                        <input type="number" class="property-input" id="propY" value="\${Math.round(activity.y)}" placeholder="Y">
                    </div>
                </div>
            \`;

            document.getElementById('propName').addEventListener('input', (e) => {
                activity.updateName(e.target.value);
                draw();
            });

            document.getElementById('propDescription').addEventListener('input', (e) => {
                activity.description = e.target.value;
            });
            
            document.getElementById('propX').addEventListener('input', (e) => {
                const x = parseInt(e.target.value) || 0;
                activity.updatePosition(x, activity.y);
                draw();
            });
            
            document.getElementById('propY').addEventListener('input', (e) => {
                const y = parseInt(e.target.value) || 0;
                activity.updatePosition(activity.x, y);
                draw();
            });
        }

        // Tab switching
        document.querySelectorAll('.config-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                // Remove active class and styles from all tabs
                document.querySelectorAll('.config-tab').forEach(t => {
                    t.classList.remove('active');
                    t.style.color = 'var(--vscode-tab-inactiveForeground)';
                    t.style.borderBottom = 'none';
                });
                document.querySelectorAll('.config-tab-pane').forEach(p => p.classList.remove('active'));
                
                // Add active class and styles to clicked tab
                tab.classList.add('active');
                tab.style.color = 'var(--vscode-tab-activeForeground)';
                tab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
                const tabName = tab.getAttribute('data-tab');
                document.getElementById(\`tab-\${tabName}\`).classList.add('active');
            });
        });

        // Toolbar buttons
        document.getElementById('saveBtn').addEventListener('click', () => {
            const data = {
                name: "pipeline1", // TODO: Get from pipeline name input
                activities: activities.map(a => {
                    // Build the activity JSON with all properties
                    const activity = {
                        name: a.name,
                        type: a.type,
                        dependsOn: connections
                            .filter(c => c.to === a)
                            .map(c => ({
                                activity: c.from.name,
                                dependencyConditions: [c.condition || 'Succeeded']
                            })),
                        policy: {
                            timeout: a.timeout || "0.12:00:00",
                            retry: a.retry || 0,
                            retryIntervalInSeconds: a.retryIntervalInSeconds || 30,
                            secureOutput: a.secureOutput || false,
                            secureInput: a.secureInput || false
                        },
                        userProperties: a.userProperties || []
                    };
                    
                    // Add optional description
                    if (a.description) {
                        activity.description = a.description;
                    }
                    
                    // Add optional state (for Copy activity)
                    if (a.state) {
                        activity.state = a.state;
                    }
                    if (a.onInactiveMarkAs) {
                        activity.onInactiveMarkAs = a.onInactiveMarkAs;
                    }
                    
                    // Preserve sourceDataset and sinkDataset for Copy activities
                    if (a.type === 'Copy') {
                        if (a.sourceDataset) activity.sourceDataset = a.sourceDataset;
                        if (a.sinkDataset) activity.sinkDataset = a.sinkDataset;
                        if (a._sourceDatasetType) activity._sourceDatasetType = a._sourceDatasetType;
                        if (a._sinkDatasetType) activity._sinkDatasetType = a._sinkDatasetType;
                        if (a._sourceObject) activity._sourceObject = a._sourceObject;
                        if (a._sinkObject) activity._sinkObject = a._sinkObject;
                    }
                    
                    // Collect all typeProperties from the activity object
                    const typeProperties = {};
                    const commonProps = ['id', 'type', 'x', 'y', 'width', 'height', 'name', 'description', 'color', 'container', 'element', 
                                         'timeout', 'retry', 'retryIntervalInSeconds', 'secureOutput', 'secureInput', 'userProperties', 'state', 'onInactiveMarkAs',
                                         'dynamicAllocation', 'minExecutors', 'maxExecutors', 'dependsOn', 'policy',
                                         'sourceDataset', 'sinkDataset', 'recursive', 'modifiedDatetimeStart', 'modifiedDatetimeEnd',
                                         'wildcardFolderPath', 'wildcardFileName', 'enablePartitionDiscovery',
                                         'writeBatchSize', 'writeBatchTimeout', 'preCopyScript', 'maxConcurrentConnections', 'writeBehavior', 
                                         'sqlWriterUseTableLock', 'disableMetricsCollection', '_sourceObject', '_sinkObject', '_sourceDatasetType', '_sinkDatasetType',
                                         'typeProperties', 'inputs', 'outputs', 'source', 'sink']; // Exclude nested objects that will be reconstructed
                    
                    for (const key in a) {
                        if (!commonProps.includes(key) && a.hasOwnProperty(key) && typeof a[key] !== 'function') {
                            typeProperties[key] = a[key];
                        }
                    }
                    
                    // For SynapseNotebook, convert dynamicAllocation fields back to conf object
                    if (a.type === 'SynapseNotebook') {
                        if (a.dynamicAllocation !== undefined || a.minExecutors || a.maxExecutors) {
                            typeProperties.conf = {};
                            if (a.dynamicAllocation !== undefined) {
                                typeProperties.conf['spark.dynamicAllocation.enabled'] = a.dynamicAllocation;
                            }
                            if (a.minExecutors !== undefined) {
                                typeProperties.conf['spark.dynamicAllocation.minExecutors'] = a.minExecutors;
                            }
                            if (a.maxExecutors !== undefined) {
                                typeProperties.conf['spark.dynamicAllocation.maxExecutors'] = a.maxExecutors;
                            }
                        }
                    }
                    
                    // For Copy activity, reconstruct nested source/sink structures and inputs/outputs
                    if (a.type === 'Copy') {
                        console.log('[Extension] Copy activity - reconstructing source/sink');
                        console.log('[Extension] Source dataset:', a.sourceDataset);
                        console.log('[Extension] Sink dataset:', a.sinkDataset);
                        
                        // Reconstruct source object
                        if (a._sourceObject) {
                            typeProperties.source = JSON.parse(JSON.stringify(a._sourceObject));
                            console.log('[Extension] Restored _sourceObject:', typeProperties.source);
                            // Update with any changed values
                            if (a.sourceType) typeProperties.source.type = a.sourceType;
                            if (typeProperties.source.storeSettings) {
                                if (a.recursive !== undefined) typeProperties.source.storeSettings.recursive = a.recursive;
                                if (a.modifiedDatetimeStart !== undefined) typeProperties.source.storeSettings.modifiedDatetimeStart = a.modifiedDatetimeStart;
                                if (a.modifiedDatetimeEnd !== undefined) typeProperties.source.storeSettings.modifiedDatetimeEnd = a.modifiedDatetimeEnd;
                                if (a.wildcardFolderPath !== undefined) typeProperties.source.storeSettings.wildcardFolderPath = a.wildcardFolderPath;
                                if (a.wildcardFileName !== undefined) typeProperties.source.storeSettings.wildcardFileName = a.wildcardFileName;
                                if (a.enablePartitionDiscovery !== undefined) typeProperties.source.storeSettings.enablePartitionDiscovery = a.enablePartitionDiscovery;
                            }
                        }
                        
                        // Reconstruct sink object
                        if (a._sinkObject) {
                            typeProperties.sink = JSON.parse(JSON.stringify(a._sinkObject));
                            // Update with any changed values
                            if (a.sinkType) typeProperties.sink.type = a.sinkType;
                            if (a.writeBatchSize !== undefined) typeProperties.sink.writeBatchSize = a.writeBatchSize;
                            if (a.writeBatchTimeout !== undefined) typeProperties.sink.writeBatchTimeout = a.writeBatchTimeout;
                            if (a.preCopyScript !== undefined) typeProperties.sink.preCopyScript = a.preCopyScript;
                            if (a.maxConcurrentConnections !== undefined) typeProperties.sink.maxConcurrentConnections = a.maxConcurrentConnections;
                            if (a.writeBehavior !== undefined) typeProperties.sink.writeBehavior = a.writeBehavior;
                            if (a.sqlWriterUseTableLock !== undefined) typeProperties.sink.sqlWriterUseTableLock = a.sqlWriterUseTableLock;
                            if (a.disableMetricsCollection !== undefined) typeProperties.sink.disableMetricsCollection = a.disableMetricsCollection;
                            console.log('[Extension] Reconstructed sink:', typeProperties.sink);
                        }
                        
                        // Add inputs/outputs for Copy activity
                        if (a.sourceDataset) {
                            activity.inputs = [{
                                referenceName: a.sourceDataset,
                                type: 'DatasetReference'
                            }];
                            console.log('[Extension] Added inputs:', activity.inputs);
                        }
                        if (a.sinkDataset) {
                            activity.outputs = [{
                                referenceName: a.sinkDataset,
                                type: 'DatasetReference'
                            }];
                            console.log('[Extension] Added outputs:', activity.outputs);
                        }
                    }
                    
                    activity.typeProperties = typeProperties;
                    console.log('[Extension] Final activity object:', JSON.stringify(activity, null, 2));
                    
                    return activity;
                })
            };
            
            console.log('[Webview] Sending save message with filePath:', currentFilePath);
            vscode.postMessage({ 
                type: 'save', 
                data: data,
                filePath: currentFilePath 
            });
        });

        document.getElementById('clearBtn').addEventListener('click', () => {
            if (confirm('Clear all activities?')) {
                // Remove all activity DOM elements
                activities.forEach(a => a.remove());
                activities = [];
                connections = [];
                selectedActivity = null;
                showProperties(null);
                draw();
            }
        });

        document.getElementById('zoomInBtn').addEventListener('click', () => {
            scale *= 1.2;
            ctx.scale(1.2, 1.2);
            draw();
        });

        document.getElementById('zoomOutBtn').addEventListener('click', () => {
            scale /= 1.2;
            ctx.scale(1 / 1.2, 1 / 1.2);
            draw();
        });

        document.getElementById('fitBtn').addEventListener('click', () => {
            scale = 1;
            ctx.setTransform(1, 0, 0, 1, 0, 0);
            draw();
        });

        // Handle messages from extension
        window.addEventListener('message', event => {
            const message = event.data;
            
            if (message.type === 'initSchemas') {
                datasetSchemas = message.datasetSchemas;
                datasetList = message.datasetList || [];
                datasetContents = message.datasetContents || {};
                console.log('Dataset schemas loaded:', Object.keys(datasetSchemas));
                console.log('Dataset list loaded:', datasetList);
                console.log('Dataset contents loaded:', Object.keys(datasetContents).length, 'datasets');
            } else if (message.type === 'addActivity') {
                const canvasWrapper = document.getElementById('canvasWrapper');
                const activity = new Activity(message.activityType, 100, 100, canvasWrapper);
                activities.push(activity);
                draw();
            } else if (message.type === 'loadPipeline') {
                currentFilePath = message.filePath || null; // Store the file path
                loadPipelineFromJson(message.data);
            }
        });

        function loadPipelineFromJson(pipelineJson) {
            try {
                // Clear existing
                activities.forEach(a => a.remove());
                activities = [];
                connections = [];
                
                // Extract activities from Synapse format
                const pipelineActivities = pipelineJson.properties?.activities || pipelineJson.activities || [];
                
                // Create activities first
                const canvasWrapper = document.getElementById('canvasWrapper');
                const activityMap = new Map();
                
                pipelineActivities.forEach((activityData, index) => {
                    const x = 100 + (index % 5) * 200;
                    const y = 100 + Math.floor(index / 5) * 150;
                    
                    const activity = new Activity(activityData.type, x, y, canvasWrapper);
                    activity.name = activityData.name;
                    activity.description = activityData.description || '';
                    
                    // Load policy properties
                    if (activityData.policy) {
                        activity.timeout = activityData.policy.timeout;
                        activity.retry = activityData.policy.retry;
                        activity.retryIntervalInSeconds = activityData.policy.retryIntervalInSeconds;
                        activity.secureOutput = activityData.policy.secureOutput;
                        activity.secureInput = activityData.policy.secureInput;
                    }
                    
                    // Load state (for Copy activity)
                    if (activityData.state) {
                        activity.state = activityData.state;
                    }
                    if (activityData.onInactiveMarkAs) {
                        activity.onInactiveMarkAs = activityData.onInactiveMarkAs;
                    }
                    
                    activity.userProperties = activityData.userProperties || [];
                    
                    // Load inputs/outputs (for Copy activity)
                    if (activityData.inputs) {
                        activity.inputs = activityData.inputs;
                    }
                    if (activityData.outputs) {
                        activity.outputs = activityData.outputs;
                    }
                    
                    // Copy all typeProperties to activity object
                    if (activityData.typeProperties) {
                        // Handle special case: conf object for Spark settings
                        if (activityData.typeProperties.conf) {
                            const conf = activityData.typeProperties.conf;
                            if (conf['spark.dynamicAllocation.enabled'] !== undefined) {
                                activity.dynamicAllocation = conf['spark.dynamicAllocation.enabled'];
                            }
                            if (conf['spark.dynamicAllocation.minExecutors'] !== undefined) {
                                activity.minExecutors = conf['spark.dynamicAllocation.minExecutors'];
                            }
                            if (conf['spark.dynamicAllocation.maxExecutors'] !== undefined) {
                                activity.maxExecutors = conf['spark.dynamicAllocation.maxExecutors'];
                            }
                            // Don't copy the raw conf object
                            delete activityData.typeProperties.conf;
                        }
                        
                        // Handle Copy activity source/sink nested structures
                        if (activityData.type === 'Copy') {
                            const tp = activityData.typeProperties;
                            
                            console.log('[Load] Copy activity detected:', activityData.name);
                            console.log('[Load] inputs:', activityData.inputs);
                            console.log('[Load] outputs:', activityData.outputs);
                            console.log('[Load] typeProperties keys:', Object.keys(tp));
                            
                            // Parse inputs (source dataset)
                            if (activityData.inputs && activityData.inputs.length > 0) {
                                if (typeof activityData.inputs[0] === 'object' && activityData.inputs[0].referenceName) {
                                    activity.sourceDataset = activityData.inputs[0].referenceName;
                                } else {
                                    activity.sourceDataset = activityData.inputs[0];
                                }
                                console.log('[Load] Set sourceDataset to:', activity.sourceDataset);
                                
                                // Get dataset type from loaded contents
                                if (activity.sourceDataset && datasetContents[activity.sourceDataset]) {
                                    activity._sourceDatasetType = datasetContents[activity.sourceDataset].properties?.type;
                                    console.log('[Load] Source dataset type:', activity._sourceDatasetType);
                                }
                            } else {
                                console.log('[Load] No inputs found for Copy activity');
                            }
                            
                            // Parse outputs (sink dataset)
                            if (activityData.outputs && activityData.outputs.length > 0) {
                                if (typeof activityData.outputs[0] === 'object' && activityData.outputs[0].referenceName) {
                                    activity.sinkDataset = activityData.outputs[0].referenceName;
                                } else {
                                    activity.sinkDataset = activityData.outputs[0];
                                }
                                console.log('[Load] Set sinkDataset to:', activity.sinkDataset);
                                
                                // Get dataset type from loaded contents
                                if (activity.sinkDataset && datasetContents[activity.sinkDataset]) {
                                    activity._sinkDatasetType = datasetContents[activity.sinkDataset].properties?.type;
                                    console.log('[Load] Sink dataset type:', activity._sinkDatasetType);
                                }
                            } else {
                                console.log('[Load] No outputs found for Copy activity');
                            }
                            
                            // Handle incorrectly nested structure (typeProperties.typeProperties)
                            // This happens when the save created a double-nested structure
                            let sourceObj = tp.source;
                            let sinkObj = tp.sink;
                            
                            if (tp.typeProperties) {
                                console.log('[Load] Found nested typeProperties, using deeper level');
                                if (tp.typeProperties.source) sourceObj = tp.typeProperties.source;
                                if (tp.typeProperties.sink) sinkObj = tp.typeProperties.sink;
                            }
                            
                            // Flatten source properties
                            if (sourceObj) {
                                // Store the full source object for saving later
                                activity._sourceObject = sourceObj;
                                console.log('[Load] Source object:', sourceObj);
                                
                                // Flatten storeSettings
                                if (sourceObj.storeSettings) {
                                    const store = sourceObj.storeSettings;
                                    activity.recursive = store.recursive;
                                    activity.modifiedDatetimeStart = store.modifiedDatetimeStart;
                                    activity.modifiedDatetimeEnd = store.modifiedDatetimeEnd;
                                    activity.wildcardFolderPath = store.wildcardFolderPath;
                                    activity.wildcardFileName = store.wildcardFileName;
                                    activity.enablePartitionDiscovery = store.enablePartitionDiscovery;
                                    activity.maxConcurrentConnections = store.maxConcurrentConnections;
                                    console.log('[Load] Flattened source fields - wildcardFolderPath:', activity.wildcardFolderPath);
                                }
                            }
                            
                            // Flatten sink properties
                            if (sinkObj) {
                                // Store the full sink object for saving later
                                activity._sinkObject = sinkObj;
                                console.log('[Load] Sink object:', sinkObj);
                                
                                activity.writeBatchSize = sinkObj.writeBatchSize;
                                activity.writeBatchTimeout = sinkObj.writeBatchTimeout;
                                activity.preCopyScript = sinkObj.preCopyScript;
                                activity.maxConcurrentConnections = sinkObj.maxConcurrentConnections;
                                activity.writeBehavior = sinkObj.writeBehavior;
                                activity.sqlWriterUseTableLock = sinkObj.sqlWriterUseTableLock;
                                activity.disableMetricsCollection = sinkObj.disableMetricsCollection;
                                console.log('[Load] Flattened sink fields - writeBatchSize:', activity.writeBatchSize, 'preCopyScript:', activity.preCopyScript);
                            }
                            
                            // Copy other typeProperties
                            activity.enableStaging = tp.enableStaging;
                            activity.stagingSettings = tp.stagingSettings;
                            activity.parallelCopies = tp.parallelCopies;
                            activity.enableSkipIncompatibleRow = tp.enableSkipIncompatibleRow;
                            activity.logSettings = tp.logSettings;
                            activity.dataIntegrationUnits = tp.dataIntegrationUnits;
                            activity.translator = tp.translator;
                        } else {
                            Object.assign(activity, activityData.typeProperties);
                        }
                    }
                    
                    activities.push(activity);
                    activityMap.set(activityData.name, activity);
                });
                
                // Create connections based on dependsOn
                pipelineActivities.forEach((activityData) => {
                    if (activityData.dependsOn && activityData.dependsOn.length > 0) {
                        const toActivity = activityMap.get(activityData.name);
                        if (toActivity) {
                            activityData.dependsOn.forEach(dep => {
                                const fromActivity = activityMap.get(dep.activity);
                                if (fromActivity) {
                                    const condition = dep.dependencyConditions?.[0] || 'Succeeded';
                                    const connection = new Connection(fromActivity, toActivity, condition);
                                    connections.push(connection);
                                }
                            });
                        }
                    }
                });
                
                draw();
                showProperties(null);
                console.log(\`Loaded \${activities.length} activities from pipeline JSON\`);
            } catch (error) {
                console.error('Error loading pipeline:', error);
            }
        }

        // Initial draw
        draw();
    </script>
</body>
</html>`;
	}
}

module.exports = {
	PipelineEditorProvider
};