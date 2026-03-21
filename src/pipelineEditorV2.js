const vscode = require('vscode');
const fs = require('fs');
const path = require('path');
const activitiesConfig = require('./activities-config-verified.json');
const activitySchemas = require('./activity-schemas-v2.json');
const copyActivityConfig = require('./copy-activity-config.json');
const engine = require('./activityEngine/engine');

class PipelineEditorV2Provider {
	static panels = new Map();           // Map<filePath, panel>
	static dirtyStates = new Map();      // Map<filePath, isDirty>
	static stateCache = new Map();       // Map<filePath, pipelineData>
	static initializedPanels = new Set(); // Set<filePath> — panels that sent 'ready'
	static pendingLoads = new Map();     // Map<filePath, postMessage payload>

	constructor(context) {
		this.context = context;
	}

	markPanelAsDirty(filePath, isDirty) {
		const panel = PipelineEditorV2Provider.panels.get(filePath);
		if (panel) {
			const basename = path.basename(filePath, '.json');
			panel.title = isDirty ? `● ${basename} [V2]` : `${basename} [V2]`;
			PipelineEditorV2Provider.dirtyStates.set(filePath, isDirty);
		}
	}

	createOrShow(filePath = null) {
		const column = vscode.window.activeTextEditor
			? vscode.window.activeTextEditor.viewColumn
			: undefined;

		if (filePath && PipelineEditorV2Provider.panels.has(filePath)) {
			PipelineEditorV2Provider.panels.get(filePath).reveal(column);
			return PipelineEditorV2Provider.panels.get(filePath);
		}

		const title = filePath
			? `${path.basename(filePath, '.json')} [V2]`
			: 'Pipeline Editor V2';

		const panel = vscode.window.createWebviewPanel(
			'adfPipelineEditorV2',
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

		if (filePath) {
			PipelineEditorV2Provider.panels.set(filePath, panel);
		}

		panel.webview.html = this._getHtmlContent(panel.webview);

		panel.webview.onDidReceiveMessage(
			async message => {
				switch (message.type) {
					case 'ready':
						// Webview is initialised — send schemas, then any queued pipeline load
						if (!PipelineEditorV2Provider.initializedPanels.has(filePath)) {
							if (filePath) PipelineEditorV2Provider.initializedPanels.add(filePath);
							this._postInitSchemas(panel);
							const pending = filePath && PipelineEditorV2Provider.pendingLoads.get(filePath);
							if (pending) {
								PipelineEditorV2Provider.pendingLoads.delete(filePath);
								panel.webview.postMessage(pending);
							}
						}
						break;
					case 'alert':
						vscode.window.showInformationMessage(message.text);
						break;
					case 'error':
						vscode.window.showErrorMessage(message.text);
						break;
					case 'contentChanged':
						if (filePath) this.markPanelAsDirty(filePath, message.isDirty);
						break;
					case 'cacheState':
						if (filePath && message.data) {
							PipelineEditorV2Provider.stateCache.set(filePath, message.data);
						}
						break;
					case 'savePipeline': {
						try {
							// Check all activity types are supported before attempting serialization
							const unsupported = [...new Set(
								(message.activities || []).map(a => a.type).filter(t => !engine.isActivityTypeSupported(t))
							)];
							if (unsupported.length) {
								vscode.window.showErrorMessage(
									`Cannot save: activity type(s) not yet supported in V2 editor: ${unsupported.join(', ')}. ` +
									'Use the original editor for pipelines with these activities.'
								);
								panel.webview.postMessage({ type: 'saveResult', success: false });
								break;
							}

							// Validate — warn but let the user choose to save anyway
							const errors = engine.validateActivityList(message.activities || []);
							if (Object.keys(errors).length > 0) {
								const summary = Object.entries(errors)
									.map(([name, errs]) => `${name}: ${errs.join(', ')}`)
									.join('; ');
								const choice = await vscode.window.showWarningMessage(
									`Validation warnings — ${summary}. Save anyway?`,
									'Save', 'Cancel'
								);
								if (choice !== 'Save') {
									panel.webview.postMessage({ type: 'saveResult', success: false });
									break;
								}
							}

							// Serialize and write
							const pipelineJson = engine.serializePipeline(
								message.pipelineData,
								message.activities,
								message.connections
							);
							if (!filePath) throw new Error('No file path associated with this panel');
							fs.writeFileSync(filePath, JSON.stringify(pipelineJson, null, 4), 'utf8');
							panel.webview.postMessage({ type: 'saveResult', success: true });
							vscode.window.showInformationMessage(`Saved: ${path.basename(filePath)}`);
						} catch (err) {
							console.error('[V2 Save]', err);
							panel.webview.postMessage({ type: 'saveResult', success: false, error: err.message });
							vscode.window.showErrorMessage(`V2 save failed: ${err.message}`);
						}
						break;
					}
					case 'log':
						console.log('[V2 Webview]', message.text);
						break;
				}
			},
			undefined,
			this.context.subscriptions
		);

		panel.onDidDispose(
			() => {
				if (filePath) {
					PipelineEditorV2Provider.panels.delete(filePath);
					PipelineEditorV2Provider.dirtyStates.delete(filePath);
					PipelineEditorV2Provider.stateCache.delete(filePath);
					PipelineEditorV2Provider.initializedPanels.delete(filePath);
					PipelineEditorV2Provider.pendingLoads.delete(filePath);
				}
			},
			null,
			this.context.subscriptions
		);

		return panel;
	}

	loadPipelineFile(filePath) {
		try {
			const content = fs.readFileSync(filePath, 'utf8');
			const pipelineJson = JSON.parse(content);

			// Deserialize activities to flat canvas objects before sending to webview.
			// Supported types go through engine.deserializeActivity(); unsupported types
			// are passed through as raw ADF JSON so they can still be placed on the canvas.
			const rawActivities = pipelineJson.properties?.activities || pipelineJson.activities || [];
			const flatActivities = rawActivities.map(raw =>
				engine.isActivityTypeSupported(raw.type)
					? engine.deserializeActivity(raw)
					: raw
			);

			const msg = { type: 'loadPipeline', data: pipelineJson, flatActivities, filePath };

			const panel = this.createOrShow(filePath);

			if (PipelineEditorV2Provider.initializedPanels.has(filePath)) {
				// Webview already ready — post directly
				panel.webview.postMessage(msg);
			} else {
				// Webview not yet ready — queue; will be sent when 'ready' is received
				PipelineEditorV2Provider.pendingLoads.set(filePath, msg);
			}
		} catch (error) {
			vscode.window.showErrorMessage(`Failed to load pipeline: ${error.message}`);
		}
	}

	_postInitSchemas(panel) {
		let datasetList = [];
		let datasetContents = {};
		let pipelineList = [];
		let linkedServicesList = [];

		if (vscode.workspace.workspaceFolders?.length > 0) {
			const workspaceRoot = vscode.workspace.workspaceFolders[0].uri.fsPath;
			const pathsToCheck = [
				workspaceRoot,
				path.join(workspaceRoot, 'adf-clone-extension')
			];

			for (const basePath of pathsToCheck) {
				if (!fs.existsSync(basePath)) continue;

				const datasetDir = path.join(basePath, 'dataset');
				if (fs.existsSync(datasetDir)) {
					for (const file of fs.readdirSync(datasetDir).filter(f => f.endsWith('.json'))) {
						const name = file.replace('.json', '');
						if (!datasetList.includes(name)) {
							datasetList.push(name);
							try {
								datasetContents[name] = JSON.parse(
									fs.readFileSync(path.join(datasetDir, file), 'utf8')
								);
							} catch { /* skip unreadable */ }
						}
					}
				}

				const pipelineDir = path.join(basePath, 'pipeline');
				if (fs.existsSync(pipelineDir)) {
					for (const file of fs.readdirSync(pipelineDir).filter(f => f.endsWith('.json'))) {
						const name = file.replace('.json', '');
						if (!pipelineList.includes(name)) pipelineList.push(name);
					}
				}

				const linkedServiceDir = path.join(basePath, 'linkedService');
				if (fs.existsSync(linkedServiceDir)) {
					for (const file of fs.readdirSync(linkedServiceDir).filter(f => f.endsWith('.json'))) {
						try {
							const ls = JSON.parse(fs.readFileSync(path.join(linkedServiceDir, file), 'utf8'));
							const lsType = ls.properties?.type;
							if ((lsType === 'AzureSqlDatabase' || lsType === 'AzureSqlDW') &&
								!linkedServicesList.find(l => l.name === ls.name)) {
								linkedServicesList.push({ name: ls.name, type: lsType });
							}
						} catch { /* skip */ }
					}
				}
			}
		}

		panel.webview.postMessage({
			type: 'initSchemas',
			activitiesConfig,
			activitySchemas,
			copyActivityConfig,
			datasetList,
			datasetContents,
			pipelineList,
			linkedServicesList
		});
	}

	_getHtmlContent(webview) {
		const cssUri = webview.asWebviewUri(
			vscode.Uri.joinPath(this.context.extensionUri, 'media', 'pipelineEditorV2.css')
		);
		const scriptUri = webview.asWebviewUri(
			vscode.Uri.joinPath(this.context.extensionUri, 'media', 'pipelineEditorV2.js')
		);
		const htmlPath = vscode.Uri.joinPath(
			this.context.extensionUri, 'media', 'pipelineEditorV2.html'
		).fsPath;
		let html = fs.readFileSync(htmlPath, 'utf8');
		return html
			.replace(/\{\{CSS_URI\}\}/g, cssUri.toString())
			.replace(/\{\{SCRIPT_URI\}\}/g, scriptUri.toString())
			.replace(/\{\{CSP_SOURCE\}\}/g, webview.cspSource);
	}
}

module.exports = { PipelineEditorV2Provider };
