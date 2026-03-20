const vscode = require('vscode');
const fs = require('fs');
const path = require('path');
const activitiesConfig = require('./activities-config-verified.json');
const activitySchemas = require('./activity-schemas.json');
const copyActivityConfig = require('./copy-activity-config.json');

class PipelineEditorV2Provider {
	static panels = new Map();      // Map<filePath, panel>
	static dirtyStates = new Map(); // Map<filePath, isDirty>
	static stateCache = new Map();  // Map<filePath, pipelineData>

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

		// Post schemas once the panel has loaded
		setImmediate(() => this._postInitSchemas(panel));

		panel.webview.onDidReceiveMessage(
			async message => {
				switch (message.type) {
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
					case 'saveNotImplemented':
						vscode.window.showInformationMessage(
							'Save is not yet available in the V2 editor. Use the original editor (right-click pipeline → Open Pipeline File) to save changes.'
						);
						break;
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
				}
			},
			null,
			this.context.subscriptions
		);

		return panel;
	}

	loadPipelineFile(filePath) {
		const panel = this.createOrShow(filePath);
		try {
			const content = fs.readFileSync(filePath, 'utf8');
			const pipelineJson = JSON.parse(content);
			// Small delay to let the webview JS finish initialising
			setTimeout(() => {
				panel.webview.postMessage({
					type: 'loadPipeline',
					data: pipelineJson,
					filePath
				});
			}, 300);
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
