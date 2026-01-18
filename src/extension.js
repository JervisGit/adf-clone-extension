const vscode = require('vscode');
const { PipelineEditorProvider } = require('./pipelineEditor');
const { PipelineTreeDataProvider } = require('./pipelineTreeProvider');

function activate(context) {
	console.log('ADF Pipeline Clone extension is now active!');

	// Register the pipeline editor provider
	const editorProvider = new PipelineEditorProvider(context);
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.openPipeline', () => {
			editorProvider.createOrShow();
		})
	);

	// Register command to add activities
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.addActivity', (activityType) => {
			editorProvider.addActivity(activityType);
		})
	);

	// Register the pipeline files tree view
	const pipelineTreeProvider = new PipelineTreeDataProvider(context);
	const pipelineTreeView = vscode.window.createTreeView('adf-pipelines', {
		treeDataProvider: pipelineTreeProvider,
		showCollapseAll: true
	});
	context.subscriptions.push(pipelineTreeView);

	// Register command to open pipeline file
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.openPipelineFile', (item) => {
			if (item && item.filePath) {
				editorProvider.loadPipelineFile(item.filePath);
			}
		})
	);

	// Register refresh command for pipeline files
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.refreshPipelines', () => {
			pipelineTreeProvider.refresh();
		})
	);

	// Register create commands
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.createPipeline', async (folderItem) => {
			const name = await vscode.window.showInputBox({
				prompt: 'Enter pipeline name',
				placeHolder: 'MyPipeline'
			});
			
			if (name) {
				const fs = require('fs');
				const path = require('path');
				const filePath = path.join(folderItem.folderPath, `${name}.json`);
				
				const pipelineTemplate = {
					name: name,
					properties: {
						activities: [],
						annotations: []
					}
				};
				
				fs.writeFileSync(filePath, JSON.stringify(pipelineTemplate, null, 2));
				pipelineTreeProvider.refresh();
				editorProvider.loadPipelineFile(filePath);
			}
		})
	);

	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.createDataset', async (folderItem) => {
			const name = await vscode.window.showInputBox({
				prompt: 'Enter dataset name',
				placeHolder: 'MyDataset'
			});
			
			if (name) {
				const fs = require('fs');
				const path = require('path');
				const filePath = path.join(folderItem.folderPath, `${name}.json`);
				
				const datasetTemplate = {
					name: name,
					properties: {
						linkedServiceName: {
							referenceName: "YourLinkedService",
							type: "LinkedServiceReference"
						},
						annotations: [],
						type: "DelimitedText",
						typeProperties: {
							location: {
								type: "AzureBlobFSLocation"
							},
							columnDelimiter: ",",
							escapeChar: "\\\\",
							firstRowAsHeader: true,
							quoteChar: "\""
						},
						schema: []
					}
				};
				
				fs.writeFileSync(filePath, JSON.stringify(datasetTemplate, null, 2));
				pipelineTreeProvider.refresh();
				vscode.window.showInformationMessage(`Dataset ${name} created`);
			}
		})
	);

	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.createTrigger', async (folderItem) => {
			const name = await vscode.window.showInputBox({
				prompt: 'Enter trigger name',
				placeHolder: 'MyTrigger'
			});
			
			if (name) {
				const fs = require('fs');
				const path = require('path');
				const filePath = path.join(folderItem.folderPath, `${name}.json`);
				
				const triggerTemplate = {
					name: name,
					properties: {
						annotations: [],
						runtimeState: "Stopped",
						pipelines: [],
						type: "ScheduleTrigger",
						typeProperties: {
							recurrence: {
								frequency: "Day",
								interval: 1,
								startTime: new Date().toISOString(),
								timeZone: "UTC"
							}
						}
					}
				};
				
				fs.writeFileSync(filePath, JSON.stringify(triggerTemplate, null, 2));
				pipelineTreeProvider.refresh();
				vscode.window.showInformationMessage(`Trigger ${name} created`);
			}
		})
	);

	// Open pipeline editor when the view becomes visible for the first time
	let viewOpened = false;
	pipelineTreeView.onDidChangeVisibility((e) => {
		if (e.visible && !viewOpened) {
			viewOpened = true;
			editorProvider.createOrShow();
		}
	});
}

function deactivate() {}

module.exports = {
	activate,
	deactivate
};

