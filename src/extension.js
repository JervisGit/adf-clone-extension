const vscode = require('vscode');
const { PipelineEditorProvider } = require('./pipelineEditor');
const { TriggerEditorProvider } = require('./triggerEditor');
const { DatasetEditorProvider } = require('./datasetEditor');
const { PipelineTreeDataProvider } = require('./pipelineTreeProvider');
const { DatasetTreeDataProvider } = require('./datasetTreeProvider');
const { PipelineRunsTreeDataProvider } = require('./pipelineRunsTreeProvider');
const { PipelineRunViewerProvider } = require('./pipelineRunViewer');
const { buildDatasetJson } = require('./datasetUtils');
const datasetConfig = require('./dataset-config.json');

function activate(context) {
	console.log('ADF Pipeline Clone extension is now active!');

	// Register the pipeline editor provider
	const editorProvider = new PipelineEditorProvider(context);
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.openPipeline', () => {
			editorProvider.createOrShow();
		})
	);

	// Register the trigger editor provider
	const triggerEditorProvider = new TriggerEditorProvider(context);
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.openTrigger', () => {
			triggerEditorProvider.createOrShow();
		})
	);

	// Register the dataset editor provider
	const datasetEditorProvider = new DatasetEditorProvider(context);
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.openDataset', () => {
			datasetEditorProvider.createOrShow();
		})
	);

	// Register command to open trigger file
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.openTriggerFile', (item) => {
			if (item && item.filePath) {
				triggerEditorProvider.loadTriggerFile(item.filePath);
			}
		})
	);

	// Register command to open dataset file
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.openDatasetFile', (item) => {
			if (item && item.filePath) {
				datasetEditorProvider.loadDatasetFile(item.filePath);
			}
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

	// Dataset tree provider no longer needed as a separate view
	// Datasets are now shown in the main pipelines view

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

	// Refresh datasets through main pipeline tree provider
	// No separate refresh command needed

	// Register the pipeline runs tree view
	const pipelineRunsTreeProvider = new PipelineRunsTreeDataProvider(context);
	const pipelineRunsTreeView = vscode.window.createTreeView('adf-pipeline-runs', {
		treeDataProvider: pipelineRunsTreeProvider,
		showCollapseAll: true
	});
	context.subscriptions.push(pipelineRunsTreeView);

	// Register the pipeline run viewer provider
	const pipelineRunViewerProvider = new PipelineRunViewerProvider(context);
	pipelineRunsTreeProvider.setViewerProvider(pipelineRunViewerProvider);

	// Register command to refresh pipeline runs
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.refreshPipelineRuns', () => {
			pipelineRunsTreeProvider.refresh();
		})
	);

	// Register command to select container
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.selectContainer', () => {
			pipelineRunsTreeProvider.selectContainer();
		})
	);

	// Register command to view pipeline run details
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.viewPipelineRun', (run) => {
			pipelineRunsTreeProvider.viewPipelineRun(run);
		})
	);

	// Register command to select date filter
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.selectDateFilter', () => {
			pipelineRunsTreeProvider.selectDateFilter();
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
			// Step 1: Ask for dataset name
			const name = await vscode.window.showInputBox({
				prompt: 'Enter dataset name',
				placeHolder: 'MyDataset'
			});
			
			if (!name) return;
			
			// Step 2: Ask for dataset source type
			const datasetTypes = [
				{ label: 'Azure SQL Database', value: 'AzureSqlTable', requiresFileType: false },
				{ label: 'Azure Blob Storage', value: 'AzureBlobStorage', requiresFileType: true },
				{ label: 'Azure Data Lake Storage Gen2', value: 'AzureDataLakeStorageGen2', requiresFileType: true }
			];
			
			const selectedType = await vscode.window.showQuickPick(
				datasetTypes.map(t => ({ label: t.label, description: t.value, type: t })),
				{ placeHolder: 'Select dataset source type' }
			);
			
			if (!selectedType) return;
			
			let fileType = null;
			
			// Step 3: If requires file type, ask for it
			if (selectedType.type.requiresFileType) {
				const fileTypes = [
					{ label: 'Parquet', value: 'Parquet' },
					{ label: 'Delimited Text (CSV/TSV)', value: 'DelimitedText' },
					{ label: 'JSON', value: 'Json' },
					{ label: 'Avro', value: 'Avro' },
					{ label: 'ORC', value: 'Orc' },
					{ label: 'XML', value: 'Xml' },
					{ label: 'Binary', value: 'Binary' },
					{ label: 'Excel', value: 'Excel' },
					{ label: 'Iceberg', value: 'Iceberg' }
				];
				
				const selectedFileType = await vscode.window.showQuickPick(
					fileTypes.map(t => ({ label: t.label, description: t.value, fileType: t })),
					{ placeHolder: 'Select file type' }
				);
				
				if (!selectedFileType) return;
				fileType = selectedFileType.fileType.value;
			}
			
			// Step 4: Create the dataset file using config-driven approach
			const fs = require('fs');
			const path = require('path');
			const filePath = path.join(folderItem.folderPath, `${name}.json`);
			
			// Build minimal form data - config will handle defaults
			const formData = {
				name: name,
				datasetType: selectedType.type.value,
				fileType: fileType,
				linkedService: 'YourLinkedService'
			};
			
			// Use config-driven JSON builder
			const datasetTemplate = buildDatasetJson(formData, datasetConfig, selectedType.type.value, fileType);
			
			fs.writeFileSync(filePath, JSON.stringify(datasetTemplate, null, 2));
			pipelineTreeProvider.refresh();
			// Open the newly created dataset in the editor
			datasetEditorProvider.loadDatasetFile(filePath);
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
								frequency: "Minute",
								interval: 15,
								startTime: new Date().toISOString().slice(0, 19),
								timeZone: "Singapore Standard Time"
							}
						}
					}
				};
				
				fs.writeFileSync(filePath, JSON.stringify(triggerTemplate, null, 2));
				pipelineTreeProvider.refresh();
				// Open the newly created trigger in the editor
				triggerEditorProvider.loadTriggerFile(filePath);
			}
		})
	);
}

function deactivate() {}

module.exports = {
	activate,
	deactivate
};

