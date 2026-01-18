const vscode = require('vscode');
const { PipelineEditorProvider } = require('./pipelineEditor');
const { ActivitiesTreeDataProvider } = require('./activitiesTreeProvider');

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

	// Register the activities tree view
	const activitiesProvider = new ActivitiesTreeDataProvider(context);
	const treeView = vscode.window.createTreeView('adf-activities', {
		treeDataProvider: activitiesProvider,
		showCollapseAll: true
	});
	context.subscriptions.push(treeView);

	// Open pipeline editor when the view becomes visible for the first time
	let viewOpened = false;
	treeView.onDidChangeVisibility((e) => {
		if (e.visible && !viewOpened) {
			viewOpened = true;
			editorProvider.createOrShow();
		}
	});

	// Register refresh command
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.refreshActivities', () => {
			activitiesProvider.refresh();
		})
	);

	// Register add activity from tree command
	context.subscriptions.push(
		vscode.commands.registerCommand('adf-pipeline-clone.addActivityFromTree', (item) => {
			if (item && item.activityType) {
				editorProvider.addActivity(item.activityType);
			}
		})
	);
}

function deactivate() {}

module.exports = {
	activate,
	deactivate
};

