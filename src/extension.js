const vscode = require('vscode');
const { PipelineEditorProvider } = require('./pipelineEditor');

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
}

function deactivate() {}

module.exports = {
	activate,
	deactivate
};
