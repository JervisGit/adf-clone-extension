import * as vscode from 'vscode';
import { PipelineEditorProvider } from './pipelineEditor';

export function activate(context: vscode.ExtensionContext) {
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
		vscode.commands.registerCommand('adf-pipeline-clone.addActivity', (activityType: string) => {
			editorProvider.addActivity(activityType);
		})
	);
}

export function deactivate() {}
