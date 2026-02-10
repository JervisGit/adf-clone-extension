/**
 * Example usage of ADLS REST Client in VS Code Extension
 * This demonstrates how to integrate with VS Code commands
 */

const vscode = require('vscode');
const { ADLSRestClient } = require('./adlsRestClient');

/**
 * Register ADLS-related commands with VS Code
 */
function registerADLSCommands(context) {
    
    // Command to list pipeline runs from ADLS
    let listPipelineRunsCommand = vscode.commands.registerCommand(
        'adf-pipeline-clone.listPipelineRuns',
        async () => {
            try {
                // Show progress indicator
                await vscode.window.withProgress({
                    location: vscode.ProgressLocation.Notification,
                    title: "Retrieving pipeline runs from ADLS...",
                    cancellable: false
                }, async (progress) => {
                    const config = {
                        storageAccountName: 'testadlsjervis123',
                        containerName: 'confirmed'
                    };
                    
                    const client = new ADLSRestClient(config.storageAccountName);
                    
                    progress.report({ increment: 30, message: "Authenticating..." });
                    
                    // Get pipeline run files
                    const pipelineRuns = await client.getPipelineRunFiles(config.containerName);
                    
                    progress.report({ increment: 70, message: "Processing results..." });
                    
                    // Display results in a quick pick
                    const items = pipelineRuns.map(run => ({
                        label: `$(folder) ${run.folder}`,
                        description: run.path,
                        detail: `Activities: ${Array.isArray(run.activities) ? run.activities.length : 'Unknown'}`,
                        data: run
                    }));
                    
                    if (items.length === 0) {
                        vscode.window.showInformationMessage('No pipeline runs found');
                        return;
                    }
                    
                    // Display summary
                    vscode.window.showInformationMessage(
                        `Found ${pipelineRuns.length} pipeline run(s) with ${pipelineRuns.reduce((sum, r) => sum + (Array.isArray(r.activities) ? r.activities.length : 0), 0)} total activities`
                    );
                    
                    // Let user select a pipeline run
                    const selected = await vscode.window.showQuickPick(items, {
                        placeHolder: 'Select a pipeline run to view details',
                        matchOnDescription: true,
                        matchOnDetail: true
                    });
                    
                    if (selected) {
                        // Show the content in a new document
                        const doc = await vscode.workspace.openTextDocument({
                            content: JSON.stringify(selected.data.content, null, 2),
                            language: 'json'
                        });
                        await vscode.window.showTextDocument(doc);
                    }
                });
                
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to retrieve pipeline runs: ${error.message}`);
                console.error(error);
            }
        }
    );
    
    // Command to read a specific file from ADLS
    let readFileCommand = vscode.commands.registerCommand(
        'adf-pipeline-clone.readADLSFile',
        async () => {
            try {
                // Prompt for file path
                const filePath = await vscode.window.showInputBox({
                    prompt: 'Enter the file path (relative to container)',
                    placeHolder: 'e.g., pipeline-runs/folder-name/activity_runs.json',
                    value: 'pipeline-runs/'
                });
                
                if (!filePath) {
                    return;
                }
                
                await vscode.window.withProgress({
                    location: vscode.ProgressLocation.Notification,
                    title: `Reading ${filePath}...`,
                    cancellable: false
                }, async (progress) => {
                    const config = {
                        storageAccountName: 'testadlsjervis123',
                        containerName: 'confirmed'
                    };
                    
                    const client = new ADLSRestClient(config.storageAccountName);
                    
                    progress.report({ increment: 50 });
                    
                    const content = await client.readFile(config.containerName, filePath);
                    
                    // Try to parse as JSON for better formatting
                    let displayContent = content;
                    let language = 'text';
                    try {
                        const parsed = JSON.parse(content);
                        displayContent = JSON.stringify(parsed, null, 2);
                        language = 'json';
                    } catch (e) {
                        // Not JSON, display as text
                    }
                    
                    const doc = await vscode.workspace.openTextDocument({
                        content: displayContent,
                        language: language
                    });
                    await vscode.window.showTextDocument(doc);
                });
                
                vscode.window.showInformationMessage(`Successfully read file: ${filePath}`);
                
            } catch (error) {
                vscode.window.showErrorMessage(`Failed to read file: ${error.message}`);
                console.error(error);
            }
        }
    );
    
    context.subscriptions.push(listPipelineRunsCommand);
    context.subscriptions.push(readFileCommand);
}

/**
 * Simple function to get pipeline runs (can be called from anywhere in extension)
 */
async function getPipelineRunsFromADLS(storageAccountName = 'testadlsjervis123', containerName = 'confirmed') {
    const client = new ADLSRestClient(storageAccountName);
    return await client.getPipelineRunFiles(containerName);
}

module.exports = {
    registerADLSCommands,
    getPipelineRunsFromADLS
};
