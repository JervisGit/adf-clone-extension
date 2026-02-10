const vscode = require('vscode');
const { ADLSRestClient } = require('./adlsRestClient');
const { exec } = require('child_process');
const util = require('util');

const execPromise = util.promisify(exec);

/**
 * Tree data provider for displaying pipeline runs from ADLS
 */
class PipelineRunsTreeDataProvider {
    constructor(context) {
        this.context = context;
        this._onDidChangeTreeData = new vscode.EventEmitter();
        this.onDidChangeTreeData = this._onDidChangeTreeData.event;
        
        // Configuration
        this.storageAccountName = 'testadlsjervis123';
        this.selectedContainer = null;
        this.pipelineRunsFolder = 'pipeline-runs';
        
        // Cache
        this.pipelineRuns = [];
        this.isAzLoggedIn = false;
    }

    /**
     * Check if user is logged in via Azure CLI
     */
    async checkAzLogin() {
        try {
            await execPromise('az account show');
            this.isAzLoggedIn = true;
            return true;
        } catch {
            this.isAzLoggedIn = false;
            return false;
        }
    }

    /**
     * Refresh the tree view
     */
    async refresh() {
        this._onDidChangeTreeData.fire();
    }

    /**
     * Get tree item for display
     */
    getTreeItem(element) {
        return element;
    }

    /**
     * Get children for tree hierarchy
     */
    async getChildren(element) {
        // Check if user is logged in
        const isLoggedIn = await this.checkAzLogin();
        
        if (!isLoggedIn) {
            return [new MessageItem('⚠️ Please login using "az login"', 'not-logged-in')];
        }

        // If no container is selected, prompt to select one
        if (!this.selectedContainer) {
            return [new MessageItem('📦 Click to select a container', 'select-container')];
        }

        // Root level - show pipeline runs
        if (!element) {
            try {
                await this.loadPipelineRuns();
                
                if (this.pipelineRuns.length === 0) {
                    return [new MessageItem('No pipeline runs found', 'no-runs')];
                }
                
                return this.pipelineRuns.map(run => new PipelineRunItem(run));
            } catch (error) {
                console.error('Error loading pipeline runs:', error);
                return [new MessageItem(`❌ Error: ${error.message}`, 'error')];
            }
        }
        
        // If element is a pipeline run, show its activities (future implementation)
        if (element instanceof PipelineRunItem) {
            // For now, return empty - will implement activity display later
            return [];
        }

        return [];
    }

    /**
     * Load pipeline runs from ADLS
     */
    async loadPipelineRuns() {
        try {
            const client = new ADLSRestClient(this.storageAccountName);
            
            // List all directories in pipeline-runs folder
            const paths = await client.listPaths(
                this.selectedContainer, 
                this.pipelineRunsFolder, 
                false
            );
            
            // Filter only directories and parse their information
            const runFolders = paths
                .filter(path => path.isDirectory)
                .map(path => {
                    const folderName = path.name.split('/').pop();
                    return this.parsePipelineRunFolder(folderName, path);
                })
                .filter(run => run !== null);
            
            // Sort by timestamp (latest first)
            runFolders.sort((a, b) => b.timestamp - a.timestamp);
            
            this.pipelineRuns = runFolders;
        } catch (error) {
            console.error('Error loading pipeline runs:', error);
            throw error;
        }
    }

    /**
     * Parse pipeline run folder name
     * Expected format: 20260209-132140_PipelineWaitTest_6d87c205
     */
    parsePipelineRunFolder(folderName, pathInfo) {
        try {
            // Split by underscore
            const parts = folderName.split('_');
            if (parts.length < 2) {
                return null;
            }
            
            const timestampPart = parts[0];
            const pipelineName = parts.slice(1, -1).join('_');
            const runId = parts[parts.length - 1];
            
            // Parse timestamp: 20260209-132140 -> 2026-02-09 13:21:40
            const dateStr = timestampPart.substring(0, 8);
            const timeStr = timestampPart.substring(9);
            
            const year = dateStr.substring(0, 4);
            const month = dateStr.substring(4, 6);
            const day = dateStr.substring(6, 8);
            const hour = timeStr.substring(0, 2);
            const minute = timeStr.substring(2, 4);
            const second = timeStr.substring(4, 6);
            
            const timestamp = new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`);
            
            return {
                folderName,
                pipelineName,
                runId,
                timestamp,
                timestampStr: timestampPart,
                path: pathInfo.name
            };
        } catch (error) {
            console.error(`Error parsing folder name ${folderName}:`, error);
            return null;
        }
    }

    /**
     * Select a container
     */
    async selectContainer() {
        try {
            // For now, we'll provide a simple input box
            // In the future, we could list containers from the storage account
            const container = await vscode.window.showInputBox({
                prompt: 'Enter the container name',
                placeHolder: 'e.g., confirmed',
                value: this.selectedContainer || 'confirmed'
            });
            
            if (container) {
                this.selectedContainer = container;
                await this.refresh();
                vscode.window.showInformationMessage(`Container set to: ${container}`);
            }
        } catch (error) {
            vscode.window.showErrorMessage(`Failed to select container: ${error.message}`);
        }
    }

    /**
     * View pipeline run details
     */
    async viewPipelineRun(run) {
        try {
            await vscode.window.withProgress({
                location: vscode.ProgressLocation.Notification,
                title: `Loading pipeline run: ${run.pipelineName}`,
                cancellable: false
            }, async (progress) => {
                const client = new ADLSRestClient(this.storageAccountName);
                
                progress.report({ increment: 30, message: "Reading activity runs..." });
                
                // Read activity_runs.json
                const activityRunsPath = `${this.pipelineRunsFolder}/${run.folderName}/activity_runs.json`;
                const content = await client.readFile(this.selectedContainer, activityRunsPath);
                const activityRuns = JSON.parse(content);
                
                progress.report({ increment: 70, message: "Formatting output..." });
                
                // Show the content in a new document
                const doc = await vscode.workspace.openTextDocument({
                    content: JSON.stringify(activityRuns, null, 2),
                    language: 'json'
                });
                await vscode.window.showTextDocument(doc);
            });
        } catch (error) {
            vscode.window.showErrorMessage(`Failed to load pipeline run: ${error.message}`);
        }
    }
}

/**
 * Tree item representing a pipeline run
 */
class PipelineRunItem extends vscode.TreeItem {
    constructor(run) {
        const label = `${run.pipelineName}`;
        super(label, vscode.TreeItemCollapsibleState.None);
        
        this.run = run;
        this.contextValue = 'pipeline-run';
        this.iconPath = new vscode.ThemeIcon('play-circle');
        
        // Format timestamp for description
        const date = run.timestamp;
        const dateStr = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        const timeStr = date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        
        this.description = `${dateStr} ${timeStr}`;
        this.tooltip = `Pipeline: ${run.pipelineName}\nRun ID: ${run.runId}\nTimestamp: ${date.toISOString()}\nFolder: ${run.folderName}`;
        
        // Command to view details when clicked
        this.command = {
            command: 'adf-pipeline-clone.viewPipelineRun',
            title: 'View Pipeline Run',
            arguments: [run]
        };
    }
}

/**
 * Tree item for displaying messages
 */
class MessageItem extends vscode.TreeItem {
    constructor(message, contextValue) {
        super(message, vscode.TreeItemCollapsibleState.None);
        this.contextValue = contextValue;
        
        if (contextValue === 'select-container') {
            this.iconPath = new vscode.ThemeIcon('package');
            this.command = {
                command: 'adf-pipeline-clone.selectContainer',
                title: 'Select Container'
            };
        } else if (contextValue === 'not-logged-in') {
            this.iconPath = new vscode.ThemeIcon('warning');
        } else if (contextValue === 'error') {
            this.iconPath = new vscode.ThemeIcon('error');
        } else {
            this.iconPath = new vscode.ThemeIcon('info');
        }
    }
}

module.exports = { PipelineRunsTreeDataProvider };
