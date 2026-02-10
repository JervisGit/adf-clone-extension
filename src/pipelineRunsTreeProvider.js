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
        this.dateFilter = '24h'; // Default to last 24 hours. Options: 'all', '24h', '7d', '30d', 'custom'
        this.customStartDate = null;
        this.customEndDate = null;
        
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
            return [new MessageItem('Please login using "az login"', 'not-logged-in')];
        }

        // If no container is selected, prompt to select one
        if (!this.selectedContainer) {
            return [new MessageItem('Click to select a container', 'select-container')];
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
                return [new MessageItem(`Error: ${error.message}`, 'error')];
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
                .filter(run => run !== null)
                .filter(run => this.filterByDateRange(run)); // Apply date filter
            
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
     * Filter pipeline runs by date range
     * All filtering is done in UTC since run.timestamp is in UTC
     */
    filterByDateRange(run) {
        if (this.dateFilter === 'all') {
            return true;
        }

        const runTime = run.timestamp;
        const now = new Date();

        if (this.dateFilter === '24h') {
            const cutoff = new Date(now.getTime() - (24 * 60 * 60 * 1000));
            return runTime >= cutoff;
        } else if (this.dateFilter === '7d') {
            const cutoff = new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000));
            return runTime >= cutoff;
        } else if (this.dateFilter === '30d') {
            const cutoff = new Date(now.getTime() - (30 * 24 * 60 * 60 * 1000));
            return runTime >= cutoff;
        } else if (this.dateFilter === 'custom' && this.customStartDate && this.customEndDate) {
            // customStartDate and customEndDate are already in UTC (parsed from SGT with +08:00)
            return runTime >= this.customStartDate && runTime <= this.customEndDate;
        }

        return true;
    }

    /**
     * Select date filter
     */
    async selectDateFilter() {
        const options = [
            { label: '$(calendar) All Time', description: 'Show all pipeline runs', value: 'all' },
            { label: '$(clock) Last 24 Hours', description: 'Show runs from the last 24 hours', value: '24h' },
            { label: '$(calendar) Last 7 Days', description: 'Show runs from the last 7 days', value: '7d' },
            { label: '$(calendar) Last 30 Days', description: 'Show runs from the last 30 days', value: '30d' },
            { label: '$(calendar) Custom Range', description: 'Select a custom date range', value: 'custom' }
        ];

        const selected = await vscode.window.showQuickPick(options, {
            placeHolder: 'Select date range filter'
        });

        if (!selected) {
            return;
        }

        if (selected.value === 'custom') {
            // Prompt for start date and time (Singapore timezone)
            const startDateTimeStr = await vscode.window.showInputBox({
                prompt: 'Enter start date and time (Singapore timezone, UTC+8)',
                placeHolder: 'YYYY-MM-DD HH:MM (e.g., 2026-02-01 00:00)',
                validateInput: (value) => {
                    if (!/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(value)) {
                        return 'Please enter date and time in YYYY-MM-DD HH:MM format';
                    }
                    return null;
                }
            });

            if (!startDateTimeStr) {
                return;
            }

            // Prompt for end date and time
            const nowSGT = new Date(Date.now() + (8 * 60 * 60 * 1000));
            const defaultEndTime = `${nowSGT.getUTCFullYear()}-${String(nowSGT.getUTCMonth() + 1).padStart(2, '0')}-${String(nowSGT.getUTCDate()).padStart(2, '0')} ${String(nowSGT.getUTCHours()).padStart(2, '0')}:${String(nowSGT.getUTCMinutes()).padStart(2, '0')}`;
            
            const endDateTimeStr = await vscode.window.showInputBox({
                prompt: 'Enter end date and time (Singapore timezone, UTC+8)',
                placeHolder: 'YYYY-MM-DD HH:MM (e.g., 2026-02-10 23:59)',
                value: defaultEndTime,
                validateInput: (value) => {
                    if (!/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(value)) {
                        return 'Please enter date and time in YYYY-MM-DD HH:MM format';
                    }
                    return null;
                }
            });

            if (!endDateTimeStr) {
                return;
            }

            // Parse dates and times in Singapore timezone (convert to UTC for storage)
            // Input is in SGT (UTC+8), so we subtract 8 hours to get UTC
            const startParts = startDateTimeStr.split(' ');
            const startDate = startParts[0];
            const startTime = startParts[1];
            this.customStartDate = new Date(`${startDate}T${startTime}:00+08:00`);
            
            const endParts = endDateTimeStr.split(' ');
            const endDate = endParts[0];
            const endTime = endParts[1];
            this.customEndDate = new Date(`${endDate}T${endTime}:59+08:00`);
        }

        this.dateFilter = selected.value;
        await this.refresh();
        
        const filterLabel = selected.label.replace(/\$\([^)]+\)\s*/g, '');
        vscode.window.showInformationMessage(`Date filter set to: ${filterLabel}`);
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
        
        // Format timestamp in Singapore timezone (UTC+8)
        // run.timestamp is in UTC, so we add 8 hours to display in SGT
        const utcDate = run.timestamp;
        const sgtTime = new Date(utcDate.getTime() + (8 * 60 * 60 * 1000));
        
        // Format manually to avoid timezone confusion
        const year = sgtTime.getUTCFullYear();
        const month = String(sgtTime.getUTCMonth() + 1).padStart(2, '0');
        const day = String(sgtTime.getUTCDate()).padStart(2, '0');
        const hours = String(sgtTime.getUTCHours()).padStart(2, '0');
        const minutes = String(sgtTime.getUTCMinutes()).padStart(2, '0');
        const seconds = String(sgtTime.getUTCSeconds()).padStart(2, '0');
        
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthName = monthNames[sgtTime.getUTCMonth()];
        
        this.description = `${monthName} ${day}, ${year} ${hours}:${minutes}:${seconds}`;
        this.tooltip = `Pipeline: ${run.pipelineName}\nRun ID: ${run.runId}\nTimestamp (SGT): ${year}-${month}-${day} ${hours}:${minutes}:${seconds}\nFolder: ${run.folderName}`;
        
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
