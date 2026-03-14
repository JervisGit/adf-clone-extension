const vscode = require('vscode');
const path = require('path');
const fs = require('fs');
const { ADLSRestClient } = require('./adlsRestClient');

const REQUEST_FOLDER = 'pipeline-run-requests';
const PENDING_FOLDER = `${REQUEST_FOLDER}/pending`;
const PROCESSED_FOLDER = `${REQUEST_FOLDER}/processed`;
const STATUS_CACHE_KEY = 'pipelineRequestStatusCache';

/**
 * Generates an 8-character hex ID
 */
function generateRequestId() {
    const chars = '0123456789abcdef';
    let id = '';
    for (let i = 0; i < 8; i++) {
        id += chars[Math.floor(Math.random() * chars.length)];
    }
    return id;
}

/**
 * Returns current UTC time formatted as YYYYMMDD-HHmmss
 */
function formatTimestampForFilename(date) {
    const y = date.getUTCFullYear();
    const mo = String(date.getUTCMonth() + 1).padStart(2, '0');
    const d = String(date.getUTCDate()).padStart(2, '0');
    const h = String(date.getUTCHours()).padStart(2, '0');
    const mi = String(date.getUTCMinutes()).padStart(2, '0');
    const s = String(date.getUTCSeconds()).padStart(2, '0');
    return `${y}${mo}${d}-${h}${mi}${s}`;
}

class PipelineRequestTreeDataProvider {
    constructor(context) {
        this.context = context;
        this._onDidChangeTreeData = new vscode.EventEmitter();
        this.onDidChangeTreeData = this._onDidChangeTreeData.event;

        this.storageAccountName = 'testadlsjervis123';
        this.selectedContainer = null;
        this.requests = [];
        this.statusFilter = 'all'; // 'all' | 'pending' | 'succeeded' | 'failed' | 'denied' | 'cancelled'

        this.viewerProvider = null;

        // On activation: load cached state for change detection then refresh
        this._loadAndRefresh();
    }

    async _loadAndRefresh() {
        if (!this.selectedContainer) {
            // Try to read last-used container from workspace state
            const saved = this.context.workspaceState.get('pipelineRequestContainer');
            if (saved) {
                this.selectedContainer = saved;
            }
        }
        await this.refresh();
    }

    setViewerProvider(provider) {
        this.viewerProvider = provider;
    }

    refresh() {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element) {
        return element;
    }

    async getChildren(element) {
        if (element) return [];

        if (!this.selectedContainer) {
            return [new MessageItem('Click to select a container', 'select-container')];
        }

        try {
            await this._loadRequests();

            if (this.requests.length === 0) {
                return [new MessageItem('No pipeline run requests found', 'no-requests')];
            }

            return this.requests.map(req => new PipelineRequestItem(req));
        } catch (error) {
            console.error('Error loading pipeline requests:', error);
            return [new MessageItem(`Error: ${error.message}`, 'error')];
        }
    }

    async _loadRequests() {
        const client = new ADLSRestClient(this.storageAccountName);

        const loaded = [];
        for (const folder of [PENDING_FOLDER, PROCESSED_FOLDER]) {
            let paths = [];
            try {
                paths = await client.listPaths(this.selectedContainer, folder, false);
            } catch (error) {
                // Folder doesn't exist yet — that's fine
                if (error.message.includes('404') || error.message.includes('PathNotFound') || error.message.includes('FilesystemNotFound')) {
                    continue;
                }
                throw error;
            }

            const jsonFiles = paths.filter(p => !p.isDirectory && p.name.endsWith('.json'));
            for (const file of jsonFiles) {
                try {
                    const content = await client.readFile(this.selectedContainer, file.name);
                    const data = JSON.parse(content);
                    data._blobPath = file.name;
                    data._blobFolder = folder;
                    loaded.push(data);
                } catch (err) {
                    console.error(`Failed to read request file ${file.name}:`, err.message);
                }
            }
        }

        // Apply status filter
        const filtered = this.statusFilter === 'all'
            ? loaded
            : loaded.filter(r => r.status === this.statusFilter);

        // Sort newest first
        filtered.sort((a, b) => new Date(b.requestedAt) - new Date(a.requestedAt));

        // Detect status changes vs previously cached values and notify user
        this._detectStatusChanges(filtered);

        this.requests = filtered;
    }

    _detectStatusChanges(freshRequests) {
        const cache = this.context.workspaceState.get(STATUS_CACHE_KEY, {});
        const newCache = {};

        for (const req of freshRequests) {
            newCache[req.requestId] = req.status;
            const prev = cache[req.requestId];

            if (prev && prev !== req.status) {
                if (req.status === 'succeeded') {
                    vscode.window.showInformationMessage(
                        `✅ Pipeline "${req.pipelineName}" run succeeded${req.runId ? ` — Run ID: ${req.runId}` : ''}`
                    );
                } else if (req.status === 'failed') {
                    const errSuffix = req.errorMessage ? ` — ${req.errorMessage}` : '';
                    vscode.window.showErrorMessage(
                        `❌ Pipeline "${req.pipelineName}" run failed${errSuffix}`
                    );
                } else if (req.status === 'denied') {
                    const reasonSuffix = req.errorMessage ? ` — ${req.errorMessage}` : '';
                    vscode.window.showWarningMessage(
                        `🚫 Pipeline "${req.pipelineName}" run request was denied${reasonSuffix}`
                    );
                }
            }
        }

        this.context.workspaceState.update(STATUS_CACHE_KEY, newCache);
    }

    async selectContainer() {
        const container = await vscode.window.showInputBox({
            prompt: 'Enter the blob container name for pipeline run requests',
            placeHolder: 'e.g., my-container',
            value: this.selectedContainer || ''
        });

        if (container) {
            this.selectedContainer = container;
            await this.context.workspaceState.update('pipelineRequestContainer', container);
            await this.refresh();
            vscode.window.showInformationMessage(`Container set to: ${container}`);
        }
    }

    async selectStatusFilter() {
        const options = [
            { label: '$(list-unordered) All', value: 'all' },
            { label: '$(clock) Pending', value: 'pending' },
            { label: '$(pass) Succeeded', value: 'succeeded' },
            { label: '$(error) Failed', value: 'failed' },
            { label: '$(circle-slash) Denied', value: 'denied' },
            { label: '$(trash) Cancelled', value: 'cancelled' }
        ];

        const picked = await vscode.window.showQuickPick(options, {
            placeHolder: 'Filter requests by status'
        });

        if (picked) {
            this.statusFilter = picked.value;
            await this.refresh();
        }
    }

    async viewRequest(req) {
        if (!this.viewerProvider) return;
        await this.viewerProvider.openRequest(this.storageAccountName, this.selectedContainer, req);
    }

    /**
     * Submit a new pipeline run request by prompting the user for pipeline name.
     * Called from the title bar "+" button on the Pipeline Run Requests view.
     */
    async requestPipelineRunDirectly() {
        if (!this.selectedContainer) {
            const pick = await vscode.window.showWarningMessage(
                'No container selected for pipeline run requests. Select one now?',
                'Yes', 'No'
            );
            if (pick !== 'Yes') return;
            await this.selectContainer();
            if (!this.selectedContainer) return;
        }

        const pipelineItems = this._getWorkspacePipelineItems();
        let pipelineName, pipelineFilePath;

        if (pipelineItems.length > 0) {
            const picked = await vscode.window.showQuickPick(pipelineItems, {
                placeHolder: 'Select a pipeline to run'
            });
            if (!picked) return;
            pipelineName = picked.pipelineName;
            pipelineFilePath = picked.filePath;
        } else {
            const input = await vscode.window.showInputBox({
                prompt: 'Enter the pipeline name to run',
                placeHolder: 'e.g. SamplePipeline'
            });
            if (!input || !input.trim()) return;
            pipelineName = input.trim();
            pipelineFilePath = null;
        }

        await this._submitRequest(pipelineName, pipelineFilePath);
    }

    /**
     * Submit a new pipeline run request to the blob container.
     * Called from the "Request Pipeline Run" command on a pipeline file item.
     * @param {object} pipelineFileItem - FileItem from the pipeline tree (has .filePath)
     */
    async requestPipelineRun(pipelineFileItem) {
        if (!this.selectedContainer) {
            const pick = await vscode.window.showWarningMessage(
                'No container selected for pipeline run requests. Select one now?',
                'Yes', 'No'
            );
            if (pick !== 'Yes') return;
            await this.selectContainer();
            if (!this.selectedContainer) return;
        }

        // Derive pipeline name from the JSON file
        let pipelineName = path.basename(pipelineFileItem.filePath, '.json');
        try {
            const raw = fs.readFileSync(pipelineFileItem.filePath, 'utf-8');
            const parsed = JSON.parse(raw);
            if (parsed.name) pipelineName = parsed.name;
        } catch {
            // fallback to filename
        }

        await this._submitRequest(pipelineName, pipelineFileItem.filePath);
    }

    /**
     * Core submission logic shared by both entry points.
     * @param {string} pipelineName
     * @param {string|null} pipelineFilePath - local file path to read parameter definitions from
     */
    async _submitRequest(pipelineName, pipelineFilePath = null) {
        // Read pipeline parameter definitions from local file if available
        let paramDefs = {};
        if (pipelineFilePath) {
            try {
                const raw = fs.readFileSync(pipelineFilePath, 'utf-8');
                const parsed = JSON.parse(raw);
                paramDefs = parsed.properties?.parameters || {};
            } catch { /* ignore, treat as no params */ }
        }

        // Prompt user for each defined parameter with type-aware UI
        const parameters = await this._promptParameters(pipelineName, paramDefs);
        if (parameters === null) return; // user cancelled

        // requestedBy: try git config user.email, fall back to input
        let requestedBy = await this._getGitUserEmail();
        if (!requestedBy) {
            const emailInput = await vscode.window.showInputBox({
                prompt: 'Enter your name or email (will be recorded with the request)',
                placeHolder: 'user@company.com'
            });
            if (emailInput === undefined) return;
            requestedBy = emailInput || 'unknown';
        }

        // Confirmation
        const paramSummary = Object.keys(parameters).length
            ? `\nParameters: ${JSON.stringify(parameters)}`
            : '';
        const confirm = await vscode.window.showWarningMessage(
            `Submit pipeline run request?\n\nPipeline: ${pipelineName}${paramSummary}\nRequested by: ${requestedBy}`,
            { modal: true },
            'Submit'
        );
        if (confirm !== 'Submit') return;

        await vscode.window.withProgress(
            { location: vscode.ProgressLocation.Notification, title: 'Submitting pipeline run request...', cancellable: false },
            async () => {
                const now = new Date();
                const requestId = generateRequestId();
                const timestampStr = formatTimestampForFilename(now);
                const safeFileName = `${timestampStr}_${pipelineName}_${requestId}.json`;
                const blobPath = `${PENDING_FOLDER}/${safeFileName}`;

                const requestPayload = {
                    requestId,
                    pipelineName,
                    requestedAt: now.toISOString(),
                    requestedBy,
                    parameters,
                    status: 'pending',
                    statusUpdatedAt: null,
                    runId: null,
                    runStartedAt: null,
                    runCompletedAt: null,
                    runStatus: null,
                    errorMessage: null
                };

                const client = new ADLSRestClient(this.storageAccountName);
                await client.writeFile(this.selectedContainer, blobPath, JSON.stringify(requestPayload, null, 2));

                // Update status cache so we don't fire a spurious "changed" notification on next refresh
                const cache = this.context.workspaceState.get(STATUS_CACHE_KEY, {});
                cache[requestId] = 'pending';
                await this.context.workspaceState.update(STATUS_CACHE_KEY, cache);
            }
        );

        vscode.window.showInformationMessage(
            `Pipeline run request for "${pipelineName}" submitted successfully. Refresh the Requests panel to track its status.`
        );

        await this.refresh();
    }

    async _getGitUserEmail() {
        try {
            const { exec } = require('child_process');
            const { promisify } = require('util');
            const execP = promisify(exec);
            const { stdout } = await execP('git config user.email');
            return stdout.trim() || null;
        } catch {
            return null;
        }
    }

    /**
     * Returns QuickPick items for every pipeline JSON in the workspace's pipeline/ folder.
     */
    _getWorkspacePipelineItems() {
        const items = [];
        const workspaceFolders = vscode.workspace.workspaceFolders;
        if (!workspaceFolders) return items;

        for (const wf of workspaceFolders) {
            const pipelineDir = path.join(wf.uri.fsPath, 'pipeline');
            if (!fs.existsSync(pipelineDir)) continue;

            const files = fs.readdirSync(pipelineDir).filter(f => f.endsWith('.json'));
            for (const file of files) {
                const filePath = path.join(pipelineDir, file);
                try {
                    const raw = fs.readFileSync(filePath, 'utf-8');
                    const parsed = JSON.parse(raw);
                    const name = parsed.name || path.basename(file, '.json');
                    items.push({ label: name, description: file, pipelineName: name, filePath });
                } catch {
                    const name = path.basename(file, '.json');
                    items.push({ label: name, description: file, pipelineName: name, filePath });
                }
            }
        }
        return items;
    }

    /**
     * Prompts the user for each parameter defined in the pipeline.
     * Uses type-aware UI: QuickPick for Bool, InputBox for everything else.
     * Returns an object of {paramName: value}, or null if the user cancelled.
     */
    async _promptParameters(pipelineName, paramDefs) {
        const paramNames = Object.keys(paramDefs);
        const parameters = {};

        if (paramNames.length === 0) {
            return parameters;
        }

        for (const name of paramNames) {
            const def = paramDefs[name];
            const type = (def.type || 'String').toLowerCase();
            const defaultValue = def.defaultValue !== undefined ? String(def.defaultValue) : '';

            if (type === 'bool') {
                const picked = await vscode.window.showQuickPick(
                    ['true', 'false'],
                    { placeHolder: `${pipelineName} › ${name} (Bool)${defaultValue ? ` — default: ${defaultValue}` : ''}` }
                );
                if (picked === undefined) return null;
                parameters[name] = picked === 'true';
            } else {
                const value = await vscode.window.showInputBox({
                    prompt: `${pipelineName} › Parameter: ${name} (${def.type || 'String'})`,
                    placeHolder: defaultValue || `Enter value for ${name}`,
                    value: defaultValue,
                    validateInput: (val) => {
                        if (!val.trim()) return null;
                        if (type === 'int' && isNaN(parseInt(val, 10))) return 'Must be an integer';
                        if (type === 'float' && isNaN(parseFloat(val))) return 'Must be a number';
                        if ((type === 'array' || type === 'object') && val.trim()) {
                            try { JSON.parse(val); } catch { return 'Must be valid JSON'; }
                        }
                        return null;
                    }
                });
                if (value === undefined) return null;

                if (type === 'int') {
                    parameters[name] = value.trim() ? parseInt(value, 10) : (def.defaultValue ?? 0);
                } else if (type === 'float') {
                    parameters[name] = value.trim() ? parseFloat(value) : (def.defaultValue ?? 0.0);
                } else if (type === 'array' || type === 'object') {
                    parameters[name] = value.trim() ? JSON.parse(value) : (def.defaultValue ?? (type === 'array' ? [] : {}));
                } else {
                    parameters[name] = value;
                }
            }
        }
        return parameters;
    }

    /**
     * Cancel a pending request by overwriting its blob with status "cancelled".
     */
    async cancelRequest(item) {
        // Context menu passes the TreeItem; click command passes raw req — handle both
        const req = item.req ?? item;

        if (req.status !== 'pending') {
            vscode.window.showWarningMessage('Only pending requests can be cancelled.');
            return;
        }

        const confirm = await vscode.window.showWarningMessage(
            `Cancel the run request for pipeline "${req.pipelineName}"?`,
            { modal: true },
            'Cancel Request'
        );
        if (confirm !== 'Cancel Request') return;

        await vscode.window.withProgress(
            { location: vscode.ProgressLocation.Notification, title: 'Cancelling request...', cancellable: false },
            async () => {
                const now = new Date();
                const updated = { ...req, status: 'cancelled', statusUpdatedAt: now.toISOString() };
                delete updated._blobPath;
                delete updated._blobFolder;
                const client = new ADLSRestClient(this.storageAccountName);
                // Write to processed/ then delete from pending/
                const fileName = req._blobPath.split('/').pop();
                const processedPath = `${PROCESSED_FOLDER}/${fileName}`;
                await client.writeFile(this.selectedContainer, processedPath, JSON.stringify(updated, null, 2));
                await client.deleteFile(this.selectedContainer, req._blobPath);

                const cache = this.context.workspaceState.get(STATUS_CACHE_KEY, {});
                cache[req.requestId] = 'cancelled';
                await this.context.workspaceState.update(STATUS_CACHE_KEY, cache);
            }
        );

        vscode.window.showInformationMessage(`Request for "${req.pipelineName}" has been cancelled.`);
        await this.refresh();
    }
}

class PipelineRequestItem extends vscode.TreeItem {
    constructor(req) {
        super(req.pipelineName, vscode.TreeItemCollapsibleState.None);

        this.req = req;
        this.contextValue = `pipeline-request-${req.status}`;

        // Icon by status
        const iconMap = {
            pending: 'clock',
            succeeded: 'pass',
            failed: 'error',
            denied: 'circle-slash',
            cancelled: 'circle-slash'
        };
        this.iconPath = new vscode.ThemeIcon(iconMap[req.status] || 'circle-outline');

        // Description: requested time in SGT
        const utc = new Date(req.requestedAt);
        const sgt = new Date(utc.getTime() + 8 * 60 * 60 * 1000);
        const d = String(sgt.getUTCDate()).padStart(2, '0');
        const h = String(sgt.getUTCHours()).padStart(2, '0');
        const mi = String(sgt.getUTCMinutes()).padStart(2, '0');
        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        this.description = `${monthNames[sgt.getUTCMonth()]} ${d}, ${sgt.getUTCFullYear()} ${h}:${mi} — ${req.status}`;

        const paramStr = Object.keys(req.parameters || {}).length
            ? `\nParameters: ${JSON.stringify(req.parameters)}`
            : '';
        const runStr = req.runId ? `\nRun ID: ${req.runId}` : '';
        const errStr = req.errorMessage ? `\nError: ${req.errorMessage}` : '';
        this.tooltip = `Pipeline: ${req.pipelineName}\nRequested by: ${req.requestedBy || 'unknown'}${paramStr}\nStatus: ${req.status}${runStr}${errStr}`;

        this.command = {
            command: 'adf-pipeline-clone.viewPipelineRequest',
            title: 'View Request Details',
            arguments: [req]
        };
    }
}

class MessageItem extends vscode.TreeItem {
    constructor(message, contextValue) {
        super(message, vscode.TreeItemCollapsibleState.None);
        this.contextValue = contextValue;

        if (contextValue === 'select-container') {
            this.iconPath = new vscode.ThemeIcon('package');
            this.command = {
                command: 'adf-pipeline-clone.selectRequestContainer',
                title: 'Select Container'
            };
        } else if (contextValue === 'error') {
            this.iconPath = new vscode.ThemeIcon('error');
        } else {
            this.iconPath = new vscode.ThemeIcon('info');
        }
    }
}

module.exports = { PipelineRequestTreeDataProvider };
