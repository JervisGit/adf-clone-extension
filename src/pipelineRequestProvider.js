const vscode = require('vscode');
const path = require('path');
const fs = require('fs');
const { ADLSRestClient } = require('./adlsRestClient');

const REQUEST_FOLDER = 'pipeline-run-requests';
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
        this.statusFilter = 'all'; // 'all' | 'pending' | 'running' | 'succeeded' | 'failed'

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

        let paths = [];
        try {
            paths = await client.listPaths(this.selectedContainer, REQUEST_FOLDER, false);
        } catch (error) {
            // If the folder doesn't exist yet that's fine — no requests yet
            if (error.message.includes('404') || error.message.includes('PathNotFound') || error.message.includes('FilesystemNotFound')) {
                this.requests = [];
                return;
            }
            throw error;
        }

        const jsonFiles = paths.filter(p => !p.isDirectory && p.name.endsWith('.json'));

        const loaded = [];
        for (const file of jsonFiles) {
            try {
                const content = await client.readFile(this.selectedContainer, file.name);
                const data = JSON.parse(content);
                data._blobPath = file.name; // store blob path for updates/viewing
                loaded.push(data);
            } catch (err) {
                console.error(`Failed to read request file ${file.name}:`, err.message);
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
                } else if (req.status === 'running' && prev === 'pending') {
                    vscode.window.showInformationMessage(
                        `🔄 Pipeline "${req.pipelineName}" run has started${req.runId ? ` — Run ID: ${req.runId}` : ''}`
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
            { label: '$(sync~spin) Running', value: 'running' },
            { label: '$(pass) Succeeded', value: 'succeeded' },
            { label: '$(error) Failed', value: 'failed' }
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

        const pipelineName = await vscode.window.showInputBox({
            prompt: 'Enter the pipeline name to run',
            placeHolder: 'e.g. SamplePipeline'
        });
        if (!pipelineName || !pipelineName.trim()) return;

        await this._submitRequest(pipelineName.trim());
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

        await this._submitRequest(pipelineName);
    }

    /**
     * Core submission logic shared by both entry points.
     */
    async _submitRequest(pipelineName) {
        const paramChoice = await vscode.window.showQuickPick(
            [
                { label: '$(dash) No parameters', value: 'none' },
                { label: '$(symbol-key) Enter parameters as JSON', value: 'json' }
            ],
            { placeHolder: `Request run for pipeline: ${pipelineName}` }
        );
        if (!paramChoice) return;

        let parameters = {};
        if (paramChoice.value === 'json') {
            const paramInput = await vscode.window.showInputBox({
                prompt: 'Enter pipeline parameters as JSON',
                placeHolder: '{"startDate": "2026-03-01", "env": "prod"}',
                validateInput: (val) => {
                    if (!val.trim()) return null; // allow empty
                    try { JSON.parse(val); return null; }
                    catch { return 'Invalid JSON'; }
                }
            });
            if (paramInput === undefined) return; // cancelled
            if (paramInput.trim()) {
                parameters = JSON.parse(paramInput);
            }
        }

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
                const blobPath = `${REQUEST_FOLDER}/${safeFileName}`;

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
}

class PipelineRequestItem extends vscode.TreeItem {
    constructor(req) {
        super(req.pipelineName, vscode.TreeItemCollapsibleState.None);

        this.req = req;
        this.contextValue = 'pipeline-request';

        // Icon by status
        const iconMap = {
            pending: 'clock',
            running: 'sync~spin',
            succeeded: 'pass',
            failed: 'error'
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
