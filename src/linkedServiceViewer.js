const vscode = require('vscode');
const fs = require('fs');
const path = require('path');

const LS_ICON_MAP = {
    'AzureBlobFS':      'adls_blob.png',
    'AzureBlobStorage': 'adls_blob.png',
    'AzureSqlDatabase': 'sql_db.png',
    'AzureSqlDW':       'az_sql_dw.png',
    'AzureKeyVault':    'key_vault.png',
    'HttpServer':       'http.png',
};

class LinkedServiceViewerPanel {
    constructor(context) {
        this.context = context;
        this.panel = null;      // legacy all-services panel
        this.panels = new Map(); // filePath → WebviewPanel (per-item panels)
    }

    /** Open a detail panel for a single linked service item from the tree. */
    viewLinkedService(item) {
        const filePath = item.filePath;

        // Re-use existing panel for this file if already open
        if (this.panels.has(filePath)) {
            this.panels.get(filePath).reveal();
            return;
        }

        let json;
        try {
            json = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        } catch {
            vscode.window.showErrorMessage(`Could not read linked service: ${filePath}`);
            return;
        }

        const name = json.name || path.basename(filePath, '.json');
        const panel = vscode.window.createWebviewPanel(
            'linkedServiceDetail',
            name,
            vscode.ViewColumn.One,
            {
                enableScripts: false,
                localResourceRoots: [vscode.Uri.joinPath(this.context.extensionUri, 'media', 'icons')]
            }
        );

        panel.webview.html = this._buildDetailHtml(panel.webview, json);
        panel.onDidDispose(() => this.panels.delete(filePath));
        this.panels.set(filePath, panel);
    }

    /** Legacy: open all-services panel (used by viewLinkedServices command). */
    show() {
        if (this.panel) {
            this.panel.reveal();
            return;
        }

        this.panel = vscode.window.createWebviewPanel(
            'linkedServicesViewer',
            'Linked Services',
            vscode.ViewColumn.One,
            { enableScripts: true, retainContextWhenHidden: true }
        );

        this.panel.webview.html = this._getHtml();

        this.panel.onDidDispose(() => {
            this.panel = null;
        });
    }

    _buildDetailHtml(webview, json) {
        const name        = json.name || 'Unknown';
        const type        = json.properties?.type || '';
        const props       = json.properties?.typeProperties || {};
        const description = json.properties?.description || '';
        const connectVia  = json.properties?.connectVia?.referenceName || 'AutoResolveIntegrationRuntime';
        const humanType   = this._getHumanType(type);

        const iconFile = LS_ICON_MAP[type];
        const iconTag  = iconFile
            ? `<img src="${webview.asWebviewUri(vscode.Uri.joinPath(this.context.extensionUri, 'media', 'icons', iconFile))}" alt="${this._escapeHtml(type)}" />`
            : '';

        const details    = this._extractDetails(type, props);
        const detailRows = Object.entries(details).map(([label, value]) => `
            <div class="detail-row">
                <div class="detail-label">${this._escapeHtml(label)}</div>
                <div class="detail-value${value === '<masked>' ? ' masked' : ''}">${this._escapeHtml(value)}</div>
            </div>`).join('');

        const descRow = description ? `
            <div class="detail-row">
                <div class="detail-label">Description</div>
                <div class="detail-value">${this._escapeHtml(description)}</div>
            </div>` : '';

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${this._escapeHtml(name)}</title>
    <style>
        body { font-family: var(--vscode-font-family); color: var(--vscode-foreground); padding: 24px; line-height: 1.6; }
        .header { display: flex; align-items: center; gap: 14px; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--vscode-panel-border); }
        .header img { width: 40px; height: 40px; object-fit: contain; }
        .header-text { display: flex; flex-direction: column; gap: 2px; }
        .header-name { font-size: 18px; font-weight: 700; }
        .header-type { font-size: 13px; color: var(--vscode-descriptionForeground); }
        .detail-row { display: flex; gap: 16px; padding: 6px 0; border-bottom: 1px solid var(--vscode-panel-border); font-size: 13px; }
        .detail-label { font-weight: 600; color: var(--vscode-descriptionForeground); min-width: 180px; flex-shrink: 0; }
        .detail-value { word-break: break-all; }
        .masked { font-style: italic; opacity: 0.6; }
    </style>
</head>
<body>
    <div class="header">
        ${iconTag}
        <div class="header-text">
            <div class="header-name">${this._escapeHtml(name)}</div>
            <div class="header-type">${this._escapeHtml(humanType)}</div>
        </div>
    </div>
    <div class="section">
        ${detailRows}
        ${descRow}
        <div class="detail-row">
            <div class="detail-label">Integration Runtime</div>
            <div class="detail-value">${this._escapeHtml(connectVia)}</div>
        </div>
    </div>
</body>
</html>`;
    }

    _getHtml() {
        const linkedServices = this._scanLinkedServices();
        const cardsHtml = linkedServices.map(ls => this._renderCard(ls)).join('');

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Linked Services</title>
    <style>
        body {
            font-family: var(--vscode-font-family);
            color: var(--vscode-foreground);
            padding: 20px;
            line-height: 1.6;
        }
        h1 {
            font-size: 20px;
            margin-bottom: 20px;
            border-bottom: 1px solid var(--vscode-panel-border);
            padding-bottom: 10px;
        }
        .card {
            background: var(--vscode-editor-background);
            border: 1px solid var(--vscode-panel-border);
            border-radius: 4px;
            padding: 16px;
            margin-bottom: 16px;
        }
        .card-header {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 12px;
        }
        .card-name {
            font-size: 16px;
            font-weight: 600;
            color: var(--vscode-textLink-foreground);
        }
        .card-type {
            font-size: 12px;
            background: var(--vscode-badge-background);
            color: var(--vscode-badge-foreground);
            padding: 2px 8px;
            border-radius: 3px;
        }
        .card-row {
            display: flex;
            gap: 20px;
            margin-bottom: 6px;
            font-size: 13px;
        }
        .card-label {
            font-weight: 600;
            color: var(--vscode-descriptionForeground);
            min-width: 140px;
        }
        .card-value {
            word-break: break-all;
        }
        .masked {
            font-style: italic;
            color: var(--vscode-disabledForeground);
        }
        .empty-state {
            text-align: center;
            color: var(--vscode-descriptionForeground);
            padding: 40px;
        }
    </style>
</head>
<body>
    <h1>Linked Services (Read-Only)</h1>
    ${linkedServices.length === 0 ? '<div class="empty-state">No linked services found in the workspace.</div>' : cardsHtml}
</body>
</html>`;
    }

    _scanLinkedServices() {
        const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri?.fsPath;
        if (!workspaceRoot) return [];

        // Search in workspace root AND the adf-clone-extension subfolder (same as pipeline editor)
        const searchRoots = [
            workspaceRoot,
            require('path').join(workspaceRoot, 'adf-clone-extension'),
        ];

        const seen = new Set();
        const results = [];

        for (const base of searchRoots) {
            const linkedServiceDir = require('path').join(base, 'linkedService');
            if (!require('fs').existsSync(linkedServiceDir)) continue;

            for (const file of require('fs').readdirSync(linkedServiceDir).filter(f => f.endsWith('.json'))) {
                if (seen.has(file)) continue;           // prefer workspace root version
                seen.add(file);
                try {
                    const filePath = require('path').join(linkedServiceDir, file);
                    const json = JSON.parse(require('fs').readFileSync(filePath, 'utf8'));
                    results.push(this._parseLSFile(json));
                } catch {
                    // skip invalid files
                }
            }
        }
        return results;
    }

    _parseLSFile(json) {
        const name = json.name || 'Unnamed';
        const type = json.properties?.type || 'Unknown';
        const props = json.properties?.typeProperties || {};
        const description = json.properties?.description || '';
        const connectVia = json.properties?.connectVia?.referenceName || 'AutoResolveIntegrationRuntime';

        return { name, type, props, description, connectVia };
    }

    _renderCard(ls) {
        const humanType = this._getHumanType(ls.type);
        const details = this._extractDetails(ls.type, ls.props);

        const detailRows = Object.entries(details)
            .map(([label, value]) => `
                <div class="card-row">
                    <div class="card-label">${this._escapeHtml(label)}:</div>
                    <div class="card-value">${this._escapeHtml(value)}</div>
                </div>
            `).join('');

        return `
            <div class="card">
                <div class="card-header">
                    <div class="card-name">${this._escapeHtml(ls.name)}</div>
                    <div class="card-type">${this._escapeHtml(humanType)}</div>
                </div>
                ${detailRows}
                ${ls.description ? `<div class="card-row"><div class="card-label">Description:</div><div class="card-value">${this._escapeHtml(ls.description)}</div></div>` : ''}
                <div class="card-row">
                    <div class="card-label">Integration Runtime:</div>
                    <div class="card-value">${this._escapeHtml(ls.connectVia)}</div>
                </div>
            </div>
        `;
    }

    _getHumanType(type) {
        const map = {
            'AzureBlobFS': 'Azure Data Lake Storage Gen2',
            'AzureBlobStorage': 'Azure Blob Storage',
            'AzureSqlDatabase': 'Azure SQL Database',
            'AzureSqlDW': 'Azure Synapse Analytics (SQL Pool)',
            'AzureKeyVault': 'Azure Key Vault',
            'AzureDataExplorer': 'Azure Data Explorer (Kusto)',
            'CosmosDb': 'Azure Cosmos DB',
            'AzureFunction': 'Azure Function',
            'HDInsight': 'HDInsight',
            'HttpServer': 'HTTP',
        };
        return map[type] || type;
    }

    _extractDetails(type, props) {
        const details = {};
        const sensitiveKeys = /key|token|password|secret|credential|connectionString|sasUri|sasToken|encryptedCredential|accountKey/i;

        // Extract relevant fields by type
        if (type === 'AzureBlobFS') {
            details['URL'] = props.url || '';
        } else if (type === 'AzureBlobStorage') {
            // Extract StorageAccountName from connectionString instead of masking it
            const conn = props.connectionString?.value ?? props.connectionString ?? '';
            const m = String(conn).match(/AccountName=([^;]+)/i);
            if (m) {
                details['Storage Account'] = m[1];
                details['Endpoint Suffix'] = 'blob.core.windows.net';
            } else {
                details['Service Endpoint'] = props.serviceEndpoint || '<unknown>';
            }
        } else if (type === 'AzureSqlDatabase' || type === 'AzureSqlDW') {
            if (props.server) {
                details['Server'] = props.server;
            } else {
                // Extract from connectionString e.g. "Data Source=tcp:host,1433"
                const conn = props.connectionString?.value ?? props.connectionString ?? '';
                const m = String(conn).match(/Data Source=tcp:([^,]+)/i);
                if (m) details['Server'] = m[1];
            }
            if (props.database) details['Database'] = props.database;
            details['Auth Type'] = props.authenticationType || 'Managed Identity';
        } else if (type === 'AzureKeyVault') {
            details['Base URL'] = props.baseUrl || '';
        } else if (type === 'CosmosDb') {
            details['Account Endpoint'] = props.accountEndpoint || '';
            details['Database'] = props.database || '';
        } else if (type === 'AzureFunction') {
            details['Function App URL'] = props.functionAppUrl || '';
        } else {
            // Generic display for other types
            for (const key in props) {
                if (typeof props[key] === 'string' && props[key].trim()) {
                    details[key] = sensitiveKeys.test(key) ? '<masked>' : props[key];
                }
            }
        }

        return details;
    }

    _escapeHtml(text) {
        if (!text) return '';
        return String(text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
}

module.exports = { LinkedServiceViewerPanel };
