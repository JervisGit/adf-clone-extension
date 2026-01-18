const vscode = require('vscode');

class ActivitiesTreeDataProvider {
    constructor(context) {
        this.context = context;
        this._onDidChangeTreeData = new vscode.EventEmitter();
        this.onDidChangeTreeData = this._onDidChangeTreeData.event;
    }

    refresh() {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element) {
        return element;
    }

    getChildren(element) {
        if (!element) {
            // Root level - return main sections
            return [
                new SectionTreeItem('Datasets', 'datasets', vscode.TreeItemCollapsibleState.Collapsed),
                new SectionTreeItem('Pipelines', 'pipelines', vscode.TreeItemCollapsibleState.Collapsed),
                new SectionTreeItem('Triggers', 'triggers', vscode.TreeItemCollapsibleState.Collapsed)
            ];
        } else if (element.contextValue === 'section') {
            // Return empty for now - can be populated later
            return [new EmptyTreeItem('No items')];
        }
        return [];
    }
}

class SectionTreeItem extends vscode.TreeItem {
    constructor(label, sectionId, collapsibleState) {
        super(label, collapsibleState);
        this.sectionId = sectionId;
        this.contextValue = 'section';
        
        // Set icons based on section
        if (sectionId === 'datasets') {
            this.iconPath = new vscode.ThemeIcon('database');
        } else if (sectionId === 'pipelines') {
            this.iconPath = new vscode.ThemeIcon('symbol-namespace');
        } else if (sectionId === 'triggers') {
            this.iconPath = new vscode.ThemeIcon('debug-start');
        }
        
        // Make pipelines section open the editor when clicked
        if (sectionId === 'pipelines') {
            this.command = {
                command: 'adf-pipeline-clone.openPipeline',
                title: 'Open Pipeline Editor'
            };
        }
    }
}

class EmptyTreeItem extends vscode.TreeItem {
    constructor(label) {
        super(label, vscode.TreeItemCollapsibleState.None);
        this.contextValue = 'empty';
        this.description = '';
    }
}

module.exports = { ActivitiesTreeDataProvider };
