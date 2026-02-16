const vscode = require('vscode');
const path = require('path');
const fs = require('fs');

class DatasetTreeDataProvider {
	constructor(context) {
		this.context = context;
		this._onDidChangeTreeData = new vscode.EventEmitter();
		this.onDidChangeTreeData = this._onDidChangeTreeData.event;
		
		// Watch for file system changes
		this.setupFileWatcher();
	}

	setupFileWatcher() {
		if (vscode.workspace.workspaceFolders) {
			const pattern = new vscode.RelativePattern(
				vscode.workspace.workspaceFolders[0],
				'dataset/**/*.json'
			);
			
			const watcher = vscode.workspace.createFileSystemWatcher(pattern);
			watcher.onDidCreate(() => this.refresh());
			watcher.onDidChange(() => this.refresh());
			watcher.onDidDelete(() => this.refresh());
			
			this.context.subscriptions.push(watcher);
		}
	}

	refresh() {
		this._onDidChangeTreeData.fire();
	}

	getTreeItem(element) {
		return element;
	}

	async getChildren(element) {
		if (!vscode.workspace.workspaceFolders) {
			return [];
		}

		const workspaceRoot = vscode.workspace.workspaceFolders[0].uri.fsPath;
		const datasetDir = path.join(workspaceRoot, 'dataset');

		if (!element) {
			// Root level - show the datasets folder
			if (fs.existsSync(datasetDir) || true) { // Always show even if doesn't exist
				return [new FolderItem('Datasets', datasetDir, 'dataset')];
			}
			return [];
		} else if (element.folderType) {
			// Show files in the folder
			if (!fs.existsSync(element.folderPath)) {
				return [];
			}
			
			const files = fs.readdirSync(element.folderPath)
				.filter(file => file.endsWith('.json'))
				.sort()
				.map(file => new DatasetFileItem(
					file,
					path.join(element.folderPath, file)
				));
			
			return files;
		}

		return [];
	}
}

class FolderItem extends vscode.TreeItem {
	constructor(label, folderPath, folderType) {
		super(label, vscode.TreeItemCollapsibleState.Expanded);
		this.folderPath = folderPath;
		this.folderType = folderType;
		this.contextValue = `folder-${folderType}`;
		this.iconPath = new vscode.ThemeIcon('folder');
	}
}

class DatasetFileItem extends vscode.TreeItem {
	constructor(label, filePath) {
		super(label, vscode.TreeItemCollapsibleState.None);
		this.filePath = filePath;
		this.contextValue = 'dataset';
		this.iconPath = new vscode.ThemeIcon('database');
		
		// Use dataset editor command
		this.command = {
			command: 'adf-pipeline-clone.openDatasetFile',
			title: 'Open Dataset',
			arguments: [this]
		};
		
		// Add tooltip with file path
		this.tooltip = filePath;
	}
}

module.exports = { DatasetTreeDataProvider };
