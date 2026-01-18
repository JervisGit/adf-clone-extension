const vscode = require('vscode');
const path = require('path');
const fs = require('fs');

class PipelineTreeDataProvider {
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
				'{pipeline,dataset,trigger}/**/*.json'
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

		if (!element) {
			// Root level - show three main categories
			const folders = [];
			
			const datasetDir = path.join(workspaceRoot, 'dataset');
			const pipelineDir = path.join(workspaceRoot, 'pipeline');
			const triggerDir = path.join(workspaceRoot, 'trigger');
			
			if (fs.existsSync(datasetDir) || true) { // Always show even if doesn't exist
				folders.push(new FolderItem('Datasets', datasetDir, 'dataset'));
			}
			if (fs.existsSync(pipelineDir) || true) {
				folders.push(new FolderItem('Pipelines', pipelineDir, 'pipeline'));
			}
			if (fs.existsSync(triggerDir) || true) {
				folders.push(new FolderItem('Triggers', triggerDir, 'trigger'));
			}
			
			return folders;
		} else if (element.folderType) {
			// Show files in the folder
			if (!fs.existsSync(element.folderPath)) {
				return [];
			}
			
			const files = fs.readdirSync(element.folderPath)
				.filter(file => file.endsWith('.json'))
				.map(file => new FileItem(
					file,
					path.join(element.folderPath, file),
					element.folderType
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

class FileItem extends vscode.TreeItem {
	constructor(label, filePath, fileType) {
		super(label, vscode.TreeItemCollapsibleState.None);
		this.filePath = filePath;
		this.fileType = fileType;
		this.contextValue = fileType;
		this.iconPath = new vscode.ThemeIcon('json');
		this.command = {
			command: 'adf-pipeline-clone.openPipelineFile',
			title: 'Open Pipeline',
			arguments: [this]
		};
	}
}

module.exports = { PipelineTreeDataProvider };
