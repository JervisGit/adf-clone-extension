const vscode = require('vscode');
const datasetConfig = require('./dataset-config.json');
const { buildDatasetJson, validateDatasetForm } = require('./datasetUtils');

class DatasetEditorProvider {
	static panels = new Map(); // Map<filePath, panel>
	static dirtyStates = new Map(); // Map<filePath, isDirty>
	static stateCache = new Map(); // Map<filePath, datasetData>

	constructor(context) {
		this.context = context;
	}

	markPanelAsDirty(filePath, isDirty) {
		const panel = DatasetEditorProvider.panels.get(filePath);
		if (panel) {
			const path = require('path');
			const basename = path.basename(filePath, '.json');
			panel.title = isDirty ? `● ${basename}` : basename;
			DatasetEditorProvider.dirtyStates.set(filePath, isDirty);
		}
	}

	createOrShow(filePath = null) {
		const column = vscode.window.activeTextEditor
			? vscode.window.activeTextEditor.viewColumn
			: undefined;

		// If opening a specific file, check if panel already exists
		if (filePath && DatasetEditorProvider.panels.has(filePath)) {
			DatasetEditorProvider.panels.get(filePath).reveal(column);
			return DatasetEditorProvider.panels.get(filePath);
		}

		// Create title from filename if provided
		const path = require('path');
		const title = filePath 
			? path.basename(filePath, '.json')
			: 'Dataset Editor';

		// Create a new panel
		const panel = vscode.window.createWebviewPanel(
			'adfDatasetEditor',
			title,
			column || vscode.ViewColumn.One,
			{
				enableScripts: true,
				retainContextWhenHidden: true,
				localResourceRoots: [
					vscode.Uri.joinPath(this.context.extensionUri, 'media')
				]
			}
		);

		// Store panel with associated file path
		if (filePath) {
			DatasetEditorProvider.panels.set(filePath, panel);
		}

		// Set the webview's initial html content
		panel.webview.html = this.getHtmlContent();

		// Send dataset config and linked services list to webview
		setImmediate(() => {
			const fs = require('fs');
			let linkedServicesList = [];
			
			if (vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0) {
				const workspaceRoot = vscode.workspace.workspaceFolders[0].uri.fsPath;
				console.log('[DatasetEditor] Workspace root:', workspaceRoot);
				
				// Try multiple possible locations for linkedService folder
				const possiblePaths = [
					path.join(workspaceRoot, 'linkedService'),
					path.join(workspaceRoot, 'adf-clone-extension', 'linkedService'),
					path.join(workspaceRoot, '..', 'adf-clone-extension', 'linkedService')
				];
				
				let linkedServicePath = null;
				for (const testPath of possiblePaths) {
					console.log('[DatasetEditor] Testing path:', testPath);
					if (fs.existsSync(testPath)) {
						linkedServicePath = testPath;
						console.log('[DatasetEditor] Found linked services at:', linkedServicePath);
						break;
					}
				}
				
				if (linkedServicePath) {
					const files = fs.readdirSync(linkedServicePath).filter(f => f.endsWith('.json'));
					console.log('[DatasetEditor] Found linked service files:', files);
					
					linkedServicesList = files.map(f => {
						try {
							const filePath = path.join(linkedServicePath, f);
							const content = JSON.parse(fs.readFileSync(filePath, 'utf8'));
							const lsData = {
								name: f.replace('.json', ''),
								type: content.properties?.type || 'Unknown'
							};
							console.log('[DatasetEditor] Loaded linked service:', lsData);
							return lsData;
						} catch (err) {
							console.error(`[DatasetEditor] Error reading linked service ${f}:`, err);
							return {
								name: f.replace('.json', ''),
								type: 'Unknown'
							};
						}
					});
				} else {
					console.log('[DatasetEditor] Linked service path not found in any location');
				}
			}
			
			console.log('[DatasetEditor] Sending initialize message with linked services:', linkedServicesList);
			
			// Send configuration to webview
			panel.webview.postMessage({
				type: 'initialize',
				config: datasetConfig,
				linkedServices: linkedServicesList
			});
		});

		// Handle messages from webview
		panel.webview.onDidReceiveMessage(
			async message => {
				switch (message.type) {
					case 'save':
						// Validate before building JSON
						const saveValidation = validateDatasetForm(
							message.formData,
							datasetConfig,
							message.datasetType,
							message.fileType
						);
						if (!saveValidation.valid) {
							const bulletList = saveValidation.errors.map(e => `• ${e}`).join('\n');
							vscode.window.showErrorMessage(`Cannot save dataset:\n${bulletList}`, { modal: false });
							break;
						}
						// Build JSON from form data using config
						const datasetJson = buildDatasetJson(
							message.formData,
							datasetConfig,
							message.datasetType,
							message.fileType
						);
						await this.saveDataset(datasetJson, message.filePath || filePath);
						break;
					case 'validate':
						const validation = validateDatasetForm(
							message.formData,
							datasetConfig,
							message.datasetType,
							message.fileType
						);
						panel.webview.postMessage({
							type: 'validationResult',
							validation: validation
						});
						break;
					case 'dataChanged':
						if (filePath) {
							DatasetEditorProvider.stateCache.set(filePath, message.data);
							this.markPanelAsDirty(filePath, true);
						}
						break;
				}
			},
			undefined,
			this.context.subscriptions
		);

		// Reset when the current panel is closed
		panel.onDidDispose(
			async () => {
				// Check if there are unsaved changes before closing
				if (filePath && DatasetEditorProvider.dirtyStates.get(filePath)) {
					const path = require('path');
					const basename = path.basename(filePath, '.json');
					const answer = await vscode.window.showWarningMessage(
						`Do you want to save the changes you made to ${basename}?`,
						{ modal: true },
						'Save',
						"Don't Save",
						'Cancel'
					);
					
					if (answer === 'Save') {
						const cachedData = DatasetEditorProvider.stateCache.get(filePath);
						if (cachedData) {
							await this.saveDataset(cachedData, filePath);
							vscode.window.showInformationMessage(`Saved ${basename}`);
						}
					} else if (answer === 'Cancel') {
						// Reopen the panel with the cached state
						const cachedData = DatasetEditorProvider.stateCache.get(filePath);
						if (cachedData) {
							setImmediate(() => {
								const newPanel = this.createOrShow(filePath);
								setImmediate(() => {
									newPanel.webview.postMessage({
										type: 'loadDataset',
										data: cachedData
									});
									this.markPanelAsDirty(filePath, true);
								});
							});
						}
						return;
					}
				}
				
				// Remove panel from map
				if (filePath) {
					DatasetEditorProvider.panels.delete(filePath);
					DatasetEditorProvider.dirtyStates.delete(filePath);
					DatasetEditorProvider.stateCache.delete(filePath);
				}
			},
			null,
			this.context.subscriptions
		);
		
		return panel;
	}

	loadDatasetFile(filePath) {
		const panel = this.createOrShow(filePath);
		
		const fs = require('fs');
		try {
			const content = fs.readFileSync(filePath, 'utf8');
			const datasetJson = JSON.parse(content);
			
			// Send to webview after initialize completes
			setImmediate(() => {
				panel.webview.postMessage({
					type: 'loadDataset',
					data: datasetJson,
					filePath: filePath
				});
			});
		} catch (error) {
			vscode.window.showErrorMessage(`Failed to load dataset: ${error.message}`);
		}
	}

	async saveDataset(datasetData, filePath) {
		const fs = require('fs');
		const path = require('path');
		
		try {
			// Get workspace folder
			if (!vscode.workspace.workspaceFolders) {
				vscode.window.showErrorMessage('No workspace folder open');
				return;
			}
			
			const workspaceRoot = vscode.workspace.workspaceFolders[0].uri.fsPath;
			
			// Determine save path
			let savePath = filePath;
			if (!savePath) {
				// Ask user for filename
				const name = datasetData.name || 'NewDataset';
				savePath = path.join(workspaceRoot, 'dataset', `${name}.json`);
				
				// Ensure dataset directory exists
				const datasetDir = path.join(workspaceRoot, 'dataset');
				if (!fs.existsSync(datasetDir)) {
					fs.mkdirSync(datasetDir, { recursive: true });
				}
			}
			
			// Write file
			const jsonString = JSON.stringify(datasetData, null, 2);
			fs.writeFileSync(savePath, jsonString, 'utf8');
			
			// Mark as clean
			this.markPanelAsDirty(savePath, false);
			DatasetEditorProvider.stateCache.set(savePath, datasetData);
			
			vscode.window.showInformationMessage(`Dataset saved: ${path.basename(savePath)}`);
			
			// Refresh tree view if it exists
			vscode.commands.executeCommand('adf-pipeline-clone.refreshPipelines');
			
		} catch (error) {
			vscode.window.showErrorMessage(`Failed to save dataset: ${error.message}`);
		}
	}

	getHtmlContent() {
		return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline';">
    <title>Dataset Editor</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: var(--vscode-editor-background);
            color: var(--vscode-editor-foreground);
            overflow: hidden;
        }

        .container {
            display: flex;
            flex-direction: column;
            height: 100vh;
        }

        .toolbar {
            height: 48px;
            background: var(--vscode-editorGroupHeader-tabsBackground);
            border-bottom: 1px solid var(--vscode-panel-border);
            display: flex;
            align-items: center;
            padding: 0 16px;
            gap: 8px;
        }

        .toolbar-button {
            padding: 6px 12px;
            border: 1px solid var(--vscode-button-border);
            background: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
        }

        .toolbar-button:hover {
            background: var(--vscode-button-hoverBackground);
        }

        .toolbar-spacer {
            flex: 1;
        }

        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .preview-area {
            flex: 1;
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--vscode-descriptionForeground);
            font-size: 18px;
        }

        .config-panel {
            height: 350px;
            min-height: 350px;
            background: var(--vscode-panel-background);
            border-top: 1px solid var(--vscode-panel-border);
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .config-panel.minimized {
            height: 40px;
            min-height: 40px;
        }

        .config-panel.minimized .config-content {
            display: none;
        }

        .config-header {
            padding: 12px 16px;
            border-bottom: 1px solid var(--vscode-panel-border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 13px;
            font-weight: 600;
            background: var(--vscode-editorGroupHeader-tabsBackground);
        }

        .config-collapse-btn {
            background: transparent;
            border: none;
            cursor: pointer;
            font-size: 14px;
            color: var(--vscode-foreground);
            padding: 4px 8px;
        }

        .config-content {
            flex: 1;
            overflow-y: auto;
            padding: 16px;
        }

        .form-section {
            margin-bottom: 24px;
        }

        .form-section-title {
            font-size: 13px;
            font-weight: 600;
            margin-bottom: 12px;
            color: var(--vscode-foreground);
        }

        .form-group {
            margin-bottom: 16px;
        }

        .form-group.hidden {
            display: none;
        }

        .form-label {
            display: block;
            margin-bottom: 4px;
            font-size: 12px;
            color: var(--vscode-foreground);
        }

        .form-label.required::after {
            content: " *";
            color: var(--vscode-errorForeground);
        }

        .form-input {
            width: 100%;
            padding: 6px 8px;
            background: var(--vscode-input-background);
            color: var(--vscode-input-foreground);
            border: 1px solid var(--vscode-input-border);
            border-radius: 2px;
            font-size: 13px;
            font-family: inherit;
        }

        .form-input:focus {
            outline: 1px solid var(--vscode-focusBorder);
            outline-offset: -1px;
        }

        .form-select {
            width: 100%;
            padding: 6px 8px;
            background: var(--vscode-dropdown-background);
            color: var(--vscode-dropdown-foreground);
            border: 1px solid var(--vscode-dropdown-border);
            border-radius: 2px;
            font-size: 13px;
        }

        .form-checkbox {
            margin-right: 8px;
        }

        .radio-group {
            display: flex;
            gap: 16px;
            flex-wrap: wrap;
            margin-top: 2px;
        }

        .radio-option {
            display: flex;
            align-items: center;
            gap: 5px;
            cursor: pointer;
            font-size: 13px;
        }

        .radio-option input[type="radio"] {
            margin: 0;
            cursor: pointer;
        }

        .form-help {
            font-size: 11px;
            color: var(--vscode-descriptionForeground);
            margin-top: 4px;
        }

        .validation-error {
            color: var(--vscode-errorForeground);
            font-size: 11px;
            margin-top: 4px;
        }

        .error-list {
            background: var(--vscode-inputValidation-errorBackground);
            border: 1px solid var(--vscode-inputValidation-errorBorder);
            padding: 12px;
            margin: 16px 0;
            border-radius: 4px;
        }

        .error-list-title {
            font-weight: 600;
            margin-bottom: 8px;
            color: var(--vscode-errorForeground);
        }

        .error-list ul {
            list-style-position: inside;
            margin: 0;
            padding: 0;
        }

        .error-list li {
            margin: 4px 0;
            font-size: 12px;
        }

        .parameters-container {
            border: 1px solid var(--vscode-focusBorder);
            border-radius: 4px;
            padding: 12px;
            background: var(--vscode-editor-background);
        }

        .parameters-table {
            margin-bottom: 12px;
        }

        .parameter-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr auto;
            gap: 8px;
            margin-bottom: 8px;
            align-items: center;
        }

        .parameter-row input,
        .parameter-row select {
            margin: 0;
        }

        .add-parameter-btn,
        .remove-parameter-btn {
            padding: 6px 12px;
            border: 1px solid var(--vscode-button-border);
            background: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
            cursor: pointer;
            border-radius: 2px;
            font-size: 12px;
        }

        .add-parameter-btn:hover,
        .remove-parameter-btn:hover {
            background: var(--vscode-button-hoverBackground);
        }

        .remove-parameter-btn {
            padding: 2px 8px;
            font-size: 16px;
            line-height: 1;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="toolbar">
            <button class="toolbar-button" id="saveBtn">Save</button>
            <button class="toolbar-button" id="newBtn">New Dataset</button>
            <div class="toolbar-spacer"></div>
        </div>
        
        <div class="main-content">
            <div class="preview-area">
                Configure your dataset below
            </div>
            
            <div class="config-panel" id="configPanel">
                <div class="config-header">
                    <span>Dataset Configuration</span>
                    <button class="config-collapse-btn" id="collapseBtn">▼</button>
                </div>
                
                <div class="config-content" id="configContent">
                    <div id="errorContainer"></div>
                    
                    <div class="form-section">
                        <div class="form-section-title">General</div>
                        
                        <div class="form-group">
                            <label class="form-label required">Dataset name</label>
                            <input type="text" class="form-input" id="name" placeholder="MyDataset" />
                        </div>
                        
                        <div class="form-group">
                            <label class="form-label required">Dataset type</label>
                            <select class="form-select" id="datasetType">
                                <option value="">Select dataset type...</option>
                            </select>
                        </div>
                        
                        <div class="form-group hidden" id="fileTypeGroup">
                            <label class="form-label required">File type</label>
                            <select class="form-select" id="fileType">
                                <option value="">Select file type...</option>
                            </select>
                        </div>
                        
                        <div class="form-group">
                            <label class="form-label required">Linked service</label>
                            <select class="form-select" id="linkedService">
                                <option value="">Select linked service...</option>
                            </select>
                        </div>
                    </div>
                    
                    <div id="dynamicFieldsContainer"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const vscode = acquireVsCodeApi();
        let currentConfig = null;
        let linkedServices = [];
        let currentFilePath = null;
        let pendingDatasetLoad = null;

        // Utility function to get value from nested object by path
        function getValueByPath(obj, path) {
            if (!path) return undefined;
            const keys = path.split('.');
            let current = obj;
            for (const key of keys) {
                if (current === null || current === undefined) return undefined;
                current = current[key];
            }
            return current;
        }

        // Initialize
        window.addEventListener('message', event => {
            const message = event.data;
            console.log('[Webview] Received message:', message.type, message);
            
            switch (message.type) {
                case 'initialize':
                    console.log('[Webview] Initializing with linked services:', message.linkedServices);
                    currentConfig = message.config;
                    linkedServices = message.linkedServices;
                    initializeEditor();
                    
                    // If there's a pending dataset to load, load it now
                    if (pendingDatasetLoad) {
                        console.log('[Webview] Loading pending dataset');
                        loadDataset(pendingDatasetLoad.data);
                        currentFilePath = pendingDatasetLoad.filePath;
                        pendingDatasetLoad = null;
                    }
                    break;
                case 'loadDataset':
                    // If not initialized yet, queue the load
                    if (!currentConfig) {
                        console.log('[Webview] Queueing dataset load until initialized');
                        pendingDatasetLoad = { data: message.data, filePath: message.filePath };
                    } else {
                        console.log('[Webview] Loading dataset now');
                        loadDataset(message.data);
                        currentFilePath = message.filePath;
                    }
                    break;
                case 'validationResult':
                    displayValidationErrors(message.validation);
                    break;
            }
        });

        function initializeEditor() {
            console.log('[Webview] initializeEditor called');
            console.log('[Webview] currentConfig:', currentConfig);
            console.log('[Webview] linkedServices:', linkedServices);
            
            // Populate dataset type dropdown
            const datasetTypeSelect = document.getElementById('datasetType');
            for (const [key, config] of Object.entries(currentConfig.datasetTypes)) {
                const option = document.createElement('option');
                option.value = key;
                option.textContent = config.displayName;
                datasetTypeSelect.appendChild(option);
            }
            console.log('[Webview] Populated dataset types');

            // Populate linked services dropdown
            const linkedServiceSelect = document.getElementById('linkedService');
            console.log('[Webview] linkedServiceSelect element:', linkedServiceSelect);
            console.log('[Webview] Number of linked services to add:', linkedServices.length);
            
            linkedServices.forEach(ls => {
                console.log('[Webview] Adding linked service to dropdown:', ls);
                const option = document.createElement('option');
                option.value = ls.name;
                option.textContent = ls.name;
                option.setAttribute('data-type', ls.type);
                linkedServiceSelect.appendChild(option);
            });
            
            console.log('[Webview] linkedServiceSelect.options.length:', linkedServiceSelect.options.length);

            // Set up event listeners
            document.getElementById('datasetType').addEventListener('change', onDatasetTypeChange);
            document.getElementById('fileType').addEventListener('change', onFileTypeChange);
            document.getElementById('saveBtn').addEventListener('click', saveDataset);
            document.getElementById('newBtn').addEventListener('click', newDataset);
            document.getElementById('collapseBtn').addEventListener('click', toggleConfigPanel);

            // Notify of data changes
            document.querySelectorAll('input, select, textarea').forEach(el => {
                el.addEventListener('input', notifyDataChanged);
            });
        }

        function filterLinkedServices(datasetType) {
            console.log('[Webview] filterLinkedServices called with datasetType:', datasetType);
            console.log('[Webview] currentConfig:', currentConfig);
            console.log('[Webview] linkedServices count:', linkedServices.length);
            
            const config = currentConfig.datasetTypes[datasetType];
            console.log('[Webview] Dataset config:', config);
            
            const linkedServiceSelect = document.getElementById('linkedService');
            const currentValue = linkedServiceSelect.value;
            console.log('[Webview] Current linked service value:', currentValue);
            
            // Get allowed linked service types
            const allowedTypes = config?.linkedServiceTypes || [];
            console.log('[Webview] Allowed linked service types:', allowedTypes);
            
            // Save first option (Select...)
            const firstOption = linkedServiceSelect.options[0];
            console.log('[Webview] First option text:', firstOption?.textContent);
            
            // Clear all options
            linkedServiceSelect.innerHTML = '';
            
            // Re-add first option
            linkedServiceSelect.appendChild(firstOption);
            
            // Filter and add matching linked services
            let addedCount = 0;
            linkedServices.forEach(ls => {
                console.log('[Webview] Checking linked service:', ls.name, 'type:', ls.type, 'allowed:', allowedTypes.includes(ls.type));
                if (allowedTypes.length === 0 || allowedTypes.includes(ls.type)) {
                    const option = document.createElement('option');
                    option.value = ls.name;
                    option.textContent = ls.name;
                    option.setAttribute('data-type', ls.type);
                    linkedServiceSelect.appendChild(option);
                    addedCount++;
                    console.log('[Webview] Added linked service:', ls.name);
                }
            });
            
            console.log('[Webview] Total linked services added after filter:', addedCount);
            console.log('[Webview] Final linkedServiceSelect.options.length:', linkedServiceSelect.options.length);
            
            // Restore previous value if still valid
            if (currentValue && Array.from(linkedServiceSelect.options).some(opt => opt.value === currentValue)) {
                linkedServiceSelect.value = currentValue;
                console.log('[Webview] Restored previous value:', currentValue);
            }
        }

        function onDatasetTypeChange(event) {
            const datasetType = event.target.value;
            const config = currentConfig.datasetTypes[datasetType];
            
            // Filter linked services based on dataset type
            filterLinkedServices(datasetType);
            
            if (config.requiresFileType) {
                // Show file type selector
                const fileTypeGroup = document.getElementById('fileTypeGroup');
                fileTypeGroup.classList.remove('hidden');
                
                // Populate file type options
                const fileTypeSelect = document.getElementById('fileType');
                fileTypeSelect.innerHTML = '<option value="">Select file type...</option>';
                
                for (const [key, fileConfig] of Object.entries(config.fileTypes)) {
                    const option = document.createElement('option');
                    option.value = key;
                    option.textContent = fileConfig.displayName;
                    fileTypeSelect.appendChild(option);
                }
            } else {
                // Hide file type selector and render fields directly
                document.getElementById('fileTypeGroup').classList.add('hidden');
                renderDynamicFields(datasetType, null);
            }
        }

        function onFileTypeChange(event) {
            const datasetType = document.getElementById('datasetType').value;
            const fileType = event.target.value;
            renderDynamicFields(datasetType, fileType);
        }

        function renderDynamicFields(datasetType, fileType) {
            const container = document.getElementById('dynamicFieldsContainer');
            container.innerHTML = '';
            
            const config = currentConfig.datasetTypes[datasetType];
            let fieldsConfig = null;
            
            if (fileType && config.fileTypes && config.fileTypes[fileType]) {
                fieldsConfig = config.fileTypes[fileType].fields;
            } else if (config.fields) {
                fieldsConfig = config.fields;
            }
            
            if (!fieldsConfig) return;
            
            // Render sections
            for (const [sectionName, fields] of Object.entries(fieldsConfig)) {
                const section = document.createElement('div');
                section.className = 'form-section';
                
                const title = document.createElement('div');
                title.className = 'form-section-title';
                title.textContent = sectionName.charAt(0).toUpperCase() + sectionName.slice(1);
                section.appendChild(title);
                
                // Render fields in section
                for (const [fieldKey, fieldConfig] of Object.entries(fields)) {
                    if (fieldConfig.type === 'hidden') continue;
                    
                    const fieldEl = renderField(fieldKey, fieldConfig);
                    section.appendChild(fieldEl);
                }
                
                container.appendChild(section);
            }
            
            // Add common fields (parameters) if not already present
            if (currentConfig.commonFields && !document.getElementById('parameters')) {
                for (const [fieldKey, fieldConfig] of Object.entries(currentConfig.commonFields)) {
                    if (fieldKey === 'name' || fieldKey === 'linkedService') continue; // Skip already rendered
                    
                    const fieldEl = renderField(fieldKey, fieldConfig);
                    container.appendChild(fieldEl);
                }
            }
            
            // Add data change listeners to new fields
            container.querySelectorAll('input, select, textarea').forEach(el => {
                el.addEventListener('input', notifyDataChanged);
                
                // Add listener for conditional field visibility
                el.addEventListener('change', (e) => {
                    updateFieldVisibility(e.target.id, e.target.value);
                });
                
                // Initial visibility check for select/input fields with values
                if ((el.tagName === 'SELECT' || el.tagName === 'INPUT') && el.value && el.type !== 'radio') {
                    updateFieldVisibility(el.id, el.value);
                }
            });

            // Initial visibility check for radio groups
            container.querySelectorAll('.radio-group').forEach(radioContainer => {
                const checked = radioContainer.querySelector('input[type="radio"]:checked');
                if (checked) {
                    updateFieldVisibility(radioContainer.id, checked.value);
                }
            });
        }

        function addParameterRow(fieldKey, valueTypes) {
            const table = document.getElementById(\`\${fieldKey}-table\`);
            const row = document.createElement('div');
            row.className = 'parameter-row';
            
            const nameInput = document.createElement('input');
            nameInput.type = 'text';
            nameInput.className = 'form-input param-name';
            nameInput.placeholder = 'Parameter name';
            row.appendChild(nameInput);
            
            const typeSelect = document.createElement('select');
            typeSelect.className = 'form-select param-type';
            valueTypes.forEach(type => {
                const option = document.createElement('option');
                option.value = type;
                option.textContent = type;
                typeSelect.appendChild(option);
            });
            row.appendChild(typeSelect);
            
            const valueInput = document.createElement('input');
            valueInput.type = 'text';
            valueInput.className = 'form-input param-value';
            valueInput.placeholder = 'Default value';
            row.appendChild(valueInput);
            
            const removeBtn = document.createElement('button');
            removeBtn.type = 'button';
            removeBtn.className = 'remove-parameter-btn';
            removeBtn.textContent = '×';
            removeBtn.onclick = () => row.remove();
            row.appendChild(removeBtn);
            
            table.appendChild(row);
            notifyDataChanged();
        }

        function collectParameters(fieldKey) {
            const table = document.getElementById(\`\${fieldKey}-table\`);
            if (!table) return null;
            
            const parameters = {};
            table.querySelectorAll('.parameter-row').forEach(row => {
                const name = row.querySelector('.param-name').value;
                const type = row.querySelector('.param-type').value;
                const defaultValue = row.querySelector('.param-value').value;
                
                if (name) {
                    parameters[name] = {
                        type: type.toLowerCase(), // Azure expects lowercase: string, int, float, bool, etc.
                        defaultValue: defaultValue
                    };
                }
            });
            
            return Object.keys(parameters).length > 0 ? parameters : null;
        }

        function renderField(fieldKey, fieldConfig) {
            const group = document.createElement('div');
            group.className = 'form-group';
            group.id = \`field-\${fieldKey}\`;
            
            // Handle conditional visibility
            if (fieldConfig.showWhen) {
                group.setAttribute('data-show-when-field', fieldConfig.showWhen.field);
                if (fieldConfig.showWhen.notEmpty) {
                    group.setAttribute('data-show-when-not-empty', 'true');
                } else if (fieldConfig.showWhen.value !== undefined) {
                    group.setAttribute('data-show-when-value', fieldConfig.showWhen.value);
                }
                group.style.display = 'none'; // Initially hidden
            }
            
            const label = document.createElement('label');
            label.className = 'form-label' + (fieldConfig.required ? ' required' : '');
            label.textContent = fieldConfig.label;
            group.appendChild(label);
            
            let input;
            switch (fieldConfig.type) {
                case 'text':
                    input = document.createElement(fieldConfig.multiline ? 'textarea' : 'input');
                    input.className = 'form-input';
                    input.id = fieldKey;
                    input.placeholder = fieldConfig.placeholder || '';
                    if (fieldConfig.multiline) {
                        input.rows = 3;
                    }
                    break;
                    
                case 'select':
                    input = document.createElement('select');
                    input.className = 'form-select';
                    input.id = fieldKey;
                    
                    const emptyOption = document.createElement('option');
                    emptyOption.value = '';
                    emptyOption.textContent = 'Select...';
                    emptyOption.disabled = true;
                    emptyOption.selected = true;
                    input.appendChild(emptyOption);
                    
                    (fieldConfig.options || []).forEach(opt => {
                        const option = document.createElement('option');
                        // Handle both string options and object options {label, value}
                        if (typeof opt === 'object' && opt.label && opt.value !== undefined) {
                            option.value = opt.value;
                            option.textContent = opt.label;
                            if (opt.omitFromJson) {
                                option.dataset.omitFromJson = 'true';
                            }
                        } else {
                            option.value = opt;
                            option.textContent = opt;
                        }
                        input.appendChild(option);
                    });
                    break;
                    
                case 'select-text':
                    // Create a container for both select and text input with toggle
                    const container = document.createElement('div');
                    container.className = 'select-text-container';
                    container.id = fieldKey;
                    
                    // Create select dropdown
                    const selectInput = document.createElement('select');
                    selectInput.className = 'form-select';
                    selectInput.id = \`\${fieldKey}-select\`;
                    
                    const emptyOpt = document.createElement('option');
                    emptyOpt.value = '';
                    emptyOpt.textContent = 'Select...';
                    emptyOpt.disabled = true;
                    emptyOpt.selected = true;
                    selectInput.appendChild(emptyOpt);
                    
                    (fieldConfig.options || []).forEach(opt => {
                        const option = document.createElement('option');
                        if (typeof opt === 'object' && opt.label && opt.value !== undefined) {
                            option.value = opt.value;
                            option.textContent = opt.label;
                            if (opt.omitFromJson) {
                                option.dataset.omitFromJson = 'true';
                            }
                        } else {
                            option.value = opt;
                            option.textContent = opt;
                        }
                        selectInput.appendChild(option);
                    });
                    
                    // Create text input (hidden by default)
                    const textInput = document.createElement('input');
                    textInput.type = 'text';
                    textInput.className = 'form-input';
                    textInput.id = \`\${fieldKey}-text\`;
                    textInput.placeholder = fieldConfig.placeholder || '';
                    textInput.style.display = 'none';
                    
                    // Create toggle checkbox
                    const toggleContainer = document.createElement('div');
                    toggleContainer.style.marginTop = '5px';
                    toggleContainer.style.display = 'flex';
                    toggleContainer.style.alignItems = 'center';
                    toggleContainer.style.gap = '5px';
                    
                    const toggleCheckbox = document.createElement('input');
                    toggleCheckbox.type = 'checkbox';
                    toggleCheckbox.id = \`\${fieldKey}-manual\`;
                    toggleCheckbox.className = 'form-checkbox';
                    
                    const toggleLabel = document.createElement('label');
                    toggleLabel.textContent = 'Enter manually';
                    toggleLabel.style.fontSize = '12px';
                    toggleLabel.style.cursor = 'pointer';
                    toggleLabel.htmlFor = \`\${fieldKey}-manual\`;
                    
                    toggleCheckbox.addEventListener('change', (e) => {
                        if (e.target.checked) {
                            selectInput.style.display = 'none';
                            textInput.style.display = 'block';
                            textInput.value = selectInput.value;
                        } else {
                            textInput.style.display = 'none';
                            selectInput.style.display = 'block';
                            selectInput.value = textInput.value || '';
                        }
                        notifyDataChanged();
                    });
                    
                    toggleContainer.appendChild(toggleCheckbox);
                    toggleContainer.appendChild(toggleLabel);
                    
                    container.appendChild(selectInput);
                    container.appendChild(textInput);
                    container.appendChild(toggleContainer);
                    
                    input = container;
                    break;
                    
                case 'radio': {
                    const radioContainer = document.createElement('div');
                    radioContainer.className = 'radio-group';
                    radioContainer.id = fieldKey;
                    if (fieldConfig.omitFromJson) {
                        radioContainer.dataset.omitFromJson = 'true';
                    }
                    (fieldConfig.options || []).forEach(opt => {
                        const radioWrapper = document.createElement('label');
                        radioWrapper.className = 'radio-option';
                        const radioInput = document.createElement('input');
                        radioInput.type = 'radio';
                        radioInput.name = fieldKey;
                        radioInput.value = opt.value;
                        if (fieldConfig.default !== undefined && opt.value === fieldConfig.default) {
                            radioInput.checked = true;
                        }
                        radioInput.addEventListener('change', () => {
                            updateFieldVisibility(fieldKey, radioInput.value);
                            notifyDataChanged();
                        });
                        radioWrapper.appendChild(radioInput);
                        radioWrapper.appendChild(document.createTextNode(' ' + opt.label));
                        radioContainer.appendChild(radioWrapper);
                    });
                    input = radioContainer;
                    break;
                }

                case 'boolean':
                    input = document.createElement('input');
                    input.type = 'checkbox';
                    input.className = 'form-checkbox';
                    input.id = fieldKey;
                    break;
                    
                case 'number':
                    input = document.createElement('input');
                    input.type = 'number';
                    input.className = 'form-input';
                    input.id = fieldKey;
                    input.placeholder = fieldConfig.placeholder || '';
                    if (fieldConfig.min !== undefined) input.min = fieldConfig.min;
                    if (fieldConfig.max !== undefined) input.max = fieldConfig.max;
                    break;
                    
                case 'keyvalue-parameters':
                    // Create a container for parameters with add/remove functionality
                    input = document.createElement('div');
                    input.id = fieldKey;
                    input.className = 'parameters-container';
                    input.setAttribute('data-value-types', JSON.stringify(fieldConfig.valueTypes || ['String']));
                    
                    const parametersTable = document.createElement('div');
                    parametersTable.className = 'parameters-table';
                    parametersTable.id = \`\${fieldKey}-table\`;
                    input.appendChild(parametersTable);
                    
                    const addBtn = document.createElement('button');
                    addBtn.type = 'button';
                    addBtn.className = 'add-parameter-btn';
                    addBtn.textContent = '+ Add Parameter';
                    addBtn.onclick = () => addParameterRow(fieldKey, fieldConfig.valueTypes || ['String']);
                    input.appendChild(addBtn);
                    break;
                    
                default:
                    input = document.createElement('input');
                    input.className = 'form-input';
                    input.id = fieldKey;
            }
            
            if (fieldConfig.default !== undefined) {
                if (fieldConfig.type === 'radio') {
                    // defaults are already applied when creating radio inputs above
                } else if (fieldConfig.type === 'boolean' && !input.checked) {
                    input.checked = fieldConfig.default;
                } else if (fieldConfig.type === 'select-text') {
                    // For select-text, use querySelector since the element is not yet in the DOM
                    const selectEl = input.querySelector('select');
                    if (selectEl) {
                        // Find first non-disabled option matching the default value
                        const defaultVal = String(fieldConfig.default);
                        for (let i = 0; i < selectEl.options.length; i++) {
                            if (!selectEl.options[i].disabled && selectEl.options[i].value === defaultVal) {
                                selectEl.selectedIndex = i;
                                break;
                            }
                        }
                    }
                } else if (fieldConfig.type === 'select') {
                    // Loop to find first non-disabled match (avoids landing on disabled placeholder)
                    const defaultVal = String(fieldConfig.default);
                    for (let i = 0; i < input.options.length; i++) {
                        if (!input.options[i].disabled && input.options[i].value === defaultVal) {
                            input.selectedIndex = i;
                            break;
                        }
                    }
                } else if (!input.value) {
                    input.value = fieldConfig.default;
                }
            }
            
            group.appendChild(input);
            
            if (fieldConfig.helpText) {
                const help = document.createElement('div');
                help.className = 'form-help';
                help.textContent = fieldConfig.helpText;
                group.appendChild(help);
            }
            
            return group;
        }

        function updateFieldVisibility(triggerFieldId, triggerValue) {
            console.log('[Webview] Checking field visibility for trigger:', triggerFieldId, 'value:', triggerValue);
            
            // Find all fields that depend on this trigger field
            document.querySelectorAll(\`[data-show-when-field="\${triggerFieldId}"]\`).forEach(dependentField => {
                const notEmptyCondition = dependentField.getAttribute('data-show-when-not-empty');
                const expectedValue = dependentField.getAttribute('data-show-when-value');
                
                let shouldShow = false;
                if (notEmptyCondition === 'true') {
                    // Show when trigger field has any non-empty value
                    shouldShow = (triggerValue !== '' && triggerValue !== null && triggerValue !== undefined);
                    console.log('[Webview] Field', dependentField.id, 'should', shouldShow ? 'show' : 'hide', 
                        '(trigger value not empty:', triggerValue, ')');
                } else {
                    // Show when trigger field matches expected value
                    shouldShow = (triggerValue === expectedValue);
                    console.log('[Webview] Field', dependentField.id, 'should', shouldShow ? 'show' : 'hide', 
                        '(trigger value:', triggerValue, 'expected:', expectedValue, ')');
                }
                
                dependentField.style.display = shouldShow ? 'block' : 'none';
                
                // Clear value if hiding
                if (!shouldShow) {
                    const input = dependentField.querySelector('input, select, textarea');
                    if (input && input.type !== 'checkbox') {
                        input.value = '';
                    }
                }
            });
        }

        function collectFormData() {
            const data = {
                name: document.getElementById('name').value,
                datasetType: document.getElementById('datasetType').value,
                fileType: document.getElementById('fileType').value,
                linkedService: document.getElementById('linkedService').value
            };
            
            // Collect parameters if present
            const parametersContainer = document.getElementById('parameters');
            if (parametersContainer) {
                data.parameters = collectParameters('parameters');
            }
            
            // Collect radio group values (always included — validator needs them for showWhen conditions;
            // omitFromJson is enforced server-side in buildDatasetJson, not here)
            document.querySelectorAll('#dynamicFieldsContainer .radio-group').forEach(radioContainer => {
                const fieldGroup = radioContainer.closest('.form-group');
                if (fieldGroup && fieldGroup.style.display === 'none') return;
                const checked = radioContainer.querySelector('input[type="radio"]:checked');
                if (checked) {
                    data[radioContainer.id] = checked.value;
                }
            });

            // Collect dynamic fields
            document.querySelectorAll('#dynamicFieldsContainer input, #dynamicFieldsContainer select, #dynamicFieldsContainer textarea').forEach(el => {
                // Skip hidden fields
                const fieldGroup = el.closest('.form-group');
                if (fieldGroup && fieldGroup.style.display === 'none') {
                    return;
                }

                // Skip radio inputs (handled above as a group)
                if (el.type === 'radio') {
                    return;
                }

                // Skip if this is part of a select-text control (we'll handle those separately)
                if (el.id.endsWith('-select') || el.id.endsWith('-text') || el.id.endsWith('-manual')) {
                    return;
                }
                
                if (el.type === 'checkbox') {
                    data[el.id] = el.checked;
                } else if (!el.closest('.parameters-container')) {
                    // Skip parameters container's inputs as they're handled above
                    // Check if the selected option has omitFromJson flag
                    if (el.tagName === 'SELECT') {
                        const selectedOpt = el.options[el.selectedIndex];
                        if (selectedOpt && selectedOpt.dataset.omitFromJson === 'true') {
                            return; // Don't write to JSON
                        }
                    }
                    data[el.id] = el.value;
                }
            });
            
            // Collect select-text field values
            document.querySelectorAll('#dynamicFieldsContainer .select-text-container').forEach(container => {
                const fieldGroup = container.closest('.form-group');
                if (fieldGroup && fieldGroup.style.display === 'none') {
                    return;
                }
                
                const fieldKey = container.id;
                const selectInput = document.getElementById(\`\${fieldKey}-select\`);
                const textInput = document.getElementById(\`\${fieldKey}-text\`);
                
                // Get value from visible input
                if (textInput && textInput.style.display !== 'none') {
                    data[fieldKey] = textInput.value;
                    data[fieldKey + '__isExpression'] = true;
                } else if (selectInput) {
                    // Check if the selected option has omitFromJson flag - if so, exclude from formData entirely
                    const selectedOption = selectInput.options[selectInput.selectedIndex];
                    if (selectedOption && selectedOption.dataset.omitFromJson === 'true') {
                        return; // Skip this field - don't write to JSON
                    }
                    data[fieldKey] = selectInput.value;
                }
            });
            
            return data;
        }

        function saveDataset() {
            const formData = collectFormData();
            
            // Clear previous errors
            document.getElementById('errorContainer').innerHTML = '';
            
            // Send to backend for saving (backend runs full validation via validateDatasetForm)
            vscode.postMessage({
                type: 'save',
                formData: formData,
                datasetType: formData.datasetType,
                fileType: formData.fileType,
                filePath: currentFilePath
            });
        }

        function newDataset() {
            // Clear form
            document.querySelectorAll('input, select, textarea').forEach(el => {
                if (el.type === 'checkbox') {
                    el.checked = false;
                } else {
                    el.value = '';
                }
            });
            document.getElementById('dynamicFieldsContainer').innerHTML = '';
            document.getElementById('fileTypeGroup').classList.add('hidden');
            currentFilePath = null;
        }

        function loadDataset(datasetJson) {
            console.log('[Webview] loadDataset called with:', datasetJson);
            
            // Populate form from JSON
            document.getElementById('name').value = datasetJson.name || '';
            document.getElementById('linkedService').value = datasetJson.properties?.linkedServiceName?.referenceName || '';
            
            console.log('[Webview] Set name to:', datasetJson.name);
            console.log('[Webview] Set linked service to:', datasetJson.properties?.linkedServiceName?.referenceName);
            
            // Determine dataset type and file type from JSON
            const { datasetType, fileType } = detectDatasetTypeFromJson(datasetJson);
            console.log('[Webview] Detected datasetType:', datasetType, 'fileType:', fileType);
            
            if (datasetType) {
                const datasetTypeSelect = document.getElementById('datasetType');
                datasetTypeSelect.value = datasetType;
                
                // Trigger dataset type change to populate file types and fields
                const config = currentConfig.datasetTypes[datasetType];
                
                if (config.requiresFileType && fileType) {
                    // Show and populate file type dropdown
                    const fileTypeGroup = document.getElementById('fileTypeGroup');
                    fileTypeGroup.classList.remove('hidden');
                    
                    const fileTypeSelect = document.getElementById('fileType');
                    fileTypeSelect.innerHTML = '<option value="">Select file type...</option>';
                    
                    for (const [key, fileConfig] of Object.entries(config.fileTypes)) {
                        const option = document.createElement('option');
                        option.value = key;
                        option.textContent = fileConfig.displayName;
                        fileTypeSelect.appendChild(option);
                    }
                    
                    fileTypeSelect.value = fileType;
                    
                    // Render fields for this file type
                    renderDynamicFields(datasetType, fileType);
                    
                    // Make file type read-only
                    fileTypeSelect.disabled = true;
                    fileTypeSelect.style.opacity = '0.6';
                    fileTypeSelect.style.cursor = 'not-allowed';
                } else {
                    // No file type needed, render fields directly
                    renderDynamicFields(datasetType, null);
                }
                
                // Make dataset type read-only
                datasetTypeSelect.disabled = true;
                datasetTypeSelect.style.opacity = '0.6';
                datasetTypeSelect.style.cursor = 'not-allowed';
                
                console.log('[Webview] About to filter linked services for datasetType:', datasetType);
                // Filter linked services after fields are rendered
                filterLinkedServices(datasetType);
                console.log('[Webview] Finished filtering linked services');
            }
            
            // Load field values and parameters (after fields are rendered)
            setTimeout(() => {
                // Load dynamic field values from JSON
                const { datasetType: dt, fileType: ft } = detectDatasetTypeFromJson(datasetJson);
                const config = dt ? currentConfig.datasetTypes[dt] : null;
                let fieldsConfig = null;
                
                if (config) {
                    if (ft && config.fileTypes && config.fileTypes[ft]) {
                        fieldsConfig = config.fileTypes[ft].fields;
                    } else {
                        fieldsConfig = config.fields;
                    }
                    
                    // Load values for each field
                    if (fieldsConfig) {
                        for (const [sectionName, fields] of Object.entries(fieldsConfig)) {
                            for (const [fieldKey, fieldConfig] of Object.entries(fields)) {
                                if (fieldConfig.jsonPath && fieldConfig.type !== 'hidden') {
                                    let value = getValueByPath(datasetJson, fieldConfig.jsonPath);
                                    let element = document.getElementById(fieldKey);
                                    
                                    if (element && value !== undefined && value !== null) {
                                        if (fieldConfig.type === 'boolean') {
                                            element.checked = Boolean(value);
                                        } else if (fieldConfig.type === 'select-text') {
                                            // For select-text, set the select dropdown value
                                            const selectInput = document.getElementById(\`\${fieldKey}-select\`);
                                            if (selectInput) {
                                                // Handle Expression objects: {value: '...', type: 'Expression'}
                                                if (value && typeof value === 'object' && value.type === 'Expression') {
                                                    const textInput = document.getElementById(\`\${fieldKey}-text\`);
                                                    const manualCheckbox = document.getElementById(\`\${fieldKey}-manual\`);
                                                    if (textInput && manualCheckbox) {
                                                        manualCheckbox.checked = true;
                                                        selectInput.style.display = 'none';
                                                        textInput.style.display = 'block';
                                                        textInput.value = value.value;
                                                    }
                                                    continue;
                                                }
                                                // When loading from JSON, prefer non-omitFromJson options first
                                                // e.g. rowDelimiter "" should load as "No delimiter", not "Default"
                                                let found = false;
                                                for (let i = 0; i < selectInput.options.length; i++) {
                                                    if (!selectInput.options[i].disabled &&
                                                        selectInput.options[i].value === String(value) &&
                                                        selectInput.options[i].dataset.omitFromJson !== 'true') {
                                                        selectInput.selectedIndex = i;
                                                        found = true;
                                                        break;
                                                    }
                                                }
                                                // Second pass: accept any non-disabled match (e.g. omitFromJson options)
                                                if (!found) {
                                                    for (let i = 0; i < selectInput.options.length; i++) {
                                                        if (!selectInput.options[i].disabled && selectInput.options[i].value === String(value)) {
                                                            selectInput.selectedIndex = i;
                                                            found = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                if (!found) {
                                                    // Value not in options - switch to manual mode
                                                    const textInput = document.getElementById(\`\${fieldKey}-text\`);
                                                    const manualCheckbox = document.getElementById(\`\${fieldKey}-manual\`);
                                                    if (textInput && manualCheckbox) {
                                                        manualCheckbox.checked = true;
                                                        selectInput.style.display = 'none';
                                                        textInput.style.display = 'block';
                                                        textInput.value = value;
                                                    }
                                                }
                                            }
                                        } else {
                                            if (element.tagName === 'SELECT') {
                                                // Find first non-disabled option matching the value (avoids hitting disabled placeholder)
                                                let found = false;
                                                for (let i = 0; i < element.options.length; i++) {
                                                    if (!element.options[i].disabled && element.options[i].value === String(value)) {
                                                        element.selectedIndex = i;
                                                        found = true;
                                                        break;
                                                    }
                                                }
                                                if (!found) {
                                                    element.value = value; // fallback
                                                }
                                            } else {
                                                element.value = value;
                                            }
                                            
                                            // Trigger visibility check for fields that others depend on
                                            updateFieldVisibility(fieldKey, value);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                // Handle derivedFrom: set radio fields based on which sibling fields have values
                if (fieldsConfig) {
                    for (const [sectionName, fields] of Object.entries(fieldsConfig)) {
                        for (const [fieldKey, fieldConfig] of Object.entries(fields)) {
                            if (fieldConfig.type === 'radio' && fieldConfig.derivedFrom) {
                                const radioContainer = document.getElementById(fieldKey);
                                if (!radioContainer) continue;
                                let matched = false;
                                for (const rule of fieldConfig.derivedFrom) {
                                    const checkEl = document.getElementById(rule.field);
                                    if (checkEl && checkEl.value !== '' && checkEl.value !== undefined) {
                                        const radioInput = radioContainer.querySelector(\`input[value="\${rule.thenValue}"]\`);
                                        if (radioInput) {
                                            radioInput.checked = true;
                                            updateFieldVisibility(fieldKey, rule.thenValue);
                                            matched = true;
                                        }
                                        break;
                                    }
                                }
                                // If no match found, trigger visibility for the current default
                                if (!matched) {
                                    const checkedRadio = radioContainer.querySelector('input[type="radio"]:checked');
                                    if (checkedRadio) {
                                        updateFieldVisibility(fieldKey, checkedRadio.value);
                                    }
                                }
                            }
                        }
                    }
                }

                // Load parameters if present
                if (datasetJson.properties?.parameters) {
                    const parametersContainer = document.getElementById('parameters');
                    if (parametersContainer) {
                        const valueTypes = JSON.parse(parametersContainer.getAttribute('data-value-types') || '["string"]');
                        Object.entries(datasetJson.properties.parameters).forEach(([name, param]) => {
                            addParameterRow('parameters', valueTypes);
                            const rows = document.querySelectorAll('#parameters-table .parameter-row');
                            const lastRow = rows[rows.length - 1];
                            lastRow.querySelector('.param-name').value = name;
                            lastRow.querySelector('.param-type').value = param.type || 'String';
                            lastRow.querySelector('.param-value').value = param.defaultValue || '';
                        });
                    }
                }
            }, 100);
        }
        
        function detectDatasetTypeFromJson(datasetJson) {
            const type = datasetJson.properties?.type;
            const locationType = datasetJson.properties?.typeProperties?.location?.type;
            
            // Determine dataset type based on location type or properties.type
            let datasetType = null;
            let fileType = null;
            
            if (type === 'AzureSqlTable') {
                datasetType = 'AzureSqlTable';
                fileType = null;
            } else if (locationType === 'AzureBlobStorageLocation') {
                datasetType = 'AzureBlobStorage';
                fileType = type; // Type is the file format (Parquet, DelimitedText, etc.)
            } else if (locationType === 'AzureBlobFSLocation') {
                datasetType = 'AzureDataLakeStorageGen2';
                fileType = type; // Type is the file format
            }
            
            return { datasetType, fileType };
        }

        function toggleConfigPanel() {
            const panel = document.getElementById('configPanel');
            const btn = document.getElementById('collapseBtn');
            panel.classList.toggle('minimized');
            btn.textContent = panel.classList.contains('minimized') ? '▲' : '▼';
        }

        function notifyDataChanged() {
            const formData = collectFormData();
            vscode.postMessage({
                type: 'dataChanged',
                data: formData
            });
        }

        function displayValidationErrors(validation) {
            const container = document.getElementById('errorContainer');
            container.innerHTML = '';
            
            if (!validation.valid && validation.errors.length > 0) {
                const errorDiv = document.createElement('div');
                errorDiv.className = 'error-list';
                
                const title = document.createElement('div');
                title.className = 'error-list-title';
                title.textContent = 'Please fix the following errors:';
                errorDiv.appendChild(title);
                
                const ul = document.createElement('ul');
                validation.errors.forEach(error => {
                    const li = document.createElement('li');
                    li.textContent = error;
                    ul.appendChild(li);
                });
                errorDiv.appendChild(ul);
                
                container.appendChild(errorDiv);
            }
        }
    </script>
</body>
</html>`;
	}
}

module.exports = { DatasetEditorProvider };
