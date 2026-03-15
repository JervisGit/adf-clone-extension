const vscode = require('vscode');
const activitiesConfig = require('./activities-config-verified.json');
const activitySchemas = require('./activity-schemas.json');
const datasetSchemas = require('./dataset-schemas.json');
const irConfig = require('./ir-config.json');
const copyActivityConfig = require('./copy-activity-config.json');
const { buildCopySource, buildCopySink } = require('./copyActivityUtils');

class PipelineEditorProvider {
	static panels = new Map(); // Map<filePath, panel>
	static dirtyStates = new Map(); // Map<filePath, isDirty>
	static stateCache = new Map(); // Map<filePath, pipelineData>

	constructor(context) {
		this.context = context;
	}

	markPanelAsDirty(filePath, isDirty) {
		const panel = PipelineEditorProvider.panels.get(filePath);
		if (panel) {
			const path = require('path');
			const basename = path.basename(filePath, '.json');
			panel.title = isDirty ? `● ${basename}` : basename;
			PipelineEditorProvider.dirtyStates.set(filePath, isDirty);
		}
	}

	createOrShow(filePath = null) {
		const column = vscode.window.activeTextEditor
			? vscode.window.activeTextEditor.viewColumn
			: undefined;

		// If opening a specific file, check if panel already exists
		if (filePath && PipelineEditorProvider.panels.has(filePath)) {
			PipelineEditorProvider.panels.get(filePath).reveal(column);
			return PipelineEditorProvider.panels.get(filePath);
		}

		// Create title from filename if provided
		const path = require('path');
		const title = filePath 
			? path.basename(filePath, '.json')
			: 'Synapse Pipeline Editor';

		// Otherwise, create a new panel
		const panel = vscode.window.createWebviewPanel(
			'adfPipelineEditor',
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
			PipelineEditorProvider.panels.set(filePath, panel);
		}

		// Set the webview's initial html content
		panel.webview.html = this.getHtmlContent(panel.webview);

		// Send dataset schemas and list to webview after a short delay to ensure it's loaded
		setImmediate(() => {
			const fs = require('fs');
			const path = require('path');
			let datasetList = [];
			let datasetContents = {};
			let pipelineList = [];
			let linkedServicesList = [];
			
			if (vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0) {
				const workspaceRoot = vscode.workspace.workspaceFolders[0].uri.fsPath;
				console.log('[Extension] Workspace root:', workspaceRoot);
				
				// Build list of paths to check (root + common subfolders)
				const pathsToCheck = [
					workspaceRoot,
					path.join(workspaceRoot, 'adf-clone-extension'),
					path.join(workspaceRoot, 'adf-activity-ui')
				];
				
				// Helper function to load from a specific path
				const loadFromPath = (basePath) => {
					console.log('[Extension] Checking path:', basePath);
					if (!fs.existsSync(basePath)) {
						console.log('[Extension] Path does not exist:', basePath);
						return;
					}
					
					// Load datasets
					const datasetPath = path.join(basePath, 'dataset');
					if (fs.existsSync(datasetPath)) {
						const files = fs.readdirSync(datasetPath).filter(f => f.endsWith('.json'));
						console.log(`[Extension] Found ${files.length} dataset files in ${datasetPath}`);
						files.forEach(file => {
							const name = file.replace('.json', '');
							if (!datasetList.includes(name)) {
								datasetList.push(name);
								try {
									const filePath = path.join(datasetPath, file);
									const content = fs.readFileSync(filePath, 'utf8');
									datasetContents[name] = JSON.parse(content);
								} catch (err) {
									console.error(`[Extension] Error reading dataset ${file}:`, err);
								}
							}
						});
					}
					
					// Load pipelines
					const pipelinePath = path.join(basePath, 'pipeline');
					if (fs.existsSync(pipelinePath)) {
						const files = fs.readdirSync(pipelinePath).filter(f => f.endsWith('.json'));
						console.log(`[Extension] Found ${files.length} pipeline files in ${pipelinePath}`);
						files.forEach(file => {
							const name = file.replace('.json', '');
							if (!pipelineList.includes(name)) {
								pipelineList.push(name);
							}
						});
					}
					
					// Load linked services
					const linkedServicePath = path.join(basePath, 'linkedService');
					if (fs.existsSync(linkedServicePath)) {
						const files = fs.readdirSync(linkedServicePath).filter(f => f.endsWith('.json'));
						console.log(`[Extension] Found ${files.length} linked service files in ${linkedServicePath}`);
						files.forEach(file => {
							try {
								const filePath = path.join(linkedServicePath, file);
								const content = fs.readFileSync(filePath, 'utf8');
								const linkedService = JSON.parse(content);
								const lsName = linkedService.name;
								const lsType = linkedService.properties?.type;
								console.log(`[Extension] Linked service ${file}: name=${lsName}, type=${lsType}`);
								
								// Filter for Script and Stored Procedure activities: only Azure SQL Database and Azure Synapse Analytics
								if (lsType === 'AzureSqlDatabase' || lsType === 'AzureSqlDW') {
									// Avoid duplicates
									if (!linkedServicesList.find(ls => ls.name === lsName)) {
										linkedServicesList.push({
											name: lsName,
											type: lsType
										});
										console.log(`[Extension] Added linked service: ${lsName} (${lsType})`);
									}
								} else {
									console.log(`[Extension] Skipped linked service ${lsName} - type ${lsType} not supported for Script activity`);
								}
							} catch (err) {
								console.error(`[Extension] Error reading linked service ${file}:`, err);
							}
						});
					}
				};
				
				// Load from all paths
				pathsToCheck.forEach(loadFromPath);
				
				console.log('[Extension] Final linked services list:', linkedServicesList);
			}
			
			panel.webview.postMessage({
				type: 'initSchemas',
				datasetSchemas: datasetSchemas,
				datasetList: datasetList,
				datasetContents: datasetContents,
				pipelineList: pipelineList,
				linkedServicesList: linkedServicesList,
				copyActivityConfig: copyActivityConfig
			});
		});

		// Handle messages from the webview
		panel.webview.onDidReceiveMessage(
			async message => {
				switch (message.type) {
					case 'alert':
						vscode.window.showInformationMessage(message.text);
						break;
					case 'error':
						vscode.window.showErrorMessage(message.text);
						break;
					case 'validationError':
						vscode.window.showErrorMessage(message.message);
						break;
					case 'save':
						console.log('[Extension] Received save message:', message);
						// Use filePath from message if available, otherwise use closure filePath
						const saveFilePath = message.filePath || filePath;
						await this.savePipelineToWorkspace(message.data, saveFilePath);
						// Clear dirty state after successful save
						if (saveFilePath) {
							this.markPanelAsDirty(saveFilePath, false);
							// Notify webview that save completed
							panel.webview.postMessage({ type: 'saveCompleted' });
						}
						break;
					case 'contentChanged':
						// Mark panel as dirty when content changes
						if (filePath) {
							this.markPanelAsDirty(filePath, message.isDirty);
						}
						break;
					case 'cacheState':
						// Cache the current pipeline state for potential save on close
						if (filePath && message.data) {
							PipelineEditorProvider.stateCache.set(filePath, message.data);
						}
						break;
					case 'log':
						console.log('Webview:', message.text);
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
				if (filePath && PipelineEditorProvider.dirtyStates.get(filePath)) {
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
						// Get cached state and save it
						const cachedData = PipelineEditorProvider.stateCache.get(filePath);
						if (cachedData) {
							// Validate activities before saving
							const invalidActivities = [];
							if (cachedData.activities) {
								cachedData.activities.forEach(a => {
									if ((a.type === 'SetVariable' || a.type === 'AppendVariable') && !a.typeProperties?.variableName) {
										invalidActivities.push(a.name);
									}
								});
							}
							
							if (invalidActivities.length > 0) {
								vscode.window.showErrorMessage(
									`Cannot save ${basename}: The following activities are missing variable names: ${invalidActivities.join(', ')}. Please set variable names before saving.`
								);
								return;
							}
							
							await this.savePipelineToWorkspace(cachedData, filePath);
							vscode.window.showInformationMessage(`Saved ${basename}`);
						}
					} else if (answer === 'Cancel') {
						// User wants to cancel the close, but we can't prevent dispose
						// Reopen the panel with the cached state
						const cachedData = PipelineEditorProvider.stateCache.get(filePath);
						if (cachedData) {
							setImmediate(() => {
								const newPanel = this.createOrShow(filePath);
								setImmediate(() => {
									newPanel.webview.postMessage({
										type: 'loadPipeline',
										data: cachedData,
										filePath: filePath
									});
									// Restore dirty state
									this.markPanelAsDirty(filePath, true);
								});
							});
						}
						return;
					}
				}
				
				// Remove panel from map
				if (filePath) {
					PipelineEditorProvider.panels.delete(filePath);
					PipelineEditorProvider.dirtyStates.delete(filePath);
					PipelineEditorProvider.stateCache.delete(filePath);
				}
				this.pendingPipelineFile = null;
			},
			null,
			this.context.subscriptions
		);

		// Load pending pipeline file if any
		if (this.pendingPipelineFile) {
			const fileToLoad = this.pendingPipelineFile;
			this.pendingPipelineFile = null;
			this.loadPipelineFile(fileToLoad);
		}
		
		return panel;
	}

	addActivity(activityType) {
		// Find the most recently used panel
		const panels = Array.from(PipelineEditorProvider.panels.values());
		const panel = panels.length > 0 ? panels[panels.length - 1] : null;
		
		if (panel) {
			panel.webview.postMessage({
				type: 'addActivity',
				activityType: activityType
			});
		} else {
			vscode.window.showWarningMessage('Please open the pipeline editor first');
		}
	}

	loadPipelineFile(filePath) {
		// Create or show panel for this file
		const panel = this.createOrShow(filePath);
		
		const fs = require('fs');
		try {
			const content = fs.readFileSync(filePath, 'utf8');
			const pipelineJson = JSON.parse(content);
			
			// Send to webview
			panel.webview.postMessage({
				type: 'loadPipeline',
				data: pipelineJson,
				filePath: filePath
			});
		} catch (error) {
			vscode.window.showErrorMessage(`Failed to load pipeline: ${error.message}`);
		}
	}

	async savePipelineToWorkspace(pipelineData, filePath) {
		const fs = require('fs');
		const path = require('path');
		
		console.log('[Extension] savePipelineToWorkspace called');
		console.log('[Extension] Pipeline data:', JSON.stringify(pipelineData, null, 2));
		console.log('[Extension] File path:', filePath);
		
		try {
			// Get workspace folder
			if (!vscode.workspace.workspaceFolders) {
				vscode.window.showErrorMessage('No workspace folder open');
				return;
			}
			
			const workspaceRoot = vscode.workspace.workspaceFolders[0].uri.fsPath;
			const pipelineDir = path.join(workspaceRoot, 'pipeline');
			
			// Create pipeline directory if it doesn't exist
			if (!fs.existsSync(pipelineDir)) {
				fs.mkdirSync(pipelineDir, { recursive: true });
			}
			
			console.log('[Extension] Converting to Synapse format...');
			console.log('[Extension] Number of activities:', pipelineData.activities?.length || 0);
			
			// Convert to Synapse format
			const synapseJson = {
				name: pipelineData.name || "pipeline1",
				properties: {
					activities: (pipelineData.activities || []).map(a => {
						console.log('[Extension] Processing activity:', a.name, 'Type:', a.type);
						
						// For Copy: save webview's pre-built source/sink BEFORE flattening overwrites them.
						// The webview sends snk_/src_ field changes already built into typeProperties.source/sink.
						const _cpPrebuilt = (a.type === 'Copy' && a.typeProperties)
							? JSON.parse(JSON.stringify(a.typeProperties)) : null;

						// Flatten typeProperties from webview onto activity object
						if (a.typeProperties && typeof a.typeProperties === 'object') {
							for (const key in a.typeProperties) {
								if (a.typeProperties.hasOwnProperty(key)) {
									a[key] = a.typeProperties[key];
								}
							}
						}
						
						const activity = {
							name: a.name,
							type: a.type
						};
						
						if (a.description) activity.description = a.description;
						if (a.state) activity.state = a.state;
						if (a.onInactiveMarkAs) activity.onInactiveMarkAs = a.onInactiveMarkAs;
						if (a.dependsOn) activity.dependsOn = a.dependsOn;
						if (a.userProperties) activity.userProperties = a.userProperties;
						
                        // Build policy object - check if policy object exists from webview, or build from individual fields
                        if (a.policy && typeof a.policy === 'object') {
                            // Policy object already exists from webview
                            activity.policy = a.policy;
                        } else if (a.timeout || a.retry !== undefined || a.retryIntervalInSeconds !== undefined || 
                            a.secureOutput !== undefined || a.secureInput !== undefined) {
                            // Build policy from individual fields
                            activity.policy = {};
                            if (a.timeout) activity.policy.timeout = a.timeout;
                            if (a.retry !== undefined) activity.policy.retry = a.retry;
                            if (a.retryIntervalInSeconds !== undefined) activity.policy.retryIntervalInSeconds = a.retryIntervalInSeconds;
                            if (a.secureOutput !== undefined) activity.policy.secureOutput = a.secureOutput;
                            if (a.secureInput !== undefined) activity.policy.secureInput = a.secureInput;
                        }
                        
                        // For Script activity, linkedServiceName should be at activity level
                        if (a.type === 'Script' && a.linkedServiceName) {
                            activity.linkedServiceName = a.linkedServiceName;
                        }
                        
                        // For SqlServerStoredProcedure activity, linkedServiceName should be at activity level
                        if (a.type === 'SqlServerStoredProcedure' && a.linkedServiceName) {
                            activity.linkedServiceName = a.linkedServiceName;
                            // Add Synapse parameters if present
                            if (a._selectedLinkedServiceType === 'AzureSynapse' && a.linkedServiceProperties && a.linkedServiceProperties.DBName) {
                                if (!activity.linkedServiceName.parameters) {
                                    activity.linkedServiceName.parameters = {};
                                }
                                activity.linkedServiceName.parameters.DBName = a.linkedServiceProperties.DBName;
                            }
                        }
                        
                        // Collect typeProperties — start from the webview's already-correctly-formatted
                        // typeProperties (built by buildPipelineDataForSave) so all activity types
                        // (AppendVariable, SetVariable, Until, ForEach, IfCondition, etc.) get their
                        // fields preserved.  Special-case handlers below override as needed.
                        const typeProperties = a.typeProperties ? Object.assign({}, a.typeProperties) : {};
                        const commonProps = ['id', 'type', 'x', 'y', 'width', 'height', 'name', 'description', 'color', 'container', 'element', 
                                             'timeout', 'retry', 'retryIntervalInSeconds', 'secureOutput', 'secureInput', 'userProperties', 'state', 'onInactiveMarkAs',
                                             'dynamicAllocation', 'minExecutors', 'maxExecutors', 'numExecutors', 'dependsOn', 'policy',
                                             'sourceDataset', 'sinkDataset', 'recursive', 'modifiedDatetimeStart', 'modifiedDatetimeEnd',
                                             'wildcardFolderPath', 'wildcardFileName', 'enablePartitionDiscovery',
                                             'writeBatchSize', 'writeBatchTimeout', 'preCopyScript', 'maxConcurrentConnections', 'writeBehavior', 
                                             'sqlWriterUseTableLock', 'disableMetricsCollection', '_sourceObject', '_sinkObject',
                                             '_sourceDatasetType', '_sinkDatasetType', '_sourceLocationType', '_sinkLocationType',
                                             '_datasetLocationType', 'inputs', 'outputs', 'source', 'sink', 'typeProperties',
                                             'linkedServiceName', '_selectedLinkedServiceType', 'linkedServiceProperties',
                                             'null'];
				
					for (const key in a) {
						// Skip src_/snk_ prefixed fields — these are Copy activity form fields handled separately
						if (!commonProps.includes(key) && a.hasOwnProperty(key) && typeof a[key] !== 'function'
							&& !key.startsWith('src_') && !key.startsWith('snk_')) {
						// For SynapseNotebook, convert dynamicAllocation fields back to conf object
						if (a.type === 'SynapseNotebook') {
							// Always add snapshot: true
							typeProperties.snapshot = true;
							
						// If conf already exists (from webview), preserve it
						// Otherwise, rebuild it from individual fields
						if (typeProperties.conf && typeProperties.conf['spark.dynamicAllocation.enabled'] !== undefined) {
							// Conf object already exists with enabled property - keep it as is
							// Just ensure numExecutors is set if provided
							if (a.numExecutors !== undefined && a.numExecutors !== '') {
								typeProperties.numExecutors = parseInt(a.numExecutors);
							}
						} else if (a.dynamicAllocation !== undefined || a.minExecutors !== undefined || a.maxExecutors !== undefined || a.numExecutors !== undefined) {
							// Need to build/rebuild conf object from individual fields
							if (!typeProperties.conf) {
								typeProperties.conf = {};
							}
							
							// Convert 'Enabled'/'Disabled' to boolean
							const isDynamicEnabled = a.dynamicAllocation === 'Enabled';
							
							if (a.dynamicAllocation !== undefined) {
								typeProperties.conf['spark.dynamicAllocation.enabled'] = isDynamicEnabled;
							}
							
							// Only add min/max executors if dynamicAllocation is Enabled and values are provided
							if (isDynamicEnabled) {
								if (a.minExecutors !== undefined && a.minExecutors !== '') {
									typeProperties.conf['spark.dynamicAllocation.minExecutors'] = parseInt(a.minExecutors);
								}
								if (a.maxExecutors !== undefined && a.maxExecutors !== '') {
									typeProperties.conf['spark.dynamicAllocation.maxExecutors'] = parseInt(a.maxExecutors);
								}
							} else {
								// When disabled, set min/max to numExecutors value
								if (a.numExecutors !== undefined && a.numExecutors !== '') {
									const numExec = parseInt(a.numExecutors);
									typeProperties.conf['spark.dynamicAllocation.minExecutors'] = numExec;
									typeProperties.conf['spark.dynamicAllocation.maxExecutors'] = numExec;
									typeProperties.numExecutors = numExec;
								}
							}
						}
					}
					
					// Handle SetVariable specific fields
						if (a.type === 'SetVariable') {
							// Remove UI-specific fields
							delete typeProperties.variableType;
							delete typeProperties.pipelineVariableType;
							delete typeProperties.returnValues;
							
							// If it's a pipeline return value
							if (a.variableType === 'Pipeline return value' && a.returnValues) {
								typeProperties.variableName = 'pipelineReturnValue';
								typeProperties.setSystemVariable = true;
								
								// Convert returnValues to Azure format
								const valueArray = [];
								for (const key in a.returnValues) {
									if (a.returnValues.hasOwnProperty(key)) {
										const item = a.returnValues[key];
										const valueObj = {
											key: key,
											value: { type: item.type }
										};
										
										// Handle different types
										if (item.type === 'Null') {
											// Null has no content field
										} else if (item.type === 'Array') {
											// Array needs nested content structure
											valueObj.value.content = item.content || [];
										} else if (item.type === 'Int' || item.type === 'Float') {
											// Numbers without quotes
											valueObj.value.content = parseFloat(item.value) || 0;
										} else if (item.type === 'Boolean') {
											// Boolean value
											valueObj.value.content = item.value === 'true' || item.value === true;
										} else {
											// String, Expression, Object
											valueObj.value.content = item.value || '';
										}
										
										valueArray.push(valueObj);
									}
								}
								
								typeProperties.value = valueArray;
							}
							// Else: Pipeline variable - variableName and value are already in typeProperties
						}
						
// For Copy activity: config-driven source/sink reconstruction + inputs/outputs
					if (a.type === 'Copy') {
						console.log('[Extension] Copy activity - config-driven source/sink build');
						const srcTypeConfig = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[a._sourceDatasetType];
						const snkTypeConfig = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[a._sinkDatasetType];

						// Build source
						if (srcTypeConfig) {
							// Config-driven: reads src_ prefixed fields from activity object
							const source = buildCopySource(a, srcTypeConfig, a._sourceLocationType, a._sourceObject);
							if (source) typeProperties.source = source;
							console.log('[Extension] Built source from config:', typeProperties.source?.type);
						} else if (a._sourceObject) {
							// Fallback for dataset types not yet in config
							typeProperties.source = JSON.parse(JSON.stringify(a._sourceObject));
							console.log('[Extension] Fallback: using _sourceObject for unknown type', a._sourceDatasetType);
						} else if (a.typeProperties && a.typeProperties.source) {
							// Pre-built by webview
							typeProperties.source = a.typeProperties.source;
						} else if (a._sourceDatasetType) {
							// Minimal fallback for brand-new activities
							typeProperties.source = { type: a._sourceDatasetType + 'Source' };
						}

						// Build sink
						if (snkTypeConfig) {
							// Config-driven: reads snk_ prefixed fields from activity object
							const sink = buildCopySink(a, snkTypeConfig, a._sinkLocationType, a._sinkObject);
							if (sink) typeProperties.sink = sink;
							console.log('[Extension] Built sink from config:', typeProperties.sink?.type);
						} else if (a._sinkObject) {
							// Fallback for dataset types not yet in config
							typeProperties.sink = JSON.parse(JSON.stringify(a._sinkObject));
							console.log('[Extension] Fallback: using _sinkObject for unknown type', a._sinkDatasetType);
						} else if (a.typeProperties && a.typeProperties.sink) {
							// Pre-built by webview
							typeProperties.sink = a.typeProperties.sink;
						} else if (a._sinkDatasetType) {
							// Minimal fallback
							typeProperties.sink = { type: a._sinkDatasetType + 'Sink' };
						}

						// Build inputs/outputs
						if (a.sourceDataset || (a.inputs && a.inputs.length > 0)) {
							const sourceRef = a.sourceDataset || (a.inputs[0].referenceName || a.inputs[0]);
							activity.inputs = [{ referenceName: sourceRef, type: 'DatasetReference' }];
							console.log('[Extension] Added inputs:', activity.inputs);
						}
						if (a.sinkDataset || (a.outputs && a.outputs.length > 0)) {
							const sinkRef = a.sinkDataset || (a.outputs[0].referenceName || a.outputs[0]);
							activity.outputs = [{ referenceName: sinkRef, type: 'DatasetReference' }];
							console.log('[Extension] Added outputs:', activity.outputs);
						}
					}
						
						// Handle Validation specific fields
						if (a.type === 'Validation') {
							console.log('[Extension] Validation activity - processing fields');
							
							// Dataset is already stored as reference object, so it's in typeProperties
							// Just ensure it's properly formatted
							if (typeProperties.dataset && typeof typeProperties.dataset === 'object') {
								console.log('[Extension] Validation dataset:', typeProperties.dataset);
							}
							
							// Ensure timeout has default value if not set
							if (!typeProperties.timeout || typeProperties.timeout === '') {
								typeProperties.timeout = '0.12:00:00';
							}
							
							// Ensure sleep has default value if not set
							if (typeProperties.sleep === undefined || typeProperties.sleep === null || typeProperties.sleep === '') {
								typeProperties.sleep = 10;
							}
							
							// Handle childItems: only include if dataset is Blob/ADLS AND not "ignore"
							const isStorageDataset = a._datasetLocationType === 'AzureBlobStorageLocation' || a._datasetLocationType === 'AzureBlobFSLocation';
							if (isStorageDataset && typeProperties.childItems !== undefined && typeProperties.childItems !== 'ignore') {
								// Convert string "true"/"false" to boolean
								typeProperties.childItems = typeProperties.childItems === 'true' || typeProperties.childItems === true;
								console.log('[Extension] Validation childItems:', typeProperties.childItems);
							} else {
								// Remove childItems if not storage dataset, "ignore", or undefined
								delete typeProperties.childItems;
								console.log('[Extension] Validation childItems removed (not applicable or ignore)');
							}
							
							// Remove internal tracking fields
							delete typeProperties._datasetLocationType;
						}
					}
				}

					// For Copy activity: config-driven source/sink + inputs/outputs
					// Must be OUTSIDE the for-loop because all Copy-specific keys are in commonProps
					if (a.type === 'Copy') {
						console.log('[Extension] Copy activity - config-driven build');
						const srcTypeConfig = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[a._sourceDatasetType];
						const snkTypeConfig = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[a._sinkDatasetType];
						const prebuiltSource = _cpPrebuilt && _cpPrebuilt.source;
						const prebuiltSink   = _cpPrebuilt && _cpPrebuilt.sink;

						// SOURCE: use webview's pre-built object as base (it already has user field values),
						// then patch in any missing ADF-required storeSettings defaults.
						if (prebuiltSource) {
							const src = JSON.parse(JSON.stringify(prebuiltSource));
							if (srcTypeConfig) src.type = srcTypeConfig.sourceTypeName;
							if (srcTypeConfig && srcTypeConfig.hasStoreSettings) {
								if (!src.storeSettings) {
									const rt = (a._sourceLocationType && srcTypeConfig.storeReadSettingsTypes && srcTypeConfig.storeReadSettingsTypes[a._sourceLocationType])
										|| srcTypeConfig.defaultStoreReadSettings || 'AzureBlobFSReadSettings';
									src.storeSettings = { type: rt };
								}
								// No unconditional defaults patched here — the webview save loop
								// already writes recursive/enablePartitionDiscovery conditionally
								// based on src_filePathType. Patching here would re-add recursive
								// even when List of files is selected.
							}
							typeProperties.source = src;
						} else if (srcTypeConfig) {
							const source = buildCopySource(a, srcTypeConfig, a._sourceLocationType, a._sourceObject);
							if (source) typeProperties.source = source;
						} else if (a._sourceObject) {
							typeProperties.source = JSON.parse(JSON.stringify(a._sourceObject));
						} else if (a._sourceDatasetType) {
							typeProperties.source = { type: a._sourceDatasetType + 'Source' };
						}
						console.log('[Extension] Built source:', typeProperties.source?.type);

						// SINK: use webview's pre-built object as base, then patch missing per-type defaults.
						if (prebuiltSink) {
							const snk = JSON.parse(JSON.stringify(prebuiltSink));
							if (snkTypeConfig) snk.type = snkTypeConfig.sinkTypeName;
							// Apply sinkDefaults only for fields the user hasn't explicitly set
							if (snkTypeConfig && snkTypeConfig.sinkDefaults) {
								for (const [dk, dv] of Object.entries(snkTypeConfig.sinkDefaults)) {
									if (!(dk in snk)) snk[dk] = dv;
								}
							}
							if (snkTypeConfig && snkTypeConfig.hasStoreSettings && !snk.storeSettings) {
								const wt = (a._sinkLocationType && snkTypeConfig.storeWriteSettingsTypes && snkTypeConfig.storeWriteSettingsTypes[a._sinkLocationType])
									|| snkTypeConfig.defaultStoreWriteSettings || 'AzureBlobFSWriteSettings';
								snk.storeSettings = { type: wt };
							}
							typeProperties.sink = snk;
						} else if (snkTypeConfig) {
							const sink = buildCopySink(a, snkTypeConfig, a._sinkLocationType, a._sinkObject);
							if (sink) typeProperties.sink = sink;
						} else if (a._sinkObject) {
							typeProperties.sink = JSON.parse(JSON.stringify(a._sinkObject));
						} else if (a._sinkDatasetType) {
							typeProperties.sink = { type: a._sinkDatasetType + 'Sink' };
						}
						console.log('[Extension] Built sink:', typeProperties.sink?.type);

						if (a.sourceDataset) {
							activity.inputs = [{ referenceName: a.sourceDataset, type: 'DatasetReference' }];
						}
						if (a.sinkDataset) {
							activity.outputs = [{ referenceName: a.sinkDataset, type: 'DatasetReference' }];
						}

						// Apply copy-level defaults (policy, enableStaging, translator)
						const _copyDefs = copyActivityConfig.copyDefaults || {};
						if (_copyDefs.policy && !activity.policy) {
							activity.policy = JSON.parse(JSON.stringify(_copyDefs.policy));
						}
						if (_copyDefs.typeProperties) {
							Object.assign(typeProperties, _copyDefs.typeProperties);
						}
						// Suppress translator for non-tabular sink formats
						if (snkTypeConfig && snkTypeConfig.noTranslator) {
							delete typeProperties.translator;
						}
					}

					activity.typeProperties = typeProperties;
					return activity;
					}),
                    ...(pipelineData.variables && Object.keys(pipelineData.variables).length > 0 ? { variables: pipelineData.variables } : {}),
                    ...(pipelineData.parameters && Object.keys(pipelineData.parameters).length > 0 ? { parameters: pipelineData.parameters } : {}),
                    ...(pipelineData.concurrency && pipelineData.concurrency !== 1 ? { concurrency: parseInt(pipelineData.concurrency) } : {}),
					annotations: [],
					lastPublishTime: new Date().toISOString()
				}
			};
			
			// Determine file path
			if (!filePath) {
				// Create new file with unique name
				let fileName = `${synapseJson.name}.json`;
				filePath = path.join(pipelineDir, fileName);
				
				// Check if file exists, add number if needed
				let counter = 1;
				while (fs.existsSync(filePath)) {
					fileName = `${synapseJson.name}_${counter}.json`;
					filePath = path.join(pipelineDir, fileName);
					counter++;
				}
			}
			
			// Write file
			console.log('[Extension] Writing to file:', filePath);
			console.log('[Extension] Final Synapse format:', JSON.stringify(synapseJson, null, 2));
			fs.writeFileSync(filePath, JSON.stringify(synapseJson, null, 2));
			console.log('[Extension] File written successfully');
			vscode.window.showInformationMessage(`Pipeline saved: ${path.basename(filePath)}`);
			
		} catch (error) {
			console.error('[Extension] Save error:', error);
			vscode.window.showErrorMessage(`Failed to save pipeline: ${error.message}`);
		}
	}

	savePipeline(data) {
		vscode.window.showInformationMessage('Pipeline saved!');
		console.log('Pipeline data:', data);
	}

	getHtmlContent() {
		return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline';">
    <title>Pipeline Editor</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            overflow: hidden;
            background: var(--vscode-editor-background);
            color: var(--vscode-editor-foreground);
        }

        .container {
            display: flex;
            flex-direction: column;
            height: 100vh;
        }

        .main-content {
            display: flex;
            flex: 1;
            overflow: hidden;
            min-height: 0;
        }

        /* Sidebar */
        .sidebar {
            width: 250px;
            min-width: 250px;
            max-width: 250px;
            background: var(--vscode-sideBar-background);
            border-right: 1px solid var(--vscode-panel-border);
            display: flex !important;
            flex-direction: column;
            overflow-y: auto !important;
            overflow-x: hidden;
            flex-shrink: 0;
        }

        .sidebar-header {
            padding: 16px;
            border-bottom: 1px solid var(--vscode-panel-border);
            font-size: 14px;
            font-weight: 600;
        }

        .activity-group {
            border-bottom: 1px solid var(--vscode-panel-border);
        }

        .activity-group-title {
            display: flex;
            align-items: center;
            padding: 10px 12px;
            cursor: pointer;
            font-size: 13px;
            color: var(--vscode-foreground);
            user-select: none;
            font-weight: 400;
        }

        .activity-group-title:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .category-arrow {
            margin-right: 8px;
            font-size: 12px;
            font-weight: bold;
            transition: transform 0.2s ease;
            display: inline-block;
        }

        .activity-group.collapsed .category-arrow {
            transform: rotate(-90deg);
        }

        .activity-group:not(.collapsed) .category-arrow {
            transform: rotate(0deg);
        }

        .activity-group-content {
            background: var(--vscode-sideBar-background);
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-in-out;
            display: block;
        }

        .activity-group:not(.collapsed) .activity-group-content {
            max-height: 300px;
            overflow-y: auto;
            overflow-x: hidden;
            display: block;
        }

        .activity-item {
            padding: 8px 12px 8px 36px;
            cursor: move;
            font-size: 13px;
            color: var(--vscode-foreground);
            transition: background 0.15s;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .activity-item:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .activity-icon {
            width: 20px;
            height: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 16px;
        }

        /* Canvas Area */
        .canvas-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            position: relative;
            overflow: hidden;
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

        .toolbar-spacer {
            flex: 1;
        }

        .expand-properties-btn {
            padding: 6px 12px;
            border: 1px solid var(--vscode-button-border);
            background: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
        }

        .expand-properties-btn:hover {
            background: var(--vscode-button-secondaryHoverBackground);
        }

        body:not(.properties-visible) .expand-properties-btn {
            display: block;
        }

        body.properties-visible .expand-properties-btn {
            display: none;
        }

        .toolbar-button {
            padding: 6px 12px;
            border: 1px solid var(--vscode-button-border);
            background: var(--vscode-button-secondaryBackground);
            color: var(--vscode-button-secondaryForeground);
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            transition: all 0.2s;
        }

        .toolbar-button:hover {
            background: var(--vscode-button-secondaryHoverBackground);
        }

        .canvas-wrapper {
            flex: 1;
            position: relative;
            overflow: auto;
            background: var(--vscode-editor-background);
            border: 1px solid var(--vscode-panel-border);
        }

        #canvas {
            position: absolute;
            top: 0;
            left: 0;
            pointer-events: none;
            z-index: 1;
        }

        /* Activity Box - DOM-based */
        .activity-box {
            position: absolute;
            width: 180px;
            min-height: 56px;
            background: #f0f0f0;
            border: 1px solid #c8c8c8;
            border-radius: 3px;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.08);
            cursor: pointer;
            z-index: 10;
            user-select: none;
            will-change: transform;
        }

        .activity-box.dragging {
            cursor: move;
            opacity: 0.8;
            z-index: 100;
        }

        .activity-box:hover {
            background: #e8e8e8;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.12);
        }

        .activity-box.selected {
            background: #ffffff;
            border: 1px solid #0078d4;
            box-shadow: 0 4px 12px rgba(0, 120, 212, 0.2);
            min-height: 88px;
        }

        .activity-header {
            padding: 4px 8px;
            background: rgba(0, 0, 0, 0.05);
            border-bottom: 1px solid rgba(0, 0, 0, 0.08);
            border-radius: 3px 3px 0 0;
        }

        .activity-box.selected .activity-header {
            background: #0078d4;
            border-bottom: none;
        }

        .activity-type-label {
            font-size: 11px;
            color: #605e5c;
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        .activity-box.selected .activity-type-label {
            color: #ffffff;
        }

        .activity-body {
            display: flex;
            align-items: center;
            padding: 8px;
            gap: 8px;
        }

        .activity-icon-large {
            display: flex;
            align-items: center;
            justify-content: center;
            width: 28px;
            height: 28px;
            flex-shrink: 0;
            font-size: 20px;
            color: var(--activity-color, #0078d4);
        }

        .activity-label {
            font-size: 13px;
            font-weight: 400;
            color: #323130;
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        /* Connection Points */
        .connection-point {
            position: absolute;
            width: 8px;
            height: 8px;
            background: #c8c8c8;
            border: 2px solid #ffffff;
            border-radius: 50%;
            opacity: 0;
            transition: opacity 0.2s ease;
            z-index: 15;
            cursor: crosshair;
        }

        .activity-box:hover .connection-point,
        .activity-box.selected .connection-point {
            opacity: 1;
        }

        .connection-point:hover {
            background: var(--activity-color, #0078d4);
            transform: scale(1.3);
        }

        .connection-point.top {
            top: -5px;
            left: 50%;
            transform: translateX(-50%);
        }

        .connection-point.right {
            right: -5px;
            top: 50%;
            transform: translateY(-50%);
        }

        .connection-point.bottom {
            bottom: -5px;
            left: 50%;
            transform: translateX(-50%);
        }

        .connection-point.left {
            left: -5px;
            top: 50%;
            transform: translateY(-50%);
        }

        .connection-point.top:hover {
            transform: translateX(-50%) scale(1.3);
        }

        .connection-point.right:hover {
            transform: translateY(-50%) scale(1.3);
        }

        .connection-point.bottom:hover {
            transform: translateX(-50%) scale(1.3);
        }

        .connection-point.left:hover {
            transform: translateY(-50%) scale(1.3);
        }

        /* Activity Actions */
        .activity-actions {
            display: none;
            align-items: center;
            padding: 4px 8px;
            gap: 4px;
            border-top: 1px solid #edebe9;
        }

        .activity-box.selected .activity-actions {
            display: flex;
        }

        .action-icon-btn {
            width: 24px;
            height: 24px;
            border: none;
            background: transparent;
            border-radius: 2px;
            cursor: pointer;
            font-size: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.1s ease;
            color: #605e5c;
        }

        .action-icon-btn:hover {
            background: rgba(0, 0, 0, 0.05);
        }

        .action-icon-btn.info {
            margin-left: auto;
            color: #0078d4;
            font-weight: bold;
        }

        /* Properties Panel (Right Sidebar) */
        .properties-panel {
            width: 300px;
            min-width: 300px;
            max-width: 300px;
            background: var(--vscode-sideBar-background);
            border-left: 1px solid var(--vscode-panel-border);
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            overflow-x: hidden;
            flex-shrink: 0;
            transition: width 0.3s ease, min-width 0.3s ease, max-width 0.3s ease;
        }

        .properties-panel.collapsed {
            width: 0;
            min-width: 0;
            max-width: 0;
            border-left: none;
            overflow: hidden;
            padding: 0;
        }

        .properties-collapse-btn {
            background: transparent;
            border: none;
            cursor: pointer;
            font-size: 18px;
            color: var(--vscode-foreground);
            padding: 4px 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.2s ease;
            border-radius: 4px;
        }

        .properties-collapse-btn:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .properties-header {
            padding: 16px;
            border-bottom: 1px solid var(--vscode-panel-border);
            font-size: 14px;
            font-weight: 600;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .properties-content {
            padding: 16px;
        }

        /* Configuration Panel (Bottom) */
        .config-panel {
            height: 250px !important;
            min-height: 250px !important;
            max-height: 250px !important;
            background: var(--vscode-panel-background) !important;
            border-top: 1px solid var(--vscode-panel-border) !important;
            display: flex !important;
            flex-direction: column !important;
            overflow: visible !important;
            flex-shrink: 0 !important;
            z-index: 1000 !important;
            transition: height 0.2s ease;
        }

        .config-panel.minimized {
            height: 40px !important;
            min-height: 40px !important;
            max-height: 40px !important;
        }

        .config-panel.minimized .config-content {
            display: none;
        }

        .config-collapse-btn {
            background: transparent;
            border: none;
            cursor: pointer;
            font-size: 14px;
            color: var(--vscode-foreground);
            padding: 4px 8px;
            margin-left: auto;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: background 0.2s ease;
            border-radius: 4px;
        }

        .config-collapse-btn:hover {
            background: var(--vscode-list-hoverBackground);
        }

        .config-tabs {
            display: flex !important;
            background: var(--vscode-sideBar-background) !important;
            border-bottom: 1px solid var(--vscode-panel-border);
            padding: 0 16px;
            gap: 4px;
            height: 40px !important;
            align-items: center;
        }

        .config-tab {
            padding: 8px 16px;
            border: none;
            background: transparent;
            color: var(--vscode-tab-inactiveForeground);
            cursor: pointer;
            font-size: 13px;
            border-bottom: 2px solid transparent;
            transition: all 0.2s;
        }

        .config-tab:hover {
            color: var(--vscode-tab-activeForeground);
        }

        .config-tab.active {
            color: var(--vscode-tab-activeForeground);
            border-bottom-color: var(--vscode-focusBorder);
        }

        .config-content {
            flex: 1 !important;
            overflow-y: auto !important;
            padding: 16px;
            background: var(--vscode-editor-background) !important;
            display: block !important;
        }

        .config-tab-pane {
            display: none;
        }

        .config-tab-pane.active {
            display: block;
        }

        .property-group {
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .property-label {
            font-size: 12px;
            font-weight: 600;
            color: var(--vscode-descriptionForeground);
            min-width: 150px;
            flex-shrink: 0;
        }

        .property-input {
            flex: 1;
            padding: 6px 8px;
            background: var(--vscode-input-background);
            color: var(--vscode-input-foreground);
            border: 1px solid var(--vscode-input-border);
            border-radius: 3px;
            font-size: 13px;
        }

        .property-input:focus {
            outline: none;
            border-color: var(--vscode-focusBorder);
        }

        .info-icon {
            position: relative;
            cursor: help;
            user-select: none;
        }
        
        .info-icon:hover::after {
            content: attr(title);
            position: absolute;
            left: 20px;
            top: 50%;
            transform: translateY(-50%);
            background: var(--vscode-editorHoverWidget-background);
            border: 1px solid var(--vscode-editorHoverWidget-border);
            color: var(--vscode-editorHoverWidget-foreground);
            padding: 6px 10px;
            border-radius: 4px;
            white-space: nowrap;
            z-index: 1000;
            font-size: 12px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
            pointer-events: none;
        }

        .empty-state {
            padding: 24px;
            text-align: center;
            color: var(--vscode-descriptionForeground);
            font-size: 13px;
        }

        /* Container Activities */
        .container-activity {
            min-height: 100px;
            width: 240px;
            background: #faf9f8;
            border: 2px solid var(--activity-color, #0078d4);
            border-style: solid;
        }

        .container-activity:hover {
            background: #f3f2f1;
        }

        .container-activity.selected {
            background: #ffffff;
            border-width: 2px;
        }

        .container-info {
            padding: 8px 12px;
            font-size: 11px;
            color: #605e5c;
            border-top: 1px solid rgba(0, 0, 0, 0.08);
        }

        .container-stat {
            padding: 2px 0;
        }

        /* Activities Tab - Branch Editor */
        .branch-editor {
            border: 1px solid var(--vscode-panel-border);
            border-radius: 4px;
            margin-bottom: 16px;
            overflow: hidden;
        }

        .branch-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 10px 12px;
            background: var(--vscode-sideBar-background);
            border-bottom: 1px solid var(--vscode-panel-border);
            font-weight: 600;
            font-size: 12px;
        }

        .branch-content {
            padding: 12px;
            background: var(--vscode-editor-background);
            min-height: 80px;
        }

        .activity-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            background: var(--vscode-button-secondaryBackground);
            border: 1px solid var(--vscode-button-border);
            border-radius: 12px;
            font-size: 12px;
            margin: 4px 4px 4px 0;
        }

        .activity-pill-icon {
            font-size: 14px;
        }

        .edit-activities-btn {
            padding: 6px 12px;
            background: var(--vscode-button-background);
            color: var(--vscode-button-foreground);
            border: none;
            border-radius: 3px;
            cursor: pointer;
            font-size: 12px;
            font-weight: 500;
        }

        .edit-activities-btn:hover {
            background: var(--vscode-button-hoverBackground);
        }

        .empty-branch {
            color: var(--vscode-descriptionForeground);
            font-style: italic;
            font-size: 12px;
        }

        /* Context Menu */
        .context-menu {
            position: absolute;
            background: var(--vscode-menu-background);
            border: 1px solid var(--vscode-menu-border);
            border-radius: 4px;
            padding: 4px 0;
            min-width: 150px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
            z-index: 1000;
            display: none;
        }

        .context-menu-item {
            padding: 6px 12px;
            cursor: pointer;
            font-size: 13px;
        }

        .context-menu-item:hover {
            background: var(--vscode-menu-selectionBackground);
            color: var(--vscode-menu-selectionForeground);
        }

        .context-menu-separator {
            height: 1px;
            background: var(--vscode-menu-separatorBackground);
            margin: 4px 0;
        }
    </style>
</head>
<body>
    <div class="container">
    <div class="main-content">
        <!-- Sidebar with Activities -->
        <div class="sidebar">
            <div class="sidebar-header">Activities</div>
            ${activitiesConfig.categories.map(category => `
            <div class="activity-group collapsed">
                <div class="activity-group-title" onclick="toggleCategory(this)">
                    <span class="category-arrow">▼</span> ${category.name}
                </div>
                <div class="activity-group-content">
                    ${category.activities.map(activity => `
                    <div class="activity-item" draggable="true" data-type="${activity.type}">
                        <div class="activity-icon">${activity.icon}</div>
                        <span>${activity.name}</span>
                    </div>`).join('')}
                </div>
            </div>`).join('')}
        </div>

        <!-- Canvas Area -->
        <div class="canvas-container">
            <div class="toolbar">
                <button class="toolbar-button" id="backToMainBtn" onclick="backToMainPipeline()" style="display: none; margin-right: 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground);">← Back to Pipeline</button>
                <div id="breadcrumb" style="flex: 1; padding: 0 12px; font-size: 13px;">
                    <span style="font-weight: 600;">Pipeline</span>
                </div>
                <button class="toolbar-button" id="saveBtn">Save</button>
                <button class="toolbar-button" id="clearBtn">Clear</button>
                <button class="toolbar-button" id="zoomInBtn">Zoom In</button>
                <button class="toolbar-button" id="zoomOutBtn">Zoom Out</button>
                <button class="toolbar-button" id="fitBtn">Fit to Screen</button>
                <div class="toolbar-spacer"></div>
                <button class="expand-properties-btn" id="expandPropertiesBtn" onclick="toggleProperties()">Properties</button>
            </div>
            <div class="canvas-wrapper" id="canvasWrapper">
                <canvas id="canvas"></canvas>
            </div>
        </div>

        <!-- Properties Panel (Right Sidebar) -->
        <div class="properties-panel">
            <div class="properties-header">
                <span>Pipeline Properties</span>
                <button class="properties-collapse-btn" onclick="toggleProperties()" title="Collapse Properties Panel">»</button>
            </div>
            <div id="propertiesContent" class="properties-content">
                <div class="empty-state">Pipeline properties and settings</div>
            </div>
        </div>
    </div>
    </div>

    <!-- Configuration Panel (Bottom) -->
    <div class="config-panel" style="position: fixed; bottom: 0; left: 0; right: 0; height: 250px; background: var(--vscode-panel-background); border-top: 1px solid var(--vscode-panel-border); display: flex; flex-direction: column; z-index: 100;">
        <div class="config-tabs" id="configTabs">
            <!-- Pipeline-level tabs (shown when no activity selected) -->
            <button class="config-tab pipeline-tab active" data-tab="parameters" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-activeForeground); border-bottom: 2px solid var(--vscode-focusBorder);">Parameters</button>
            <button class="config-tab pipeline-tab" data-tab="pipeline-variables" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Variables</button>
            <button class="config-tab pipeline-tab" data-tab="pipeline-settings" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Settings</button>
            <button class="config-tab pipeline-tab" data-tab="output" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Output</button>
            
            <!-- Activity-level tabs (shown when activity selected, dynamically generated) -->
            <div id="activityTabsContainer"></div>
            
            <button class="config-collapse-btn" id="configCollapseBtn" onclick="toggleConfig()" title="Collapse Configuration Panel">»</button>
        </div>
        <div class="config-content" id="configContent" style="flex: 1; overflow-y: auto; padding: 16px; background: var(--vscode-editor-background);">
            <!-- Pipeline-level tab panes -->
            <div class="config-tab-pane pipeline-pane active" id="tab-parameters">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Parameters</div>
                <div style="margin-bottom: 12px;">
                    <button id="addParameterBtn" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 12px;">+ Add Parameter</button>
                </div>
                <div id="parametersList"></div>
            </div>
            <div class="config-tab-pane pipeline-pane" id="tab-pipeline-variables">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Variables</div>
                <div style="margin-bottom: 12px;">
                    <button id="addVariableBtn" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 12px;">+ Add Variable</button>
                </div>
                <div id="variablesList"></div>
            </div>
            <div class="config-tab-pane pipeline-pane" id="tab-pipeline-settings">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Settings</div>
                <div class="property-group">
                    <div class="property-label">Concurrency</div>
                    <input type="number" id="concurrencyInput" class="property-input" min="1" placeholder="" style="width: 100px;">
                </div>
            </div>
            <div class="config-tab-pane pipeline-pane" id="tab-output">
                <div style="margin-bottom: 12px; font-weight: 600; color: var(--vscode-foreground);">Pipeline Output</div>
                <div class="empty-state">Pipeline execution output will appear here</div>
            </div>
            
            <!-- Activity-level tab panes (dynamically generated) -->
            <div id="activityPanesContainer"></div>
        </div>
    </div>
    </div>

    <!-- Context Menu -->
    <div class="context-menu" id="contextMenu">
        <div class="context-menu-item" data-action="delete">Delete</div>
        <div class="context-menu-separator"></div>
        <div class="context-menu-item" data-action="copy">Copy</div>
        <div class="context-menu-item" data-action="paste">Paste</div>
    </div>

    <script>
        // Toggle category function for collapsible categories
        function toggleCategory(element) {
            const activityGroup = element.closest('.activity-group');
            activityGroup.classList.toggle('collapsed');
        }

        console.log('=== Pipeline Editor Script Starting ===');
        const vscode = acquireVsCodeApi();
        console.log('vscode API acquired');
        
        // Dataset schemas and list will be sent via message
        let datasetSchemas = {};
        let datasetList = [];
        let datasetContents = {};
        
        // Pipeline list will be sent via message
        let pipelineList = [];

        // Copy activity config — populated from initSchemas message
        let copyActivityConfig = {};

        // ── Path helpers for config-driven Copy activity ──────────────────────────────
        // (mirrors setValueByPath / getValueByPath from datasetUtils.js, usable in browser)
        function _setValueByPath(obj, path, value) {
            if (!obj || !path) return obj;
            const keys = path.split('.');
            let cur = obj;
            for (let i = 0; i < keys.length - 1; i++) {
                if (!cur[keys[i]] || typeof cur[keys[i]] !== 'object') cur[keys[i]] = {};
                cur = cur[keys[i]];
            }
            if (value !== undefined && value !== null) {
                cur[keys[keys.length - 1]] = value;
            }
            return obj;
        }
        function _getValueByPath(obj, path) {
            if (!obj || !path) return undefined;
            const keys = path.split('.');
            let cur = obj;
            for (const k of keys) {
                if (cur === undefined || cur === null) return undefined;
                cur = cur[k];
            }
            return cur;
        }
        
        // Toggle properties panel
        function toggleProperties() {
            const panel = document.querySelector('.properties-panel');
            panel.classList.toggle('collapsed');
            // Toggle body class for button visibility
            if (panel.classList.contains('collapsed')) {
                document.body.classList.remove('properties-visible');
            } else {
                document.body.classList.add('properties-visible');
            }
        }

        // Initialize properties state
        document.body.classList.add('properties-visible');
        
        // Toggle config panel function
        function toggleConfig() {
            const panel = document.querySelector('.config-panel');
            const btn = document.getElementById('configCollapseBtn');
            panel.classList.toggle('minimized');
            // Change button icon
            btn.textContent = panel.classList.contains('minimized') ? '«' : '»';
        }
        
        // Canvas state
        let canvas = document.getElementById('canvas');
        let ctx = canvas.getContext('2d');
        console.log('Canvas:', canvas);
        console.log('Canvas context:', ctx);
        let activities = [];
        let connections = [];
        let selectedActivity = null;
        let currentFilePath = null; // Track the current file path

        // Derive pipeline name from current file path (falls back to 'pipeline1')
        function getPipelineNameFromPath() {
            if (!currentFilePath) return 'pipeline1';
            const parts = currentFilePath.replace(/\\\\/g, '/').split('/');
            const filename = parts[parts.length - 1];
            return filename.replace('.json', '').replace('.JSON', '');
        }
        let draggedActivity = null;
        
        // Pipeline-level properties (when no activity selected)
        let pipelineData = {
            parameters: {},
            variables: {},
            concurrency: 1
        };
        
        // Dirty state tracking for unsaved changes
        let isDirty = false;
        let originalState = null;
        
        // Mark editor as having unsaved changes
        function markAsDirty() {
            if (!isDirty) {
                isDirty = true;
                vscode.postMessage({ type: 'contentChanged', isDirty: true });
            }
            // Always send current state for caching
            sendCurrentStateToExtension();
        }
        
        // Validate activities before saving
        function validateActivities() {
            const invalidActivities = [];
            activities.forEach(a => {
                if ((a.type === 'SetVariable' || a.type === 'AppendVariable') && !a.variableName) {
                    invalidActivities.push(a.name + ' (' + a.type + ') - missing variable name');
                }
                if (a.type === 'SetVariable') {
                    const varType = a.variableType || 'Pipeline variable';
                    if (varType === 'Pipeline variable' && (a.value === undefined || a.value === null || a.value === '')) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - missing value');
                    }
                }
                if (a.type === 'Fail' && (!a.message || !a.errorCode)) {
                    const missing = [];
                    if (!a.message) missing.push('Fail message');
                    if (!a.errorCode) missing.push('Error code');
                    invalidActivities.push(a.name + ' (' + a.type + ') - missing ' + missing.join(' and '));
                }
                if (a.type === 'Filter' && (!a.items || !a.condition)) {
                    const missing = [];
                    if (!a.items) missing.push('Items');
                    if (!a.condition) missing.push('Condition');
                    invalidActivities.push(a.name + ' (' + a.type + ') - missing ' + missing.join(' and '));
                }
                if (a.type === 'Delete') {
                    // Validate dataset is selected
                    if (!a.dataset) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Dataset must be selected');
                    }
                    
                    // Only validate datetime filters if they're applicable (not for listOfFiles mode)
                    if (a.filePathType !== 'listOfFiles') {
                        // Validate that start is before end if both are specified
                        if (a.modifiedDatetimeStart && a.modifiedDatetimeEnd) {
                            const startDate = new Date(a.modifiedDatetimeStart);
                            const endDate = new Date(a.modifiedDatetimeEnd);
                            if (startDate >= endDate) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Start time must be before End time in Filter by last modified');
                            }
                        }
                    }
                }
                if (a.type === 'ExecutePipeline') {
                    // Validate pipeline is selected
                    if (!a.pipeline) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Invoked pipeline must be selected');
                    }
                }
                if (a.type === 'Validation') {
                    // Validate dataset is selected
                    if (!a.dataset || (typeof a.dataset === 'object' && !a.dataset.referenceName)) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Dataset must be selected');
                    }
                }
                if (a.type === 'GetMetadata') {
                    // Validate dataset is selected
                    if (!a.dataset || (typeof a.dataset === 'object' && !a.dataset.referenceName)) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Dataset must be selected');
                    }
                    // Validate field list has at least one valid field
                    const hasValidFields = a.fieldList && a.fieldList.length > 0 && a.fieldList.some(f => f.value && f.value.trim() !== '');
                    if (!hasValidFields) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Field list must have at least one valid field');
                    }
                    // Validate that start is before end if both are specified
                    if (a.modifiedDatetimeStart && a.modifiedDatetimeEnd) {
                        const startDate = new Date(a.modifiedDatetimeStart);
                        const endDate = new Date(a.modifiedDatetimeEnd);
                        if (startDate >= endDate) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Start time must be before End time in Filter by last modified');
                        }
                    }
                }
                if (a.type === 'Lookup') {
                    // Validate dataset is selected
                    if (!a.dataset) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Source dataset must be selected');
                    }
                    
                    // For SQL datasets, validate query/stored procedure fields if useQuery is set
                    if (a._datasetType === 'AzureSqlTable' || a._datasetType === 'AzureSynapseAnalytics') {
                        if (a.useQuery === 'Query' && (!a.sqlReaderQuery || a.sqlReaderQuery.trim() === '')) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Query must be provided when Query option is selected');
                        }
                        if (a.useQuery === 'Stored procedure' && (!a.sqlReaderStoredProcedureName || a.sqlReaderStoredProcedureName.trim() === '')) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Stored procedure name must be provided when Stored procedure option is selected');
                        }
                        
                        // Validate stored procedure parameters - check for empty names
                        if (a.useQuery === 'Stored procedure' && a.storedProcedureParameters && typeof a.storedProcedureParameters === 'object') {
                            const paramNames = new Set();
                            for (const [paramName, paramData] of Object.entries(a.storedProcedureParameters)) {
                                if (!paramName || paramName.trim() === '') {
                                    invalidActivities.push(a.name + ' (' + a.type + ') - Stored procedure parameter name cannot be empty');
                                    break;
                                }
                                // Check for duplicates (should not happen due to object keys, but just in case)
                                if (paramNames.has(paramName)) {
                                    invalidActivities.push(a.name + ' (' + a.type + ') - Duplicate stored procedure parameter name: ' + paramName);
                                }
                                paramNames.add(paramName);
                            }
                        }
                    }
                    
                    // For HTTP datasets, validate request method is set
                    if (a._datasetType === 'HttpFile' && !a.requestMethod) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Request method must be selected');
                    }
                }
                if (a.type === 'Script') {
                    // Validate linked service is selected
                    if (!a.linkedServiceName || (typeof a.linkedServiceName === 'object' && !a.linkedServiceName.referenceName)) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Linked service must be selected');
                    }
                    // Validate that ALL scripts have text
                    if (!a.scripts || a.scripts.length === 0) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - At least one script is required');
                    } else {
                        const emptyScripts = [];
                        a.scripts.forEach((script, idx) => {
                            if (!script.text || script.text.trim() === '') {
                                emptyScripts.push(idx + 1);
                            }
                            
                            // Check for duplicate parameter names within this script (only for named parameters)
                            if (script.parameters && script.parameters.length > 0) {
                                const paramNames = new Map(); // Track parameter names and their indices
                                script.parameters.forEach((param, paramIdx) => {
                                    const paramName = param.name ? param.name.trim() : '';
                                    // Only check duplicates for parameters that have names
                                    if (paramName !== '') {
                                        if (paramNames.has(paramName)) {
                                            // Duplicate parameter name
                                            const firstIdx = paramNames.get(paramName);
                                            invalidActivities.push(a.name + ' (' + a.type + ') - Script ' + (idx + 1) + ' has duplicate parameter name "' + paramName + '" at positions ' + (firstIdx + 1) + ' and ' + (paramIdx + 1));
                                        } else {
                                            paramNames.set(paramName, paramIdx);
                                        }
                                    }
                                    // Unnamed parameters are allowed, so we skip them
                                });
                            }
                        });
                        if (emptyScripts.length > 0) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Script(s) ' + emptyScripts.join(', ') + ' cannot be empty');
                        }
                    }
                }
                if (a.type === 'SqlServerStoredProcedure') {
                    // Validate linked service is selected
                    if (!a.linkedServiceName || (typeof a.linkedServiceName === 'object' && !a.linkedServiceName.referenceName)) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Linked service must be selected');
                    }
                    // Validate stored procedure name is entered
                    if (!a.storedProcedureName || a.storedProcedureName.trim() === '') {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Stored procedure name must be entered');
                    }
                    // Validate that if parameters exist, their names are not empty
                    if (a.storedProcedureParameters && typeof a.storedProcedureParameters === 'object') {
                        const emptyParamNames = [];
                        let paramIndex = 1;
                        for (const [paramName, paramData] of Object.entries(a.storedProcedureParameters)) {
                            if (!paramName || paramName.trim() === '') {
                                emptyParamNames.push('Parameter ' + paramIndex);
                            }
                            paramIndex++;
                        }
                        if (emptyParamNames.length > 0) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - ' + emptyParamNames.join(', ') + ' must have a name');
                        }
                    }
                }
                if (a.type === 'WebActivity') {
                    // Validate URL is entered
                    if (!a.url || a.url.trim() === '') {
                        invalidActivities.push(a.name + ' (' + a.type + ') - URL must be entered');
                    }
                    // Validate method is selected
                    if (!a.method) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Method must be selected');
                    }
                    // Validate Basic authentication fields
                    if (a.authenticationType === 'Basic') {
                        if (!a.username || a.username.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Username is required for Basic authentication');
                        }
                        if (!a.password) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Password is required for Basic authentication');
                        } else if (typeof a.password === 'object') {
                            // Validate Azure Key Vault Secret structure
                            if (!a.password.store || !a.password.store.referenceName) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for password');
                            }
                            if (!a.password.secretName || a.password.secretName.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for password');
                            }
                        }
                    }
                    // Validate MSI (System-assigned managed identity) authentication fields
                    if (a.authenticationType === 'MSI') {
                        if (!a.resource || a.resource.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for MSI authentication');
                        }
                    }
                    // Validate ClientCertificate authentication fields
                    if (a.authenticationType === 'ClientCertificate') {
                        if (!a.pfx) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Pfx is required for Client Certificate authentication');
                        } else if (typeof a.pfx === 'object') {
                            // Validate Azure Key Vault Secret structure
                            if (!a.pfx.store || !a.pfx.store.referenceName) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for Pfx');
                            }
                            if (!a.pfx.secretName || a.pfx.secretName.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for Pfx');
                            }
                        }
                        if (!a.pfxPassword) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Password is required for Client Certificate authentication');
                        } else if (typeof a.pfxPassword === 'object') {
                            // Validate Azure Key Vault Secret structure
                            if (!a.pfxPassword.store || !a.pfxPassword.store.referenceName) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for password');
                            }
                            if (!a.pfxPassword.secretName || a.pfxPassword.secretName.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for password');
                            }
                        }
                    }
                    // Validate ServicePrincipal authentication fields
                    if (a.authenticationType === 'ServicePrincipal') {
                        // Check authentication reference method (default is Inline if not specified)
                        const authMethod = a.servicePrincipalAuthMethod || 'Inline';
                        
                        if (authMethod === 'Inline') {
                            // Tenant and Service Principal ID are required for Inline method
                            if (!a.tenant || a.tenant.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Tenant is required for Service Principal authentication');
                            }
                            if (!a.servicePrincipalId || a.servicePrincipalId.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Service principal ID is required for Service Principal authentication');
                            }
                            
                            // Validate resource for inline method
                            if (!a.servicePrincipalResource || a.servicePrincipalResource.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for Service Principal authentication');
                            }
                            
                            // Get credential type (default is Service Principal Key if not specified)
                            const credentialType = a.servicePrincipalCredentialType || 'Service Principal Key';
                            
                            if (credentialType === 'Service Principal Key') {
                                // Validate servicePrincipalKey
                                if (!a.servicePrincipalKey) {
                                    invalidActivities.push(a.name + ' (' + a.type + ') - Service principal key is required');
                                } else if (typeof a.servicePrincipalKey === 'object') {
                                    // Validate Azure Key Vault Secret structure
                                    if (!a.servicePrincipalKey.store || !a.servicePrincipalKey.store.referenceName) {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for service principal key');
                                    }
                                    if (!a.servicePrincipalKey.secretName || a.servicePrincipalKey.secretName.trim() === '') {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for service principal key');
                                    }
                                }
                            } else if (credentialType === 'Service Principal Certificate') {
                                // Validate servicePrincipalCert
                                if (!a.servicePrincipalCert) {
                                    invalidActivities.push(a.name + ' (' + a.type + ') - Service principal certificate is required');
                                } else if (typeof a.servicePrincipalCert === 'object') {
                                    // Validate Azure Key Vault Secret structure
                                    if (!a.servicePrincipalCert.store || !a.servicePrincipalCert.store.referenceName) {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for service principal certificate');
                                    }
                                    if (!a.servicePrincipalCert.secretName || a.servicePrincipalCert.secretName.trim() === '') {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for service principal certificate');
                                    }
                                }
                            }
                        } else if (authMethod === 'Credential') {
                            // Validate credential and credentialResource for credential method
                            if (!a.credential || a.credential.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Credentials is required for Service Principal authentication');
                            }
                            if (!a.credentialResource || a.credentialResource.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for Service Principal authentication');
                            }
                        }
                    }
                    // Validate UserAssignedManagedIdentity authentication fields
                    if (a.authenticationType === 'UserAssignedManagedIdentity') {
                        if (!a.credentialUserAssigned || a.credentialUserAssigned.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Credential is required for User-assigned managed identity authentication');
                        }
                        if (!a.resource || a.resource.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for User-assigned managed identity authentication');
                        }
                    }
                    // Validate httpRequestTimeout format and range if provided
                    if (a.httpRequestTimeout) {
                        const timeoutValue = String(a.httpRequestTimeout).trim();
                        console.log('[Validation] httpRequestTimeout raw:', JSON.stringify(a.httpRequestTimeout));
                        console.log('[Validation] httpRequestTimeout trimmed:', JSON.stringify(timeoutValue));
                        console.log('[Validation] httpRequestTimeout length:', timeoutValue.length);
                        const timeoutPattern = /^(\\d{2}):(\\d{2}):(\\d{2})$/;
                        const match = timeoutValue.match(timeoutPattern);
                        console.log('[Validation] Regex match result:', match);
                        
                        if (!match) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - HTTP request timeout must be in format HH:MM:SS');
                        } else {
                            const hours = parseInt(match[1], 10);
                            const minutes = parseInt(match[2], 10);
                            const seconds = parseInt(match[3], 10);
                            const totalMinutes = hours * 60 + minutes + seconds / 60;
                            
                            if (totalMinutes < 1 || totalMinutes > 10) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - HTTP request timeout must be between 1 and 10 minutes (valid range: 00:01:00 to 00:10:00)');
                            }
                        }
                    }
                    // Validate headers have names and values
                    if (a.headers && a.headers.length > 0) {
                        const invalidHeaders = [];
                        const headerNames = new Set();
                        const duplicateHeaders = [];
                        
                        a.headers.forEach((header, idx) => {
                            if (!header.name || header.name.trim() === '' || !header.value || header.value.trim() === '') {
                                invalidHeaders.push(idx + 1);
                            } else {
                                const headerName = header.name.trim();
                                if (headerNames.has(headerName)) {
                                    duplicateHeaders.push(headerName);
                                } else {
                                    headerNames.add(headerName);
                                }
                            }
                        });
                        
                        if (invalidHeaders.length > 0) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Header(s) ' + invalidHeaders.join(', ') + ' must have both Name and Value');
                        }
                        if (duplicateHeaders.length > 0) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Duplicate header name(s): ' + [...new Set(duplicateHeaders)].join(', '));
                        }
                    }
                }
                if (a.type === 'WebHook') {
                    // Validate URL is entered
                    if (!a.url || a.url.trim() === '') {
                        invalidActivities.push(a.name + ' (' + a.type + ') - URL must be entered');
                    }
                    // Method is always POST for WebHook, but validate it's set
                    if (!a.method) {
                        invalidActivities.push(a.name + ' (' + a.type + ') - Method must be set');
                    }
                    // Validate Basic authentication fields
                    if (a.authenticationType === 'Basic') {
                        if (!a.username || a.username.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Username is required for Basic authentication');
                        }
                        if (!a.password) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Password is required for Basic authentication');
                        } else if (typeof a.password === 'object') {
                            // Validate Azure Key Vault Secret structure
                            if (!a.password.store || !a.password.store.referenceName) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for password');
                            }
                            if (!a.password.secretName || a.password.secretName.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for password');
                            }
                        }
                    }
                    // Validate MSI (System-assigned managed identity) authentication fields
                    if (a.authenticationType === 'MSI') {
                        if (!a.resource || a.resource.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for MSI authentication');
                        }
                    }
                    // Validate ClientCertificate authentication fields
                    if (a.authenticationType === 'ClientCertificate') {
                        if (!a.pfx) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Pfx is required for Client Certificate authentication');
                        } else if (typeof a.pfx === 'object') {
                            // Validate Azure Key Vault Secret structure
                            if (!a.pfx.store || !a.pfx.store.referenceName) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for Pfx');
                            }
                            if (!a.pfx.secretName || a.pfx.secretName.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for Pfx');
                            }
                        }
                        if (!a.pfxPassword) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Password is required for Client Certificate authentication');
                        } else if (typeof a.pfxPassword === 'object') {
                            // Validate Azure Key Vault Secret structure
                            if (!a.pfxPassword.store || !a.pfxPassword.store.referenceName) {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for password');
                            }
                            if (!a.pfxPassword.secretName || a.pfxPassword.secretName.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for password');
                            }
                        }
                    }
                    // Validate ServicePrincipal authentication fields
                    if (a.authenticationType === 'ServicePrincipal') {
                        const authMethod = a.servicePrincipalAuthMethod || 'Inline';
                        
                        if (authMethod === 'Inline') {
                            if (!a.tenant || a.tenant.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Tenant is required for Service Principal authentication');
                            }
                            if (!a.servicePrincipalId || a.servicePrincipalId.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Service principal ID is required for Service Principal authentication');
                            }
                            if (!a.servicePrincipalResource || a.servicePrincipalResource.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for Service Principal authentication');
                            }
                            
                            const credentialType = a.servicePrincipalCredentialType || 'Service Principal Key';
                            
                            if (credentialType === 'Service Principal Key') {
                                if (!a.servicePrincipalKey) {
                                    invalidActivities.push(a.name + ' (' + a.type + ') - Service principal key is required');
                                } else if (typeof a.servicePrincipalKey === 'object') {
                                    if (!a.servicePrincipalKey.store || !a.servicePrincipalKey.store.referenceName) {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for service principal key');
                                    }
                                    if (!a.servicePrincipalKey.secretName || a.servicePrincipalKey.secretName.trim() === '') {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for service principal key');
                                    }
                                }
                            } else if (credentialType === 'Service Principal Certificate') {
                                if (!a.servicePrincipalCert) {
                                    invalidActivities.push(a.name + ' (' + a.type + ') - Service principal certificate is required');
                                } else if (typeof a.servicePrincipalCert === 'object') {
                                    if (!a.servicePrincipalCert.store || !a.servicePrincipalCert.store.referenceName) {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Azure Key Vault linked service must be selected for service principal certificate');
                                    }
                                    if (!a.servicePrincipalCert.secretName || a.servicePrincipalCert.secretName.trim() === '') {
                                        invalidActivities.push(a.name + ' (' + a.type + ') - Secret name is required for service principal certificate');
                                    }
                                }
                            }
                        } else if (authMethod === 'Credential') {
                            if (!a.credential || a.credential.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Credentials is required for Service Principal authentication');
                            }
                            if (!a.credentialResource || a.credentialResource.trim() === '') {
                                invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for Service Principal authentication');
                            }
                        }
                    }
                    // Validate UserAssignedManagedIdentity authentication fields
                    if (a.authenticationType === 'UserAssignedManagedIdentity') {
                        if (!a.credentialUserAssigned || a.credentialUserAssigned.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Credential is required for User-assigned managed identity authentication');
                        }
                        if (!a.resource || a.resource.trim() === '') {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Resource is required for User-assigned managed identity authentication');
                        }
                    }
                    // Validate headers have names and values
                    if (a.headers && a.headers.length > 0) {
                        const invalidHeaders = [];
                        const headerNames = new Set();
                        const duplicateHeaders = [];
                        
                        a.headers.forEach((header, idx) => {
                            if (!header.name || header.name.trim() === '' || !header.value || header.value.trim() === '') {
                                invalidHeaders.push(idx + 1);
                            } else {
                                const headerName = header.name.trim();
                                if (headerNames.has(headerName)) {
                                    duplicateHeaders.push(headerName);
                                } else {
                                    headerNames.add(headerName);
                                }
                            }
                        });
                        
                        if (invalidHeaders.length > 0) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Header(s) ' + invalidHeaders.join(', ') + ' must have both Name and Value');
                        }
                        if (duplicateHeaders.length > 0) {
                            invalidActivities.push(a.name + ' (' + a.type + ') - Duplicate header name(s): ' + [...new Set(duplicateHeaders)].join(', '));
                        }
                    }
                }
                if (a.type === 'Copy') {
                    // Validate rangeError-annotated number fields in the sink dataset config
                    const _snkConf = copyActivityConfig.datasetTypes?.[a._sinkDatasetType];
                    for (const [_fk, _fc] of Object.entries(_snkConf?.fields?.sink || {})) {
                        if (!_fc.rangeError) continue;
                        const _v = a[_fk];
                        if (_v === undefined || _v === null || _v === '') continue;
                        const _num = Number(_v);
                        if (isNaN(_num) || (_fc.min !== undefined && _num < _fc.min) || (_fc.max !== undefined && _num > _fc.max)) {
                            invalidActivities.push(a.name + ' (Copy) - ' + _fc.rangeError);
                        }
                    }
                    // Same for source dataset config
                    const _srcConf = copyActivityConfig.datasetTypes?.[a._sourceDatasetType];
                    for (const [_fk, _fc] of Object.entries(_srcConf?.fields?.source || {})) {
                        if (!_fc.rangeError) continue;
                        const _v = a[_fk];
                        if (_v === undefined || _v === null || _v === '') continue;
                        const _num = Number(_v);
                        if (isNaN(_num) || (_fc.min !== undefined && _num < _fc.min) || (_fc.max !== undefined && _num > _fc.max)) {
                            invalidActivities.push(a.name + ' (Copy) - ' + _fc.rangeError);
                        }
                    }
                }
            });
            
            if (invalidActivities.length > 0) {
                return {
                    valid: false,
                    message: 'The following activities have required fields missing:\\n\\n' + 
                             invalidActivities.join('\\n') + 
                             '\\n\\nPlease fill in all required fields in the Settings tab before saving.'
                };
            }
            
            return { valid: true };
        }
        
        // Shared function to build pipeline data for saving (used by both save button and cache)
        function buildPipelineDataForSave(pipelineName, skipValidation = false) {
            // Validate required fields before saving (only when actually saving, not caching)
            if (!skipValidation) {
                for (const a of activities) {
                    if (a.type === 'IfCondition') {
                        if (!a.expression || a.expression.trim() === '') {
                            vscode.postMessage({
                                type: 'validationError',
                                message: 'Activity "' + a.name + '" requires an Expression. Please set the expression in the Activities tab before saving.'
                            });
                            throw new Error('IfCondition activity "' + a.name + '" is missing required Expression field');
                        }
                    }
                    if (a.type === 'ForEach') {
                        if (!a.items || a.items.trim() === '') {
                            vscode.postMessage({
                                type: 'validationError',
                                message: 'Activity "' + a.name + '" requires an Items expression. Please set the items in the Settings tab before saving.'
                            });
                            throw new Error('ForEach activity "' + a.name + '" is missing required Items field');
                        }
                        if (!a.activities || a.activities.length === 0) {
                            vscode.postMessage({
                                type: 'validationError',
                                message: 'Activity "' + a.name + '" must have at least one activity in its body. Please add activities in the Activities tab before saving.'
                            });
                            throw new Error('ForEach activity "' + a.name + '" has no body activities');
                        }
                    }
                    if (a.type === 'Switch') {
                        if (!a.on || a.on.trim() === '') {
                            vscode.postMessage({
                                type: 'validationError',
                                message: 'Activity "' + a.name + '" requires an On Expression. Please set the expression in the Activities tab before saving.'
                            });
                            throw new Error('Switch activity "' + a.name + '" is missing required On Expression field');
                        }
                        if (!a.cases || a.cases.length === 0) {
                            vscode.postMessage({
                                type: 'validationError',
                                message: 'Activity "' + a.name + '" must have at least one case. Please add a case in the Activities tab before saving.'
                            });
                            throw new Error('Switch activity "' + a.name + '" has no cases');
                        }
                    }
                    if (a.type === 'Until') {
                        if (!a.expression || a.expression.trim() === '') {
                            vscode.postMessage({
                                type: 'validationError',
                                message: 'Activity "' + a.name + '" requires an Expression. Please set the expression in the Activities tab before saving.'
                            });
                            throw new Error('Until activity "' + a.name + '" is missing required Expression field');
                        }
                        if (!a.activities || a.activities.length === 0) {
                            vscode.postMessage({
                                type: 'validationError',
                                message: 'Activity "' + a.name + '" must have at least one activity in its body. Please add activities in the Activities tab before saving.'
                            });
                            throw new Error('Until activity "' + a.name + '" has no body activities');
                        }
                    }
                }

                // Recursively validate required fields on all activities, including those
                // nested inside Until, ForEach, IfCondition, Switch, etc.
                // Supports both live Activity instances (flat properties) and
                // serialized POJOs (properties under typeProperties sub-object).
                const schemaData = ${JSON.stringify(activitySchemas)};

                function validateActivityListDeep(actList, contextPath) {
                    actList.forEach(function(a) {
                        const schema = schemaData[a.type];
                        if (schema) {
                            // Collect all property groups to check
                            const propGroups = [
                                schema.typeProperties,
                                schema.sourceProperties,
                                schema.sinkProperties
                            ].filter(Boolean);

                            // Simple value types where empty string = not set
                            const checkableTypes = ['text', 'string', 'expression'];
                            // Reference types where value should be an object with a non-empty referenceName
                            const referenceTypes = ['dataset', 'dataset-lookup', 'getmetadata-dataset', 'validation-dataset', 'pipeline', 'script-linkedservice', 'storedprocedure-linkedservice'];

                            propGroups.forEach(function(group) {
                                Object.entries(group).forEach(function([fieldKey, fieldDef]) {
                                    // Only check unconditionally required fields of known types
                                    // (skip conditional fields to avoid false positives, skip
                                    //  complex array/object types that have their own dedicated UI)
                                    if (!fieldDef.required || fieldDef.conditional) return;
                                    const isSimple    = checkableTypes.includes(fieldDef.type);
                                    const isReference = referenceTypes.includes(fieldDef.type);
                                    if (!isSimple && !isReference) return;

                                    // Support both flat (live instance) and POJO (typeProperties layout)
                                    const val = a[fieldKey] !== undefined ? a[fieldKey] : (a.typeProperties && a.typeProperties[fieldKey]);

                                    let isEmpty;
                                    if (isReference) {
                                        // Reference fields store an object { referenceName: '...' } when set,
                                        // or are null/undefined/empty-string/''/empty-object when not set.
                                        isEmpty = (val === undefined || val === null || val === '' ||
                                                   (typeof val === 'object' && (!val.referenceName || val.referenceName === '')));
                                    } else {
                                        // For expression fields, the value may be stored as an object
                                        // { value: '...', type: 'Expression' } when loaded back from JSON
                                        // (after Object.assign of typeProperties onto the live Activity).
                                        const strVal = (typeof val === 'object' && val !== null && 'value' in val) ? val.value : val;
                                        isEmpty = (strVal === undefined || strVal === null || strVal === '' || (typeof strVal === 'string' && strVal.trim() === ''));
                                    }

                                    if (isEmpty) {
                                        const locationLabel = contextPath ? ' (inside "' + contextPath + '")' : '';
                                        vscode.postMessage({
                                            type: 'validationError',
                                            message: 'Activity "' + a.name + '"' + locationLabel + ' requires "' + fieldDef.label + '". Please fill in this field before saving.'
                                        });
                                        throw new Error('Activity "' + a.name + '" is missing required field "' + fieldKey + '"');
                                    }
                                });
                            });
                        }

                        // Special case: SetVariable — variableName and value are both marked conditional
                        // (conditional on variableType === 'Pipeline variable') so the generic loop above
                        // skips them.  But 'Pipeline variable' IS the default, so treat both as required.
                        if (a.type === 'SetVariable') {
                            const varType = a.variableType || (a.typeProperties && a.typeProperties.variableType) || 'Pipeline variable';
                            if (varType === 'Pipeline variable') {
                                const varName = a.variableName || (a.typeProperties && a.typeProperties.variableName);
                                const varEmpty = !varName || (typeof varName === 'string' && varName.trim() === '');
                                if (varEmpty) {
                                    const locationLabel = contextPath ? ' (inside "' + contextPath + '")' : '';
                                    vscode.postMessage({
                                        type: 'validationError',
                                        message: 'Activity "' + a.name + '"' + locationLabel + ' requires "Name" (variable name). Please fill in this field before saving.'
                                    });
                                    throw new Error('Activity "' + a.name + '" is missing required field variableName');
                                }
                                const varValue = a.value !== undefined ? a.value : (a.typeProperties && a.typeProperties.value);
                                const valEmpty = varValue === undefined || varValue === null || varValue === '' || (typeof varValue === 'string' && varValue.trim() === '');
                                if (valEmpty) {
                                    const locationLabel = contextPath ? ' (inside "' + contextPath + '")' : '';
                                    vscode.postMessage({
                                        type: 'validationError',
                                        message: 'Activity "' + a.name + '"' + locationLabel + ' requires "Value". Please fill in this field before saving.'
                                    });
                                    throw new Error('Activity "' + a.name + '" is missing required field value');
                                }
                            }
                        }

                        // Special case: XML source — validationMode must be 'none' when maxConcurrentConnections is set
                        if (a.type === 'Copy' && a._sourceDatasetType === 'Xml') {
                            const xmlValidMode = a.src_validationMode || (a.typeProperties?.source?.formatSettings?.validationMode);
                            const xmlMaxConn = a.src_maxConcurrentConnections !== undefined
                                ? a.src_maxConcurrentConnections
                                : a.typeProperties?.source?.storeSettings?.maxConcurrentConnections;
                            if (xmlMaxConn !== undefined && xmlMaxConn !== null && xmlMaxConn !== '' &&
                                xmlValidMode && xmlValidMode !== 'none') {
                                const locationLabel = contextPath ? ' (inside "' + contextPath + '")' : '';
                                vscode.postMessage({
                                    type: 'validationError',
                                    message: 'Activity "' + a.name + '"' + locationLabel + ': Validation mode must be "none" when Max concurrent connections is specified.'
                                });
                                throw new Error('Activity "' + a.name + '": XML validationMode must be none when maxConcurrentConnections is set');
                            }
                        }

                        // Special case: Azure SQL DB + Synapse Analytics source — Query/SP name required when selected
                        if (a.type === 'Copy' && (a._sourceDatasetType === 'AzureSqlTable' || a._sourceDatasetType === 'AzureSqlDWTable')) {
                            const useQuery = a.src_useQuery;
                            const locationLabel = contextPath ? ' (inside "' + contextPath + '")' : '';
                            if (useQuery === 'Query') {
                                const q = a.src_sqlReaderQuery;
                                if (!q || (typeof q === 'string' && q.trim() === '')) {
                                    vscode.postMessage({
                                        type: 'validationError',
                                        message: 'Activity "' + a.name + '"' + locationLabel + ': Query is required when "Query" is selected as the query type.'
                                    });
                                    throw new Error('Activity "' + a.name + '" is missing required field src_sqlReaderQuery');
                                }
                            } else if (useQuery === 'Stored procedure') {
                                const sp = a.src_sqlReaderStoredProcedureName;
                                if (!sp || (typeof sp === 'string' && sp.trim() === '')) {
                                    vscode.postMessage({
                                        type: 'validationError',
                                        message: 'Activity "' + a.name + '"' + locationLabel + ': Stored procedure name is required when "Stored procedure" is selected as the query type.'
                                    });
                                    throw new Error('Activity "' + a.name + '" is missing required field src_sqlReaderStoredProcedureName');
                                }
                            }
                        }

                        // Recurse into all container branches
                        const nested   = a.activities        || (a.typeProperties && a.typeProperties.activities);
                        const trueActs = a.ifTrueActivities  || (a.typeProperties && a.typeProperties.ifTrueActivities);
                        const falseActs= a.ifFalseActivities || (a.typeProperties && a.typeProperties.ifFalseActivities);
                        const defaultActs = a.defaultActivities || (a.typeProperties && a.typeProperties.defaultActivities);
                        const cases    = a.cases             || (a.typeProperties && a.typeProperties.cases);

                        const nextPath = contextPath ? contextPath + ' > ' + a.name : a.name;
                        if (nested    && Array.isArray(nested))     validateActivityListDeep(nested,     nextPath);
                        if (trueActs  && Array.isArray(trueActs))   validateActivityListDeep(trueActs,   nextPath + ' > True');
                        if (falseActs && Array.isArray(falseActs))  validateActivityListDeep(falseActs,  nextPath + ' > False');
                        if (defaultActs && Array.isArray(defaultActs)) validateActivityListDeep(defaultActs, nextPath + ' > Default');
                        if (cases && Array.isArray(cases)) {
                            cases.forEach(function(c, idx) {
                                if (c.activities && Array.isArray(c.activities)) {
                                    validateActivityListDeep(c.activities, nextPath + ' > Case ' + (c.value || idx));
                                }
                            });
                        }
                    });
                }

                validateActivityListDeep(activities, '');
            }
            
            // Use pipeline-level variables from pipelineData, then merge with activity-derived variables
            const variables = { ...pipelineData.variables };
            
            // Recursively collect variables from AppendVariable/SetVariable activities,
            // including those nested inside ForEach, IfCondition, etc.
            // Nested activities may be serialized POJOs (with typeProperties sub-object) or
            // live Activity instances (with flat properties) — handle both layouts.
            function collectVariables(actList) {
                actList.forEach(a => {
                    // Support flat (Activity instance), serialized POJO (typeProperties layout),
                    // and POJO with _variableType/_pipelineVariableType metadata stashed by buildNestedActivityTypeProperties
                    const varName = a.variableName || a.typeProperties?.variableName;
                    const varType = a.variableType || a.typeProperties?.variableType || a._variableType;
                    const pipelineVarType = a.pipelineVariableType || a.typeProperties?.pipelineVariableType || a._pipelineVariableType;

                    if (a.type === 'AppendVariable' && varName) {
                        if (!variables[varName]) {
                            variables[varName] = { type: 'Array' };
                        }
                    } else if (a.type === 'SetVariable' && varName && varType === 'Pipeline variable') {
                        if (!variables[varName]) {
                            const typeMap = { 'String': 'String', 'Boolean': 'Boolean', 'Array': 'Array', 'Integer': 'Integer' };
                            const resolvedType = typeMap[pipelineVarType] || 'String';
                            variables[varName] = { type: resolvedType };
                        }
                    }
                    // Recurse into container activity children (handles both layouts)
                    const nested = a.activities || a.typeProperties?.activities;
                    const trueActs = a.ifTrueActivities || a.typeProperties?.ifTrueActivities;
                    const falseActs = a.ifFalseActivities || a.typeProperties?.ifFalseActivities;
                    if (nested && Array.isArray(nested)) collectVariables(nested);
                    if (trueActs && Array.isArray(trueActs)) collectVariables(trueActs);
                    if (falseActs && Array.isArray(falseActs)) collectVariables(falseActs);
                    // Recurse into Switch cases and defaultActivities
                    const switchCases = a.cases || a.typeProperties?.cases;
                    if (switchCases && Array.isArray(switchCases)) {
                        switchCases.forEach(c => {
                            const caseActs = c.activities;
                            if (caseActs && Array.isArray(caseActs)) collectVariables(caseActs);
                        });
                    }
                    const defaultActs = a.defaultActivities || a.typeProperties?.defaultActivities;
                    if (defaultActs && Array.isArray(defaultActs)) collectVariables(defaultActs);
                });
            }
            collectVariables(activities);

            // Recursively strip UI-only metadata from a nested activity POJO and any of
            // its sub-activities (ForEach body, IfCondition branches). Used by the ForEach
            // body serializer to ensure _variableType/_pipelineVariableType (kept alive for
            // collectVariables above) are removed before the JSON is written to disk.
            function deepCleanNestedActivity(act) {
                const c = Object.assign({}, act);
                delete c.x; delete c.y;
                delete c._variableType; delete c._pipelineVariableType;
                if (c.typeProperties) {
                    c.typeProperties = Object.assign({}, c.typeProperties);
                    if (Array.isArray(c.typeProperties.activities))
                        c.typeProperties.activities = c.typeProperties.activities.map(deepCleanNestedActivity);
                    if (Array.isArray(c.typeProperties.ifTrueActivities))
                        c.typeProperties.ifTrueActivities = c.typeProperties.ifTrueActivities.map(deepCleanNestedActivity);
                    if (Array.isArray(c.typeProperties.ifFalseActivities))
                        c.typeProperties.ifFalseActivities = c.typeProperties.ifFalseActivities.map(deepCleanNestedActivity);
                    if (Array.isArray(c.typeProperties.cases))
                        c.typeProperties.cases = c.typeProperties.cases.map(swCase => ({
                            ...swCase,
                            activities: Array.isArray(swCase.activities) ? swCase.activities.map(deepCleanNestedActivity) : []
                        }));
                    if (Array.isArray(c.typeProperties.defaultActivities))
                        c.typeProperties.defaultActivities = c.typeProperties.defaultActivities.map(deepCleanNestedActivity);
                }
                return c;
            }
            
            const result = {
                name: pipelineName,
                activities: activities.map(a => {
                    // Build the activity JSON with all properties
                    const activity = {
                        name: a.name,
                        type: a.type,
                        dependsOn: connections
                            .filter(c => c.to === a)
                            .map(c => ({
                                activity: c.from.name,
                                dependencyConditions: [c.condition || 'Succeeded']
                            })),
                        userProperties: a.userProperties || []
                    };
                    
                    // For SetVariable, always include policy section with secureOutput and secureInput
                    if (a.type === 'SetVariable') {
                        activity.policy = {
                            secureOutput: a.secureOutput || false,
                            secureInput: a.secureInput || false
                        };
                    } else if (a.type === 'GetMetadata' || a.type === 'Script' || a.type === 'SqlServerStoredProcedure' || a.type === 'WebActivity' || a.type === 'Lookup' || a.type === 'Delete' || a.type === 'Validation' || a.type === 'SynapseNotebook') {
                        // Always include full policy section with defaults for these activity types
                        activity.policy = {
                            timeout: a.timeout || "0.12:00:00",
                            retry: a.retry !== undefined ? a.retry : 0,
                            retryIntervalInSeconds: a.retryIntervalInSeconds !== undefined ? a.retryIntervalInSeconds : 30,
                            secureOutput: a.secureOutput || false,
                            secureInput: a.secureInput || false
                        };
                    } else if (a.type === 'WebHook') {
                        // For WebHook, always include policy with secureOutput and secureInput
                        activity.policy = {
                            secureOutput: a.secureOutput || false,
                            secureInput: a.secureInput || false
                        };
                    } else {
                        // For other activities, only add policy if any non-default values are set
                        const hasNonDefaultPolicy = 
                            (a.timeout && a.timeout !== "0.12:00:00") ||
                            (a.retry && a.retry !== 0) ||
                            (a.retryIntervalInSeconds && a.retryIntervalInSeconds !== 30) ||
                            a.secureOutput === true ||
                            a.secureInput === true;
                        
                        if (hasNonDefaultPolicy) {
                            activity.policy = {
                                timeout: a.timeout || "0.12:00:00",
                                retry: a.retry || 0,
                                retryIntervalInSeconds: a.retryIntervalInSeconds || 30,
                                secureOutput: a.secureOutput || false,
                                secureInput: a.secureInput || false
                            };
                        }
                    }
                    
                    if (a.description) activity.description = a.description;
                    // Don't add state for container activities (IfCondition, ForEach, Until, Switch)
                    if (a.state && !a.isContainer) activity.state = a.state;
                    if (a.onInactiveMarkAs) activity.onInactiveMarkAs = a.onInactiveMarkAs;
                    
                    // For SqlServerStoredProcedure, preserve linked service metadata for extension processing
                    if (a.type === 'SqlServerStoredProcedure') {
                        if (a.linkedServiceName) activity.linkedServiceName = a.linkedServiceName;
                        if (a._selectedLinkedServiceType) activity._selectedLinkedServiceType = a._selectedLinkedServiceType;
                        if (a.linkedServiceProperties) activity.linkedServiceProperties = a.linkedServiceProperties;
                    }
                    
                    // For Copy activities, preserve dataset references and build config-driven source/sink
                    if (a.type === 'Copy') {
                        if (a.sourceDataset) activity.sourceDataset = a.sourceDataset;
                        if (a.sinkDataset) activity.sinkDataset = a.sinkDataset;
                        if (a._sourceDatasetType) activity._sourceDatasetType = a._sourceDatasetType;
                        if (a._sinkDatasetType) activity._sinkDatasetType = a._sinkDatasetType;
                        if (a._sourceLocationType) activity._sourceLocationType = a._sourceLocationType;
                        if (a._sinkLocationType) activity._sinkLocationType = a._sinkLocationType;
                        // Keep _sourceObject/_sinkObject as fallback for types not yet in config
                        if (a._sourceObject) activity._sourceObject = a._sourceObject;
                        if (a._sinkObject) activity._sinkObject = a._sinkObject;

                        // --- Build source from config ---
                        const _srcTypeConf = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[a._sourceDatasetType];
                        if (_srcTypeConf && _srcTypeConf.sourceTypeName) {
                            const _src = { type: _srcTypeConf.sourceTypeName };
                            if (_srcTypeConf.hasStoreSettings) {
                                const _locT = a._sourceLocationType;
                                const _rType = (_locT && _srcTypeConf.storeReadSettingsTypes && _srcTypeConf.storeReadSettingsTypes[_locT])
                                    || (a._sourceObject && a._sourceObject.storeSettings && a._sourceObject.storeSettings.type)
                                    || _srcTypeConf.defaultStoreReadSettings
                                    || 'AzureBlobFSReadSettings';
                                _src.storeSettings = { type: _rType };
                                if (_srcTypeConf.formatReadType) _src.formatSettings = { type: _srcTypeConf.formatReadType };
                            }
                            const _srcFields = (_srcTypeConf.fields && _srcTypeConf.fields.source) || {};
                            for (const [_fk, _fc] of Object.entries(_srcFields)) {
                                if (!_fc.jsonPath) continue;
                                // Check conditional
                                if (_fc.conditional) {
                                    const _condVal = a[_fc.conditional.field];
                                    const _condExpected = _fc.conditional.value;
                                    const _condMet = _fc.conditional.notEmpty
                                        ? (_condVal !== undefined && _condVal !== null && _condVal !== '')
                                        : (Array.isArray(_condExpected) ? _condExpected.includes(_condVal) : _condVal === _condExpected);
                                    if (!_condMet) continue;
                                }
                                // Check nestedConditional
                                if (_fc.nestedConditional) {
                                    const _nCondVal = a[_fc.nestedConditional.field];
                                    const _nCondExpected = _fc.nestedConditional.value;
                                    const _nCondMet = Array.isArray(_nCondExpected) ? _nCondExpected.includes(_nCondVal) : _nCondVal === _nCondExpected;
                                    if (!_nCondMet) continue;
                                }
                                // Check conditionalAll — all conditions must be met (AND logic)
                                if (_fc.conditionalAll) {
                                    const _allMet = _fc.conditionalAll.every(_cond => {
                                        const _cv = a[_cond.field];
                                        return Array.isArray(_cond.value) ? _cond.value.includes(_cv) : _cv === _cond.value;
                                    });
                                    if (!_allMet) continue;
                                }
                                let _v = a[_fk];
                                if (_fc.omitWhenValue !== undefined && _v === _fc.omitWhenValue) continue;
                                // For filterEmpty arrays (e.g. additional-columns), strip blank-name entries
                                if (_fc.filterEmpty && Array.isArray(_v)) {
                                    _v = _v.filter(item => item[_fc.filterEmpty] && String(item[_fc.filterEmpty]).trim() !== '');
                                } else if (_fc.filterEmpty && !Array.isArray(_v)) {
                                    // Stale non-array value (e.g. from old plain-text save) — skip entirely
                                    continue;
                                }
                                // Skip empty arrays and empty objects
                                const _srcIsEmpty = (Array.isArray(_v) && _v.length === 0) || (_v !== undefined && _v !== null && typeof _v === 'object' && !Array.isArray(_v) && Object.keys(_v).length === 0);
                                if (!_srcIsEmpty && _v !== undefined && _v !== null && _v !== '') {
                                    _setValueByPath(_src, _fc.jsonPath, _v);
                                } else if (_fc.writeDefault === true && _fc.default !== undefined) {
                                    _setValueByPath(_src, _fc.jsonPath, _fc.default);
                                }
                            }
                            if (!activity.typeProperties) activity.typeProperties = {};
                            // XML: namespacePrefixPairs stored on activity (via namespace-prefixes widget) → formatSettings.namespacePrefixes
                            if (a._sourceDatasetType === 'Xml' && a.namespacePrefixPairs && Object.keys(a.namespacePrefixPairs).length > 0) {
                                if (_src.formatSettings) _src.formatSettings.namespacePrefixes = a.namespacePrefixPairs;
                            }
                            activity.typeProperties.source = _src;
                        } else if (a._sourceObject) {
                            if (!activity.typeProperties) activity.typeProperties = {};
                            activity.typeProperties.source = JSON.parse(JSON.stringify(a._sourceObject));
                        }

                        // --- Build sink from config ---
                        const _snkTypeConf = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[a._sinkDatasetType];
                        if (_snkTypeConf && _snkTypeConf.sinkTypeName) {
                            const _snk = { type: _snkTypeConf.sinkTypeName };
                            if (_snkTypeConf.hasStoreSettings) {
                                const _locT = a._sinkLocationType;
                                const _wType = (_locT && _snkTypeConf.storeWriteSettingsTypes && _snkTypeConf.storeWriteSettingsTypes[_locT])
                                    || (a._sinkObject && a._sinkObject.storeSettings && a._sinkObject.storeSettings.type)
                                    || _snkTypeConf.defaultStoreWriteSettings
                                    || 'AzureBlobFSWriteSettings';
                                _snk.storeSettings = { type: _wType };
                                if (_snkTypeConf.formatWriteType) _snk.formatSettings = { type: _snkTypeConf.formatWriteType };
                            }
                            const _snkFields = (_snkTypeConf.fields && _snkTypeConf.fields.sink) || {};
                            for (const [_fk, _fc] of Object.entries(_snkFields)) {
                                if (!_fc.jsonPath) continue;
                                // Skip if conditional is not met (supports single value or array of values)
                                if (_fc.conditional) {
                                    const _condVal = a[_fc.conditional.field];
                                    const _condExpected = _fc.conditional.value;
                                    const _condMet = _fc.conditional.notEmpty
                                        ? (_condVal !== undefined && _condVal !== null && _condVal !== '')
                                        : (Array.isArray(_condExpected) ? _condExpected.includes(_condVal) : _condVal === _condExpected);
                                    if (!_condMet) continue;
                                }
                                // Check conditionalAll — all conditions must be met (AND logic)
                                if (_fc.conditionalAll) {
                                    const _allMet = _fc.conditionalAll.every(_cond => {
                                        const _cv = a[_cond.field];
                                        return Array.isArray(_cond.value) ? _cond.value.includes(_cv) : _cv === _cond.value;
                                    });
                                    if (!_allMet) continue;
                                }
                                const _v = a[_fk];
                                if (_fc.omitWhenValue !== undefined && _v === _fc.omitWhenValue) continue;
                                // For noEmpty arrays, filter out blank entries before writing
                                let _writeV = (_fc.noEmpty && Array.isArray(_v))
                                    ? _v.filter(s => typeof s === 'string' ? s.trim() !== '' : s !== null && s !== undefined)
                                    : _v;
                                // For filterEmpty object-arrays, filter and guard against non-array stale values
                                if (_fc.filterEmpty && Array.isArray(_writeV)) {
                                    _writeV = _writeV.filter(item => item[_fc.filterEmpty] && String(item[_fc.filterEmpty]).trim() !== '');
                                } else if (_fc.filterEmpty && !Array.isArray(_writeV)) {
                                    continue;
                                }
                                // Skip empty objects (e.g. empty storedProcedureParameters) and empty arrays
                                const _isEmpty = (Array.isArray(_writeV) && _writeV.length === 0) || (_writeV !== undefined && _writeV !== null && typeof _writeV === 'object' && !Array.isArray(_writeV) && Object.keys(_writeV).length === 0);
                                if (_writeV !== undefined && _writeV !== null && _writeV !== '' && !_isEmpty) {
                                    _setValueByPath(_snk, _fc.jsonPath, _writeV);
                                } else if (_fc.writeDefault === true && _fc.default !== undefined) {
                                    _setValueByPath(_snk, _fc.jsonPath, _fc.default);
                                }
                            }
                            // AzureSqlDWTable: write writeBehavior constant based on copy method
                            if (a._sinkDatasetType === 'AzureSqlDWTable') {
                                const _cm = a['snk_copyMethod'];
                                if (_cm === 'BulkInsert') _snk.writeBehavior = 'Insert';
                                else if (_cm === 'Upsert') _snk.writeBehavior = 'Upsert';
                            }
                            if (!activity.typeProperties) activity.typeProperties = {};
                            activity.typeProperties.sink = _snk;
                        } else if (a._sinkObject) {
                            if (!activity.typeProperties) activity.typeProperties = {};
                            activity.typeProperties.sink = JSON.parse(JSON.stringify(a._sinkObject));
                        }
                    }
                    
                    // Collect typeProperties
                    const typeProperties = {};
                    const commonProps = ['id', 'type', 'x', 'y', 'width', 'height', 'name', 'description', 'color', 'container', 'element', 
                                         'timeout', 'retry', 'retryIntervalInSeconds', 'secureOutput', 'secureInput', 'userProperties', 'state', 'onInactiveMarkAs',
                                         'dynamicAllocation', 'minExecutors', 'maxExecutors', 'numExecutors', 'dependsOn', 'policy',
                                         'sourceDataset', 'sinkDataset', 'recursive', 'modifiedDatetimeStart', 'modifiedDatetimeEnd',
                                         'wildcardFolderPath', 'wildcardFileName', 'enablePartitionDiscovery',
                                         'writeBatchSize', 'writeBatchTimeout', 'preCopyScript', 'maxConcurrentConnections', 'writeBehavior', 
                                         'sqlWriterUseTableLock', 'disableMetricsCollection', '_sourceObject', '_sinkObject', '_sourceDatasetType', '_sinkDatasetType',
                                         '_sourceLocationType', '_sinkLocationType',
                                         'typeProperties', 'inputs', 'outputs', 'source', 'sink', 'linkedServiceName', '_selectedLinkedServiceType', 'linkedServiceProperties',
                                         'storedProcedureName', 'storedProcedureParameters',
                                         'isContainer', 'ifTrueActivities', 'ifFalseActivities', 'expression', 'activities', 'cases', 'defaultActivities',
                                         'null']; // safeguard against spurious 'null'-keyed UI artefacts
                    
                    for (const key in a) {
                        if (!commonProps.includes(key) && a.hasOwnProperty(key) && typeof a[key] !== 'function'
                            // Skip src_/snk_ prefixed fields — handled in the Copy build block above
                            && !key.startsWith('src_') && !key.startsWith('snk_')) {
                            // Convert notebook and sparkPool strings to reference objects with Expression format
                            if (key === 'notebook' && typeof a[key] === 'string' && a[key]) {
                                typeProperties[key] = {
                                    referenceName: {
                                        value: a[key],
                                        type: 'Expression'
                                    },
                                    type: 'NotebookReference'
                                };
                            } else if (key === 'sparkPool' && typeof a[key] === 'string' && a[key]) {
                                typeProperties[key] = {
                                    referenceName: {
                                        value: a[key],
                                        type: 'Expression'
                                    },
                                    type: 'BigDataPoolReference'
                                };
                            } else {
                                typeProperties[key] = a[key];
                            }
                        }
                    }
                    
                    // Handle SynapseNotebook specific fields
                    if (a.type === 'SynapseNotebook') {
                        typeProperties.snapshot = true;
                        if (typeProperties.executorSize) {
                            typeProperties.driverSize = typeProperties.executorSize;
                        }
                        
                        if (a.dynamicAllocation || a.minExecutors || a.maxExecutors || a.numExecutors) {
                            typeProperties.conf = {};
                            const isDynamicEnabled = a.dynamicAllocation === 'Enabled';
                            
                            if (a.dynamicAllocation !== undefined) {
                                typeProperties.conf['spark.dynamicAllocation.enabled'] = isDynamicEnabled;
                            }
                            
                            if (isDynamicEnabled) {
                                if (a.minExecutors) typeProperties.conf['spark.dynamicAllocation.minExecutors'] = parseInt(a.minExecutors);
                                if (a.maxExecutors) typeProperties.conf['spark.dynamicAllocation.maxExecutors'] = parseInt(a.maxExecutors);
                            } else if (a.numExecutors) {
                                const numExec = parseInt(a.numExecutors);
                                typeProperties.conf['spark.dynamicAllocation.minExecutors'] = numExec;
                                typeProperties.conf['spark.dynamicAllocation.maxExecutors'] = numExec;
                                typeProperties.numExecutors = numExec;
                            }
                        }
                    }
                    
                    // Handle SetVariable specific fields
                    if (a.type === 'SetVariable') {
                        // Remove UI-specific fields that shouldn't be in the JSON
                        delete typeProperties.variableType;
                        delete typeProperties.pipelineVariableType;
                        delete typeProperties.returnValues;
                        
                        // If it's a pipeline return value
                        if (a.variableType === 'Pipeline return value' && a.returnValues) {
                            typeProperties.variableName = 'pipelineReturnValue';
                            typeProperties.setSystemVariable = true;
                            
                            // Convert returnValues key-value object to Azure format
                            const valueArray = [];
                            for (const key in a.returnValues) {
                                if (a.returnValues.hasOwnProperty(key)) {
                                    const item = a.returnValues[key];
                                    const valueObj = {
                                        key: key,
                                        value: { type: item.type }
                                    };
                                    
                                    // Handle different types
                                    if (item.type === 'Null') {
                                        // Null type has no content
                                    } else if (item.type === 'Array') {
                                        // Array needs nested content structure
                                        valueObj.value.content = item.content || [];
                                    } else if (item.type === 'Int' || item.type === 'Float') {
                                        // Numbers without quotes
                                        valueObj.value.content = parseFloat(item.value) || 0;
                                    } else if (item.type === 'Boolean') {
                                        // Boolean value
                                        valueObj.value.content = item.value === 'true' || item.value === true;
                                    } else {
                                        // String, Expression, Object
                                        valueObj.value.content = item.value || '';
                                    }
                                    
                                    valueArray.push(valueObj);
                                }
                            }
                            
                            typeProperties.value = valueArray;
                        } else {
                            // Pipeline variable mode - parse numeric types
                            if (a.pipelineVariableType === 'Integer' && typeProperties.value) {
                                typeProperties.value = parseInt(typeProperties.value, 10) || 0;
                            }
                        }
                    }
                    
                    // Handle Fail activity - ensure message and errorCode are in typeProperties
                    if (a.type === 'Fail') {
                        // Message and errorCode are already in typeProperties from the common loop above
                        // Just ensure they're present with proper defaults if missing
                        if (!typeProperties.message) typeProperties.message = '';
                        if (!typeProperties.errorCode) typeProperties.errorCode = '';
                    }
                    
                    // Handle Filter activity - ensure items/condition are expression objects
                    if (a.type === 'Filter') {
                        typeProperties.items = {
                            value: a.items || '',
                            type: 'Expression'
                        };
                        typeProperties.condition = {
                            value: a.condition || '',
                            type: 'Expression'
                        };
                    }
                    
                    // Handle ExecutePipeline activity - build pipeline reference
                    if (a.type === 'ExecutePipeline') {
                        if (a.pipeline) {
                            typeProperties.pipeline = {
                                referenceName: a.pipeline,
                                type: 'PipelineReference'
                            };
                        }
                        
                        // Set waitOnCompletion, default to true if not specified
                        if (a.waitOnCompletion !== undefined) {
                            typeProperties.waitOnCompletion = a.waitOnCompletion;
                        } else {
                            typeProperties.waitOnCompletion = true;
                        }
                        
                        // Remove the string pipeline property as it's been converted to object
                        delete typeProperties.pipeline;
                        
                        // Re-add the proper pipeline reference
                        if (a.pipeline) {
                            typeProperties.pipeline = {
                                referenceName: a.pipeline,
                                type: 'PipelineReference'
                            };
                        }
                    }
                    
                    // Handle GetMetadata activity - build typeProperties with dataset, fieldList, storeSettings, formatSettings
                    if (a.type === 'GetMetadata') {
                        // Build dataset reference
                        if (a.dataset) {
                            typeProperties.dataset = {
                                referenceName: a.dataset,
                                type: 'DatasetReference'
                            };
                        }
                        
                        // Build field list array (filter out empty values)
                        if (a.fieldList && a.fieldList.length > 0) {
                            typeProperties.fieldList = a.fieldList
                                .filter(field => field.value && field.value.trim() !== '')
                                .map(field => {
                                    if (field.type === 'dynamic') {
                                        return {
                                            value: field.value || '',
                                            type: 'Expression'
                                        };
                                    } else {
                                        return field.value;
                                    }
                                });
                        }
                        
                        // Determine dataset location type
                        let locationType = null;
                        if (a.dataset && datasetContents[a.dataset]) {
                            const dsLocation = datasetContents[a.dataset].properties?.typeProperties?.location;
                            if (dsLocation) {
                                locationType = dsLocation.type;
                            }
                        }
                        
                        // For Blob/ADLS datasets, add storeSettings and formatSettings
                        if (locationType === 'AzureBlobFSLocation' || locationType === 'AzureBlobStorageLocation') {
                            const storeType = locationType === 'AzureBlobFSLocation' ? 'AzureBlobFSReadSettings' : 'AzureBlobStorageReadSettings';
                            
                            const storeSettings = {
                                type: storeType
                            };
                            
                            // Add modified datetime start if set
                            if (a.modifiedDatetimeStart) {
                                const startDate = new Date(a.modifiedDatetimeStart);
                                storeSettings.modifiedDatetimeStart = startDate.toISOString();
                            }
                            
                            // Add modified datetime end if set
                            if (a.modifiedDatetimeEnd) {
                                const endDate = new Date(a.modifiedDatetimeEnd);
                                storeSettings.modifiedDatetimeEnd = endDate.toISOString();
                            }
                            
                            storeSettings.enablePartitionDiscovery = false;
                            typeProperties.storeSettings = storeSettings;
                            
                            // Always add formatSettings for storage datasets
                            typeProperties.formatSettings = {
                                type: 'DelimitedTextReadSettings'
                            };
                            
                            // Add skipLineCount if set
                            if (a.skipLineCount && a.skipLineCount > 0) {
                                typeProperties.formatSettings.skipLineCount = parseInt(a.skipLineCount);
                            }
                        }
                        
                        // Remove UI-only fields from typeProperties
                        delete typeProperties.dataset;
                        delete typeProperties.fieldList;
                        delete typeProperties.timeoutSettings;
                        delete typeProperties.sleepSettings;
                        delete typeProperties.modifiedDatetimeStart;
                        delete typeProperties.modifiedDatetimeEnd;
                        delete typeProperties.skipLineCount;
                        delete typeProperties._datasetLocationType;
                        
                        // Re-add dataset and fieldList at root level of typeProperties
                        if (a.dataset) {
                            typeProperties.dataset = {
                                referenceName: a.dataset,
                                type: 'DatasetReference'
                            };
                        }
                        
                        if (a.fieldList && a.fieldList.length > 0) {
                            typeProperties.fieldList = a.fieldList
                                .filter(field => field.value && field.value.trim() !== '')
                                .map(field => {
                                    if (field.type === 'dynamic') {
                                        return {
                                            value: field.value || '',
                                            type: 'Expression'
                                        };
                                    } else {
                                        return field.value;
                                    }
                                });
                        }
                    }
                    
                    // Handle Lookup activity - build typeProperties with dataset and source settings
                    if (a.type === 'Lookup') {
                        // Build dataset reference
                        if (a.dataset) {
                            typeProperties.dataset = {
                                referenceName: a.dataset,
                                type: 'DatasetReference'
                            };
                        }
                        
                        // Add firstRowOnly (default to true if not specified)
                        typeProperties.firstRowOnly = a.firstRowOnly !== undefined ? a.firstRowOnly : true;
                        
                        // Determine dataset type
                        let datasetType = a._datasetType;
                        if (!datasetType && a.dataset && datasetContents[a.dataset]) {
                            datasetType = datasetContents[a.dataset].properties?.type;
                        }
                        
                        // For SQL datasets, add source configuration based on useQuery selection
                        if (datasetType === 'AzureSqlTable' || datasetType === 'AzureSynapseAnalytics') {
                            const source = {
                                type: datasetType === 'AzureSqlTable' ? 'AzureSqlSource' : 'SqlDWSource'
                            };
                            
                            // Add query or stored procedure based on useQuery selection
                            if (a.useQuery === 'Query' && a.sqlReaderQuery) {
                                source.sqlReaderQuery = a.sqlReaderQuery;
                            } else if (a.useQuery === 'Stored procedure' && a.sqlReaderStoredProcedureName) {
                                source.sqlReaderStoredProcedureName = a.sqlReaderStoredProcedureName;
                            }
                            
                            // Add query timeout in HH:MM:SS format
                            if (a.queryTimeout !== undefined && a.queryTimeout !== '') {
                                const minutes = parseInt(a.queryTimeout) || 120;
                                const hours = Math.floor(minutes / 60);
                                const mins = minutes % 60;
                                const hourStr = hours < 10 ? '0' + hours : hours.toString();
                                const minStr = mins < 10 ? '0' + mins : mins.toString();
                                source.queryTimeout = hourStr + ':' + minStr + ':00';
                            } else {
                                source.queryTimeout = "02:00:00"; // Default 2 hours
                            }
                            
                            // Add isolation level if specified
                            if (a.isolationLevel) {
                                source.isolationLevel = a.isolationLevel;
                            }
                            
                            // Add partition option - check all three partition option fields based on useQuery
                            let partitionOptionValue = 'None';
                            if (a.useQuery === 'Table' && a.partitionOption) {
                                partitionOptionValue = a.partitionOption;
                            } else if (a.useQuery === 'Query' && a.partitionOptionQuery) {
                                partitionOptionValue = a.partitionOptionQuery;
                            } else if (a.useQuery === 'Stored procedure' && a.partitionOptionStoredProc) {
                                partitionOptionValue = a.partitionOptionStoredProc;
                            }
                            source.partitionOption = partitionOptionValue;
                            
                            // Add partition settings if DynamicRange is selected
                            if (partitionOptionValue === 'DynamicRange') {
                                const partitionSettings = {};
                                
                                if (a.useQuery === 'Table') {
                                    if (a.partitionColumnName) partitionSettings.partitionColumnName = a.partitionColumnName;
                                    if (a.partitionUpperBound) partitionSettings.partitionUpperBound = a.partitionUpperBound;
                                    if (a.partitionLowerBound) partitionSettings.partitionLowerBound = a.partitionLowerBound;
                                } else if (a.useQuery === 'Query') {
                                    if (a.partitionColumnNameQuery) partitionSettings.partitionColumnName = a.partitionColumnNameQuery;
                                    if (a.partitionUpperBoundQuery) partitionSettings.partitionUpperBound = a.partitionUpperBoundQuery;
                                    if (a.partitionLowerBoundQuery) partitionSettings.partitionLowerBound = a.partitionLowerBoundQuery;
                                }
                                
                                if (Object.keys(partitionSettings).length > 0) {
                                    source.partitionSettings = partitionSettings;
                                }
                            }
                            
                            // Add stored procedure parameters if stored procedure is selected
                            if (a.useQuery === 'Stored procedure' && a.storedProcedureParameters && typeof a.storedProcedureParameters === 'object') {
                                source.storedProcedureParameters = a.storedProcedureParameters;
                            }
                            
                            typeProperties.source = source;
                        }
                        // For storage datasets (ADLS, Blob, etc.)
                        else if (datasetType === 'DelimitedText' || datasetType === 'Parquet' || datasetType === 'Json' || datasetType === 'Avro' || datasetType === 'ORC' || datasetType === 'Xml') {
                            let sourceType = datasetType + 'Source';
                            const source = {
                                type: sourceType
                            };
                            
                            // Determine store settings type from dataset
                            let storeType = 'AzureBlobFSReadSettings';
                            if (a.dataset && datasetContents[a.dataset]) {
                                const locationType = datasetContents[a.dataset].properties?.typeProperties?.location?.type;
                                if (locationType === 'AzureBlobFSLocation') {
                                    storeType = 'AzureBlobFSReadSettings';
                                } else if (locationType === 'AzureBlobStorageLocation') {
                                    storeType = 'AzureBlobStorageReadSettings';
                                }
                            }
                            
                            const storeSettings = {
                                type: storeType
                            };
                            
                            // Add conditional fields based on filePathType
                            if (a.filePathType === 'listOfFiles' && a.fileListPath) {
                                storeSettings.fileListPath = a.fileListPath;
                            } else if (a.filePathType === 'wildcardFilePath') {
                                if (a.wildcardFolderPath) {
                                    storeSettings.wildcardFolderPath = a.wildcardFolderPath;
                                }
                                if (a.wildcardFileName) {
                                    storeSettings.wildcardFileName = a.wildcardFileName;
                                }
                            } else if (a.filePathType === 'prefix' && a.prefix) {
                                storeSettings.prefix = a.prefix;
                            }
                            
                            // Add modified datetime filters if specified
                            if (a.modifiedDatetimeStart) {
                                const startDate = new Date(a.modifiedDatetimeStart);
                                storeSettings.modifiedDatetimeStart = startDate.toISOString();
                            }
                            if (a.modifiedDatetimeEnd) {
                                const endDate = new Date(a.modifiedDatetimeEnd);
                                storeSettings.modifiedDatetimeEnd = endDate.toISOString();
                            }
                            
                            // Add recursive flag (default to true)
                            storeSettings.recursive = a.recursive !== undefined ? a.recursive : true;
                            
                            // Add partition discovery
                            storeSettings.enablePartitionDiscovery = a.enablePartitionDiscovery || false;
                            
                            // Add partition root path if partition discovery is enabled
                            if (a.enablePartitionDiscovery && a.partitionRootPath) {
                                storeSettings.partitionRootPath = a.partitionRootPath;
                            }
                            
                            // Add max concurrent connections
                            if (a.maxConcurrentConnections) {
                                storeSettings.maxConcurrentConnections = parseInt(a.maxConcurrentConnections);
                            }
                            
                            source.storeSettings = storeSettings;
                            
                            // Add format settings for certain types
                            if (datasetType === 'DelimitedText') {
                                source.formatSettings = {
                                    type: 'DelimitedTextReadSettings'
                                };
                                if (a.skipLineCount && a.skipLineCount > 0) {
                                    source.formatSettings.skipLineCount = parseInt(a.skipLineCount);
                                }
                            } else if (datasetType === 'Xml') {
                                source.formatSettings = {
                                    type: 'XmlReadSettings'
                                };
                                
                                // Add validation mode
                                if (a.validationMode) {
                                    source.formatSettings.validationMode = a.validationMode;
                                }
                                
                                // Add detectDataType
                                if (a.detectDataType !== undefined) {
                                    source.formatSettings.detectDataType = a.detectDataType;
                                }
                                
                                // Add namespaces
                                if (a.namespaces !== undefined) {
                                    source.formatSettings.namespaces = a.namespaces;
                                }
                                
                                // Add namespace prefix pairs
                                if (a.namespacePrefixPairs && Object.keys(a.namespacePrefixPairs).length > 0) {
                                    source.formatSettings.namespacePrefixes = a.namespacePrefixPairs;
                                }
                            }
                            
                            typeProperties.source = source;
                        }
                        // For HTTP datasets
                        else if (datasetType === 'HttpFile') {
                            const source = {
                                type: 'HttpSource'
                            };
                            
                            // Add request settings
                            const requestSettings = {
                                requestMethod: a.requestMethod || 'GET'
                            };
                            
                            if (a.additionalHeaders) {
                                requestSettings.additionalHeaders = a.additionalHeaders;
                            }
                            
                            if (a.requestBody) {
                                requestSettings.requestBody = a.requestBody;
                            }
                            
                            if (a.requestTimeout) {
                                requestSettings.requestTimeout = a.requestTimeout;
                            }
                            
                            source.httpRequestTimeout = requestSettings.requestTimeout;
                            
                            // Add max concurrent connections
                            if (a.maxConcurrentConnections) {
                                source.maxConcurrentConnections = parseInt(a.maxConcurrentConnections);
                            }
                            
                            typeProperties.source = source;
                        }
                        
                        // Remove UI-only fields from typeProperties but NOT source
                        delete typeProperties.useQuery;
                        delete typeProperties.sqlReaderQuery;
                        delete typeProperties.sqlReaderStoredProcedureName;
                        delete typeProperties.queryTimeout;
                        delete typeProperties.isolationLevel;
                        delete typeProperties.partitionOption;
                        delete typeProperties.partitionOptionQuery;
                        delete typeProperties.partitionOptionStoredProc;
                        delete typeProperties.partitionColumnName;
                        delete typeProperties.partitionUpperBound;
                        delete typeProperties.partitionLowerBound;
                        delete typeProperties.partitionColumnNameQuery;
                        delete typeProperties.partitionUpperBoundQuery;
                        delete typeProperties.partitionLowerBoundQuery;
                        delete typeProperties.storedProcedureParameters;
                        delete typeProperties.filePathType;
                        delete typeProperties.prefix;
                        delete typeProperties.wildcardFolderPath;
                        delete typeProperties.wildcardFileName;
                        delete typeProperties.fileListPath;
                        delete typeProperties.modifiedDatetimeStart;
                        delete typeProperties.modifiedDatetimeEnd;
                        delete typeProperties.recursive;
                        delete typeProperties.enablePartitionDiscovery;
                        delete typeProperties.partitionRootPath;
                        delete typeProperties.maxConcurrentConnections;
                        delete typeProperties.skipLineCount;
                        delete typeProperties.requestMethod;
                        delete typeProperties.additionalHeaders;
                        delete typeProperties.requestBody;
                        delete typeProperties.requestTimeout;
                        delete typeProperties.validationMode;
                        delete typeProperties.namespaces;
                        delete typeProperties.namespacePrefixPairs;
                        delete typeProperties.detectDataType;
                        delete typeProperties._datasetType;
                        
                        // Do NOT delete dataset or source - they should be in typeProperties
                    }
                    
                    // Handle Delete activity - build complex storeSettings structure
                    if (a.type === 'Delete') {
                        // Build dataset reference
                        if (a.dataset) {
                            typeProperties.dataset = {
                                referenceName: a.dataset,
                                type: 'DatasetReference'
                            };
                        }
                        
                        typeProperties.enableLogging = false;
                        
                        // Delete activity always uses AzureBlobStorageReadSettings regardless of dataset type
                        const storeType = 'AzureBlobStorageReadSettings';
                        
                        const storeSettings = {
                            type: storeType,
                            enablePartitionDiscovery: false
                        };
                        
                        // Add conditional fields based on filePathType
                        if (a.filePathType === 'listOfFiles' && a.fileListPath) {
                            storeSettings.fileListPath = a.fileListPath;
                        } else if (a.filePathType === 'wildcardFilePath') {
                            if (a.wildcardFolderPath) storeSettings.wildcardFolderPath = a.wildcardFolderPath;
                            if (a.wildcardFileName) storeSettings.wildcardFileName = a.wildcardFileName;
                        } else if (a.filePathType === 'prefix' && a.prefix) {
                            storeSettings.prefix = a.prefix;
                        }
                        
                        // Add optional fields if set
                        if (a.maxConcurrentConnections) {
                            storeSettings.maxConcurrentConnections = parseInt(a.maxConcurrentConnections);
                        }
                        // recursive only applies for non-listOfFiles modes
                        if (a.recursive !== undefined && a.filePathType !== 'listOfFiles') {
                            storeSettings.recursive = a.recursive;
                        }
                        
                        // Add modified datetime filters (for filePathInDataset, wildcardFilePath, and prefix)
                        if (a.filePathType === 'filePathInDataset' || a.filePathType === 'wildcardFilePath' || a.filePathType === 'prefix') {
                            if (a.modifiedDatetimeStart) {
                                const startDate = new Date(a.modifiedDatetimeStart);
                                storeSettings.modifiedDatetimeStart = startDate.toISOString();
                            }
                            if (a.modifiedDatetimeEnd) {
                                const endDate = new Date(a.modifiedDatetimeEnd);
                                storeSettings.modifiedDatetimeEnd = endDate.toISOString();
                            }
                        }
                        
                        typeProperties.storeSettings = storeSettings;
                        
                        // Remove UI-only fields from typeProperties
                        delete typeProperties.dataset;
                        delete typeProperties.recursive;
                        delete typeProperties.maxConcurrentConnections;
                        delete typeProperties.filePathType;
                        delete typeProperties.prefix;
                        delete typeProperties.wildcardFolderPath;
                        delete typeProperties.wildcardFileName;
                        delete typeProperties.fileListPath;
                        delete typeProperties.modifiedDatetimeStart;
                        delete typeProperties.modifiedDatetimeEnd;
                        
                        // Re-add dataset at root level of typeProperties
                        if (a.dataset) {
                            typeProperties.dataset = {
                                referenceName: a.dataset,
                                type: 'DatasetReference'
                            };
                        }
                    }
                    
                    // Handle Script activity - build linkedServiceName and scripts array
                    if (a.type === 'Script') {
                        // Build linkedServiceName reference
                        if (a.linkedServiceName) {
                            activity.linkedServiceName = {
                                referenceName: typeof a.linkedServiceName === 'object' ? a.linkedServiceName.referenceName : a.linkedServiceName,
                                type: 'LinkedServiceReference'
                            };
                        }
                        
                        // Build scripts array
                        if (a.scripts && a.scripts.length > 0) {
                            typeProperties.scripts = a.scripts.map(script => {
                                const scriptObj = {
                                    type: script.type || 'Query'
                                };
                                
                                // Add text if present
                                if (script.text) {
                                    scriptObj.text = script.text;
                                }
                                
                                // Add parameters if present
                                if (script.parameters && script.parameters.length > 0) {
                                    scriptObj.parameters = script.parameters.map(param => {
                                        const paramObj = {
                                            name: param.name,
                                            type: param.type,
                                            value: param.value,
                                            direction: param.direction
                                        };
                                        
                                        // Add size only if direction is Output or InputOutput and type is String or Byte[]
                                        if ((param.direction === 'Output' || param.direction === 'InputOutput') && 
                                            (param.type === 'String' || param.type === 'Byte[]') && 
                                            param.size !== undefined) {
                                            paramObj.size = parseInt(param.size);
                                        }
                                        
                                        return paramObj;
                                    });
                                }
                                
                                return scriptObj;
                            });
                        }
                        
                        // Add scriptBlockExecutionTimeout if set
                        if (a.scriptBlockExecutionTimeout) {
                            typeProperties.scriptBlockExecutionTimeout = a.scriptBlockExecutionTimeout;
                        }
                        
                        // Remove UI-only fields from typeProperties
                        delete typeProperties.linkedServiceName;
                    }
                    
                    // Handle SqlServerStoredProcedure activity - build linkedServiceName and storedProcedureParameters
                    if (a.type === 'SqlServerStoredProcedure') {
                        // Build linkedServiceName reference (at activity level, not in typeProperties)
                        if (a.linkedServiceName) {
                            const linkedServiceRef = {
                                referenceName: typeof a.linkedServiceName === 'object' ? a.linkedServiceName.referenceName : a.linkedServiceName,
                                type: 'LinkedServiceReference'
                            };
                            
                            // Add parameters if it's Azure Synapse Analytics and has linked service properties
                            if (a._selectedLinkedServiceType === 'AzureSynapse' && a.linkedServiceProperties && a.linkedServiceProperties.DBName) {
                                linkedServiceRef.parameters = {
                                    DBName: a.linkedServiceProperties.DBName
                                };
                            }
                            
                            activity.linkedServiceName = linkedServiceRef;
                        }
                        
                        // Build storedProcedureName
                        if (a.storedProcedureName) {
                            typeProperties.storedProcedureName = a.storedProcedureName;
                        }
                        
                        // Build storedProcedureParameters only if present and not empty
                        if (a.storedProcedureParameters && typeof a.storedProcedureParameters === 'object' && Object.keys(a.storedProcedureParameters).length > 0) {
                            // Filter out parameters with empty names
                            const validParams = {};
                            for (const [paramName, paramData] of Object.entries(a.storedProcedureParameters)) {
                                if (paramName && paramName.trim() !== '') {
                                    validParams[paramName] = {
                                        value: paramData.value,
                                        type: paramData.type
                                    };
                                }
                            }
                            // Only add if there are valid parameters
                            if (Object.keys(validParams).length > 0) {
                                typeProperties.storedProcedureParameters = validParams;
                            }
                        }
                    }
                    
                    // Handle WebActivity - build authentication object and other settings
                    if (a.type === 'WebActivity') {
                        // Build authentication object based on authentication type
                        if (a.authenticationType && a.authenticationType !== 'None') {
                            const authentication = {};
                            
                            // Determine if we should include the type field
                            // For ServicePrincipal with Credential method, type is not needed
                            const isServicePrincipalCredential = a.authenticationType === 'ServicePrincipal' && 
                                                                  a.servicePrincipalAuthMethod === 'Credential';
                            
                            if (!isServicePrincipalCredential) {
                                authentication.type = a.authenticationType;
                            }
                            
                            // Basic authentication
                            if (a.authenticationType === 'Basic') {
                                if (a.username) authentication.username = a.username;
                                if (a.password) {
                                    // Check if password is an Azure Key Vault Secret object
                                    if (typeof a.password === 'object' && a.password.type === 'AzureKeyVaultSecret') {
                                        authentication.password = {
                                            type: 'AzureKeyVaultSecret',
                                            store: a.password.store,
                                            secretName: a.password.secretName
                                        };
                                        // Only include secretVersion if it's not 'latest'
                                        if (a.password.secretVersion && a.password.secretVersion !== 'latest') {
                                            authentication.password.secretVersion = a.password.secretVersion;
                                        }
                                    } else {
                                        // Plain text password
                                        authentication.password = a.password;
                                    }
                                }
                            }
                            // System-assigned managed identity
                            else if (a.authenticationType === 'MSI') {
                                if (a.resource) authentication.resource = a.resource;
                            }
                            // Client certificate
                            else if (a.authenticationType === 'ClientCertificate') {
                                if (a.pfx) {
                                    // Check if pfx is an Azure Key Vault Secret object
                                    if (typeof a.pfx === 'object' && a.pfx.type === 'AzureKeyVaultSecret') {
                                        authentication.pfx = {
                                            type: 'AzureKeyVaultSecret',
                                            store: a.pfx.store,
                                            secretName: a.pfx.secretName
                                        };
                                        // Only include secretVersion if it's not 'latest'
                                        if (a.pfx.secretVersion && a.pfx.secretVersion !== 'latest') {
                                            authentication.pfx.secretVersion = a.pfx.secretVersion;
                                        }
                                    } else {
                                        // Plain text pfx
                                        authentication.pfx = a.pfx;
                                    }
                                }
                                if (a.pfxPassword) {
                                    // Check if pfxPassword is an Azure Key Vault Secret object
                                    if (typeof a.pfxPassword === 'object' && a.pfxPassword.type === 'AzureKeyVaultSecret') {
                                        authentication.password = {
                                            type: 'AzureKeyVaultSecret',
                                            store: a.pfxPassword.store,
                                            secretName: a.pfxPassword.secretName
                                        };
                                        // Only include secretVersion if it's not 'latest'
                                        if (a.pfxPassword.secretVersion && a.pfxPassword.secretVersion !== 'latest') {
                                            authentication.password.secretVersion = a.pfxPassword.secretVersion;
                                        }
                                    } else {
                                        // Plain text password
                                        authentication.password = a.pfxPassword;
                                    }
                                }
                            }
                            // Service principal
                            else if (a.authenticationType === 'ServicePrincipal') {
                                if (a.servicePrincipalResource) authentication.resource = a.servicePrincipalResource;
                                
                                // Inline authentication
                                if (a.servicePrincipalAuthMethod === 'Inline' || !a.servicePrincipalAuthMethod) {
                                    if (a.tenant) authentication.userTenant = a.tenant;
                                    if (a.servicePrincipalId) authentication.username = a.servicePrincipalId;
                                    
                                    // Service principal key or certificate
                                    if (a.servicePrincipalCredentialType === 'Service Principal Key' || !a.servicePrincipalCredentialType) {
                                        if (a.servicePrincipalKey) {
                                            // Check if servicePrincipalKey is an Azure Key Vault Secret object
                                            if (typeof a.servicePrincipalKey === 'object' && a.servicePrincipalKey.type === 'AzureKeyVaultSecret') {
                                                authentication.password = {
                                                    type: 'AzureKeyVaultSecret',
                                                    store: a.servicePrincipalKey.store,
                                                    secretName: a.servicePrincipalKey.secretName
                                                };
                                                // Only include secretVersion if it's not 'latest'
                                                if (a.servicePrincipalKey.secretVersion && a.servicePrincipalKey.secretVersion !== 'latest') {
                                                    authentication.password.secretVersion = a.servicePrincipalKey.secretVersion;
                                                }
                                            } else {
                                                authentication.password = a.servicePrincipalKey;
                                            }
                                        }
                                    } else if (a.servicePrincipalCredentialType === 'Service Principal Certificate') {
                                        if (a.servicePrincipalCert) {
                                            // Check if servicePrincipalCert is an Azure Key Vault Secret object
                                            if (typeof a.servicePrincipalCert === 'object' && a.servicePrincipalCert.type === 'AzureKeyVaultSecret') {
                                                authentication.pfx = {
                                                    type: 'AzureKeyVaultSecret',
                                                    store: a.servicePrincipalCert.store,
                                                    secretName: a.servicePrincipalCert.secretName
                                                };
                                                // Only include secretVersion if it's not 'latest'
                                                if (a.servicePrincipalCert.secretVersion && a.servicePrincipalCert.secretVersion !== 'latest') {
                                                    authentication.pfx.secretVersion = a.servicePrincipalCert.secretVersion;
                                                }
                                            } else {
                                                authentication.pfx = a.servicePrincipalCert;
                                            }
                                        }
                                    }
                                }
                                // Credential-based authentication
                                else if (a.servicePrincipalAuthMethod === 'Credential') {
                                    if (a.credential) {
                                        authentication.credential = {
                                            referenceName: a.credential,
                                            type: 'CredentialReference'
                                        };
                                    }
                                    if (a.credentialResource) authentication.resource = a.credentialResource;
                                }
                            }
                            // User-assigned managed identity
                            else if (a.authenticationType === 'UserAssignedManagedIdentity') {
                                if (a.credentialUserAssigned) {
                                    authentication.credential = {
                                        referenceName: a.credentialUserAssigned,
                                        type: 'CredentialReference'
                                    };
                                }
                                if (a.resource) authentication.resource = a.resource;
                            }
                            
                            typeProperties.authentication = authentication;
                        }
                        
                        // Remove authentication-related fields from typeProperties as they're now in the authentication object
                        delete typeProperties.authenticationType;
                        delete typeProperties.username;
                        delete typeProperties.password;
                        delete typeProperties.resource;
                        delete typeProperties.pfx;
                        delete typeProperties.pfxPassword;
                        delete typeProperties.servicePrincipalAuthMethod;
                        delete typeProperties.tenant;
                        delete typeProperties.servicePrincipalId;
                        delete typeProperties.servicePrincipalCredentialType;
                        delete typeProperties.servicePrincipalKey;
                        delete typeProperties.servicePrincipalCert;
                        delete typeProperties.servicePrincipalResource;
                        delete typeProperties.credential;
                        delete typeProperties.credentialResource;
                        delete typeProperties.credentialUserAssigned;
                        
                        // Convert headers array to object format
                        if (a.headers && a.headers.length > 0) {
                            const headersObj = {};
                            a.headers.forEach(header => {
                                if (header.name && header.value) {
                                    headersObj[header.name] = header.value;
                                }
                            });
                            typeProperties.headers = headersObj;
                        }
                        
                        // Build optional advanced settings
                        if (a.httpRequestTimeout) typeProperties.httpRequestTimeout = a.httpRequestTimeout;
                        if (a.disableAsyncPattern) typeProperties.turnOffAsync = a.disableAsyncPattern;
                        if (a.disableCertValidation) typeProperties.disableCertValidation = a.disableCertValidation;
                        
                        // Add connectVia integration runtime reference from config
                        typeProperties.connectVia = {
                            referenceName: ${JSON.stringify(irConfig)}.integrationRuntime.name,
                            type: 'IntegrationRuntimeReference'
                        };
                        
                        // Remove the UI field names
                        delete typeProperties.disableAsyncPattern;
                    }
                    
                    // Handle WebHook - build authentication object and other settings
                    if (a.type === 'WebHook') {
                        // Build authentication object based on authentication type
                        if (a.authenticationType && a.authenticationType !== 'None') {
                            const authentication = {};
                            
                            // Determine if we should include the type field
                            const isServicePrincipalCredential = a.authenticationType === 'ServicePrincipal' && 
                                                                  a.servicePrincipalAuthMethod === 'Credential';
                            
                            if (!isServicePrincipalCredential) {
                                authentication.type = a.authenticationType;
                            }
                            
                            // Basic authentication
                            if (a.authenticationType === 'Basic') {
                                if (a.username) authentication.username = a.username;
                                if (a.password) {
                                    // Check if password is an Azure Key Vault Secret object
                                    if (typeof a.password === 'object' && a.password.type === 'AzureKeyVaultSecret') {
                                        authentication.password = {
                                            type: 'AzureKeyVaultSecret',
                                            store: a.password.store,
                                            secretName: a.password.secretName
                                        };
                                        // Only include secretVersion if it's not 'latest'
                                        if (a.password.secretVersion && a.password.secretVersion !== 'latest') {
                                            authentication.password.secretVersion = a.password.secretVersion;
                                        }
                                    } else {
                                        // Plain text password
                                        authentication.password = a.password;
                                    }
                                }
                            }
                            // System-assigned managed identity
                            else if (a.authenticationType === 'MSI') {
                                if (a.resource) authentication.resource = a.resource;
                            }
                            // Client certificate
                            else if (a.authenticationType === 'ClientCertificate') {
                                if (a.pfx) {
                                    // Check if pfx is an Azure Key Vault Secret object
                                    if (typeof a.pfx === 'object' && a.pfx.type === 'AzureKeyVaultSecret') {
                                        authentication.pfx = {
                                            type: 'AzureKeyVaultSecret',
                                            store: a.pfx.store,
                                            secretName: a.pfx.secretName
                                        };
                                        // Only include secretVersion if it's not 'latest'
                                        if (a.pfx.secretVersion && a.pfx.secretVersion !== 'latest') {
                                            authentication.pfx.secretVersion = a.pfx.secretVersion;
                                        }
                                    } else {
                                        // Plain text pfx
                                        authentication.pfx = a.pfx;
                                    }
                                }
                                if (a.pfxPassword) {
                                    // Check if pfxPassword is an Azure Key Vault Secret object
                                    if (typeof a.pfxPassword === 'object' && a.pfxPassword.type === 'AzureKeyVaultSecret') {
                                        authentication.password = {
                                            type: 'AzureKeyVaultSecret',
                                            store: a.pfxPassword.store,
                                            secretName: a.pfxPassword.secretName
                                        };
                                        // Only include secretVersion if it's not 'latest'
                                        if (a.pfxPassword.secretVersion && a.pfxPassword.secretVersion !== 'latest') {
                                            authentication.password.secretVersion = a.pfxPassword.secretVersion;
                                        }
                                    } else {
                                        // Plain text password
                                        authentication.password = a.pfxPassword;
                                    }
                                }
                            }
                            // Service principal
                            else if (a.authenticationType === 'ServicePrincipal') {
                                if (a.servicePrincipalResource) authentication.resource = a.servicePrincipalResource;
                                
                                // Inline authentication
                                if (a.servicePrincipalAuthMethod === 'Inline' || !a.servicePrincipalAuthMethod) {
                                    if (a.tenant) authentication.userTenant = a.tenant;
                                    if (a.servicePrincipalId) authentication.username = a.servicePrincipalId;
                                    
                                    // Service principal key or certificate
                                    if (a.servicePrincipalCredentialType === 'Service Principal Key' || !a.servicePrincipalCredentialType) {
                                        if (a.servicePrincipalKey) {
                                            // Check if servicePrincipalKey is an Azure Key Vault Secret object
                                            if (typeof a.servicePrincipalKey === 'object' && a.servicePrincipalKey.type === 'AzureKeyVaultSecret') {
                                                authentication.password = {
                                                    type: 'AzureKeyVaultSecret',
                                                    store: a.servicePrincipalKey.store,
                                                    secretName: a.servicePrincipalKey.secretName
                                                };
                                                // Only include secretVersion if it's not 'latest'
                                                if (a.servicePrincipalKey.secretVersion && a.servicePrincipalKey.secretVersion !== 'latest') {
                                                    authentication.password.secretVersion = a.servicePrincipalKey.secretVersion;
                                                }
                                            } else {
                                                authentication.password = a.servicePrincipalKey;
                                            }
                                        }
                                    } else if (a.servicePrincipalCredentialType === 'Service Principal Certificate') {
                                        if (a.servicePrincipalCert) {
                                            // Check if servicePrincipalCert is an Azure Key Vault Secret object
                                            if (typeof a.servicePrincipalCert === 'object' && a.servicePrincipalCert.type === 'AzureKeyVaultSecret') {
                                                authentication.pfx = {
                                                    type: 'AzureKeyVaultSecret',
                                                    store: a.servicePrincipalCert.store,
                                                    secretName: a.servicePrincipalCert.secretName
                                                };
                                                // Only include secretVersion if it's not 'latest'
                                                if (a.servicePrincipalCert.secretVersion && a.servicePrincipalCert.secretVersion !== 'latest') {
                                                    authentication.pfx.secretVersion = a.servicePrincipalCert.secretVersion;
                                                }
                                            } else {
                                                authentication.pfx = a.servicePrincipalCert;
                                            }
                                        }
                                    }
                                }
                                // Credential-based authentication
                                else if (a.servicePrincipalAuthMethod === 'Credential') {
                                    if (a.credential) {
                                        authentication.credential = {
                                            referenceName: a.credential,
                                            type: 'CredentialReference'
                                        };
                                    }
                                    if (a.credentialResource) authentication.resource = a.credentialResource;
                                }
                            }
                            // User-assigned managed identity
                            else if (a.authenticationType === 'UserAssignedManagedIdentity') {
                                if (a.credentialUserAssigned) {
                                    authentication.credential = {
                                        referenceName: a.credentialUserAssigned,
                                        type: 'CredentialReference'
                                    };
                                }
                                if (a.resource) authentication.resource = a.resource;
                            }
                            
                            typeProperties.authentication = authentication;
                        }
                        
                        // Remove authentication-related fields from typeProperties
                        delete typeProperties.authenticationType;
                        delete typeProperties.username;
                        delete typeProperties.password;
                        delete typeProperties.resource;
                        delete typeProperties.pfx;
                        delete typeProperties.pfxPassword;
                        delete typeProperties.servicePrincipalAuthMethod;
                        delete typeProperties.tenant;
                        delete typeProperties.servicePrincipalId;
                        delete typeProperties.servicePrincipalCredentialType;
                        delete typeProperties.servicePrincipalKey;
                        delete typeProperties.servicePrincipalCert;
                        delete typeProperties.servicePrincipalResource;
                        delete typeProperties.credential;
                        delete typeProperties.credentialResource;
                        delete typeProperties.credentialUserAssigned;
                        
                        // Convert headers array to object format
                        if (a.headers && a.headers.length > 0) {
                            const headersObj = {};
                            a.headers.forEach(header => {
                                if (header.name && header.value) {
                                    headersObj[header.name] = header.value;
                                }
                            });
                            typeProperties.headers = headersObj;
                        }
                        
                        // Remove these fields from the initial typeProperties copy
                        delete typeProperties.reportStatusOnCallBack;
                        delete typeProperties.disableCertValidation;
                        delete typeProperties.secureOutput;
                        delete typeProperties.secureInput;
                        
                        // Add timeout to typeProperties (default is 00:10:00 for WebHook)
                        typeProperties.timeout = a.timeout || '00:10:00';
                        
                        // Only add reportStatusOnCallBack if true
                        if (a.reportStatusOnCallBack === true) {
                            typeProperties.reportStatusOnCallBack = true;
                        }
                        
                        // Only add disableCertValidation if true
                        if (a.disableCertValidation === true) {
                            typeProperties.disableCertValidation = true;
                        }
                    }
                    
                    // Handle IfCondition activity
                    if (a.type === 'IfCondition') {
                        // Remove internal properties that shouldn't be serialized
                        delete typeProperties.isContainer;
                        delete typeProperties.ifTrueActivities;
                        delete typeProperties.ifFalseActivities;
                        
                        // Convert expression from string to Expression object format
                        if (a.expression !== undefined) {
                            typeProperties.expression = {
                                value: a.expression || '',
                                type: 'Expression'
                            };
                        }
                        
                        // Add nested activities only if they exist
                        // Strip x, y properties which are UI-only and shouldn't be saved to file
                        if (a.ifTrueActivities && a.ifTrueActivities.length > 0) {
                            typeProperties.ifTrueActivities = a.ifTrueActivities.map(nestedAct => {
                                const cleanAct = { ...nestedAct };
                                delete cleanAct.x;
                                delete cleanAct.y;
                                return cleanAct;
                            });
                        }
                        
                        if (a.ifFalseActivities && a.ifFalseActivities.length > 0) {
                            typeProperties.ifFalseActivities = a.ifFalseActivities.map(nestedAct => {
                                const cleanAct = { ...nestedAct };
                                delete cleanAct.x;
                                delete cleanAct.y;
                                return cleanAct;
                            });
                        }
                    }
                    
                    // Handle Switch activity
                    if (a.type === 'Switch') {
                        // Remove internal properties that shouldn't be serialized
                        delete typeProperties.isContainer;
                        delete typeProperties.cases;
                        delete typeProperties.defaultActivities;
                        
                        // Convert on expression from string to Expression object format
                        if (a.on !== undefined) {
                            typeProperties.on = {
                                value: a.on || '',
                                type: 'Expression'
                            };
                        }
                        
                        // Serialize cases array
                        if (a.cases && a.cases.length > 0) {
                            typeProperties.cases = a.cases.map(c => ({
                                value: c.value || '',
                                activities: (c.activities || []).map(nestedAct => {
                                    const cleanAct = { ...nestedAct };
                                    delete cleanAct.x;
                                    delete cleanAct.y;
                                    return cleanAct;
                                })
                            }));
                        }
                        
                        // Serialize defaultActivities
                        if (a.defaultActivities && a.defaultActivities.length > 0) {
                            typeProperties.defaultActivities = a.defaultActivities.map(nestedAct => {
                                const cleanAct = { ...nestedAct };
                                delete cleanAct.x;
                                delete cleanAct.y;
                                return cleanAct;
                            });
                        }
                    }
                    
                    // Handle ForEach activity
                    if (a.type === 'ForEach') {
                        // Remove internal properties that shouldn't be serialized
                        delete typeProperties.isContainer;
                        delete typeProperties.activities;
                        
                        // Convert items from string to Expression object format
                        if (a.items !== undefined) {
                            typeProperties.items = {
                                value: a.items || '',
                                type: 'Expression'
                            };
                        }
                        
                        // Set isSequential
                        typeProperties.isSequential = a.isSequential === true;
                        
                        // Set batchCount only if parallel and a value is provided
                        if (!typeProperties.isSequential && a.batchCount !== undefined && a.batchCount !== null && a.batchCount !== '') {
                            const batchNum = parseInt(a.batchCount);
                            if (!isNaN(batchNum) && batchNum >= 1) {
                                typeProperties.batchCount = batchNum;
                            } else {
                                delete typeProperties.batchCount;
                            }
                        } else {
                            delete typeProperties.batchCount;
                        }
                        
                        // Add nested activities only if they exist
                        // Recursively strip x, y and _variableType/_pipelineVariableType
                        // metadata at all nesting depths (IfCondition branches, etc.)
                        if (a.activities && a.activities.length > 0) {
                            typeProperties.activities = a.activities.map(deepCleanNestedActivity);
                        }
                    }

                    // Handle Until activity
                    if (a.type === 'Until') {
                        delete typeProperties.isContainer;
                        delete typeProperties.activities;
                        if (a.expression !== undefined) {
                            typeProperties.expression = { value: a.expression || '', type: 'Expression' };
                        }
                        if (a.timeout) typeProperties.timeout = a.timeout;
                        if (a.activities && a.activities.length > 0) {
                            typeProperties.activities = a.activities.map(deepCleanNestedActivity);
                        }
                    }

                    // For Copy: merge pre-built source/sink (built before the generic loop) into typeProperties
                    if (a.type === 'Copy' && activity.typeProperties) {
                        if (activity.typeProperties.source) typeProperties.source = activity.typeProperties.source;
                        if (activity.typeProperties.sink) typeProperties.sink = activity.typeProperties.sink;
                        // Suppress translator for non-tabular sink formats (e.g. AzureSqlDWTable)
                        const _snkTypeConfMerge = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[a._sinkDatasetType];
                        if (_snkTypeConfMerge && _snkTypeConfMerge.noTranslator) delete typeProperties.translator;
                    }
                    activity.typeProperties = typeProperties;
                    return activity;
                })
            };
            
            // Add variables if any exist
            if (Object.keys(variables).length > 0) {
                // Process variables to convert numeric types and filter temp keys
                const processedVars = {};
                for (const [key, varData] of Object.entries(variables)) {
                    // Skip temporary keys that haven't been renamed
                    if (key.startsWith('_temp_var_')) continue;
                    
                    processedVars[key] = {
                        type: varData.type
                    };
                    
                    // Only include defaultValue if one was explicitly set
                    if (varData.defaultValue !== undefined && varData.defaultValue !== null && varData.defaultValue !== '') {
                        if (varData.type === 'Integer') {
                            const numValue = parseInt(varData.defaultValue);
                            if (!isNaN(numValue)) processedVars[key].defaultValue = numValue;
                        } else if (varData.type === 'Boolean') {
                            processedVars[key].defaultValue = varData.defaultValue === 'true' || varData.defaultValue === true;
                        } else {
                            processedVars[key].defaultValue = varData.defaultValue;
                        }
                    }
                }
                if (Object.keys(processedVars).length > 0) {
                    result.variables = processedVars;
                }
            }
            
            // Add parameters if any exist
            if (Object.keys(pipelineData.parameters).length > 0) {
                // Process parameters to convert numeric types and filter temp keys
                const processedParams = {};
                for (const [key, param] of Object.entries(pipelineData.parameters)) {
                    // Skip temporary keys that haven't been renamed
                    if (key.startsWith('_temp_param_')) continue;
                    
                    processedParams[key] = {
                        type: param.type
                    };
                    
                    // Convert numeric types to actual numbers, not strings
                    if (param.type === 'int' || param.type === 'float') {
                        const numValue = parseFloat(param.defaultValue);
                        processedParams[key].defaultValue = isNaN(numValue) ? 0 : numValue;
                    } else if (param.type === 'bool') {
                        // Convert to boolean
                        processedParams[key].defaultValue = param.defaultValue === 'true' || param.defaultValue === true;
                    } else {
                        // Keep as string for string, array, object types
                        processedParams[key].defaultValue = param.defaultValue || '';
                    }
                }
                if (Object.keys(processedParams).length > 0) {
                    result.parameters = processedParams;
                }
            }
            
            // Add concurrency if not default (1)
            if (pipelineData.concurrency && pipelineData.concurrency !== 1) {
                result.concurrency = parseInt(pipelineData.concurrency);
            }
            
            return result;
        }
        
        // Send current pipeline state to extension for caching
        function sendCurrentStateToExtension() {
            // Extract filename from path without using Node.js path module
            let pipelineName = 'pipeline1';
            if (currentFilePath) {
                const parts = currentFilePath.replace(/\\\\/g, '/').split('/');
                const filename = parts[parts.length - 1];
                pipelineName = filename.replace('.json', '');
            }
            
            // Use shared function to build pipeline data (skip validation for caching)
            const data = buildPipelineDataForSave(pipelineName, true);
            vscode.postMessage({ type: 'cacheState', data: data, filePath: currentFilePath });
        }
        
        // Clear dirty state
        function clearDirty() {
            isDirty = false;
            originalState = captureCurrentState();
            vscode.postMessage({ type: 'contentChanged', isDirty: false });
        }
        
        // Capture current state for comparison
        function captureCurrentState() {
            return JSON.stringify({
                activities: activities.map(a => ({
                    id: a.id,
                    type: a.type,
                    name: a.name,
                    x: a.x,
                    y: a.y,
                    description: a.description
                })),
                connections: connections.map(c => ({
                    from: c.from,
                    to: c.to,
                    type: c.type
                }))
            });
        }
        let connectionStart = null;
        let isDragging = false;
        let dragOffset = { x: 0, y: 0 };
        let isPanning = false;
        let panStart = { x: 0, y: 0, scrollLeft: 0, scrollTop: 0 };
        let scale = 1;
        let panOffset = { x: 0, y: 0 };
        let animationFrameId = null;
        let needsRedraw = false;

        // Check if elements exist
        console.log('Sidebar elements:', document.querySelectorAll('.activity-item').length);
        console.log('Config panel:', document.getElementById('configContent'));
        console.log('General content:', document.getElementById('generalContent'));
        console.log('Config tabs:', document.querySelectorAll('.config-tab').length);

        // Initialize canvas
        function resizeCanvas() {
            const wrapper = document.getElementById('canvasWrapper');
            canvas.width = Math.max(wrapper.clientWidth, 2000);
            canvas.height = Math.max(wrapper.clientHeight, 2000);
            draw();
        }

        resizeCanvas();
        window.addEventListener('resize', resizeCanvas);

        // Activity class
        class Activity {
            constructor(type, x, y, container) {
                this.id = Date.now() + Math.random();
                this.type = type;
                this.x = x;
                this.y = y;
                this.width = 180;
                this.height = 56;
                this.name = type === 'SqlServerStoredProcedure' ? 'Stored procedure1' : type;
                this.description = '';
                this.color = this.getColorForType(type);
                this.container = container;
                this.element = null;
                
                // Set default values for SetVariable
                if (type === 'SetVariable') {
                    this.variableType = 'Pipeline variable';
                    this.pipelineVariableType = 'String';
                    this.secureOutput = false;
                    this.secureInput = false;
                }
                
                // Set default values for Wait
                if (type === 'Wait') {
                    this.waitTimeInSeconds = 1;
                }
                
                // Set default values for Delete
                if (type === 'Delete') {
                    this.filePathType = 'filePathInDataset';
                    this.recursive = false;
                }
                
                // Set default values for GetMetadata
                if (type === 'GetMetadata') {
                    this.fieldList = [];
                }
                
                // Set default values for Lookup
                if (type === 'Lookup') {
                    this.useQuery = 'Table';
                    this.firstRowOnly = true;
                    this.timeout = '0.12:00:00';
                    this.retry = 0;
                    this.retryIntervalInSeconds = 30;
                    this.secureOutput = false;
                    this.secureInput = false;
                }
                
                // Set default values for Script
                if (type === 'Script') {
                    this.scripts = [{ type: 'Query', text: '', parameters: [] }];
                    this.scriptBlockExecutionTimeout = '02:00:00';
                    this.timeout = '0.12:00:00';
                    this.retry = 0;
                    this.retryIntervalInSeconds = 30;
                    this.secureOutput = false;
                    this.secureInput = false;
                }
                
                // Set default values for SqlServerStoredProcedure
                if (type === 'SqlServerStoredProcedure') {
                    this.timeout = '0.12:00:00';
                    this.retry = 0;
                    this.retryIntervalInSeconds = 30;
                    this.secureOutput = false;
                    this.secureInput = false;
                    this.storedProcedureParameters = {};
                }
                
                // Set default values for WebHook
                if (type === 'WebHook') {
                    this.method = 'POST';
                    this.timeout = '00:10:00';
                    this.authenticationType = 'None';
                    this.disableCertValidation = false;
                    this.reportStatusOnCallBack = false;
                }
                
                // Set default values for IfCondition
                if (type === 'IfCondition') {
                    this.isContainer = true;
                    this.expression = '';
                    this.ifTrueActivities = [];
                    this.ifFalseActivities = [];
                    this.state = 'Activated';
                }
                
                // Set default values for ForEach
                if (type === 'ForEach') {
                    this.isContainer = true;
                    this.items = '';
                    this.isSequential = false;
                    this.activities = [];
                    this.state = 'Activated';
                }
                
                // Set default values for Until
                if (type === 'Until') {
                    this.isContainer = true;
                    this.expression = '';
                    this.timeout = '0.12:00:00';
                    this.activities = [];
                    this.state = 'Activated';
                }
                
                // Set default values for Switch
                if (type === 'Switch') {
                    this.isContainer = true;
                    this.on = '';
                    this.cases = []; // array of {value: string, activities: []}
                    this.defaultActivities = [];
                    this.state = 'Activated';
                }
                
                this.createDOMElement();
            }

            getColorForType(type) {
                const colors = {
                    'Copy': '#0078d4',
                    'Delete': '#d13438',
                    'Dataflow': '#00a4ef',
                    'Notebook': '#f2c811',
                    'ForEach': '#7fba00',
                    'IfCondition': '#ff8c00',
                    'Until': '#e81123',
                    'Switch': '#6264a7',
                    'Wait': '#00bcf2',
                    'WebActivity': '#8661c5',
                    'WebHook': '#9b59b6',
                    'StoredProcedure': '#847545'
                };
                return colors[type] || '#0078d4';
            }

            createDOMElement() {
                // Remove existing element if present
                if (this.element && this.element.parentNode) {
                    console.log('[createDOMElement] Removing existing element for:', this.name, this.id);
                    this.element.parentNode.removeChild(this.element);
                    this.element = null;
                }
                
                // For container activities (IfCondition, ForEach, Until, Switch), use special rendering
                if (this.isContainer) {
                    this.createContainerElement();
                    return;
                }
                
                console.log('[createDOMElement] Creating new element for:', this.name, this.id);
                // Create the main activity box element
                this.element = document.createElement('div');
                this.element.className = 'activity-box';
                this.element.style.left = this.x + 'px';
                this.element.style.top = this.y + 'px';
                this.element.style.setProperty('--activity-color', this.color);
                this.element.dataset.activityId = this.id;
                
                // Create header
                const header = document.createElement('div');
                header.className = 'activity-header';
                const typeLabel = document.createElement('span');
                typeLabel.className = 'activity-type-label';
                typeLabel.textContent = this.getTypeLabel();
                header.appendChild(typeLabel);
                
                // Create body
                const body = document.createElement('div');
                body.className = 'activity-body';
                
                const icon = document.createElement('div');
                icon.className = 'activity-icon-large';
                icon.textContent = this.getIcon();
                
                const label = document.createElement('div');
                label.className = 'activity-label';
                label.textContent = this.name;
                
                body.appendChild(icon);
                body.appendChild(label);
                
                // Create action buttons section (hidden by default, shown when selected)
                const actions = document.createElement('div');
                actions.className = 'activity-actions';
                
                const deleteBtn = document.createElement('button');
                deleteBtn.className = 'action-icon-btn';
                deleteBtn.innerHTML = '×';
                deleteBtn.title = 'Delete';
                deleteBtn.onclick = (e) => {
                    e.stopPropagation();
                    this.handleDelete();
                };
                
                const editBtn = document.createElement('button');
                editBtn.className = 'action-icon-btn';
                editBtn.innerHTML = '{}';
                editBtn.title = 'Edit JSON';
                editBtn.onclick = (e) => e.stopPropagation();
                
                const copyBtn = document.createElement('button');
                copyBtn.className = 'action-icon-btn';
                copyBtn.innerHTML = '⎘';
                copyBtn.title = 'Copy';
                copyBtn.onclick = (e) => e.stopPropagation();
                
                const infoBtn = document.createElement('button');
                infoBtn.className = 'action-icon-btn info';
                infoBtn.innerHTML = 'i';
                infoBtn.title = 'Info';
                infoBtn.onclick = (e) => e.stopPropagation();
                
                actions.appendChild(deleteBtn);
                actions.appendChild(editBtn);
                actions.appendChild(copyBtn);
                actions.appendChild(infoBtn);
                
                // Add connection points
                const positions = ['top', 'right', 'bottom', 'left'];
                positions.forEach(pos => {
                    const point = document.createElement('div');
                    point.className = 'connection-point ' + pos;
                    point.dataset.position = pos;
                    point.dataset.activityId = this.id;
                    this.element.appendChild(point);
                });
                
                // Assemble element
                this.element.appendChild(header);
                this.element.appendChild(body);
                this.element.appendChild(actions);
                
                // Add to container
                this.container.appendChild(this.element);
                
                // Set up event listeners
                this.setupEventListeners();
            }
            
            handleDelete() {
                if (confirm(\`Delete activity "\${this.name}"?\`)) {
                    activities = activities.filter(a => a !== this);
                    connections = connections.filter(c => c.from !== this && c.to !== this);
                    this.remove();
                    selectedActivity = null;
                    showProperties(null);
                    draw();
                }
            }
            
            createContainerElement() {
                // Remove existing element if present
                if (this.element && this.element.parentNode) {
                    console.log('[createContainerElement] Removing existing element for:', this.name, this.id);
                    this.element.parentNode.removeChild(this.element);
                    this.element = null;
                }
                
                console.log('[createContainerElement] Creating new element for:', this.name, this.id);
                // Create container activity element (larger, shows info about nested activities)
                this.element = document.createElement('div');
                this.element.className = 'activity-box container-activity';
                this.element.style.left = this.x + 'px';
                this.element.style.top = this.y + 'px';
                this.element.style.setProperty('--activity-color', this.color);
                this.element.dataset.activityId = this.id;
                
                // Create header
                const header = document.createElement('div');
                header.className = 'activity-header';
                const typeLabel = document.createElement('span');
                typeLabel.className = 'activity-type-label';
                typeLabel.textContent = this.getTypeLabel();
                header.appendChild(typeLabel);
                
                // Create body with icon and label
                const body = document.createElement('div');
                body.className = 'activity-body';
                
                const icon = document.createElement('div');
                icon.className = 'activity-icon-large';
                icon.textContent = this.getIcon();
                
                const label = document.createElement('div');
                label.className = 'activity-label';
                label.textContent = this.name;
                
                body.appendChild(icon);
                body.appendChild(label);
                
                // Create info section for container-specific information
                const infoSection = document.createElement('div');
                infoSection.className = 'container-info';
                
                // Different info display logic based on container type
                if (this.type === 'IfCondition') {
                    const trueCount = this.ifTrueActivities ? this.ifTrueActivities.length : 0;
                    const falseCount = this.ifFalseActivities ? this.ifFalseActivities.length : 0;
                    
                    infoSection.innerHTML = \`
                        <div class="container-stat">
                            <span class="branch-label">True:</span> 
                            <span class="count">\${trueCount}</span> 
                            <span class="label">\${trueCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                        <div class="container-stat">
                            <span class="branch-label">False:</span> 
                            <span class="count">\${falseCount}</span> 
                            <span class="label">\${falseCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                    \`;
                } else if (this.type === 'ForEach') {
                    const itemsCount = this.activities ? this.activities.length : 0;
                    infoSection.innerHTML = \`
                        <div class="container-stat">
                            <span class="label">Items:</span> 
                            <span class="count">\${itemsCount}</span> 
                            <span class="label">\${itemsCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                    \`;
                } else if (this.type === 'Until') {
                    const loopCount = this.activities ? this.activities.length : 0;
                    infoSection.innerHTML = \`
                        <div class="container-stat">
                            <span class="label">Loop:</span> 
                            <span class="count">\${loopCount}</span> 
                            <span class="label">\${loopCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                    \`;
                } else if (this.type === 'Switch') {
                    // Count activities across all cases (array format: [{value, activities}])
                    let totalCount = 0;
                    if (this.cases) {
                        this.cases.forEach(c => {
                            totalCount += (c.activities || []).length;
                        });
                    }
                    if (this.defaultActivities) {
                        totalCount += this.defaultActivities.length;
                    }
                    const caseCount = this.cases ? this.cases.length : 0;
                    infoSection.innerHTML = \`
                        <div class="container-stat">
                            <span class="label">Cases:</span> <span class="count">\${caseCount}</span>
                        </div>
                        <div class="container-stat">
                            <span class="label">Total:</span> 
                            <span class="count">\${totalCount}</span> 
                            <span class="label">\${totalCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                    \`;
                }
                
                // Create action buttons section
                const actions = document.createElement('div');
                actions.className = 'activity-actions';
                
                const deleteBtn = document.createElement('button');
                deleteBtn.className = 'action-icon-btn';
                deleteBtn.innerHTML = '×';
                deleteBtn.title = 'Delete';
                deleteBtn.onclick = (e) => {
                    e.stopPropagation();
                    this.handleDelete();
                };
                
                const editBtn = document.createElement('button');
                editBtn.className = 'action-icon-btn';
                editBtn.innerHTML = '{}';
                editBtn.title = 'Edit JSON';
                editBtn.onclick = (e) => e.stopPropagation();
                
                const copyBtn = document.createElement('button');
                copyBtn.className = 'action-icon-btn';
                copyBtn.innerHTML = '⎘';
                copyBtn.title = 'Copy';
                copyBtn.onclick = (e) => e.stopPropagation();
                
                const infoBtn = document.createElement('button');
                infoBtn.className = 'action-icon-btn info';
                infoBtn.innerHTML = 'i';
                infoBtn.title = 'Info';
                infoBtn.onclick = (e) => e.stopPropagation();
                
                actions.appendChild(deleteBtn);
                actions.appendChild(editBtn);
                actions.appendChild(copyBtn);
                actions.appendChild(infoBtn);
                
                // Add connection points
                const positions = ['top', 'right', 'bottom', 'left'];
                positions.forEach(pos => {
                    const point = document.createElement('div');
                    point.className = 'connection-point ' + pos;
                    point.dataset.position = pos;
                    point.dataset.activityId = this.id;
                    this.element.appendChild(point);
                });
                
                // Assemble element
                this.element.appendChild(header);
                this.element.appendChild(body);
                this.element.appendChild(infoSection);
                this.element.appendChild(actions);
                
                // Add to container
                this.container.appendChild(this.element);
                
                // Set up event listeners
                this.setupEventListeners();
            }

            
            setupEventListeners() {
                // Click to select
                this.element.addEventListener('mousedown', (e) => {
                    // Don't handle if clicking on connection point
                    if (e.target.classList.contains('connection-point')) {
                        return;
                    }
                    e.stopPropagation();
                    this.handleMouseDown(e);
                });
                
                // Right-click context menu
                this.element.addEventListener('contextmenu', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    selectedActivity = this;
                    showContextMenu(e.clientX, e.clientY);
                });
                
                // Connection point handlers
                this.element.querySelectorAll('.connection-point').forEach(point => {
                    point.addEventListener('mousedown', (e) => {
                        e.stopPropagation();
                        this.handleConnectionStart(e, point);
                    });
                });
            }
            
            handleMouseDown(e) {
                selectedActivity = this;
                draggedActivity = this;
                isDragging = true;
                const rect = this.element.getBoundingClientRect();
                const wrapperRect = this.container.getBoundingClientRect();
                dragOffset.x = e.clientX - rect.left;
                dragOffset.y = e.clientY - rect.top;
                this.element.classList.add('dragging');
                this.element.style.cursor = 'move';
                this.setSelected(true);
                showProperties(selectedActivity);
                
                // Redraw connections
                draw();
            }
            
            handleConnectionStart(e, point) {
                const position = point.dataset.position;
                const connPoint = this.getConnectionPoint(position);
                connectionStart = connPoint;
                connectionStart.activity = this;
                canvas.style.cursor = 'crosshair';
                draw();
            }

            contains(x, y) {
                return x >= this.x && x <= this.x + this.width &&
                       y >= this.y && y <= this.y + this.height;
            }
            
            updatePosition(x, y) {
                this.x = x;
                this.y = y;
                if (this.element) {
                    this.element.style.left = x + 'px';
                    this.element.style.top = y + 'px';
                }
            }
            
            updateName(name) {
                this.name = name;
                if (this.element) {
                    const label = this.element.querySelector('.activity-label');
                    if (label) {
                        label.textContent = name;
                    }
                }
            }
            
            setSelected(selected) {
                if (this.element) {
                    if (selected) {
                        this.element.classList.add('selected');
                        // Deselect all other activities
                        activities.forEach(a => {
                            if (a !== this && a.element) {
                                a.element.classList.remove('selected');
                            }
                        });
                    } else {
                        this.element.classList.remove('selected');
                    }
                }
            }
            
            remove() {
                if (this.element && this.element.parentNode) {
                    this.element.parentNode.removeChild(this.element);
                }
                this.element = null;
            }



            getTypeLabel() {
                const labels = {
                    'Copy': 'Copy data',
                    'Delete': 'Delete',
                    'Dataflow': 'Data flow',
                    'Notebook': 'Notebook',
                    'ForEach': 'ForEach',
                    'IfCondition': 'If Condition',
                    'Until': 'Until',
                    'Switch': 'Switch',
                    'Wait': 'Wait',
                    'WebActivity': 'Web Activity',
                    'WebHook': 'WebHook',
                    'StoredProcedure': 'Stored Procedure',
                    'SqlServerStoredProcedure': 'Stored procedure'
                };
                return labels[this.type] || this.type;
            }

            getIcon() {
                const icons = {
                    'Copy': '📋',
                    'Delete': '🗑️',
                    'Dataflow': '🌊',
                    'Notebook': '📓',
                    'ForEach': '🔁',
                    'IfCondition': '❓',
                    'Until': '🔁',
                    'Switch': '🔀',
                    'Wait': '⏱️',
                    'WebActivity': '🌐',
                    'WebHook': '🪝',
                    'StoredProcedure': '💾',
                    'Validation': '✅',
                    'Script': '📜'
                };
                return icons[this.type] || '📦';
            }



            getConnectionPoint(position) {
                const headerHeight = 18;
                switch (position) {
                    case 'top': return { x: this.x + this.width / 2, y: this.y };
                    case 'right': return { x: this.x + this.width, y: this.y + this.height / 2 };
                    case 'bottom': return { x: this.x + this.width / 2, y: this.y + this.height };
                    case 'left': return { x: this.x, y: this.y + this.height / 2 };
                    default: return { x: this.x + this.width / 2, y: this.y + this.height };
                }
            }
        }

        // Connection class
        class Connection {
            constructor(fromActivity, toActivity, condition = 'Succeeded') {
                this.id = Date.now() + Math.random();
                this.from = fromActivity;
                this.to = toActivity;
                this.condition = condition; // Succeeded, Failed, Skipped, Completed
            }

            draw(ctx) {
                // Smart routing based on activity positions
                const fromCenter = { x: this.from.x + this.from.width / 2, y: this.from.y + this.from.height / 2 };
                const toCenter = { x: this.to.x + this.to.width / 2, y: this.to.y + this.to.height / 2 };
                const dx = toCenter.x - fromCenter.x;
                const dy = toCenter.y - fromCenter.y;
                
                let start, end;
                
                // Determine best connection points based on relative position
                if (Math.abs(dx) > Math.abs(dy)) {
                    // Horizontal layout
                    if (dx > 0) {
                        start = this.from.getConnectionPoint('right');
                        end = this.to.getConnectionPoint('left');
                    } else {
                        start = this.from.getConnectionPoint('left');
                        end = this.to.getConnectionPoint('right');
                    }
                } else {
                    // Vertical layout
                    if (dy > 0) {
                        start = this.from.getConnectionPoint('bottom');
                        end = this.to.getConnectionPoint('top');
                    } else {
                        start = this.from.getConnectionPoint('top');
                        end = this.to.getConnectionPoint('bottom');
                    }
                }

                // Color based on condition - ADF style
                const colors = {
                    'Succeeded': '#107c10',
                    'Failed': '#d13438',
                    'Skipped': '#ffa500',
                    'Completed': '#0078d4'
                };
                
                const color = colors[this.condition] || '#107c10';
                
                // Snap to pixel grid for crisp lines
                const snapToPixel = (val) => Math.floor(val) + 0.5;
                const startX = snapToPixel(start.x);
                const startY = snapToPixel(start.y);
                const endX = snapToPixel(end.x);
                const endY = snapToPixel(end.y);
                
                // Draw orthogonal (elbowed) connection line - ADF style
                ctx.strokeStyle = color;
                ctx.lineWidth = 1.5;
                ctx.lineCap = 'butt';
                ctx.lineJoin = 'miter';
                ctx.beginPath();
                
                // Calculate midpoint for elbow
                if (Math.abs(dx) > Math.abs(dy)) {
                    // Horizontal connection with elbow
                    const midX = startX + (endX - startX) / 2;
                    ctx.moveTo(startX, startY);
                    ctx.lineTo(midX, startY);
                    ctx.lineTo(midX, endY);
                    ctx.lineTo(endX, endY);
                } else {
                    // Vertical connection with elbow
                    const midY = startY + (endY - startY) / 2;
                    ctx.moveTo(startX, startY);
                    ctx.lineTo(startX, midY);
                    ctx.lineTo(endX, midY);
                    ctx.lineTo(endX, endY);
                }
                
                ctx.stroke();

                // Draw clean arrow head pointing in the direction of the line
                const arrowSize = 7;
                
                // Determine arrow direction based on the last segment
                let arrowAngle;
                if (Math.abs(dx) > Math.abs(dy)) {
                    // Horizontal approach
                    arrowAngle = dx > 0 ? 0 : Math.PI;
                } else {
                    // Vertical approach
                    arrowAngle = dy > 0 ? Math.PI / 2 : -Math.PI / 2;
                }
                
                ctx.fillStyle = color;
                ctx.beginPath();
                ctx.moveTo(endX, endY);
                ctx.lineTo(
                    endX - arrowSize * Math.cos(arrowAngle - Math.PI / 6),
                    endY - arrowSize * Math.sin(arrowAngle - Math.PI / 6)
                );
                ctx.lineTo(
                    endX - arrowSize * Math.cos(arrowAngle + Math.PI / 6),
                    endY - arrowSize * Math.sin(arrowAngle + Math.PI / 6)
                );
                ctx.closePath();
                ctx.fill();
            }
        }

        // Draw everything
        function draw() {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            
            // Draw grid
            drawGrid();

            // Draw connections only (activities are now DOM elements)
            connections.forEach(conn => conn.draw(ctx));
        }

        // Optimized draw with requestAnimationFrame
        function requestDraw() {
            if (!needsRedraw) {
                needsRedraw = true;
                if (animationFrameId) {
                    cancelAnimationFrame(animationFrameId);
                }
                animationFrameId = requestAnimationFrame(() => {
                    draw();
                    needsRedraw = false;
                    animationFrameId = null;
                });
            }
        }

        function drawGrid() {
            const gridSize = 20;
            ctx.strokeStyle = 'rgba(128, 128, 128, 0.1)';
            ctx.lineWidth = 1;

            for (let x = 0; x < canvas.width; x += gridSize) {
                ctx.beginPath();
                ctx.moveTo(x, 0);
                ctx.lineTo(x, canvas.height);
                ctx.stroke();
            }

            for (let y = 0; y < canvas.height; y += gridSize) {
                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(canvas.width, y);
                ctx.stroke();
            }
        }

        // Function to update sidebar activity restrictions when editing branches
        function updateSidebarForBranchEditing(isInBranch, containerType) {
            // Activities that cannot be nested inside each container type.
            // ForEach: cannot contain another ForEach or Until.
            // IfCondition: cannot contain IfCondition, ForEach, Until, or Switch.
            // Until: cannot contain Validation (it waits for file existence — a top-level concern).
            const restrictedByContainer = {
                'ForEach':     ['ForEach', 'Until'],
                'IfCondition': ['IfCondition', 'ForEach', 'Until', 'Switch'],
                'Switch':      ['IfCondition', 'ForEach', 'Until', 'Switch'],
                'Until':       ['Validation', 'ForEach', 'Until']
            };
            let restrictedTypes = (isInBranch && restrictedByContainer[containerType]) || [];
            // Also union in restrictions from ancestor editing contexts.
            // E.g. when inside an IfCondition that is itself inside Until,
            // Validation should still be greyed out (inherited from Until).
            if (isInBranch) {
                editingContextStack.forEach(ctx => {
                    const ancRestricted = restrictedByContainer[ctx.parentActivity?.type] || [];
                    restrictedTypes = [...new Set([...restrictedTypes, ...ancRestricted])];
                });
            }
            document.querySelectorAll('.activity-item').forEach(item => {
                const activityType = item.getAttribute('data-type');
                const isRestricted = restrictedTypes.includes(activityType);
                
                if (isRestricted) {
                    item.style.opacity = '0.4';
                    item.style.cursor = 'not-allowed';
                    item.style.pointerEvents = 'none';
                    item.setAttribute('draggable', 'false');
                } else {
                    item.style.opacity = '1';
                    item.style.cursor = 'grab';
                    item.style.pointerEvents = 'auto';
                    item.setAttribute('draggable', 'true');
                }
            });
        }

        // Event handlers
        document.querySelectorAll('.activity-item').forEach(item => {
            item.addEventListener('dragstart', (e) => {
                e.dataTransfer.setData('activityType', item.getAttribute('data-type'));
                console.log('Drag started:', item.getAttribute('data-type'));
            });
            
            // Also add click to add at center as alternative
            item.addEventListener('dblclick', (e) => {
                const activityType = item.getAttribute('data-type');
                console.log('Double-click add:', activityType);
                const canvasWrapper = document.getElementById('canvasWrapper');
                const centerX = canvas.width / 2;
                const centerY = canvas.height / 2;
                const activity = new Activity(activityType, centerX - 60, centerY - 40 + activities.length * 20, canvasWrapper);
                activities.push(activity);
                markAsDirty();
                console.log('Activities count:', activities.length);
                draw();
            });
        });

        document.getElementById('canvasWrapper').addEventListener('dragover', (e) => {
            e.preventDefault();
            console.log('Drag over canvas');
        });

        document.getElementById('canvasWrapper').addEventListener('drop', (e) => {
            e.preventDefault();
            const activityType = e.dataTransfer.getData('activityType');
            const canvasWrapper = document.getElementById('canvasWrapper');
            const wrapperRect = canvasWrapper.getBoundingClientRect();
            const x = e.clientX - wrapperRect.left + canvasWrapper.scrollLeft;
            const y = e.clientY - wrapperRect.top + canvasWrapper.scrollTop;
            
            console.log('Dropping activity:', activityType, 'at', x, y);
            const activity = new Activity(activityType, x - 90, y - 28, canvasWrapper);
            activities.push(activity);
            markAsDirty();
            console.log('Activities count:', activities.length);
            draw();
        });

        // Canvas mousedown - deselect when clicking empty space, or start panning
        document.getElementById('canvasWrapper').addEventListener('mousedown', (e) => {
            // Only handle if clicking directly on the wrapper or the background canvas (not on an activity)
            if (e.target.id === 'canvasWrapper' || e.target.id === 'canvas') {
                selectedActivity = null;
                activities.forEach(a => a.setSelected(false));
                showProperties(null);
                draw();

                // Start panning
                const canvasWrapper = document.getElementById('canvasWrapper');
                isPanning = true;
                panStart = { x: e.clientX, y: e.clientY, scrollLeft: canvasWrapper.scrollLeft, scrollTop: canvasWrapper.scrollTop };
                canvasWrapper.style.cursor = 'grabbing';
                e.preventDefault();
            }
        });

        document.addEventListener('mousemove', (e) => {
            if (isPanning) {
                const canvasWrapper = document.getElementById('canvasWrapper');
                canvasWrapper.scrollLeft = panStart.scrollLeft - (e.clientX - panStart.x);
                canvasWrapper.scrollTop  = panStart.scrollTop  - (e.clientY - panStart.y);
                return;
            }
            if (isDragging && draggedActivity) {
                const canvasWrapper = document.getElementById('canvasWrapper');
                const wrapperRect = canvasWrapper.getBoundingClientRect();
                const x = e.clientX - wrapperRect.left + canvasWrapper.scrollLeft - dragOffset.x;
                const y = e.clientY - wrapperRect.top + canvasWrapper.scrollTop - dragOffset.y;
                draggedActivity.updatePosition(x, y);
                requestDraw(); // Use optimized draw
            } else if (connectionStart) {
                const canvasWrapper = document.getElementById('canvasWrapper');
                const wrapperRect = canvasWrapper.getBoundingClientRect();
                const mouseX = e.clientX - wrapperRect.left + canvasWrapper.scrollLeft;
                const mouseY = e.clientY - wrapperRect.top + canvasWrapper.scrollTop;
                
                draw();
                // Draw temporary connection line
                ctx.strokeStyle = '#0078d4';
                ctx.lineWidth = 2;
                ctx.setLineDash([5, 5]);
                ctx.beginPath();
                ctx.moveTo(connectionStart.x, connectionStart.y);
                ctx.lineTo(mouseX, mouseY);
                ctx.stroke();
                ctx.setLineDash([]);
            }
        });

        document.addEventListener('mouseup', (e) => {
            if (isPanning) {
                isPanning = false;
                document.getElementById('canvasWrapper').style.cursor = '';
            }

            if (connectionStart) {
                // Check if mouse is over an activity element
                const targetElement = document.elementFromPoint(e.clientX, e.clientY);
                if (targetElement) {
                    const activityBox = targetElement.closest('.activity-box');
                    if (activityBox && activityBox.dataset.activityId) {
                        const targetId = parseFloat(activityBox.dataset.activityId);
                        const targetActivity = activities.find(a => a.id === targetId);
                        if (targetActivity && targetActivity !== connectionStart.activity) {
                            // Show condition selector
                            showConnectionConditionDialog(connectionStart.activity, targetActivity, e.clientX, e.clientY);
                        }
                    }
                }
                
                connectionStart = null;
                canvas.style.cursor = 'default';
            }
            
            if (isDragging) {
                if (draggedActivity && draggedActivity.element) {
                    draggedActivity.element.classList.remove('dragging');
                    draggedActivity.element.style.cursor = 'pointer';
                }
            }
            
            isDragging = false;
            draggedActivity = null;
            draw();
        });



        function getMousePos(e) {
            const rect = canvas.getBoundingClientRect();
            return {
                x: e.clientX - rect.left,
                y: e.clientY - rect.top
            };
        }

        // Context menu
        function showContextMenu(x, y) {
            const menu = document.getElementById('contextMenu');
            menu.style.left = x + 'px';
            menu.style.top = y + 'px';
            menu.style.display = 'block';
        }

        document.addEventListener('click', () => {
            document.getElementById('contextMenu').style.display = 'none';
        });

        document.querySelectorAll('.context-menu-item').forEach(item => {
            item.addEventListener('click', (e) => {
                const action = item.getAttribute('data-action');
                
                if (action === 'delete' && selectedActivity) {
                    // Remove from DOM
                    selectedActivity.remove();
                    // Remove from arrays
                    activities = activities.filter(a => a !== selectedActivity);
                    connections = connections.filter(c => 
                        c.from !== selectedActivity && c.to !== selectedActivity
                    );
                    selectedActivity = null;
                    markAsDirty();
                    showProperties(null);
                    draw();
                }
            });
        });

        // Connection condition dialog
        function showConnectionConditionDialog(fromActivity, toActivity, x, y) {
            const dialog = document.createElement('div');
            dialog.style.cssText = \`
                position: fixed;
                left: \${x}px;
                top: \${y}px;
                background: var(--vscode-menu-background);
                border: 1px solid var(--vscode-menu-border);
                border-radius: 4px;
                padding: 8px;
                z-index: 10000;
                box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                min-width: 150px;
            \`;
            
            dialog.innerHTML = \`
                <div style="font-size: 12px; font-weight: 600; margin-bottom: 8px; color: var(--vscode-foreground);">Dependency Condition</div>
                <button class="toolbar-button" data-condition="Succeeded" style="width: 100%; margin: 2px 0; background: #00a86b; color: white;">✓ Succeeded</button>
                <button class="toolbar-button" data-condition="Failed" style="width: 100%; margin: 2px 0; background: #d13438; color: white;">✗ Failed</button>
                <button class="toolbar-button" data-condition="Completed" style="width: 100%; margin: 2px 0; background: #0078d4; color: white;">⊙ Completed</button>
                <button class="toolbar-button" data-condition="Skipped" style="width: 100%; margin: 2px 0; background: #ffa500; color: white;">⊘ Skipped</button>
            \`;
            
            document.body.appendChild(dialog);
            
            dialog.querySelectorAll('button').forEach(btn => {
                btn.addEventListener('click', () => {
                    // Check if connection already exists
                    const duplicate = connections.find(c => 
                        c.from.id === fromActivity.id && c.to.id === toActivity.id
                    );
                    
                    if (duplicate) {
                        alert('A dependency already exists between these activities');
                        document.body.removeChild(dialog);
                        return;
                    }
                    
                    const condition = btn.getAttribute('data-condition');
                    const conn = new Connection(fromActivity, toActivity, condition);
                    connections.push(conn);
                    markAsDirty();
                    document.body.removeChild(dialog);
                    draw();
                });
            });
            
            // Close on click outside
            setTimeout(() => {
                const closeHandler = (e) => {
                    if (!dialog.contains(e.target) && dialog.parentNode) {
                        document.body.removeChild(dialog);
                        document.removeEventListener('click', closeHandler);
                    }
                };
                document.addEventListener('click', closeHandler);
            }, 100);
        }

        // Helper function to get icon for activity type
        function getIconForType(type) {
            const iconMap = {
                'Copy': '📋',
                'ExecutePipeline': '▶',
                'IfCondition': '❓',
                'ForEach': '🔄',
                'Until': '⟳',
                'Switch': '🔀',
                'Wait': '⏱',
                'WebActivity': '🌐',
                'WebHook': '🔔',
                'Lookup': '🔍',
                'GetMetadata': 'ℹ',
                'Delete': '🗑',
                'Validation': '✓',
                'Filter': '⊲',
                'SetVariable': '📝',
                'AppendVariable': '➕',
                'Script': '📜',
                'SqlServerStoredProcedure': '⚡',
                'AzureFunctionActivity': 'ƒ',
                'DatabricksNotebook': '📓',
                'DataLakeAnalyticsU-SQL': '📊',
                'HDInsightSpark': '⚡',
                'HDInsightHive': '🐝',
                'HDInsightPig': '🐷',
                'HDInsightMapReduce': '🗺',
                'HDInsightStreaming': '〰',
                'ExecuteDataFlow': '📈',
                'Custom': '⚙'
            };
            return iconMap[type] || '📦';
        }

        // Shared helper: serialize a branch-canvas Activity into a clean typeProperties object,
        // applying the same type-specific transformations as buildPipelineDataForSave.
        // Returns { typeProperties, activityProps } where activityProps holds any fields that
        // belong at the activity envelope level (e.g. linkedServiceName for Script/SP).
        function buildNestedActivityTypeProperties(a) {
            const activityProps = {}; // fields hoisted to the activity envelope level
            const tp = {};

            // All props that are either UI-only, handled specially below, or belong on the
            // activity envelope rather than in typeProperties.
            const excludedProps = [
                'id', 'type', 'x', 'y', 'width', 'height', 'name', 'description',
                'color', 'container', 'element', 'userProperties', 'state',
                'dependsOn', 'isContainer', 'ifTrueActivities', 'ifFalseActivities',
                'expression', 'activities', 'cases', 'defaultActivities', 'typeProperties',
                // Policy fields → built into activityProps.policy below
                'timeout', 'retry', 'retryIntervalInSeconds', 'secureOutput', 'secureInput', 'policy',
                // SynapseNotebook raw fields → converted to conf block
                'dynamicAllocation', 'minExecutors', 'maxExecutors', 'numExecutors',
                // Delete UI fields → converted to storeSettings block
                'dataset', 'filePathType', 'wildcardFileName', 'fileListPath',
                'recursive', 'maxConcurrentConnections', 'modifiedDatetimeStart', 'modifiedDatetimeEnd',
                // GetMetadata/Lookup UI fields
                'fieldList', '_datasetLocationType', 'skipLineCount',
                // Lookup-specific UI fields → converted to source block
                '_datasetType', 'useQuery', 'sqlReaderQuery', 'sqlReaderStoredProcedureName',
                'queryTimeout', 'isolationLevel',
                'partitionOption', 'partitionOptionQuery', 'partitionOptionStoredProc',
                'partitionColumnName', 'partitionUpperBound', 'partitionLowerBound',
                'partitionColumnNameQuery', 'partitionUpperBoundQuery', 'partitionLowerBoundQuery',
                'wildcardFolderPath', 'enablePartitionDiscovery', 'partitionRootPath', 'prefix',
                'requestMethod', 'additionalHeaders', 'requestBody', 'requestTimeout',
                'validationMode', 'namespaces', 'namespacePrefixPairs', 'detectDataType',
                // ExecutePipeline UI field → converted to pipeline reference
                'pipeline',
                // SetVariable UI fields (kept in activityProps for collectVariables, not in tp)
                'variableType', 'pipelineVariableType', 'returnValues',
                // Script/SqlServerStoredProcedure fields → activity envelope or rebuilt
                'linkedServiceName', '_selectedLinkedServiceType', 'linkedServiceProperties',
                'storedProcedureName', 'storedProcedureParameters',
                'scripts', 'scriptBlockExecutionTimeout',
                // WebActivity/WebHook auth + header fields → rebuilt as objects
                'authenticationType', 'username', 'password', 'resource',
                'pfx', 'pfxPassword', 'servicePrincipalAuthMethod', 'tenant',
                'servicePrincipalId', 'servicePrincipalCredentialType', 'servicePrincipalKey',
                'servicePrincipalCert', 'servicePrincipalResource', 'credential',
                'credentialResource', 'credentialUserAssigned',
                'headers', 'httpRequestTimeout', 'disableAsyncPattern', 'disableCertValidation',
                // Safeguard: exclude any property literally named 'null' (spurious UI artefact)
                'null'
            ];

            for (const key in a) {
                if (!excludedProps.includes(key) && a.hasOwnProperty(key) && typeof a[key] !== 'function') {
                    // notebook string → NotebookReference block
                    if (key === 'notebook' && typeof a[key] === 'string' && a[key]) {
                        tp[key] = { referenceName: { value: a[key], type: 'Expression' }, type: 'NotebookReference' };
                    // sparkPool string → BigDataPoolReference block
                    } else if (key === 'sparkPool' && typeof a[key] === 'string' && a[key]) {
                        tp[key] = { referenceName: { value: a[key], type: 'Expression' }, type: 'BigDataPoolReference' };
                    } else {
                        tp[key] = a[key];
                    }
                }
            }

            // ── Policy block → activity envelope level ────────────────────────────────
            if (a.type === 'SetVariable') {
                activityProps.policy = {
                    secureOutput: a.secureOutput || false,
                    secureInput: a.secureInput || false
                };
            } else if (a.type === 'WebHook') {
                activityProps.policy = {
                    secureOutput: a.secureOutput || false,
                    secureInput: a.secureInput || false
                };
            } else if (['GetMetadata', 'Script', 'SqlServerStoredProcedure', 'WebActivity',
                        'Lookup', 'Delete', 'Validation', 'SynapseNotebook'].includes(a.type)) {
                activityProps.policy = {
                    timeout: a.timeout || '0.12:00:00',
                    retry: a.retry !== undefined ? a.retry : 0,
                    retryIntervalInSeconds: a.retryIntervalInSeconds !== undefined ? a.retryIntervalInSeconds : 30,
                    secureOutput: a.secureOutput || false,
                    secureInput: a.secureInput || false
                };
            } else {
                const hasNonDefaultPolicy =
                    (a.timeout && a.timeout !== '0.12:00:00') ||
                    (a.retry && a.retry !== 0) ||
                    (a.retryIntervalInSeconds && a.retryIntervalInSeconds !== 30) ||
                    a.secureOutput === true ||
                    a.secureInput === true;
                if (hasNonDefaultPolicy) {
                    activityProps.policy = {
                        timeout: a.timeout || '0.12:00:00',
                        retry: a.retry || 0,
                        retryIntervalInSeconds: a.retryIntervalInSeconds || 30,
                        secureOutput: a.secureOutput || false,
                        secureInput: a.secureInput || false
                    };
                }
            }

            // ── SynapseNotebook: snapshot, driverSize, conf block ──────────────────────
            if (a.type === 'SynapseNotebook') {
                tp.snapshot = true;
                if (tp.executorSize) tp.driverSize = tp.executorSize;
                if (a.dynamicAllocation !== undefined || a.minExecutors !== undefined ||
                    a.maxExecutors !== undefined || a.numExecutors !== undefined) {
                    tp.conf = {};
                    const isDynamic = a.dynamicAllocation === 'Enabled';
                    tp.conf['spark.dynamicAllocation.enabled'] = isDynamic;
                    if (isDynamic) {
                        if (a.minExecutors) tp.conf['spark.dynamicAllocation.minExecutors'] = parseInt(a.minExecutors);
                        if (a.maxExecutors) tp.conf['spark.dynamicAllocation.maxExecutors'] = parseInt(a.maxExecutors);
                    } else if (a.numExecutors) {
                        const n = parseInt(a.numExecutors);
                        tp.conf['spark.dynamicAllocation.minExecutors'] = n;
                        tp.conf['spark.dynamicAllocation.maxExecutors'] = n;
                        tp.numExecutors = n;
                    }
                }
            }

            // ── IfCondition: expression object + preserve branch children ─────────────
            if (a.type === 'IfCondition') {
                // expression string → Expression object required by ADF
                if (a.expression) tp.expression = { value: a.expression, type: 'Expression' };
                // Strip only UI position fields (x, y) from branch children here.
                // _variableType/_pipelineVariableType are intentionally kept so that
                // collectVariables() can detect SetVariable type metadata when it
                // recurses into ifTrueActivities/ifFalseActivities. These metadata
                // fields are stripped in the final pass by deepCleanNestedActivity()
                // inside buildPipelineDataForSave before the JSON is written to disk.
                const _stripUiFields = arr => (arr || []).map(act => {
                    const c = Object.assign({}, act);
                    delete c.x; delete c.y;
                    return c;
                });
                if (a.ifTrueActivities) tp.ifTrueActivities = _stripUiFields(a.ifTrueActivities);
                if (a.ifFalseActivities) tp.ifFalseActivities = _stripUiFields(a.ifFalseActivities);
            }

            // ── Until: expression object + body activities ───────────────────────────
            if (a.type === 'Until') {
                if (a.expression) tp.expression = { value: a.expression, type: 'Expression' };
                if (a.timeout) tp.timeout = a.timeout;
                const _stripU = arr => (arr || []).map(act => { const c = Object.assign({}, act); delete c.x; delete c.y; return c; });
                if (a.activities) tp.activities = _stripU(a.activities);
            }

            // ── Switch: on expression + preserve case children ─────────────────────────
            if (a.type === 'Switch') {
                if (a.on) tp.on = { value: a.on, type: 'Expression' };
                const _stripSwitchUiFields = arr => (arr || []).map(act => {
                    const c = Object.assign({}, act);
                    delete c.x; delete c.y;
                    return c;
                });
                if (a.cases) {
                    tp.cases = a.cases.map(c => ({
                        value: c.value || '',
                        activities: _stripSwitchUiFields(c.activities)
                    }));
                }
                if (a.defaultActivities) tp.defaultActivities = _stripSwitchUiFields(a.defaultActivities);
            }

            // ── Filter: expression objects ─────────────────────────────────────────────
            if (a.type === 'Filter') {
                tp.items = { value: a.items || '', type: 'Expression' };
                tp.condition = { value: a.condition || '', type: 'Expression' };
            }

            // ── Delete: dataset ref + storeSettings block ──────────────────────────────
            if (a.type === 'Delete') {
                tp.enableLogging = false;
                // Normalize dataset: may be a plain string (from UI) or an object { referenceName, type }
                // (when the activity was loaded back from saved JSON via Object.assign on typeProperties)
                const deleteDatasetName = typeof a.dataset === 'string' ? a.dataset : (a.dataset?.referenceName || null);
                // Delete activity always uses AzureBlobStorageReadSettings for both ADLS and Blob datasets.
                // (Unlike Lookup/GetMetadata which use AzureBlobFSReadSettings for ADLS.)
                const storeType = 'AzureBlobStorageReadSettings';
                const ss = { type: storeType, enablePartitionDiscovery: false };
                if (a.filePathType === 'listOfFiles' && a.fileListPath) {
                    ss.fileListPath = a.fileListPath;
                } else if (a.filePathType === 'wildcardFilePath' && a.wildcardFileName) {
                    ss.wildcardFileName = a.wildcardFileName;
                } else if (a.filePathType === 'prefix' && a.prefix) {
                    ss.prefix = a.prefix;
                }
                if (a.maxConcurrentConnections) ss.maxConcurrentConnections = parseInt(a.maxConcurrentConnections);
                if (a.recursive !== undefined) ss.recursive = a.recursive;
                if (a.filePathType !== 'listOfFiles') {
                    if (a.modifiedDatetimeStart) ss.modifiedDatetimeStart = new Date(a.modifiedDatetimeStart).toISOString();
                    if (a.modifiedDatetimeEnd) ss.modifiedDatetimeEnd = new Date(a.modifiedDatetimeEnd).toISOString();
                }
                tp.storeSettings = ss;
                if (deleteDatasetName) tp.dataset = { referenceName: deleteDatasetName, type: 'DatasetReference' };
            }

            // ── ExecutePipeline: pipeline reference ────────────────────────────────────
            if (a.type === 'ExecutePipeline') {
                if (a.pipeline) tp.pipeline = { referenceName: a.pipeline, type: 'PipelineReference' };
                tp.waitOnCompletion = a.waitOnCompletion !== undefined ? a.waitOnCompletion : true;
            }

            // ── Lookup: dataset reference + source ─────────────────────────────────────
            if (a.type === 'Lookup') {
                // Normalize dataset: may be a plain string (from UI) or an object { referenceName, type }
                const lookupDatasetName = typeof a.dataset === 'string' ? a.dataset : (a.dataset?.referenceName || null);
                if (lookupDatasetName) tp.dataset = { referenceName: lookupDatasetName, type: 'DatasetReference' };
                tp.firstRowOnly = a.firstRowOnly !== undefined ? a.firstRowOnly : true;

                let datasetType = a._datasetType;
                if (!datasetType && lookupDatasetName && datasetContents[lookupDatasetName]) {
                    datasetType = datasetContents[lookupDatasetName].properties?.type;
                }

                if (datasetType === 'AzureSqlTable' || datasetType === 'AzureSynapseAnalytics') {
                    const source = { type: datasetType === 'AzureSqlTable' ? 'AzureSqlSource' : 'SqlDWSource' };
                    if (a.useQuery === 'Query' && a.sqlReaderQuery) source.sqlReaderQuery = a.sqlReaderQuery;
                    else if (a.useQuery === 'Stored procedure' && a.sqlReaderStoredProcedureName) source.sqlReaderStoredProcedureName = a.sqlReaderStoredProcedureName;
                    if (a.queryTimeout !== undefined && a.queryTimeout !== '') {
                        const mins = parseInt(a.queryTimeout) || 120;
                        const h = Math.floor(mins / 60); const m = mins % 60;
                        source.queryTimeout = (h < 10 ? '0' + h : '' + h) + ':' + (m < 10 ? '0' + m : '' + m) + ':00';
                    } else { source.queryTimeout = '02:00:00'; }
                    if (a.isolationLevel) source.isolationLevel = a.isolationLevel;
                    let pov = 'None';
                    if (a.useQuery === 'Table' && a.partitionOption) pov = a.partitionOption;
                    else if (a.useQuery === 'Query' && a.partitionOptionQuery) pov = a.partitionOptionQuery;
                    else if (a.useQuery === 'Stored procedure' && a.partitionOptionStoredProc) pov = a.partitionOptionStoredProc;
                    source.partitionOption = pov;
                    if (pov === 'DynamicRange') {
                        const ps = {};
                        if (a.useQuery === 'Table') {
                            if (a.partitionColumnName) ps.partitionColumnName = a.partitionColumnName;
                            if (a.partitionUpperBound) ps.partitionUpperBound = a.partitionUpperBound;
                            if (a.partitionLowerBound) ps.partitionLowerBound = a.partitionLowerBound;
                        } else if (a.useQuery === 'Query') {
                            if (a.partitionColumnNameQuery) ps.partitionColumnName = a.partitionColumnNameQuery;
                            if (a.partitionUpperBoundQuery) ps.partitionUpperBound = a.partitionUpperBoundQuery;
                            if (a.partitionLowerBoundQuery) ps.partitionLowerBound = a.partitionLowerBoundQuery;
                        }
                        if (Object.keys(ps).length > 0) source.partitionSettings = ps;
                    }
                    if (a.useQuery === 'Stored procedure' && a.storedProcedureParameters && typeof a.storedProcedureParameters === 'object') {
                        source.storedProcedureParameters = a.storedProcedureParameters;
                    }
                    tp.source = source;
                } else if (['DelimitedText', 'Parquet', 'Json', 'Avro', 'ORC', 'Xml'].includes(datasetType)) {
                    const source = { type: datasetType + 'Source' };
                    let storeType = 'AzureBlobFSReadSettings';
                    if (lookupDatasetName && datasetContents[lookupDatasetName]) {
                        const lt = datasetContents[lookupDatasetName].properties?.typeProperties?.location?.type;
                        if (lt === 'AzureBlobStorageLocation') storeType = 'AzureBlobStorageReadSettings';
                    }
                    const ss = { type: storeType };
                    if (a.filePathType === 'listOfFiles' && a.fileListPath) ss.fileListPath = a.fileListPath;
                    else if (a.filePathType === 'wildcardFilePath') {
                        if (a.wildcardFolderPath) ss.wildcardFolderPath = a.wildcardFolderPath;
                        if (a.wildcardFileName) ss.wildcardFileName = a.wildcardFileName;
                    } else if (a.filePathType === 'prefix' && a.prefix) ss.prefix = a.prefix;
                    if (a.modifiedDatetimeStart) ss.modifiedDatetimeStart = new Date(a.modifiedDatetimeStart).toISOString();
                    if (a.modifiedDatetimeEnd) ss.modifiedDatetimeEnd = new Date(a.modifiedDatetimeEnd).toISOString();
                    ss.recursive = a.recursive !== undefined ? a.recursive : true;
                    ss.enablePartitionDiscovery = a.enablePartitionDiscovery || false;
                    if (a.enablePartitionDiscovery && a.partitionRootPath) ss.partitionRootPath = a.partitionRootPath;
                    if (a.maxConcurrentConnections) ss.maxConcurrentConnections = parseInt(a.maxConcurrentConnections);
                    source.storeSettings = ss;
                    if (datasetType === 'DelimitedText') {
                        source.formatSettings = { type: 'DelimitedTextReadSettings' };
                        if (a.skipLineCount && a.skipLineCount > 0) source.formatSettings.skipLineCount = parseInt(a.skipLineCount);
                    } else if (datasetType === 'Xml') {
                        source.formatSettings = { type: 'XmlReadSettings' };
                        if (a.validationMode) source.formatSettings.validationMode = a.validationMode;
                        if (a.detectDataType !== undefined) source.formatSettings.detectDataType = a.detectDataType;
                        if (a.namespaces !== undefined) source.formatSettings.namespaces = a.namespaces;
                        if (a.namespacePrefixPairs && Object.keys(a.namespacePrefixPairs).length > 0) source.formatSettings.namespacePrefixes = a.namespacePrefixPairs;
                    }
                    tp.source = source;
                } else if (datasetType === 'HttpFile') {
                    const source = { type: 'HttpSource' };
                    if (a.maxConcurrentConnections) source.maxConcurrentConnections = parseInt(a.maxConcurrentConnections);
                    tp.source = source;
                }
            }

            // ── GetMetadata: dataset ref, fieldList, storeSettings ─────────────────────
            if (a.type === 'GetMetadata') {
                // Normalize dataset: may be a plain string (from UI) or an object { referenceName, type }
                const gmDatasetName = typeof a.dataset === 'string' ? a.dataset : (a.dataset?.referenceName || null);
                if (gmDatasetName) tp.dataset = { referenceName: gmDatasetName, type: 'DatasetReference' };
                if (a.fieldList && a.fieldList.length > 0) {
                    tp.fieldList = a.fieldList
                        .filter(f => f.value && f.value.trim() !== '')
                        .map(f => f.type === 'dynamic' ? { value: f.value, type: 'Expression' } : f.value);
                }
                let locationType = null;
                if (gmDatasetName && datasetContents[gmDatasetName]) {
                    locationType = datasetContents[gmDatasetName].properties?.typeProperties?.location?.type;
                }
                if (locationType === 'AzureBlobFSLocation' || locationType === 'AzureBlobStorageLocation') {
                    const storeType = locationType === 'AzureBlobFSLocation' ? 'AzureBlobFSReadSettings' : 'AzureBlobStorageReadSettings';
                    const ss = { type: storeType };
                    if (a.modifiedDatetimeStart) ss.modifiedDatetimeStart = new Date(a.modifiedDatetimeStart).toISOString();
                    if (a.modifiedDatetimeEnd) ss.modifiedDatetimeEnd = new Date(a.modifiedDatetimeEnd).toISOString();
                    ss.enablePartitionDiscovery = false;
                    tp.storeSettings = ss;
                    tp.formatSettings = { type: 'DelimitedTextReadSettings' };
                    if (a.skipLineCount && a.skipLineCount > 0) tp.formatSettings.skipLineCount = parseInt(a.skipLineCount);
                }
            }

            // ── Validation: dataset ref ────────────────────────────────────────────────
            if (a.type === 'Validation') {
                if (a.dataset) tp.dataset = typeof a.dataset === 'object' ? a.dataset : { referenceName: a.dataset, type: 'DatasetReference' };
                if (!tp.timeout || tp.timeout === '') tp.timeout = '0.12:00:00';
                if (tp.sleep === undefined || tp.sleep === null || tp.sleep === '') tp.sleep = 10;
                const isStorage = a._datasetLocationType === 'AzureBlobStorageLocation' || a._datasetLocationType === 'AzureBlobFSLocation';
                if (isStorage && tp.childItems !== undefined && tp.childItems !== 'ignore') {
                    tp.childItems = tp.childItems === 'true' || tp.childItems === true;
                } else {
                    delete tp.childItems;
                }
                delete tp._datasetLocationType;
            }

            // ── SetVariable: remove UI-only fields, stash type metadata for collectVariables ──
            if (a.type === 'SetVariable') {
                // Stash at activity level so collectVariables can find them after backToMainPipeline;
                // these are stripped by the ForEach serializer before writing to disk.
                if (a.variableType) activityProps._variableType = a.variableType;
                if (a.pipelineVariableType) activityProps._pipelineVariableType = a.pipelineVariableType;

                delete tp.variableType;
                delete tp.pipelineVariableType;
                delete tp.returnValues;
                if (a.variableType === 'Pipeline return value' && a.returnValues) {
                    tp.variableName = 'pipelineReturnValue';
                    tp.setSystemVariable = true;
                    const valueArray = [];
                    for (const [k, item] of Object.entries(a.returnValues)) {
                        if (!a.returnValues.hasOwnProperty(k)) continue;
                        const vo = { key: k, value: { type: item.type } };
                        if (item.type === 'Null') { /* no content */ }
                        else if (item.type === 'Array') vo.value.content = item.content || [];
                        else if (item.type === 'Int' || item.type === 'Float') vo.value.content = parseFloat(item.value) || 0;
                        else if (item.type === 'Boolean') vo.value.content = item.value === 'true' || item.value === true;
                        else vo.value.content = item.value || '';
                        valueArray.push(vo);
                    }
                    tp.value = valueArray;
                }
            }

            // ── Script: linkedServiceName at activity level, scripts array ─────────────
            if (a.type === 'Script') {
                if (a.linkedServiceName) {
                    activityProps.linkedServiceName = {
                        referenceName: typeof a.linkedServiceName === 'object' ? a.linkedServiceName.referenceName : a.linkedServiceName,
                        type: 'LinkedServiceReference'
                    };
                }
                if (a.scripts && a.scripts.length > 0) {
                    tp.scripts = a.scripts.map(s => {
                        const so = { type: s.type || 'Query' };
                        if (s.text) so.text = s.text;
                        if (s.parameters && s.parameters.length > 0) {
                            so.parameters = s.parameters.map(p => {
                                const po = { name: p.name, type: p.type, value: p.value, direction: p.direction };
                                if ((p.direction === 'Output' || p.direction === 'InputOutput') &&
                                    (p.type === 'String' || p.type === 'Byte[]') && p.size !== undefined) {
                                    po.size = parseInt(p.size);
                                }
                                return po;
                            });
                        }
                        return so;
                    });
                }
                if (a.scriptBlockExecutionTimeout) tp.scriptBlockExecutionTimeout = a.scriptBlockExecutionTimeout;
            }

            // ── SqlServerStoredProcedure: linkedServiceName + storedProcedureParameters ─
            if (a.type === 'SqlServerStoredProcedure') {
                if (a.linkedServiceName) {
                    const lsRef = {
                        referenceName: typeof a.linkedServiceName === 'object' ? a.linkedServiceName.referenceName : a.linkedServiceName,
                        type: 'LinkedServiceReference'
                    };
                    if (a._selectedLinkedServiceType === 'AzureSynapse' && a.linkedServiceProperties?.DBName) {
                        lsRef.parameters = { DBName: a.linkedServiceProperties.DBName };
                    }
                    activityProps.linkedServiceName = lsRef;
                }
                if (a.storedProcedureName) tp.storedProcedureName = a.storedProcedureName;
                if (a.storedProcedureParameters && Object.keys(a.storedProcedureParameters).length > 0) {
                    const validParams = {};
                    for (const [pn, pd] of Object.entries(a.storedProcedureParameters)) {
                        if (pn && pn.trim()) validParams[pn] = { value: pd.value, type: pd.type };
                    }
                    if (Object.keys(validParams).length > 0) tp.storedProcedureParameters = validParams;
                }
            }

            // ── WebActivity: authentication object, headers ────────────────────────────
            if (a.type === 'WebActivity' || a.type === 'WebHook') {
                if (a.authenticationType && a.authenticationType !== 'None') {
                    const auth = {};
                    const isSpCredential = a.authenticationType === 'ServicePrincipal' && a.servicePrincipalAuthMethod === 'Credential';
                    if (!isSpCredential) auth.type = a.authenticationType;
                    if (a.authenticationType === 'Basic') {
                        if (a.username) auth.username = a.username;
                        if (a.password) auth.password = a.password;
                    } else if (a.authenticationType === 'MSI' || a.authenticationType === 'UserAssignedManagedIdentity') {
                        if (a.resource) auth.resource = a.resource;
                        if (a.credentialUserAssigned) auth.credential = { referenceName: a.credentialUserAssigned, type: 'CredentialReference' };
                    } else if (a.authenticationType === 'ClientCertificate') {
                        if (a.pfx) auth.pfx = a.pfx;
                        if (a.pfxPassword) auth.password = a.pfxPassword;
                    } else if (a.authenticationType === 'ServicePrincipal') {
                        if (a.servicePrincipalResource) auth.resource = a.servicePrincipalResource;
                        if (a.servicePrincipalAuthMethod === 'Credential') {
                            if (a.credential) auth.credential = { referenceName: a.credential, type: 'CredentialReference' };
                            if (a.credentialResource) auth.resource = a.credentialResource;
                        } else {
                            if (a.tenant) auth.userTenant = a.tenant;
                            if (a.servicePrincipalId) auth.username = a.servicePrincipalId;
                            if (a.servicePrincipalKey) auth.password = a.servicePrincipalKey;
                            else if (a.servicePrincipalCert) auth.pfx = a.servicePrincipalCert;
                        }
                    }
                    tp.authentication = auth;
                }
                if (a.headers && a.headers.length > 0) {
                    const headersObj = {};
                    a.headers.forEach(h => { if (h.name && h.value) headersObj[h.name] = h.value; });
                    tp.headers = headersObj;
                }
                if (a.type === 'WebActivity') {
                    if (a.httpRequestTimeout) tp.httpRequestTimeout = a.httpRequestTimeout;
                    if (a.disableAsyncPattern) tp.turnOffAsync = a.disableAsyncPattern;
                    if (a.disableCertValidation) tp.disableCertValidation = a.disableCertValidation;
                }
                if (a.type === 'WebHook') {
                    if (a.timeout) tp.timeout = a.timeout;
                    if (a.reportStatusOnCallBack === true) tp.reportStatusOnCallBack = true;
                    if (a.disableCertValidation === true) tp.disableCertValidation = true;
                }
            }

            return { typeProperties: tp, activityProps };
        }

        // Convenience wrapper used where only typeProperties is needed (e.g. legacy call sites)
        function buildNestedActivityTP(a) {
            return buildNestedActivityTypeProperties(a).typeProperties;
        }

        // Context switching for IfCondition/ForEach branch editing
        let editingContext = null; // current { parentActivity, branch, savedState }
        let editingContextStack = []; // ancestor contexts when nesting editors (oldest first)
        
        window.openIfConditionEditor = function(activityId, branch) {
            const activity = activities.find(a => a.id == activityId);
            if (!activity) return;
            
            // Save current state
            const savedState = {
                activities: activities,
                connections: connections,
                selectedActivity: selectedActivity
            };
            
            // Push current context if already inside an editor (enables multi-level nesting)
            if (editingContext) editingContextStack.push(editingContext);
            editingContext = {
                parentActivity: activity,
                branch: branch,
                savedState: savedState
            };
            
            // Get branch activities
            const branchActivities = branch === 'true' ? activity.ifTrueActivities : activity.ifFalseActivities;
            
            // Clear current canvas
            activities = [];
            connections = [];
            selectedActivity = null;
            
            // Clear only activity elements from canvas wrapper (keep canvas element)
            const canvasWrapper = document.getElementById('canvasWrapper');
            const activityElements = canvasWrapper.querySelectorAll('.activity-box');
            console.log('[openIfConditionEditor] Found', activityElements.length, 'activity elements to remove');
            activityElements.forEach(el => {
                if (el && el.parentNode) {
                    console.log('[openIfConditionEditor] Removing element:', el.dataset.activityId, el.className);
                    el.parentNode.removeChild(el);
                }
            });
            console.log('[openIfConditionEditor] After cleanup, canvasWrapper children:', canvasWrapper.children.length);
            
            // Load branch activities into main canvas
            const activityMap = new Map();
            
            console.log('[openIfConditionEditor] Loading', branchActivities?.length || 0, 'branch activities');
            if (branchActivities && Array.isArray(branchActivities)) {
                branchActivities.forEach((actData, idx) => {
                    console.log('[openIfConditionEditor] Loading activity:', actData.name, actData.type);
                    const x = actData.x !== undefined ? actData.x : (100 + (idx % 4) * 220);
                    const y = actData.y !== undefined ? actData.y : (100 + Math.floor(idx / 4) * 120);
                    
                    const act = new Activity(actData.type, x, y, canvasWrapper);
                    act.name = actData.name;
                    act.description = actData.description || '';
                    
                    // Copy typeProperties without nesting
                    if (actData.typeProperties) {
                        Object.assign(act, actData.typeProperties);
                        // Normalize expression-type properties to plain strings.
                        // They may be Expression objects when loaded back from saved JSON,
                        // and double- or triple-wrapping builds up on repeated save/load.
                        const _unwrap = v => { while (v && typeof v === 'object' && 'value' in v) v = v.value; return typeof v === 'string' ? v : ''; };
                        if (act.type === 'IfCondition' && act.expression && typeof act.expression === 'object') act.expression = _unwrap(act.expression);
                        if (act.type === 'Until'       && act.expression && typeof act.expression === 'object') act.expression = _unwrap(act.expression);
                        if (act.type === 'Switch'      && act.on         && typeof act.on         === 'object') act.on = act.on.value || '';
                        if (act.type === 'ForEach'     && act.items      && typeof act.items      === 'object') act.items = _unwrap(act.items);
                        if (act.type === 'Filter') {
                            if (act.items     && typeof act.items     === 'object') act.items     = _unwrap(act.items);
                            if (act.condition && typeof act.condition === 'object') act.condition = _unwrap(act.condition);
                        }
                    }
                    // Copy activity-level fields that live outside typeProperties in the JSON
                    // (e.g. linkedServiceName for Script / SqlServerStoredProcedure).
                    if (actData.linkedServiceName) act.linkedServiceName = actData.linkedServiceName;
                    
                    act.userProperties = actData.userProperties || [];
                    act.container = canvasWrapper;
                    // Note: createDOMElement() is already called by Activity constructor
                    activities.push(act);
                    activityMap.set(actData.name, act);
                });
                
                // Recreate connections after all activities are created
                branchActivities.forEach((actData) => {
                    if (actData.dependsOn && actData.dependsOn.length > 0) {
                        const toActivity = activityMap.get(actData.name);
                        if (toActivity) {
                            actData.dependsOn.forEach(dep => {
                                const fromActivity = activityMap.get(dep.activity);
                                if (fromActivity) {
                                    const condition = dep.dependencyConditions?.[0] || 'Succeeded';
                                    const connection = new Connection(fromActivity, toActivity, condition);
                                    connections.push(connection);
                                }
                            });
                        }
                    }
                });
            }
            
            // Redraw canvas to show connections
            draw();
            
            // Update breadcrumb
            updateBreadcrumb();
            
            // Update sidebar to grey out restricted activities
            updateSidebarForBranchEditing(true, 'IfCondition');
            
            // Show back button
            document.getElementById('backToMainBtn').style.display = 'flex';
            
            // Clear properties panel
            showProperties(null);
            
            draw();
        };
        
        window.openForEachEditor = function(activityId) {
            const activity = activities.find(a => a.id == activityId);
            if (!activity) return;
            
            // Save current state
            const savedState = {
                activities: activities,
                connections: connections,
                selectedActivity: selectedActivity
            };
            
            // Push current context if already inside an editor (enables multi-level nesting)
            if (editingContext) editingContextStack.push(editingContext);
            editingContext = {
                parentActivity: activity,
                branch: 'activities',
                savedState: savedState
            };
            
            // Get body activities
            const bodyActivities = activity.activities || [];
            
            // Clear current canvas
            activities = [];
            connections = [];
            selectedActivity = null;
            
            // Clear only activity elements from canvas wrapper (keep canvas element)
            const canvasWrapper = document.getElementById('canvasWrapper');
            const activityElements = canvasWrapper.querySelectorAll('.activity-box');
            console.log('[openForEachEditor] Found', activityElements.length, 'activity elements to remove');
            activityElements.forEach(el => {
                if (el && el.parentNode) {
                    console.log('[openForEachEditor] Removing element:', el.dataset.activityId, el.className);
                    el.parentNode.removeChild(el);
                }
            });
            console.log('[openForEachEditor] After cleanup, canvasWrapper children:', canvasWrapper.children.length);
            
            // Load body activities into main canvas
            const activityMap = new Map();
            
            console.log('[openForEachEditor] Loading', bodyActivities?.length || 0, 'body activities');
            if (bodyActivities && Array.isArray(bodyActivities)) {
                bodyActivities.forEach((actData, idx) => {
                    console.log('[openForEachEditor] Loading activity:', actData.name, actData.type);
                    const x = actData.x !== undefined ? actData.x : (100 + (idx % 4) * 220);
                    const y = actData.y !== undefined ? actData.y : (100 + Math.floor(idx / 4) * 120);
                    
                    const act = new Activity(actData.type, x, y, canvasWrapper);
                    act.name = actData.name;
                    act.description = actData.description || '';
                    
                    // Copy typeProperties without nesting
                    if (actData.typeProperties) {
                        Object.assign(act, actData.typeProperties);
                        // Normalize expression-type properties to plain strings.
                        // They may be Expression objects when loaded back from saved JSON,
                        // and double- or triple-wrapping builds up on repeated save/load.
                        const _unwrap = v => { while (v && typeof v === 'object' && 'value' in v) v = v.value; return typeof v === 'string' ? v : ''; };
                        if (act.type === 'IfCondition' && act.expression && typeof act.expression === 'object') act.expression = _unwrap(act.expression);
                        if (act.type === 'Until'       && act.expression && typeof act.expression === 'object') act.expression = _unwrap(act.expression);
                        if (act.type === 'Switch'      && act.on         && typeof act.on         === 'object') act.on = act.on.value || '';
                        if (act.type === 'ForEach'     && act.items      && typeof act.items      === 'object') act.items = _unwrap(act.items);
                        if (act.type === 'Filter') {
                            if (act.items     && typeof act.items     === 'object') act.items     = _unwrap(act.items);
                            if (act.condition && typeof act.condition === 'object') act.condition = _unwrap(act.condition);
                        }
                    }
                    // Copy activity-level fields that live outside typeProperties in the JSON
                    // (e.g. linkedServiceName for Script / SqlServerStoredProcedure).
                    if (actData.linkedServiceName) act.linkedServiceName = actData.linkedServiceName;
                    
                    act.userProperties = actData.userProperties || [];
                    act.container = canvasWrapper;
                    activities.push(act);
                    activityMap.set(actData.name, act);
                });
                
                // Recreate connections after all activities are created
                bodyActivities.forEach((actData) => {
                    if (actData.dependsOn && actData.dependsOn.length > 0) {
                        const toActivity = activityMap.get(actData.name);
                        if (toActivity) {
                            actData.dependsOn.forEach(dep => {
                                const fromActivity = activityMap.get(dep.activity);
                                if (fromActivity) {
                                    const condition = dep.dependencyConditions?.[0] || 'Succeeded';
                                    const connection = new Connection(fromActivity, toActivity, condition);
                                    connections.push(connection);
                                }
                            });
                        }
                    }
                });
            }
            
            // Redraw canvas to show connections
            draw();
            
            // Update breadcrumb
            updateBreadcrumb();
            
            // Update sidebar to grey out restricted activities
            updateSidebarForBranchEditing(true, 'ForEach');
            
            // Show back button
            document.getElementById('backToMainBtn').style.display = 'flex';
            
            // Clear properties panel
            showProperties(null);
            
            draw();
        };

        // Until uses the same canvas structure as ForEach (body = activities array).
        // Wrap so we can apply Until-specific sidebar restrictions (e.g. greying out Validation).
        window.openUntilEditor = function(activityId) {
            window.openForEachEditor(activityId);
            // Override the 'ForEach' sidebar restrictions set by openForEachEditor with Until's own set.
            updateSidebarForBranchEditing(true, 'Until');
        };

        // Open the canvas editor for a Switch case or default branch
        window.openSwitchEditor = function(activityId, branch) {
            const activity = activities.find(a => a.id == activityId);
            if (!activity) return;

            // Save current state
            const savedState = {
                activities: activities,
                connections: connections,
                selectedActivity: selectedActivity
            };

            // Push current context if inside another editor (enables nesting)
            if (editingContext) editingContextStack.push(editingContext);
            editingContext = {
                parentActivity: activity,
                branch: branch,
                savedState: savedState
            };

            // Resolve which activities to load
            let branchActivities;
            if (branch === 'default') {
                branchActivities = activity.defaultActivities || [];
            } else if (branch.startsWith('case:')) {
                const caseIdx = parseInt(branch.split(':')[1]);
                branchActivities = (activity.cases[caseIdx] && activity.cases[caseIdx].activities) || [];
            } else {
                branchActivities = [];
            }

            // Clear current canvas
            activities = [];
            connections = [];
            selectedActivity = null;

            const canvasWrapper = document.getElementById('canvasWrapper');
            const activityElements = canvasWrapper.querySelectorAll('.activity-box');
            console.log('[openSwitchEditor] Found', activityElements.length, 'activity elements to remove');
            activityElements.forEach(el => {
                if (el && el.parentNode) {
                    console.log('[openSwitchEditor] Removing element:', el.dataset.activityId, el.className);
                    el.parentNode.removeChild(el);
                }
            });

            // Load branch activities into canvas
            const activityMap = new Map();
            console.log('[openSwitchEditor] Loading', branchActivities?.length || 0, 'branch activities');
            if (branchActivities && Array.isArray(branchActivities)) {
                branchActivities.forEach((actData, idx) => {
                    console.log('[openSwitchEditor] Loading activity:', actData.name, actData.type);
                    const x = actData.x !== undefined ? actData.x : (100 + (idx % 4) * 220);
                    const y = actData.y !== undefined ? actData.y : (100 + Math.floor(idx / 4) * 120);

                    const act = new Activity(actData.type, x, y, canvasWrapper);
                    act.name = actData.name;
                    act.description = actData.description || '';

                    if (actData.typeProperties) {
                        Object.assign(act, actData.typeProperties);
                        // Normalize expression-type properties that may still be Expression objects
                        if (act.type === 'Switch' && act.on && typeof act.on === 'object') {
                            act.on = act.on.value || '';
                        }
                        if (act.type === 'IfCondition' && act.expression && typeof act.expression === 'object') {
                            act.expression = act.expression.value || '';
                        }
                        if (act.type === 'ForEach' && act.items && typeof act.items === 'object') {
                            act.items = act.items.value || '';
                        }
                        if (act.type === 'Until' && act.expression && typeof act.expression === 'object') {
                            act.expression = act.expression.value || '';
                        }
                    }

                    act.userProperties = actData.userProperties || [];
                    act.container = canvasWrapper;
                    activities.push(act);
                    activityMap.set(actData.name, act);
                });

                // Recreate connections
                branchActivities.forEach((actData) => {
                    if (actData.dependsOn && actData.dependsOn.length > 0) {
                        const toActivity = activityMap.get(actData.name);
                        if (toActivity) {
                            actData.dependsOn.forEach(dep => {
                                const fromActivity = activityMap.get(dep.activity);
                                if (fromActivity) {
                                    const condition = dep.dependencyConditions?.[0] || 'Succeeded';
                                    const connection = new Connection(fromActivity, toActivity, condition);
                                    connections.push(connection);
                                }
                            });
                        }
                    }
                });
            }

            draw();
            updateBreadcrumb();
            updateSidebarForBranchEditing(true, 'Switch');
            document.getElementById('backToMainBtn').style.display = 'flex';
            showProperties(null);
            draw();
        };

        // Add a new empty case to a Switch activity
        window.addSwitchCase = function(activityId) {
            const activity = activities.find(a => a.id == activityId);
            if (!activity) return;
            activity.cases = activity.cases || [];
            activity.cases.push({ value: '', activities: [] });
            markAsDirty();
            showProperties(activity, 'activities');
        };

        // Remove a case from a Switch activity
        window.removeSwitchCase = function(activityId, caseIdx) {
            const activity = activities.find(a => a.id == activityId);
            if (!activity) return;
            activity.cases.splice(caseIdx, 1);
            markAsDirty();
            showProperties(activity, 'activities');
        };

        // Update the value of a Switch case
        window.updateSwitchCaseValue = function(activityId, caseIdx, newValue) {
            const activity = activities.find(a => a.id == activityId);
            if (!activity || !activity.cases[caseIdx]) return;
            activity.cases[caseIdx].value = newValue;
            markAsDirty();
        };

        window.backToMainPipeline = function() {
            if (!editingContext) return;
            
            // Save branch activities - properly serialize by removing UI properties
            const branchData = activities.map(a => {
                // Build clean activity object
                const cleaned = {
                    name: a.name,
                    type: a.type,
                    x: a.x,
                    y: a.y,
                    dependsOn: connections
                        .filter(c => c.to === a)
                        .map(c => ({
                            activity: c.from.name,
                            dependencyConditions: [c.condition || 'Succeeded']
                        })),
                    userProperties: a.userProperties || []
                };
                
                if (a.description) cleaned.description = a.description;
                if (a.state) cleaned.state = a.state;
                
                const { typeProperties: _tp1, activityProps: _ap1 } = buildNestedActivityTypeProperties(a);
                cleaned.typeProperties = _tp1;
                Object.assign(cleaned, _ap1);
                return cleaned;
            });
            
            // Update parent activity's branch
            if (editingContext.branch === 'true') {
                editingContext.parentActivity.ifTrueActivities = branchData;
            } else if (editingContext.branch === 'false') {
                editingContext.parentActivity.ifFalseActivities = branchData;
            } else if (editingContext.branch === 'activities') {
                editingContext.parentActivity.activities = branchData;
            } else if (editingContext.branch === 'default') {
                editingContext.parentActivity.defaultActivities = branchData;
            } else if (editingContext.branch && editingContext.branch.startsWith('case:')) {
                const caseIdx = parseInt(editingContext.branch.split(':')[1]);
                if (editingContext.parentActivity.cases[caseIdx]) {
                    editingContext.parentActivity.cases[caseIdx].activities = branchData;
                }
            }
            
            // Update container info display
            const containerInfo = editingContext.parentActivity.element?.querySelector('.container-info');
            if (containerInfo) {
                if (editingContext.parentActivity.type === 'ForEach' || editingContext.parentActivity.type === 'Until') {
                    const bodyCount = editingContext.parentActivity.activities.length;
                    containerInfo.innerHTML = \`
                        <div class="container-stat">
                            <span class="label">Loop:</span> 
                            <span class="count">\${bodyCount}</span> 
                            <span class="label">\${bodyCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                    \`;
                } else if (editingContext.parentActivity.type === 'Switch') {
                    let swTotalCount = 0;
                    if (editingContext.parentActivity.cases) {
                        editingContext.parentActivity.cases.forEach(c => {
                            swTotalCount += (c.activities || []).length;
                        });
                    }
                    if (editingContext.parentActivity.defaultActivities) {
                        swTotalCount += editingContext.parentActivity.defaultActivities.length;
                    }
                    const swCaseCount = editingContext.parentActivity.cases ? editingContext.parentActivity.cases.length : 0;
                    containerInfo.innerHTML = \`
                        <div class="container-stat">
                            <span class="label">Cases:</span> <span class="count">\${swCaseCount}</span>
                        </div>
                        <div class="container-stat">
                            <span class="label">Total:</span> 
                            <span class="count">\${swTotalCount}</span> 
                            <span class="label">\${swTotalCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                    \`;
                } else {
                    const trueCount = editingContext.parentActivity.ifTrueActivities.length;
                    const falseCount = editingContext.parentActivity.ifFalseActivities.length;
                    containerInfo.innerHTML = \`
                        <div class="container-stat">
                            <span class="branch-label">True:</span> 
                            <span class="count">\${trueCount}</span> 
                            <span class="label">\${trueCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                        <div class="container-stat">
                            <span class="branch-label">False:</span> 
                            <span class="count">\${falseCount}</span> 
                            <span class="label">\${falseCount === 1 ? 'activity' : 'activities'}</span>
                        </div>
                    \`;
                }
            }
            
            // Clear current canvas
            activities = [];
            connections = [];
            
            // Clear only activity elements from canvas wrapper (keep canvas element)
            const canvasWrapper = document.getElementById('canvasWrapper');
            const activityElements = canvasWrapper.querySelectorAll('.activity-box');
            console.log('[backToMainPipeline] Found', activityElements.length, 'activity elements to remove');
            activityElements.forEach(el => {
                if (el && el.parentNode) {
                    console.log('[backToMainPipeline] Removing element:', el.dataset.activityId, el.className);
                    el.parentNode.removeChild(el);
                }
            });
            console.log('[backToMainPipeline] After cleanup, canvasWrapper children:', canvasWrapper.children.length);
            
            if (editingContextStack.length > 0) {
                // Still nested — go back to parent editor level
                // The savedState of the current context holds the parent level's canvas activities
                activities = editingContext.savedState.activities;
                connections = editingContext.savedState.connections;
                selectedActivity = editingContext.savedState.selectedActivity;
                
                // Pop the grandparent context — it becomes the new current
                editingContext = editingContextStack.pop();
                
                console.log('[backToMainPipeline] Restoring parent editor level,', activities.length, 'activities');
                activities.forEach(a => {
                    a.container = canvasWrapper;
                    a.element = null;
                    a.createDOMElement();
                });
                
                updateBreadcrumb();
                updateSidebarForBranchEditing(true, editingContext.parentActivity.type);
                // Keep back button visible
            } else {
                // Back at root main pipeline
                activities = editingContext.savedState.activities;
                connections = editingContext.savedState.connections;
                selectedActivity = editingContext.savedState.selectedActivity;
                
                console.log('[backToMainPipeline] Restoring', activities.length, 'main pipeline activities');
                // Re-render all activities
                activities.forEach(a => {
                    a.container = canvasWrapper;
                    a.element = null;
                    a.createDOMElement();
                });
                
                // Clear editing context and stack
                editingContext = null;
                editingContextStack = [];
                
                // Update breadcrumb
                updateBreadcrumb();
                
                // Restore sidebar to normal (remove restrictions)
                updateSidebarForBranchEditing(false, null);
                
                // Hide back button
                document.getElementById('backToMainBtn').style.display = 'none';
            }
            
            // Refresh properties panel
            if (selectedActivity) {
                showProperties(selectedActivity, 'activities');
            }
            
            markAsDirty();
            draw();
        };
        
        function updateBreadcrumb() {
            const breadcrumb = document.getElementById('breadcrumb');
            if (editingContext) {
                // Build full path: ancestors from stack + current context
                const allContexts = [...editingContextStack, editingContext];
                const sep = '<span style="margin: 0 6px; color: var(--vscode-descriptionForeground);">›</span>';
                let parts = [];
                allContexts.forEach(ctx => {
                    let branchLabel;
                    if (ctx.branch === 'true') branchLabel = 'If True';
                    else if (ctx.branch === 'false') branchLabel = 'If False';
                    else if (ctx.branch === 'activities') branchLabel = 'Body';
                    else if (ctx.branch === 'default') branchLabel = 'Default';
                    else if (ctx.branch && ctx.branch.startsWith('case:')) {
                        const caseIdx = parseInt(ctx.branch.split(':')[1]);
                        const caseVal = ctx.parentActivity.cases && ctx.parentActivity.cases[caseIdx]
                            ? ctx.parentActivity.cases[caseIdx].value
                            : caseIdx;
                        branchLabel = 'Case: ' + (caseVal || caseIdx);
                    }
                    else branchLabel = ctx.branch;
                    parts.push('<span style="color: var(--vscode-descriptionForeground);">' + ctx.parentActivity.name + '</span>');
                    parts.push('<span style="font-weight: 600;">' + branchLabel + '</span>');
                });
                breadcrumb.innerHTML = parts.join(sep);
            } else {
                breadcrumb.innerHTML = '<span style="font-weight: 600;">Pipeline</span>';
            }
        }

        // Generate Activities tab content for IfCondition
        function generateIfConditionActivitiesTab(activity) {
            const trueActivities = activity.ifTrueActivities || [];
            const falseActivities = activity.ifFalseActivities || [];
            
            const renderActivitiesList = (activities) => {
                if (activities.length === 0) {
                    return '<div class="empty-branch">No activities</div>';
                }
                return activities.map(act => {
                    const icon = getIconForType(act.type);
                    return \`<div class="activity-pill">
                        <span class="activity-pill-icon">\${icon}</span>
                        <span>\${act.name}</span>
                    </div>\`;
                }).join('');
            };
            
            return \`
                <div style="padding: 4px 0;">
                    <div class="property-group" style="margin-bottom: 20px;">
                        <div class="property-label">Expression <span style="color: #d13438;">*</span></div>
                        <textarea class="property-input" id="propExpression" rows="3" placeholder="@equals(pipeline().parameters.value, 'expected')">\${activity.expression || ''}</textarea>
                    </div>
                    
                    <div class="branch-editor">
                        <div class="branch-header">
                            <span>True</span>
                            <button class="edit-activities-btn" onclick="openIfConditionEditor('\${activity.id}', 'true')">
                                Edit Activities
                            </button>
                        </div>
                        <div class="branch-content">
                            \${renderActivitiesList(trueActivities)}
                            <div style="margin-top: 8px; font-size: 11px; color: var(--vscode-descriptionForeground);">
                                \${trueActivities.length} \${trueActivities.length === 1 ? 'activity' : 'activities'}
                            </div>
                        </div>
                    </div>
                    
                    <div class="branch-editor">
                        <div class="branch-header">
                            <span>False</span>
                            <button class="edit-activities-btn" onclick="openIfConditionEditor('\${activity.id}', 'false')">
                                Edit Activities
                            </button>
                        </div>
                        <div class="branch-content">
                            \${renderActivitiesList(falseActivities)}
                            <div style="margin-top: 8px; font-size: 11px; color: var(--vscode-descriptionForeground);">
                                \${falseActivities.length} \${falseActivities.length === 1 ? 'activity' : 'activities'}
                            </div>
                        </div>
                    </div>
                </div>
            \`;
        }

        // Generate Activities tab for Until — expression + timeout + body
        function generateUntilActivitiesTab(activity) {
            const bodyActivities = activity.activities || [];
            const renderActivitiesList = (actList) => {
                if (actList.length === 0) return '<div class="empty-branch">No activities</div>';
                return actList.map(act => {
                    const icon = getIconForType(act.type);
                    return \`<div class="activity-pill"><span class="activity-pill-icon">\${icon}</span><span>\${act.name}</span></div>\`;
                }).join('');
            };
            const exprVal = typeof activity.expression === 'object' ? (activity.expression?.value || '') : (activity.expression || '');
            return \`
                <div style="padding: 4px 0;">
                    <div class="property-group" style="margin-bottom: 16px;">
                        <div class="property-label">Expression <span style="color: #d13438;">*</span></div>
                        <textarea class="property-input" id="propUntilExpression" rows="3" placeholder="@equals(variables('done'), true)">\${exprVal}</textarea>
                        <div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-top: 4px;">Loop stops when this expression evaluates to <strong>true</strong></div>
                    </div>
                    <div class="property-group" style="margin-bottom: 16px;">
                        <div class="property-label">Timeout</div>
                        <input type="text" class="property-input" id="propUntilTimeout" value="\${activity.timeout || '0.12:00:00'}" placeholder="0.12:00:00">
                    </div>
                    <div class="branch-editor">
                        <div class="branch-header">
                            <span>Body</span>
                            <button class="edit-activities-btn" onclick="openUntilEditor('\${activity.id}')">Edit Activities</button>
                        </div>
                        <div class="branch-content">
                            \${renderActivitiesList(bodyActivities)}
                            <div style="margin-top: 8px; font-size: 11px; color: var(--vscode-descriptionForeground);">
                                \${bodyActivities.length} \${bodyActivities.length === 1 ? 'activity' : 'activities'}
                            </div>
                        </div>
                    </div>
                </div>
            \`;
        }

        // Generate Activities tab content for ForEach
        function generateForEachActivitiesTab(activity) {
            const bodyActivities = activity.activities || [];
            
            const renderActivitiesList = (actList) => {
                if (actList.length === 0) {
                    return '<div class="empty-branch">No activities</div>';
                }
                return actList.map(act => {
                    const icon = getIconForType(act.type);
                    return \`<div class="activity-pill">
                        <span class="activity-pill-icon">\${icon}</span>
                        <span>\${act.name}</span>
                    </div>\`;
                }).join('');
            };
            
            return \`
                <div style="padding: 4px 0;">
                    <div class="branch-editor">
                        <div class="branch-header">
                            <span>Body</span>
                            <button class="edit-activities-btn" onclick="openForEachEditor('\${activity.id}')">
                                Edit Activities
                            </button>
                        </div>
                        <div class="branch-content">
                            \${renderActivitiesList(bodyActivities)}
                            <div style="margin-top: 8px; font-size: 11px; color: var(--vscode-descriptionForeground);">
                                \${bodyActivities.length} \${bodyActivities.length === 1 ? 'activity' : 'activities'}
                            </div>
                        </div>
                    </div>
                </div>
            \`;
        }

        // Generate Activities tab content for Switch
        function generateSwitchActivitiesTab(activity) {
            const cases = activity.cases || [];
            const defaultActivities = activity.defaultActivities || [];

            const renderActivitiesList = (actList) => {
                if (actList.length === 0) {
                    return '<div class="empty-branch">No activities</div>';
                }
                return actList.map(act => {
                    const icon = getIconForType(act.type);
                    return \`<div class="activity-pill">
                        <span class="activity-pill-icon">\${icon}</span>
                        <span>\${act.name}</span>
                    </div>\`;
                }).join('');
            };

            const casesHtml = cases.map((c, idx) => {
                const caseActivities = c.activities || [];
                const safeValue = (c.value || '').replace(/"/g, '&quot;');
                return \`
                    <div class="branch-editor" style="margin-bottom: 12px;">
                        <div class="branch-header" style="align-items: flex-start;">
                            <div style="display: flex; align-items: center; gap: 6px; flex: 1; min-width: 0;">
                                <span style="white-space: nowrap; font-size: 11px; color: var(--vscode-descriptionForeground);">Case:</span>
                                <input type="text" class="property-input" style="flex: 1; padding: 2px 6px; font-size: 11px; min-width: 0;"
                                    value="\${safeValue}"
                                    onchange="updateSwitchCaseValue('\${activity.id}', \${idx}, this.value)"
                                    placeholder="case value">
                            </div>
                            <div style="display: flex; gap: 4px; margin-left: 6px; flex-shrink: 0;">
                                <button class="edit-activities-btn" onclick="openSwitchEditor('\${activity.id}', 'case:\${idx}')">
                                    Edit
                                </button>
                                <button class="edit-activities-btn" style="background: var(--vscode-inputValidation-errorBackground, #5a1d1d); border-color: #d13438; color: #f48771;"
                                    onclick="removeSwitchCase('\${activity.id}', \${idx})">
                                    ✕
                                </button>
                            </div>
                        </div>
                        <div class="branch-content">
                            \${renderActivitiesList(caseActivities)}
                            <div style="margin-top: 8px; font-size: 11px; color: var(--vscode-descriptionForeground);">
                                \${caseActivities.length} \${caseActivities.length === 1 ? 'activity' : 'activities'}
                            </div>
                        </div>
                    </div>
                \`;
            }).join('');

            return \`
                <div style="padding: 4px 0;">
                    <div class="property-group" style="margin-bottom: 16px;">
                        <div class="property-label">On Expression <span style="color: #d13438;">*</span></div>
                        <textarea class="property-input" id="propOn" rows="3" placeholder="@pipeline().parameters.channel">\${typeof activity.on === 'object' ? (activity.on?.value || '') : (activity.on || '')}</textarea>
                    </div>

                    \${casesHtml}

                    <div style="margin-bottom: 16px;">
                        <button class="edit-activities-btn" style="width: 100%; justify-content: center;"
                            onclick="addSwitchCase('\${activity.id}')">
                            + Add Case
                        </button>
                    </div>

                    <div class="branch-editor">
                        <div class="branch-header">
                            <span>Default</span>
                            <button class="edit-activities-btn" onclick="openSwitchEditor('\${activity.id}', 'default')">
                                Edit Activities
                            </button>
                        </div>
                        <div class="branch-content">
                            \${renderActivitiesList(defaultActivities)}
                            <div style="margin-top: 8px; font-size: 11px; color: var(--vscode-descriptionForeground);">
                                \${defaultActivities.length} \${defaultActivities.length === 1 ? 'activity' : 'activities'}
                            </div>
                        </div>
                    </div>
                </div>
            \`;
        }

        // Configuration panel
        function showProperties(activity, activeTabId = null) {
            const rightPanel = document.getElementById('propertiesContent');
            const bottomPanel = document.getElementById('generalContent');
            
            // Toggle between pipeline-level and activity-level tabs
            const pipelineTabs = document.querySelectorAll('.pipeline-tab');
            const activityTabs = document.querySelectorAll('.activity-tab');
            const pipelinePanes = document.querySelectorAll('.pipeline-pane');
            const activityPanes = document.querySelectorAll('.activity-pane');
            
            if (!activity) {
                // Show pipeline-level tabs, hide activity-level tabs
                pipelineTabs.forEach(tab => tab.style.display = '');
                document.getElementById('activityTabsContainer').innerHTML = '';
                document.getElementById('activityPanesContainer').innerHTML = '';
                pipelinePanes.forEach(pane => pane.style.display = '');
                
                // Activate first pipeline tab or the specified tab
                document.querySelectorAll('.config-tab').forEach(t => {
                    t.classList.remove('active');
                    t.style.borderBottom = 'none';
                    t.style.color = 'var(--vscode-tab-inactiveForeground)';
                });
                document.querySelectorAll('.config-tab-pane').forEach(p => {
                    p.classList.remove('active');
                    p.style.display = 'none';
                });
                
                // Select the active tab
                const tabToActivate = activeTabId || 'parameters';
                const activeTab = document.querySelector(\`.pipeline-tab[data-tab="\${tabToActivate}"]\`);
                const activePane = document.getElementById(\`tab-\${tabToActivate}\`);
                
                if (activeTab) {
                    activeTab.classList.add('active');
                    activeTab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
                    activeTab.style.color = 'var(--vscode-tab-activeForeground)';
                }
                if (activePane) {
                    activePane.classList.add('active');
                    activePane.style.display = 'block';
                }
                
                // Render pipeline properties
                renderPipelineParameters();
                renderPipelineVariables();
                
                // Update concurrency input
                const concurrencyInput = document.getElementById('concurrencyInput');
                if (concurrencyInput) {
                    concurrencyInput.value = pipelineData.concurrency || 1;
                }
                
                rightPanel.innerHTML = '<div class="empty-state">Select an activity to view its properties</div>';
                return;
            }
            
            // Get schema for activity
            const schema = ${JSON.stringify(activitySchemas)}[activity.type];
            const tabs = schema?.tabs || ['General', 'Settings', 'User Properties'];
            
            // Hide pipeline-level tabs
            pipelineTabs.forEach(tab => tab.style.display = 'none');
            pipelinePanes.forEach(pane => pane.style.display = 'none');
            
            // Helper function to generate form fields
            function generateFormField(key, prop, activity) {
                // Check conditional rendering
                if (prop.conditional) {
                    const conditionField = prop.conditional.field;
                    const conditionValue = prop.conditional.value;
                    const actualValue = activity[conditionField];
                    
                    if (prop.conditional.notEmpty) {
                        // notEmpty: true — show only when field has a defined, non-empty value (0 counts as set)
                        if (actualValue === undefined || actualValue === null || actualValue === '') {
                            return '';
                        }
                    } else {
                        // For dynamicAllocation: if undefined, default to 'Enabled' behavior (show min/max)
                        const effectiveValue = (conditionField === 'dynamicAllocation' && actualValue === undefined) 
                            ? 'Enabled' 
                            : actualValue;
                        
                        // Skip this field if condition is not met
                        // Support both single value and array of values
                        if (Array.isArray(conditionValue)) {
                            if (!conditionValue.includes(effectiveValue)) {
                                return '';
                            }
                        } else {
                            if (effectiveValue !== conditionValue) {
                                return '';
                            }
                        }
                    }
                }

                // Check conditionalAll — all conditions must be met (AND logic)
                if (prop.conditionalAll) {
                    for (const cond of prop.conditionalAll) {
                        const actualValue = activity[cond.field];
                        if (Array.isArray(cond.value)) {
                            if (!cond.value.includes(actualValue)) return '';
                        } else {
                            if (actualValue !== cond.value) return '';
                        }
                    }
                }
                
                // Check nested conditional (for fields that depend on other conditional fields)
                if (prop.nestedConditional) {
                    const nestedField = prop.nestedConditional.field;
                    const nestedValue = prop.nestedConditional.value;
                    let actualNestedValue = activity[nestedField];
                    
                    // If the nested field value is undefined, check if the nested field has a default value in schema
                    // BUT only use the default if the nested field's own conditional would be met
                    if (actualNestedValue === undefined) {
                        const allProps = {...schema.commonProperties, ...schema.typeProperties, ...schema.advancedProperties};
                        const nestedFieldProp = allProps[nestedField];
                        
                        // Check if the nested field itself would be rendered
                        let nestedFieldWouldRender = true;
                        
                        // Check the nested field's conditional
                        if (nestedFieldProp && nestedFieldProp.conditional) {
                            const nestedCondField = nestedFieldProp.conditional.field;
                            const nestedCondValue = nestedFieldProp.conditional.value;
                            const nestedActualValue = activity[nestedCondField];
                            
                            if (Array.isArray(nestedCondValue)) {
                                nestedFieldWouldRender = nestedCondValue.includes(nestedActualValue);
                            } else {
                                nestedFieldWouldRender = nestedActualValue === nestedCondValue;
                            }
                        }
                        
                        // Also check the nested field's nestedConditional
                        if (nestedFieldWouldRender && nestedFieldProp && nestedFieldProp.nestedConditional) {
                            const nestedNestedField = nestedFieldProp.nestedConditional.field;
                            const nestedNestedValue = nestedFieldProp.nestedConditional.value;
                            const nestedNestedActualValue = activity[nestedNestedField];
                            
                            if (Array.isArray(nestedNestedValue)) {
                                nestedFieldWouldRender = nestedNestedValue.includes(nestedNestedActualValue);
                            } else {
                                nestedFieldWouldRender = nestedNestedActualValue === nestedNestedValue;
                            }
                        }
                        
                        // Only use default if the nested field would actually be rendered
                        if (nestedFieldWouldRender && nestedFieldProp && nestedFieldProp.default !== undefined) {
                            actualNestedValue = nestedFieldProp.default;
                        }
                    }
                    
                    // Support both single value and array of values
                    if (Array.isArray(nestedValue)) {
                        if (!nestedValue.includes(actualNestedValue)) {
                            return '';
                        }
                    } else {
                        if (actualNestedValue !== nestedValue) {
                            return '';
                        }
                    }
                }
                
                let value = (activity[key] !== undefined && activity[key] !== null) ? activity[key] : (prop.default !== undefined ? prop.default : '');
                
                // Handle reference objects (e.g., {referenceName: "...", type: "..."})
                if (prop.type === 'reference' && typeof value === 'object' && value !== null) {
                    value = value.referenceName || JSON.stringify(value);
                }
                
                // Handle notebook/sparkPool reference objects that use text type
                if (prop.type === 'text' && typeof value === 'object' && value !== null && value.referenceName) {
                    // Handle Expression format: { referenceName: { value: "name", type: "Expression" } }
                    if (typeof value.referenceName === 'object' && value.referenceName.value) {
                        value = value.referenceName.value;
                    } else if (typeof value.referenceName === 'string') {
                        // Handle direct string format: { referenceName: "name" }
                        value = value.referenceName;
                    }
                }
                
                const required = prop.required ? ' *' : '';
                
                let fieldHtml = \`<div class="property-group">\`;
                fieldHtml += \`<div class="property-label">\${prop.label}\${required}</div>\`;
                
                switch (prop.type) {
                    case 'string':
                        if (prop.multiline) {
                            fieldHtml += \`<textarea class="property-input" data-key="\${key}" rows="3" placeholder="\${prop.label}...">\${value}</textarea>\`;
                        } else {
                            fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="\${prop.label}">\`;
                        }
                        break;
                    case 'text':
                        const readonly = prop.readonly ? 'readonly' : '';
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="\${prop.placeholder || prop.label}" \${readonly}>\`;
                        break;
                    case 'textarea':
                        const rows = prop.rows || 3;
                        const placeholder = prop.placeholder || prop.label || '';
                        fieldHtml += \`<textarea class="property-input" data-key="\${key}" rows="\${rows}" placeholder="\${placeholder}" style="width: 100%; font-family: var(--vscode-editor-font-family); font-size: 12px;">\${value}</textarea>\`;
                        break;
                    case 'number':
                        const min = prop.min !== undefined ? \`min="\${prop.min}"\` : '';
                        const max = prop.max !== undefined ? \`max="\${prop.max}"\` : '';
                        const numPlaceholder = prop.placeholder ? \`placeholder="\${prop.placeholder}"\` : '';
                        fieldHtml += \`<input type="number" class="property-input" data-key="\${key}" value="\${value}" \${min} \${max} \${numPlaceholder}>\`;
                        break;
                    case 'boolean':
                        const checked = value ? 'checked' : '';
                        const disabledAttr = prop.readonly ? 'disabled' : '';
                        fieldHtml += \`<div style="flex: 1;"><input type="checkbox" class="property-input" data-key="\${key}" \${checked} \${disabledAttr} style="width: auto; margin: 0;"></div>\`;
                        break;
                    case 'select':
                        fieldHtml += \`<select class="property-input" data-key="\${key}">\`;
                        // Add placeholder option if specified and no value is set
                        if (prop.placeholder && !value) {
                            fieldHtml += \`<option value="" disabled selected>\${prop.placeholder}</option>\`;
                        }
                        
                        prop.options.forEach((opt, idx) => {
                            // Use optionValues if available, otherwise use the option itself
                            const optValue = prop.optionValues ? prop.optionValues[idx] : opt;
                            const selected = optValue === value ? 'selected' : '';
                            // Use optionLabels if available, otherwise use the option value as display
                            const displayName = (prop.optionLabels && prop.optionLabels[opt]) ? prop.optionLabels[opt] : opt;
                            fieldHtml += \`<option value="\${optValue}" \${selected}>\${displayName}</option>\`;
                        });
                        fieldHtml += \`</select>\`;
                        break;
                    case 'radio':
                        fieldHtml += \`<div style="display: flex; gap: 16px; flex: 1; align-items: center;">\`;
                        prop.options.forEach((opt, idx) => {
                            // Use optionValues if available, otherwise use the option itself
                            const optValue = prop.optionValues ? prop.optionValues[idx] : opt;
                            // Skip option if optionConditionals says it shouldn't appear for current activity state
                            if (prop.optionConditionals && prop.optionConditionals[optValue]) {
                                const optCond = prop.optionConditionals[optValue];
                                const condActual = activity[optCond.field];
                                const condPasses = Array.isArray(optCond.value) ? optCond.value.includes(condActual) : condActual === optCond.value;
                                if (!condPasses) return;
                            }
                            const checked = optValue === value ? 'checked' : '';
                            const isDisabledOpt = prop.disabledOptionValues && prop.disabledOptionValues.includes(optValue);
                            // Custom display names
                            let displayName;
                            if (opt === 'storedProcedure') displayName = 'Stored procedure';
                            else if (opt === 'filePathInDataset') displayName = 'File path in dataset';
                            else if (opt === 'wildcardFilePath') displayName = 'Wildcard file path';
                            else if (opt === 'listOfFiles') displayName = 'List of files';
                            else displayName = opt.charAt(0).toUpperCase() + opt.slice(1);
                            fieldHtml += \`<label style="display: flex; align-items: center; gap: 6px; cursor: \${isDisabledOpt ? 'not-allowed' : 'pointer'}; opacity: \${isDisabledOpt ? '0.4' : '1'};">\`;
                            fieldHtml += \`<input type="radio" name="\${key}" data-key="\${key}" value="\${optValue}" \${checked} \${isDisabledOpt ? 'disabled' : ''} style="margin: 0;">\`;
                            fieldHtml += \`<span>\${displayName}</span>\`;
                            fieldHtml += \`</label>\`;
                        });
                        fieldHtml += \`</div>\`;
                        break;
                    case 'keyvalue':
                        fieldHtml += \`<div style="flex: 1;"><div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Key-value pairs with types</div>\`;
                        fieldHtml += \`<button class="add-kv-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Parameter</button>\`;
                        fieldHtml += \`<div class="kv-list" data-key="\${key}">\`;
                        
                        // Load existing parameters if they exist
                        if (value && typeof value === 'object') {
                            for (const [paramKey, paramValue] of Object.entries(value)) {
                                const paramVal = paramValue?.value || '';
                                const paramType = paramValue?.type || 'string';
                                const types = prop.valueTypes || ['string', 'int', 'float', 'bool'];
                                const typeOptions = types.map(t => 
                                    \`<option value="\${t}" \${t === paramType ? 'selected' : ''}>\${t}</option>\`
                                ).join('');
                                
                                // For Boolean type, use dropdown instead of text input
                                // For Array type, show nested array items with type/content
                                let valueField;
                                if (paramType === 'Boolean') {
                                    valueField = \`<select class="property-input kv-value" style="flex: 1;">
                                        <option value="true" \${paramVal === 'true' || paramVal === true ? 'selected' : ''}>true</option>
                                        <option value="false" \${paramVal === 'false' || paramVal === false || !paramVal ? 'selected' : ''}>false</option>
                                    </select>\`;
                                } else if (paramType === 'Array') {
                                    // For Array type, render nested array editor
                                    const arrayContent = paramValue?.content || [];
                                    valueField = \`<div class="array-items-container" style="flex: 1; border: 1px solid var(--vscode-input-border); border-radius: 3px; padding: 8px; background: var(--vscode-input-background);"></div>\`;
                                } else {
                                    valueField = \`<input type="text" class="property-input kv-value" value="\${paramVal}" placeholder="Value" style="flex: 1;">\`;
                                }
                                
                                fieldHtml += \`
                                    <div class="property-group kv-pair-group" style="margin-bottom: 8px; display: flex; gap: 8px; align-items: \${paramType === 'Array' ? 'flex-start' : 'center'};" data-array-content='\${paramType === 'Array' ? JSON.stringify(paramValue?.content || []) : ''}'>\`;
                                
                                if (paramType === 'Array') {
                                    fieldHtml += \`
                                        <div style="flex: 1; display: flex; flex-direction: column; gap: 8px;">
                                            <div style="display: flex; gap: 8px; align-items: center;">
                                                <input type="text" class="property-input kv-key" value="\${paramKey}" placeholder="Key" style="flex: 1;">
                                                <select class="property-input kv-type" style="flex: 0 0 100px;">\${typeOptions}</select>
                                                <button class="remove-kv-btn" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Remove</button>
                                            </div>
                                            \${valueField}
                                        </div>
                                    \`;
                                } else {
                                    fieldHtml += \`
                                        <input type="text" class="property-input kv-key" value="\${paramKey}" placeholder="Key" style="flex: 1;">
                                        \${valueField}
                                        <select class="property-input kv-type" style="flex: 0 0 100px;">\${typeOptions}</select>
                                        <button class="remove-kv-btn" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Remove</button>
                                    \`;
                                }
                                
                                fieldHtml += \`</div>\`;
                            }
                        }
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'datetime':
                        fieldHtml += \`<input type="datetime-local" class="property-input" data-key="\${key}" value="\${value}" placeholder="\${prop.placeholder || ''}" step="1">\`;
                        break;
                    case 'dataset':
                        console.log('[GenerateField] Dataset field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        fieldHtml += \`<select class="property-input dataset-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">Select dataset...</option>\`;
                        // Filter datasets if datasetFilter is specified
                        let filteredDatasets = datasetList || [];
                        if (prop.datasetFilter === 'storageOnly') {
                            // For Delete activity: only show Blob Storage and ADLS Gen2 datasets
                            filteredDatasets = filteredDatasets.filter(dsName => {
                                const dsContent = datasetContents[dsName];
                                if (!dsContent || !dsContent.properties) return false;
                                const locationType = dsContent.properties.typeProperties?.location?.type;
                                return locationType === 'AzureBlobFSLocation' || locationType === 'AzureBlobStorageLocation';
                            });
                        } else if (prop.datasetFilter && Array.isArray(prop.datasetFilter)) {
                            // Legacy: filter by dataset type (inclusion list)
                            filteredDatasets = filteredDatasets.filter(dsName => {
                                const dsContent = datasetContents[dsName];
                                if (!dsContent || !dsContent.properties) return false;
                                const dsType = dsContent.properties.type;
                                return prop.datasetFilter.includes(dsType);
                            });
                        } else if (prop.datasetFilter && typeof prop.datasetFilter === 'object' && !Array.isArray(prop.datasetFilter) && prop.datasetFilter.excludeTypes) {
                            // Exclusion list: hide datasets whose type is in excludeTypes
                            filteredDatasets = filteredDatasets.filter(dsName => {
                                const dsContent = datasetContents[dsName];
                                if (!dsContent || !dsContent.properties) return true;
                                const dsType = dsContent.properties.type;
                                return !prop.datasetFilter.excludeTypes.includes(dsType);
                            });
                        }
                        if (filteredDatasets.length > 0) {
                            filteredDatasets.forEach(ds => {
                                const selected = ds === value ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected dataset:', ds, 'matches value:', value);
                                fieldHtml += \`<option value="\${ds}" \${selected}>\${ds}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'validation-dataset':
                        console.log('[GenerateField] Validation-dataset field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        // Extract referenceName if value is an object
                        let datasetRefName = '';
                        if (typeof value === 'object' && value !== null && value.referenceName) {
                            datasetRefName = value.referenceName;
                        } else if (typeof value === 'string') {
                            datasetRefName = value;
                        }
                        fieldHtml += \`<select class="property-input validation-dataset-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">\${prop.placeholder || 'Select dataset...'}</option>\`;
                        // Filter datasets to only show specific types for Validation activity
                        let validationDatasets = datasetList || [];
                        const allowedTypes = [
                            'AzureBlobStorage',
                            'AzureBlobFSLocation',  // ADLS Gen2
                            'AzureSqlTable',
                            'AzureSynapseAnalytics',
                            'AzureSqlDWTable'  // Synapse dedicated SQL pool
                        ];
                        validationDatasets = validationDatasets.filter(dsName => {
                            const dsContent = datasetContents[dsName];
                            if (!dsContent || !dsContent.properties) return false;
                            const dsType = dsContent.properties.type;
                            const locationType = dsContent.properties.typeProperties?.location?.type;
                            // Check either dataset type or location type
                            return allowedTypes.includes(dsType) || allowedTypes.includes(locationType);
                        });
                        if (validationDatasets.length > 0) {
                            validationDatasets.forEach(ds => {
                                const selected = ds === datasetRefName ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected validation dataset:', ds, 'matches value:', datasetRefName);
                                fieldHtml += \`<option value="\${ds}" \${selected}>\${ds}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'radio-with-info':
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="display: flex; flex-direction: column; gap: 12px;">\`;
                        prop.options.forEach(opt => {
                            const checked = opt === value ? 'checked' : '';
                            const isDefault = opt === (prop.default || 'ignore');
                            const actualChecked = value === undefined && isDefault ? 'checked' : checked;
                            
                            // Info text for each option
                            let infoText = '';
                            if (opt === 'ignore') {
                                infoText = 'Check if the folder exists only';
                            } else if (opt === 'true') {
                                infoText = 'Check if the folder exists and has items in it';
                            } else if (opt === 'false') {
                                infoText = 'Check if the folder exists and that it is empty';
                            }
                            
                            fieldHtml += \`<label style="display: flex; align-items: center; gap: 8px; cursor: pointer;">\`;
                            fieldHtml += \`<input type="radio" name="\${key}" data-key="\${key}" value="\${opt}" \${actualChecked} style="margin: 0;">\`;
                            fieldHtml += \`<span style="text-transform: capitalize;">\${opt}</span>\`;
                            // Add info icon with hover tooltip
                            fieldHtml += \`<span class="info-icon" title="\${infoText}" style="display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; border: 1px solid var(--vscode-foreground); font-size: 11px; cursor: help; color: var(--vscode-foreground);">i</span>\`;
                            fieldHtml += \`</label>\`;
                        });
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'pipeline':
                        console.log('[GenerateField] Pipeline field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        fieldHtml += \`<select class="property-input pipeline-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">\${prop.placeholder || 'Select pipeline...'}</option>\`;
                        if (pipelineList && pipelineList.length > 0) {
                            // Get current pipeline name from file path to exclude it (prevent self-reference)
                            let currentPipelineName = null;
                            if (currentFilePath) {
                                const parts = currentFilePath.replace(/\\\\/g, '/').split('/');
                                const filename = parts[parts.length - 1];
                                currentPipelineName = filename.replace('.json', '');
                            }
                            
                            pipelineList.forEach(pl => {
                                // Skip current pipeline to prevent self-reference
                                if (pl === currentPipelineName) {
                                    console.log('[GenerateField] Skipping current pipeline:', pl);
                                    return;
                                }
                                const selected = pl === value ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected pipeline:', pl, 'matches value:', value);
                                fieldHtml += \`<option value="\${pl}" \${selected}>\${pl}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'reference':
                        fieldHtml += \`<div style="display: flex; gap: 8px; flex: 1;">\`;
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="Select \${prop.label}..." readonly>\`;
                        fieldHtml += \`<button style="padding: 6px 12px; background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Browse</button>\`;
                        fieldHtml += \`</div>\`;
                        break;
                    case 'expression':
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}" placeholder="\${prop.placeholder || 'Enter expression...'}">\`;
                        break;
                    case 'getmetadata-dataset':
                        console.log('[GenerateField] GetMetadata-dataset field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        fieldHtml += \`<select class="property-input getmetadata-dataset-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">\${prop.placeholder || 'Select dataset...'}</option>\`;
                        // Filter datasets to only show specific types for GetMetadata activity
                        let getMetadataDatasets = datasetList || [];
                        const getMetadataAllowedTypes = [
                            'AzureBlobStorage',
                            'AzureBlobFSLocation',  // ADLS Gen2
                            'AzureSqlTable',
                            'AzureSynapseAnalytics',
                            'AzureSqlDWTable'  // Synapse dedicated SQL pool
                        ];
                        getMetadataDatasets = getMetadataDatasets.filter(dsName => {
                            const dsContent = datasetContents[dsName];
                            if (!dsContent || !dsContent.properties) return false;
                            const dsType = dsContent.properties.type;
                            const locationType = dsContent.properties.typeProperties?.location?.type;
                            // Check either dataset type or location type
                            return getMetadataAllowedTypes.includes(dsType) || getMetadataAllowedTypes.includes(locationType);
                        });
                        if (getMetadataDatasets.length > 0) {
                            getMetadataDatasets.forEach(ds => {
                                const selected = ds === value ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected GetMetadata dataset:', ds, 'matches value:', value);
                                fieldHtml += \`<option value="\${ds}" \${selected}>\${ds}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'dataset-lookup':
                        console.log('[GenerateField] Lookup-dataset field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        fieldHtml += \`<select class="property-input lookup-dataset-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">\${prop.placeholder || 'Select dataset...'}</option>\`;
                        // For Lookup activity, show all datasets
                        if (datasetList && datasetList.length > 0) {
                            datasetList.forEach(ds => {
                                const selected = ds === value ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected Lookup dataset:', ds, 'matches value:', value);
                                fieldHtml += \`<option value="\${ds}" \${selected}>\${ds}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'getmetadata-fieldlist':
                        // Render the field list UI with cleaner design matching Set Variable pattern
                        const fieldListData = value || [];
                        fieldHtml += \`<div style="flex: 1;"><div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Field list items with argument types</div>\`;
                        fieldHtml += \`<button class="add-getmetadata-field-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Field</button>\`;
                        fieldHtml += \`<div class="getmetadata-fieldlist-container" data-key="\${key}">\`;
                        
                        // Determine options based on dataset type
                        const isStorageDataset = activity._datasetLocationType === 'AzureBlobStorageLocation' || activity._datasetLocationType === 'AzureBlobFSLocation';
                        let argumentOptions;
                        if (isStorageDataset) {
                            argumentOptions = [
                                { value: 'childItems', label: 'Child items' },
                                { value: 'exists', label: 'Exists' },
                                { value: 'itemName', label: 'Item name' },
                                { value: 'itemType', label: 'Item type' },
                                { value: 'lastModified', label: 'Last modified' }
                            ];
                        } else {
                            argumentOptions = [
                                { value: 'columnCount', label: 'Column count' },
                                { value: 'exists', label: 'Exists' },
                                { value: 'structure', label: 'Structure' }
                            ];
                        }
                        
                        // Render existing field list items
                        fieldListData.forEach((field, idx) => {
                            const fieldValue = field.value || '';
                            const fieldType = field.type || 'predefined';
                            const isDynamic = fieldType === 'dynamic';
                            
                            // Build argument dropdown options
                            const argOptions = argumentOptions.map(opt => 
                                \`<option value="\${opt.value}" \${!isDynamic && fieldValue === opt.value ? 'selected' : ''}>\${opt.label}</option>\`
                            ).join('');
                            
                            fieldHtml += \`
                                <div class="property-group getmetadata-field-item" data-index="\${idx}" style="margin-bottom: 8px; display: flex; gap: 8px; align-items: center;">
                                    \${isDynamic ? \`<input type="text" class="property-input getmetadata-field-value" data-index="\${idx}" value="\${fieldValue}" placeholder="Dynamic content" style="flex: 1;">\` : ''}
                                    <select class="property-input getmetadata-field-type" data-index="\${idx}" style="flex: \${isDynamic ? '0 0 150px' : '1'}">
                                        \${argOptions}
                                        <option value="__dynamic__" \${isDynamic ? 'selected' : ''}>Dynamic content</option>
                                    </select>
                                    <button class="remove-getmetadata-field-btn" data-index="\${idx}" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Remove</button>
                                </div>
                            \`;
                        });
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'script-linkedservice':
                        console.log('[GenerateField] Script-linkedservice field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        // Extract referenceName if value is an object
                        let linkedServiceRefName = '';
                        if (typeof value === 'object' && value !== null && value.referenceName) {
                            linkedServiceRefName = value.referenceName;
                        } else if (typeof value === 'string') {
                            linkedServiceRefName = value;
                        }
                        fieldHtml += \`<select class="property-input script-linkedservice-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">\${prop.placeholder || 'Select linked service...'}</option>\`;
                        
                        // Get linked services list from extension (we'll receive this via message)
                        if (window.linkedServicesList && window.linkedServicesList.length > 0) {
                            window.linkedServicesList.forEach(ls => {
                                const selected = ls.name === linkedServiceRefName ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected linked service:', ls.name, 'matches value:', linkedServiceRefName);
                                fieldHtml += \`<option value="\${ls.name}" \${selected}>\${ls.name}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'script-array':
                        // Render the scripts array UI
                        const scriptsData = value || [{ type: 'Query', text: '', parameters: [] }];
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Configure one or more scripts</div>\`;
                        fieldHtml += \`<button class="add-script-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Script</button>\`;
                        fieldHtml += \`<div class="scripts-container" data-key="\${key}">\`;
                        
                        // Render existing scripts
                        scriptsData.forEach((script, scriptIdx) => {
                            const scriptType = script.type || 'Query';
                            const scriptText = script.text || '';
                            const scriptParams = script.parameters || [];
                            
                            fieldHtml += \`
                                <div class="script-item" data-script-index="\${scriptIdx}" style="border: 1px solid var(--vscode-panel-border); border-radius: 4px; padding: 12px; margin-bottom: 12px; background: var(--vscode-editor-background);">
                                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                                        <div style="font-weight: 600; font-size: 13px;">Script \${scriptIdx + 1}</div>
                                        <button class="remove-script-btn" data-script-index="\${scriptIdx}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px;">Remove Script</button>
                                    </div>
                                    
                                    <div style="margin-bottom: 12px;">
                                        <div style="display: flex; gap: 16px; align-items: center; margin-bottom: 8px;">
                                            <label style="display: flex; align-items: center; gap: 6px; cursor: pointer;">
                                                <input type="radio" name="scriptType\${scriptIdx}" class="script-type-radio" data-script-index="\${scriptIdx}" value="Query" \${scriptType === 'Query' ? 'checked' : ''} style="margin: 0;">
                                                <span>Query</span>
                                                <span class="info-icon" title="Database statements that return one or more result sets." style="display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; border: 1px solid var(--vscode-foreground); font-size: 11px; cursor: help; color: var(--vscode-foreground);">i</span>
                                            </label>
                                            <label style="display: flex; align-items: center; gap: 6px; cursor: pointer;">
                                                <input type="radio" name="scriptType\${scriptIdx}" class="script-type-radio" data-script-index="\${scriptIdx}" value="NonQuery" \${scriptType === 'NonQuery' ? 'checked' : ''} style="margin: 0;">
                                                <span>NonQuery</span>
                                                <span class="info-icon" title="Database statements that perform catalog operations (for example, querying the structure of a database or creating database objects such as tables), or change the data in a database by executing UPDATE, INSERT, or DELETE statements." style="display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; border: 1px solid var(--vscode-foreground); font-size: 11px; cursor: help; color: var(--vscode-foreground);">i</span>
                                            </label>
                                        </div>
                                    </div>
                                    
                                    <div style="margin-bottom: 12px;">
                                        <label style="display: block; font-size: 12px; font-weight: 600; color: var(--vscode-descriptionForeground); margin-bottom: 6px;">Script *</label>
                                        <textarea class="property-input script-text-area" data-script-index="\${scriptIdx}" rows="4" placeholder="Enter your script here..." style="width: 100%; font-family: monospace; font-size: 12px;">\${scriptText}</textarea>
                                    </div>
                                    
                                    <div style="margin-bottom: 8px;">
                                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                            <label style="font-size: 12px; font-weight: 600; color: var(--vscode-descriptionForeground);">Script Parameters</label>
                                            <button class="add-script-param-btn" data-script-index="\${scriptIdx}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px;">+ Add Parameter</button>
                                        </div>
                                        <div class="script-params-container" data-script-index="\${scriptIdx}">\`;
                            
                            // Render existing parameters
                            scriptParams.forEach((param, paramIdx) => {
                                const paramName = param.name || '';
                                const paramType = param.type || 'String';
                                const paramValue = param.value !== undefined && param.value !== null ? param.value : '';
                                const paramDirection = param.direction || 'Input';
                                const paramSize = param.size || '';
                                const showSize = (paramDirection === 'Output' || paramDirection === 'InputOutput') && (paramType === 'String' || paramType === 'Byte[]');
                                
                                fieldHtml += \`
                                    <div class="script-param-row" data-script-index="\${scriptIdx}" data-param-index="\${paramIdx}" style="display: grid; grid-template-columns: 40px 1fr 120px 1fr 80px 100px 80px 30px; gap: 8px; margin-bottom: 8px; align-items: center; padding: 8px; background: var(--vscode-sideBar-background); border-radius: 3px;">
                                        <div style="font-size: 11px; color: var(--vscode-descriptionForeground);">\${paramIdx + 1}</div>
                                        <input type="text" class="property-input script-param-name" value="\${paramName}" placeholder="Name" style="font-size: 11px; padding: 4px 6px;">
                                        <select class="property-input script-param-type" style="font-size: 11px; padding: 4px 6px;">
                                            <option value="Boolean" \${paramType === 'Boolean' ? 'selected' : ''}>Boolean</option>
                                            <option value="Byte[]" \${paramType === 'Byte[]' ? 'selected' : ''}>Byte[]</option>
                                            <option value="Datetime" \${paramType === 'Datetime' ? 'selected' : ''}>Datetime</option>
                                            <option value="Datetimeoffset" \${paramType === 'Datetimeoffset' ? 'selected' : ''}>Datetimeoffset</option>
                                            <option value="Decimal" \${paramType === 'Decimal' ? 'selected' : ''}>Decimal</option>
                                            <option value="Double" \${paramType === 'Double' ? 'selected' : ''}>Double</option>
                                            <option value="Guid" \${paramType === 'Guid' ? 'selected' : ''}>Guid</option>
                                            <option value="Int16" \${paramType === 'Int16' ? 'selected' : ''}>Int16</option>
                                            <option value="Int32" \${paramType === 'Int32' ? 'selected' : ''}>Int32</option>
                                            <option value="Int64" \${paramType === 'Int64' ? 'selected' : ''}>Int64</option>
                                            <option value="Single" \${paramType === 'Single' ? 'selected' : ''}>Single</option>
                                            <option value="String" \${paramType === 'String' ? 'selected' : ''}>String</option>
                                            <option value="Timespan" \${paramType === 'Timespan' ? 'selected' : ''}>Timespan</option>
                                        </select>
                                        <input type="text" class="property-input script-param-value" value="\${paramValue === null ? '' : paramValue}" placeholder="Value" \${param.value === null ? 'disabled' : ''} style="font-size: 11px; padding: 4px 6px;\${param.value === null ? ' opacity: 0.5; cursor: not-allowed;' : ''}">
                                        <label style="display: flex; align-items: center; gap: 4px; font-size: 11px; cursor: pointer;">
                                            <input type="checkbox" class="script-param-null" \${param.value === null ? 'checked' : ''} style="margin: 0;">
                                            <span>Null</span>
                                        </label>
                                        <select class="property-input script-param-direction" style="font-size: 11px; padding: 4px 6px;" data-param-type="\${paramType}">
                                            <option value="Input" \${paramDirection === 'Input' ? 'selected' : ''} \${paramType === 'Byte[]' ? 'disabled' : ''}>Input</option>
                                            <option value="Output" \${paramDirection === 'Output' ? 'selected' : ''}>Output</option>
                                            <option value="InputOutput" \${paramDirection === 'InputOutput' ? 'selected' : ''} \${paramType === 'Byte[]' ? 'disabled' : ''}>InputOutput</option>
                                        </select>
                                        <div style="position: relative; display: flex; align-items: center; gap: 4px;">
                                            <input type="number" class="property-input script-param-size" value="\${paramSize}" placeholder="Size" \${!showSize ? 'disabled' : ''} style="font-size: 11px; padding: 4px 6px; width: 100%; \${!showSize ? 'opacity: 0.5; cursor: not-allowed;' : ''}">
                                            \${showSize ? '<span class="info-icon" title="Required if the direction is output or inputoutput and type is string or byte[]." style="display: inline-flex; align-items: center; justify-content: center; width: 14px; height: 14px; border-radius: 50%; border: 1px solid var(--vscode-foreground); font-size: 9px; cursor: help; color: var(--vscode-foreground);">i</span>' : ''}
                                        </div>
                                        <button class="remove-script-param-btn" style="padding: 2px 6px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">×</button>
                                    </div>
                                \`;
                            });
                            
                            fieldHtml += \`
                                        </div>
                                    </div>
                                </div>
                            \`;
                        });
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'storedprocedure-linkedservice':
                        console.log('[GenerateField] Storedprocedure-linkedservice field -', 'key:', key, 'value:', value, 'type:', typeof value);
                        // Extract referenceName if value is an object
                        let spLinkedServiceRefName = '';
                        if (typeof value === 'object' && value !== null && value.referenceName) {
                            spLinkedServiceRefName = value.referenceName;
                        } else if (typeof value === 'string') {
                            spLinkedServiceRefName = value;
                        }
                        fieldHtml += \`<select class="property-input storedprocedure-linkedservice-select" data-key="\${key}">\`;
                        fieldHtml += \`<option value="">\${prop.placeholder || 'Select linked service...'}</option>\`;
                        
                        // Get linked services list from extension (we'll receive this via message)
                        if (window.linkedServicesList && window.linkedServicesList.length > 0) {
                            window.linkedServicesList.forEach(ls => {
                                const selected = ls.name === spLinkedServiceRefName ? 'selected' : '';
                                if (selected) console.log('[GenerateField] Selected linked service:', ls.name, 'matches value:', spLinkedServiceRefName);
                                fieldHtml += \`<option value="\${ls.name}" \${selected}>\${ls.name}</option>\`;
                            });
                        }
                        fieldHtml += \`</select>\`;
                        break;
                    case 'storedprocedure-linkedservice-properties':
                        // This field only appears when Azure Synapse Analytics linked service is selected
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Configure linked service properties</div>\`;
                        fieldHtml += \`<div class="storedprocedure-linkedservice-properties-container" data-key="\${key}">\`;
                        
                        const lsPropsValue = (value && value.DBName) || '';
                        fieldHtml += \`
                            <div style="display: flex; gap: 8px; margin-bottom: 4px;">
                                <div style="flex: 1; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">Name</div>
                                <div style="flex: 1; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">Value</div>
                                <div style="flex: 1; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">Type</div>
                            </div>
                            <div style="display: flex; gap: 8px; align-items: center;">
                                <input type="text" class="property-input" value="DBName" readonly style="flex: 1; opacity: 0.7; cursor: not-allowed;">
                                <input type="text" class="property-input storedprocedure-lsprop-value" data-key="\${key}" value="\${lsPropsValue}" placeholder="Enter value" style="flex: 1;">
                                <input type="text" class="property-input" value="String" readonly style="flex: 1; opacity: 0.7; cursor: not-allowed;">
                            </div>
                        \`;
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'storedprocedure-parameters':
                        // Render the stored procedure parameters UI
                        const spParamsData = value || {};
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Configure stored procedure parameters</div>\`;
                        fieldHtml += \`<button class="add-storedprocedure-param-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Parameter</button>\`;
                        fieldHtml += \`<div class="storedprocedure-params-container" data-key="\${key}">\`;
                        
                        // Render existing parameters
                        let paramIdx = 0;
                        for (const [paramName, paramData] of Object.entries(spParamsData)) {
                            const paramValue = paramData.value !== undefined && paramData.value !== null ? paramData.value : '';
                            const paramType = paramData.type || 'String';
                            const isTreatAsNull = paramData.value === null;
                            
                            fieldHtml += \`
                                <div class="storedprocedure-param-row" data-param-index="\${paramIdx}" style="display: grid; grid-template-columns: 40px 1fr 120px 1fr 100px 30px; gap: 8px; margin-bottom: 8px; align-items: center; padding: 8px; background: var(--vscode-sideBar-background); border-radius: 3px;">
                                    <div style="font-size: 11px; color: var(--vscode-descriptionForeground);">\${paramIdx + 1}</div>
                                    <input type="text" class="property-input storedprocedure-param-name" value="\${paramName}" placeholder="Name" style="font-size: 11px; padding: 4px 6px;">
                                    <select class="property-input storedprocedure-param-type" style="font-size: 11px; padding: 4px 6px;">
                                        <option value="Boolean" \${paramType === 'Boolean' ? 'selected' : ''}>Boolean</option>
                                        <option value="Datetime" \${paramType === 'Datetime' ? 'selected' : ''}>Datetime</option>
                                        <option value="Datetimeoffset" \${paramType === 'Datetimeoffset' ? 'selected' : ''}>Datetimeoffset</option>
                                        <option value="Decimal" \${paramType === 'Decimal' ? 'selected' : ''}>Decimal</option>
                                        <option value="Double" \${paramType === 'Double' ? 'selected' : ''}>Double</option>
                                        <option value="Guid" \${paramType === 'Guid' ? 'selected' : ''}>Guid</option>
                                        <option value="Int16" \${paramType === 'Int16' ? 'selected' : ''}>Int16</option>
                                        <option value="Int32" \${paramType === 'Int32' ? 'selected' : ''}>Int32</option>
                                        <option value="Int64" \${paramType === 'Int64' ? 'selected' : ''}>Int64</option>
                                        <option value="Single" \${paramType === 'Single' ? 'selected' : ''}>Single</option>
                                        <option value="String" \${paramType === 'String' ? 'selected' : ''}>String</option>
                                        <option value="Timespan" \${paramType === 'Timespan' ? 'selected' : ''}>Timespan</option>
                                    </select>
                                    <input type="text" class="property-input storedprocedure-param-value" value="\${isTreatAsNull ? '' : paramValue}" placeholder="Value" \${isTreatAsNull ? 'disabled' : ''} style="font-size: 11px; padding: 4px 6px;\${isTreatAsNull ? ' opacity: 0.5; cursor: not-allowed;' : ''}">
                                    <label style="display: flex; align-items: center; gap: 4px; font-size: 11px; cursor: pointer;">
                                        <input type="checkbox" class="storedprocedure-param-null" \${isTreatAsNull ? 'checked' : ''} style="margin: 0;">
                                        <span>Null</span>
                                    </label>
                                    <button class="remove-storedprocedure-param-btn" style="padding: 2px 6px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">×</button>
                                </div>
                            \`;
                            paramIdx++;
                        }
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'copy-sp-parameters': {
                        // Generic config-driven stored procedure parameters table.
                        // Stores data into activity[key] (e.g. activity['snk_storedProcedureParameters']).
                        const _cpSpData = (activity[key] && typeof activity[key] === 'object') ? activity[key] : {};
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Configure stored procedure parameters</div>\`;
                        fieldHtml += \`<button class="add-copy-sp-param-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Parameter</button>\`;
                        // Header row
                        fieldHtml += \`
                            <div style="display: grid; grid-template-columns: 40px 1fr 130px 1fr 100px 30px; gap: 8px; margin-bottom: 4px; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">
                                <div></div><div>Name</div><div>Type</div><div>Value</div><div>Treat as null</div><div></div>
                            </div>\`;
                        fieldHtml += \`<div class="copy-sp-params-container" data-key="\${key}">\`;
                        const _cpSpTypeOptions = ['Byte[]','Boolean','Datetime','Datetimeoffset','Decimal','Double','Guid','Int16','Int32','Int64','Single','String','Timespan'];
                        let _cpSpIdx = 0;
                        for (const [_cpSpName, _cpSpParam] of Object.entries(_cpSpData)) {
                            const _cpSpVal = (_cpSpParam.value !== undefined && _cpSpParam.value !== null) ? _cpSpParam.value : '';
                            const _cpSpType = _cpSpParam.type || 'String';
                            const _cpSpIsNull = _cpSpParam.value === null;
                            const _cpSpTypeOpts = _cpSpTypeOptions.map(t => \`<option value="\${t}" \${_cpSpType === t ? 'selected' : ''}>\${t}</option>\`).join('');
                            fieldHtml += \`
                                <div class="copy-sp-param-row" data-field-key="\${key}" data-param-index="\${_cpSpIdx}" style="display: grid; grid-template-columns: 40px 1fr 130px 1fr 100px 30px; gap: 8px; margin-bottom: 8px; align-items: center; padding: 8px; background: var(--vscode-sideBar-background); border-radius: 3px;">
                                    <div style="font-size: 11px; color: var(--vscode-descriptionForeground);">\${_cpSpIdx + 1}</div>
                                    <input type="text" class="property-input copy-sp-param-name" data-field-key="\${key}" value="\${_cpSpName}" placeholder="Name" style="font-size: 11px; padding: 4px 6px;">
                                    <select class="property-input copy-sp-param-type" data-field-key="\${key}" style="font-size: 11px; padding: 4px 6px;">\${_cpSpTypeOpts}</select>
                                    <input type="text" class="property-input copy-sp-param-value" data-field-key="\${key}" value="\${_cpSpIsNull ? '' : _cpSpVal}" placeholder="Value" \${_cpSpIsNull ? 'disabled' : ''} style="font-size: 11px; padding: 4px 6px;\${_cpSpIsNull ? ' opacity: 0.5; cursor: not-allowed;' : ''}">
                                    <label style="display: flex; align-items: center; gap: 4px; font-size: 11px; cursor: pointer;">
                                        <input type="checkbox" class="copy-sp-param-null" data-field-key="\${key}" \${_cpSpIsNull ? 'checked' : ''} style="margin: 0;">
                                        <span>Null</span>
                                    </label>
                                    <button class="remove-copy-sp-param-btn" data-field-key="\${key}" style="padding: 2px 6px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">×</button>
                                </div>\`;
                            _cpSpIdx++;
                        }
                        fieldHtml += \`</div></div>\`;
                        break;
                    }
                    case 'string-list': {
                        // Renders a dynamic list of text inputs stored as a string array in activity[key]
                        const _slData = Array.isArray(activity[key]) ? activity[key] : [];
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<button type="button" class="add-string-list-item-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Column</button>\`;
                        fieldHtml += \`<div class="string-list-container" data-key="\${key}">\`;
                        _slData.forEach((_slItem, _slIdx) => {
                            fieldHtml += \`
                                <div class="string-list-item-row" data-field-key="\${key}" data-item-index="\${_slIdx}" style="display: flex; gap: 8px; margin-bottom: 6px; align-items: center;">
                                    <input type="text" class="property-input string-list-item-value" data-field-key="\${key}" data-item-index="\${_slIdx}" value="\${_slItem}" placeholder="Column name" style="flex: 1; font-size: 11px; padding: 4px 6px;">
                                    <button type="button" class="remove-string-list-item-btn" data-field-key="\${key}" data-item-index="\${_slIdx}" style="padding: 2px 6px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">&times;</button>
                                </div>\`;
                        });
                        fieldHtml += \`</div></div>\`;
                        break;
                    }
                    case 'additional-columns': {
                        // Renders a list of {name, value} entries.
                        // Value is a free-text field (placeholder shown in grey; user types any value).
                        const _acData = Array.isArray(activity[key]) ? activity[key] : [];
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<button type="button" class="add-additional-col-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ New</button>\`;
                        if (_acData.length > 0) {
                            fieldHtml += \`
                                <div style="display: grid; grid-template-columns: 1fr 1fr 26px; gap: 6px; margin-bottom: 4px; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">
                                    <div>Name</div><div>Value</div><div></div>
                                </div>\`;
                            _acData.forEach((_acEntry, _acIdx) => {
                                fieldHtml += \`
                                <div class="additional-col-row" style="display: grid; grid-template-columns: 1fr 1fr 26px; gap: 6px; margin-bottom: 6px; align-items: center;">
                                    <input type="text" class="property-input ac-name-input" data-key="\${key}" data-index="\${_acIdx}" value="\${_acEntry.name || ''}" placeholder="Column name" style="font-size: 11px; padding: 3px 6px;">
                                    <input type="text" class="property-input ac-value-input" data-key="\${key}" data-index="\${_acIdx}" value="\${_acEntry.value || ''}" placeholder="Value" style="font-size: 11px; padding: 3px 6px; color: inherit;">
                                    <button type="button" class="remove-additional-col-btn" data-key="\${key}" data-index="\${_acIdx}" style="padding: 2px 4px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">&times;</button>
                                </div>\`;
                            });
                        }
                        fieldHtml += \`</div>\`;
                        break;
                    }
                    case 'copy-cmd-default-values': {
                        // Renders array of {columnName, defaultValue} for SqlDW Copy command defaultValues
                        const _dvData = Array.isArray(activity[key]) ? activity[key] : [];
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<button type="button" class="add-copy-cmd-dv-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ New</button>\`;
                        if (_dvData.length > 0) {
                            fieldHtml += \`
                                <div style="display: grid; grid-template-columns: 1fr 1fr 26px; gap: 6px; margin-bottom: 4px; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">
                                    <div>Column</div><div>Value</div><div></div>
                                </div>\`;
                            _dvData.forEach((_dvEntry, _dvIdx) => {
                                fieldHtml += \`
                                <div class="copy-cmd-dv-row" style="display: grid; grid-template-columns: 1fr 1fr 26px; gap: 6px; margin-bottom: 6px; align-items: center;">
                                    <input type="text" class="property-input copy-cmd-dv-col-input" data-key="\${key}" data-index="\${_dvIdx}" value="\${_dvEntry.columnName || ''}" placeholder="Column name" style="font-size: 11px; padding: 3px 6px;">
                                    <input type="text" class="property-input copy-cmd-dv-val-input" data-key="\${key}" data-index="\${_dvIdx}" value="\${_dvEntry.defaultValue || ''}" placeholder="Default value" style="font-size: 11px; padding: 3px 6px;">
                                    <button type="button" class="remove-copy-cmd-dv-btn" data-key="\${key}" data-index="\${_dvIdx}" style="padding: 2px 4px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">&times;</button>
                                </div>\`;
                            });
                        }
                        fieldHtml += \`</div>\`;
                        break;
                    }
                    case 'copy-cmd-additional-options': {
                        // Renders object {propName: propValue} for SqlDW Copy command additionalOptions
                        const _aoData = (activity[key] && typeof activity[key] === 'object' && !Array.isArray(activity[key])) ? activity[key] : {};
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<button type="button" class="add-copy-cmd-ao-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ New</button>\`;
                        const _aoEntries = Object.entries(_aoData);
                        if (_aoEntries.length > 0) {
                            fieldHtml += \`
                                <div style="display: grid; grid-template-columns: 1fr 1fr 26px; gap: 6px; margin-bottom: 4px; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">
                                    <div>Property</div><div>Value</div><div></div>
                                </div>\`;
                            _aoEntries.forEach(([_aoProp, _aoVal], _aoIdx) => {
                                fieldHtml += \`
                                <div class="copy-cmd-ao-row" data-pair-index="\${_aoIdx}" style="display: grid; grid-template-columns: 1fr 1fr 26px; gap: 6px; margin-bottom: 6px; align-items: center;">
                                    <input type="text" class="property-input copy-cmd-ao-prop-input" data-key="\${key}" data-index="\${_aoIdx}" value="\${_aoProp}" placeholder="Property name" style="font-size: 11px; padding: 3px 6px;">
                                    <input type="text" class="property-input copy-cmd-ao-val-input" data-key="\${key}" data-index="\${_aoIdx}" value="\${_aoVal || ''}" placeholder="Value" style="font-size: 11px; padding: 3px 6px;">
                                    <button type="button" class="remove-copy-cmd-ao-btn" data-key="\${key}" data-index="\${_aoIdx}" style="padding: 2px 4px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">&times;</button>
                                </div>\`;
                            });
                        }
                        fieldHtml += \`</div>\`;
                        break;
                    }
                    case 'namespace-prefixes':
                        // Render the namespace prefix pairs UI for XML datasets
                        const namespacePairsData = activity.namespacePrefixPairs || {};
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<button class="add-namespace-prefix-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ New</button>\`;
                        fieldHtml += \`<button class="delete-namespace-prefix-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px; margin-left: 8px;">Delete</button>\`;
                        fieldHtml += \`<div class="namespace-prefixes-container" data-key="\${key}">\`;
                        
                        // Header row
                        fieldHtml += \`
                            <div style="display: grid; grid-template-columns: 30px 1fr 1fr; gap: 8px; margin-bottom: 4px; font-size: 11px; font-weight: 600; color: var(--vscode-descriptionForeground);">
                                <div></div>
                                <div>URI</div>
                                <div>Prefix</div>
                            </div>
                        \`;
                        
                        // Render existing namespace pairs
                        let nsPairIdx = 0;
                        for (const [uri, prefix] of Object.entries(namespacePairsData)) {
                            fieldHtml += \`
                                <div class="namespace-prefix-row" data-pair-index="\${nsPairIdx}" style="display: grid; grid-template-columns: 30px 1fr 1fr; gap: 8px; margin-bottom: 8px; align-items: center;">
                                    <input type="checkbox" class="namespace-prefix-checkbox" style="margin: 0;">
                                    <input type="text" class="property-input namespace-prefix-uri" value="\${uri}" placeholder="URlName" style="font-size: 11px; padding: 4px 6px;">
                                    <input type="text" class="property-input namespace-prefix-value" value="\${prefix}" placeholder="prefix1" style="font-size: 11px; padding: 4px 6px;">
                                </div>
                            \`;
                            nsPairIdx++;
                        }
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'web-secret':
                        // For password, pfx, etc. - Azure Key Vault secret reference
                        const secretValue = value || {};
                        const secretType = secretValue.type || 'AzureKeyVaultSecret';
                        const secretStore = secretValue.store?.referenceName || '';
                        const secretName = secretValue.secretName || '';
                        const secretVersion = secretValue.secretVersion || 'latest';
                        
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="display: flex; gap: 8px; margin-bottom: 8px;">\`;
                        fieldHtml += \`<input type="text" class="property-input web-secret-store" data-key="\${key}" value="\${secretStore}" placeholder="Azure Key Vault linked service" style="flex: 1;">\`;
                        fieldHtml += \`</div>\`;
                        fieldHtml += \`<div style="display: flex; gap: 8px;">\`;
                        fieldHtml += \`<input type="text" class="property-input web-secret-name" data-key="\${key}" value="\${secretName}" placeholder="Secret name" style="flex: 1;">\`;
                        fieldHtml += \`<input type="text" class="property-input web-secret-version" data-key="\${key}" value="\${secretVersion}" placeholder="Version (optional)" style="flex: 0 0 150px;">\`;
                        fieldHtml += \`</div>\`;
                        fieldHtml += \`</div>\`;
                        break;
                    case 'web-headers':
                        // Headers - array of name-value pairs
                        const headersValue = value || [];
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Add one or more name-value pairs</div>\`;
                        fieldHtml += \`<button class="add-web-header-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Header</button>\`;
                        fieldHtml += \`<div class="web-headers-list" data-key="\${key}">\`;
                        
                        headersValue.forEach((header, idx) => {
                            const headerName = header.name || '';
                            const headerValue = header.value || '';
                            fieldHtml += \`
                                <div class="property-group web-header-item" data-index="\${idx}" style="margin-bottom: 8px; display: flex; gap: 8px; align-items: center;">
                                    <input type="text" class="property-input web-header-name" data-index="\${idx}" value="\${headerName}" placeholder="Name" style="flex: 1;">
                                    <input type="text" class="property-input web-header-value" data-index="\${idx}" value="\${headerValue}" placeholder="Value" style="flex: 1;">
                                    <button class="remove-web-header-btn" data-index="\${idx}" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Remove</button>
                                </div>
                            \`;
                        });
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'web-dataset-list':
                        // Datasets - array of dataset references
                        const datasetsValue = value || [];
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Add dataset references</div>\`;
                        fieldHtml += \`<button class="add-web-dataset-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Dataset</button>\`;
                        fieldHtml += \`<div class="web-datasets-list" data-key="\${key}">\`;
                        
                        datasetsValue.forEach((dataset, idx) => {
                            const datasetName = dataset.referenceName || '';
                            fieldHtml += \`
                                <div class="property-group web-dataset-item" data-index="\${idx}" style="margin-bottom: 8px; display: flex; gap: 8px; align-items: center;">
                                    <select class="property-input web-dataset-select" data-index="\${idx}" style="flex: 1;">
                                        <option value="">Select dataset...</option>\`;
                            
                            if (datasetList && datasetList.length > 0) {
                                datasetList.forEach(ds => {
                                    const selected = ds === datasetName ? 'selected' : '';
                                    fieldHtml += \`<option value="\${ds}" \${selected}>\${ds}</option>\`;
                                });
                            }
                            
                            fieldHtml += \`
                                    </select>
                                    <button class="remove-web-dataset-btn" data-index="\${idx}" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Remove</button>
                                </div>
                            \`;
                        });
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'web-linkedservice-list':
                        // Linked services - array of linked service references
                        const linkedServicesValue = value || [];
                        fieldHtml += \`<div style="flex: 1;">\`;
                        fieldHtml += \`<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 8px;">Add linked service references</div>\`;
                        fieldHtml += \`<button class="add-web-linkedservice-btn" data-key="\${key}" style="padding: 4px 8px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 11px; margin-bottom: 8px;">+ Add Linked Service</button>\`;
                        fieldHtml += \`<div class="web-linkedservices-list" data-key="\${key}">\`;
                        
                        linkedServicesValue.forEach((ls, idx) => {
                            const lsName = ls.referenceName || '';
                            fieldHtml += \`
                                <div class="property-group web-linkedservice-item" data-index="\${idx}" style="margin-bottom: 8px; display: flex; gap: 8px; align-items: center;">
                                    <select class="property-input web-linkedservice-select" data-index="\${idx}" style="flex: 1;">
                                        <option value="">Select linked service...</option>\`;
                            
                            if (window.linkedServicesList && window.linkedServicesList.length > 0) {
                                window.linkedServicesList.forEach(linkedService => {
                                    const selected = linkedService.name === lsName ? 'selected' : '';
                                    fieldHtml += \`<option value="\${linkedService.name}" \${selected}>\${linkedService.name}</option>\`;
                                });
                            }
                            
                            fieldHtml += \`
                                    </select>
                                    <button class="remove-web-linkedservice-btn" data-index="\${idx}" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Remove</button>
                                </div>
                            \`;
                        });
                        
                        fieldHtml += \`</div></div>\`;
                        break;
                    case 'object':
                    case 'array':
                        fieldHtml += \`<div style="padding: 8px; background: var(--vscode-input-background); border: 1px solid var(--vscode-input-border); border-radius: 2px; font-family: monospace; font-size: 12px; color: var(--vscode-descriptionForeground); flex: 1; cursor: pointer;">\`;
                        fieldHtml += \`Click to configure \${prop.label}...\`;
                        fieldHtml += \`</div>\`;
                        break;
                    default:
                        fieldHtml += \`<input type="text" class="property-input" data-key="\${key}" value="\${value}">\`;
                }
                
                fieldHtml += \`</div>\`;
                return fieldHtml;
            }
            
            // Build content for each tab
            let generalContent = '';
            if (schema && schema.commonProperties) {
                for (const [key, prop] of Object.entries(schema.commonProperties)) {
                    if (prop.section === 'policy') continue;
                    generalContent += generateFormField(key, prop, activity);
                }
                
                const policyProps = Object.entries(schema.commonProperties).filter(([k, p]) => p.section === 'policy');
                if (policyProps.length > 0) {
                    generalContent += '<div style="margin-top: 24px; margin-bottom: 12px; font-weight: 600; font-size: 13px; color: var(--vscode-foreground);">Policy</div>';
                    policyProps.forEach(([key, prop]) => {
                        generalContent += generateFormField(key, prop, activity);
                    });
                }
            }
            
            let settingsContent = '';
            if (schema && schema.typeProperties) {
                // For GetMetadata activity, add section headers
                if (activity.type === 'GetMetadata') {
                    for (const [key, prop] of Object.entries(schema.typeProperties)) {
                        // Add "Filter by last modified" header before datetime fields
                        if ((key === 'modifiedDatetimeStart' || key === 'modifiedDatetimeEnd') && 
                            (activity._datasetLocationType === 'AzureBlobStorageLocation' || activity._datasetLocationType === 'AzureBlobFSLocation') &&
                            !settingsContent.includes('Filter by last modified')) {
                            settingsContent += '<div style="margin-top: 16px; margin-bottom: 12px; font-weight: 600; font-size: 13px; color: var(--vscode-foreground);">Filter by last modified</div>';
                        }
                        settingsContent += generateFormField(key, prop, activity);
                    }
                } else {
                    for (const [key, prop] of Object.entries(schema.typeProperties)) {
                        settingsContent += generateFormField(key, prop, activity);
                    }
                }
            }
            if (!settingsContent) {
                settingsContent = '<div style="color: var(--vscode-descriptionForeground); padding: 20px; text-align: center;">No activity-specific settings available</div>';
            }
            console.log('Settings content length:', settingsContent.length);
            
            // For Lookup activity, add dataset-specific fields dynamically
            if (activity.type === 'Lookup' && activity.dataset) {
                // Ensure _datasetType is set
                if (!activity._datasetType && datasetContents[activity.dataset]) {
                    activity._datasetType = datasetContents[activity.dataset].properties?.type;
                    console.log('[ShowProps] Auto-detected dataset type for Lookup:', activity._datasetType);
                }
                
                // If dataset is selected, dynamically load dataset-specific lookup fields
                if (activity._datasetType) {
                    const datasetType = activity._datasetType;
                    console.log('Adding lookup fields for dataset type:', datasetType);
                    if (datasetSchemas[datasetType] && datasetSchemas[datasetType].lookupFields) {
                        settingsContent += '<div style="border-top: 1px solid var(--vscode-panel-border); margin: 16px 0; padding-top: 16px;"></div>';
                        settingsContent += '<div style="font-size: 13px; font-weight: bold; color: var(--vscode-foreground); margin-bottom: 12px;">Dataset Settings (' + datasetSchemas[datasetType].name + ')</div>';
                        
                        // Detect storage type — Prefix is Blob Storage-only, not supported by ADLS Gen2
                        const locationType = datasetContents[activity.dataset]?.properties?.typeProperties?.location?.type;
                        const isAdls = locationType === 'AzureBlobFSLocation';

                        const lookupFields = datasetSchemas[datasetType].lookupFields;
                        for (const [key, prop] of Object.entries(lookupFields)) {
                            // Skip firstRowOnly as it's already in typeProperties
                            if (key === 'firstRowOnly') continue;
                            // For ADLS datasets, remove "Prefix" from filePathType options (Blob Storage only)
                            if (key === 'filePathType' && isAdls && prop.optionValues) {
                                const prefixIdx = prop.optionValues.indexOf('prefix');
                                if (prefixIdx !== -1) {
                                    const filteredProp = Object.assign({}, prop, {
                                        options: prop.options.filter((_, i) => i !== prefixIdx),
                                        optionValues: prop.optionValues.filter(v => v !== 'prefix')
                                    });
                                    settingsContent += generateFormField(key, filteredProp, activity);
                                    continue;
                                }
                            }
                            // Hide the Prefix text field for ADLS datasets
                            if (key === 'prefix' && isAdls) continue;
                            settingsContent += generateFormField(key, prop, activity);
                        }
                        console.log('Added', Object.keys(datasetSchemas[datasetType].lookupFields).length, 'lookup fields');
                    }
                }
            }
            
            // Build Source tab content
            let sourceContent = '';
            if (schema && schema.sourceProperties) {
                // For Delete activity, add "Filter by last modified" section header
                if (activity.type === 'Delete') {
                    for (const [key, prop] of Object.entries(schema.sourceProperties)) {
                        // Add "Filter by last modified" header before datetime fields
                        if ((key === 'modifiedDatetimeStart' || key === 'modifiedDatetimeEnd') &&
                            (activity.filePathType === 'filePathInDataset' || activity.filePathType === 'wildcardFilePath' || activity.filePathType === 'prefix') &&
                            !sourceContent.includes('Filter by last modified')) {
                            sourceContent += '<div style="margin-top: 16px; margin-bottom: 12px; font-weight: 600; font-size: 13px; color: var(--vscode-foreground);">Filter by last modified</div>';
                        }
                        sourceContent += generateFormField(key, prop, activity);
                    }
                } else {
                    for (const [key, prop] of Object.entries(schema.sourceProperties)) {
                        sourceContent += generateFormField(key, prop, activity);
                    }
                }
                
                // Ensure _sourceDatasetType is set if we have a sourceDataset
                if (activity.sourceDataset && !activity._sourceDatasetType && datasetContents[activity.sourceDataset]) {
                    activity._sourceDatasetType = datasetContents[activity.sourceDataset].properties?.type;
                    console.log('[ShowProps] Auto-detected source dataset type:', activity._sourceDatasetType);
                }
                
                // If sourceDataset is selected, dynamically load dataset-specific fields
                if (activity.sourceDataset && activity._sourceDatasetType) {
                    const datasetType = activity._sourceDatasetType;
                    console.log('Adding source fields for dataset type:', datasetType);
                    // For Copy activity: use copyActivityConfig; for others: use datasetSchemas
                    const _copySourceConf = (activity.type === 'Copy')
                        ? (copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[datasetType])
                        : null;
                    if (_copySourceConf && _copySourceConf.fields && _copySourceConf.fields.source && Object.keys(_copySourceConf.fields.source).length > 0) {
                        sourceContent += '<div style="border-top: 1px solid var(--vscode-panel-border); margin: 16px 0; padding-top: 16px;"></div>';
                        sourceContent += '<div style="font-size: 13px; font-weight: bold; color: var(--vscode-foreground); margin-bottom: 12px;">Source Settings (' + (_copySourceConf.name || datasetType) + ')</div>';
                        // Pre-apply defaults for unset fields so conditionals resolve on fresh activities
                        for (const [_dfk, _dfc] of Object.entries(_copySourceConf.fields.source)) {
                            if (_dfc.default !== undefined && activity[_dfk] === undefined) activity[_dfk] = _dfc.default;
                        }
                        for (const [key, prop] of Object.entries(_copySourceConf.fields.source)) {
                            sourceContent += generateFormField(key, prop, activity);
                        }
                        console.log('Added', Object.keys(_copySourceConf.fields.source).length, 'copy source fields from config');
                    } else if (datasetSchemas[datasetType] && datasetSchemas[datasetType].sourceFields) {
                        sourceContent += '<div style="border-top: 1px solid var(--vscode-panel-border); margin: 16px 0; padding-top: 16px;"></div>';
                        sourceContent += '<div style="font-size: 13px; font-weight: bold; color: var(--vscode-foreground); margin-bottom: 12px;">Source Settings (' + datasetSchemas[datasetType].name + ')</div>';
                        for (const [key, prop] of Object.entries(datasetSchemas[datasetType].sourceFields)) {
                            sourceContent += generateFormField(key, prop, activity);
                        }
                        console.log('Added', Object.keys(datasetSchemas[datasetType].sourceFields).length, 'source fields');
                    }
                }
            }
            
            // Build Sink tab content
            let sinkContent = '';
            if (schema && schema.sinkProperties) {
                for (const [key, prop] of Object.entries(schema.sinkProperties)) {
                    sinkContent += generateFormField(key, prop, activity);
                }
                
                // Ensure _sinkDatasetType is set if we have a sinkDataset
                if (activity.sinkDataset && !activity._sinkDatasetType && datasetContents[activity.sinkDataset]) {
                    activity._sinkDatasetType = datasetContents[activity.sinkDataset].properties?.type;
                    console.log('[ShowProps] Auto-detected sink dataset type:', activity._sinkDatasetType);
                }
                
                // If sinkDataset is selected, dynamically load dataset-specific fields
                if (activity.sinkDataset && activity._sinkDatasetType) {
                    const datasetType = activity._sinkDatasetType;
                    console.log('Adding sink fields for dataset type:', datasetType);
                    // For Copy activity: use copyActivityConfig; for others: use datasetSchemas
                    const _copySinkConf = (activity.type === 'Copy')
                        ? (copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[datasetType])
                        : null;
                    if (_copySinkConf && _copySinkConf.fields && _copySinkConf.fields.sink && Object.keys(_copySinkConf.fields.sink).length > 0) {
                        sinkContent += '<div style="border-top: 1px solid var(--vscode-panel-border); margin: 16px 0; padding-top: 16px;"></div>';
                        sinkContent += '<div style="font-size: 13px; font-weight: bold; color: var(--vscode-foreground); margin-bottom: 12px;">Sink Settings (' + (_copySinkConf.name || datasetType) + ')</div>';
                        // Pre-apply defaults for unset fields so conditionals resolve on fresh activities
                        for (const [_dfk, _dfc] of Object.entries(_copySinkConf.fields.sink)) {
                            if (_dfc.default !== undefined && activity[_dfk] === undefined) activity[_dfk] = _dfc.default;
                        }
                        for (const [key, prop] of Object.entries(_copySinkConf.fields.sink)) {
                            sinkContent += generateFormField(key, prop, activity);
                        }
                        console.log('Added', Object.keys(_copySinkConf.fields.sink).length, 'copy sink fields from config');
                    } else if (datasetSchemas[datasetType] && datasetSchemas[datasetType].sinkFields) {
                        sinkContent += '<div style="border-top: 1px solid var(--vscode-panel-border); margin: 16px 0; padding-top: 16px;"></div>';
                        sinkContent += '<div style="font-size: 13px; font-weight: bold; color: var(--vscode-foreground); margin-bottom: 12px;">Sink Settings (' + datasetSchemas[datasetType].name + ')</div>';
                        for (const [key, prop] of Object.entries(datasetSchemas[datasetType].sinkFields)) {
                            sinkContent += generateFormField(key, prop, activity);
                        }
                        console.log('Added', Object.keys(datasetSchemas[datasetType].sinkFields).length, 'sink fields');
                    }
                }
            }
            
            // Build Mapping tab content
            let mappingContent = '<div style="color: var(--vscode-descriptionForeground); padding: 20px; text-align: center;">Mapping configuration coming soon</div>';
            
            // Build Advanced tab content
            let advancedContent = '';
            if (schema && schema.advancedProperties) {
                for (const [key, prop] of Object.entries(schema.advancedProperties)) {
                    advancedContent += generateFormField(key, prop, activity);
                }
            }
            
            activity.userProperties = activity.userProperties || [];
            let userPropsContent = '<div style="margin-bottom: 12px;">';
            userPropsContent += '<button id="addUserPropBtn" style="padding: 6px 12px; background: var(--vscode-button-background); color: var(--vscode-button-foreground); border: none; cursor: pointer; border-radius: 2px; font-size: 12px;">+ Add User Property</button>';
            userPropsContent += '</div>';
            userPropsContent += '<div id="userPropsList">';
            activity.userProperties.forEach((prop, idx) => {
                userPropsContent += \`
                    <div class="property-group" style="margin-bottom: 12px;">
                        <input type="text" class="property-input" data-idx="\${idx}" data-field="name" value="\${prop.name}" placeholder="Property name" style="flex: 1;">
                        <input type="text" class="property-input" data-idx="\${idx}" data-field="value" value="\${prop.value}" placeholder="Property value" style="flex: 1;">
                        <button class="remove-user-prop" data-idx="\${idx}" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px;">Remove</button>
                    </div>
                \`;
            });
            userPropsContent += '</div>';
            console.log('User props content length:', userPropsContent.length);
            console.log('Schema:', schema);
            console.log('Activity type:', activity.type);
            
            // Generate activity-level tabs with content already included
            const tabsContainer = document.getElementById('activityTabsContainer');
            const panesContainer = document.getElementById('activityPanesContainer');
            
            let tabsHtml = '';
            let panesHtml = '';
            
            tabs.forEach((tabName, idx) => {
                const tabId = tabName.toLowerCase().split(' ').join('-');
                const isActive = activeTabId ? (tabId === activeTabId) : (idx === 0);
                const activeClass = isActive ? ' active' : '';
                const activeStyle = isActive ? 'color: var(--vscode-tab-activeForeground); border-bottom: 2px solid var(--vscode-focusBorder);' : 'color: var(--vscode-tab-inactiveForeground);';
                
                tabsHtml += \`<button class="config-tab activity-tab\${activeClass}" data-tab="\${tabId}" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; \${activeStyle}">\${tabName}</button>\`;
                
                // Get the content for this tab
                let tabContent = '';
                if (tabId === 'general') tabContent = generalContent;
                else if (tabId === 'settings') tabContent = settingsContent;
                else if (tabId === 'source') tabContent = sourceContent;
                else if (tabId === 'sink') tabContent = sinkContent;
                else if (tabId === 'mapping') tabContent = mappingContent;
                else if (tabId === 'advanced') tabContent = advancedContent;
                else if (tabId === 'user-properties') tabContent = userPropsContent;
                else if (tabId === 'activities' && activity.type === 'Until') {
                    tabContent = generateUntilActivitiesTab(activity);
                } else if (tabId === 'activities' && activity.type === 'IfCondition') {
                    tabContent = generateIfConditionActivitiesTab(activity);
                } else if (tabId === 'activities' && activity.type === 'ForEach') {
                    tabContent = generateForEachActivitiesTab(activity);
                } else if (tabId === 'activities' && activity.type === 'Switch') {
                    tabContent = generateSwitchActivitiesTab(activity);
                }
                
                console.log(\`Tab \${tabName} (id: \${tabId}) content length: \${tabContent.length}\`);
                
                const displayStyle = isActive ? 'display: block;' : 'display: none;';
                panesHtml += \`<div class="config-tab-pane activity-pane\${activeClass}" id="tab-\${tabId}" style="\${displayStyle}">\${tabContent}</div>\`;
            });
            
            tabsContainer.innerHTML = tabsHtml;
            panesContainer.innerHTML = panesHtml;

            // Right sidebar - basic properties
            rightPanel.innerHTML = \`
                <div class="property-group">
                    <div class="property-label">Name</div>
                    <input type="text" class="property-input" id="propName" value="\${activity.name}">
                </div>
                <div class="property-group">
                    <div class="property-label">Type</div>
                    <input type="text" class="property-input" value="\${activity.type}" readonly>
                </div>
                <div class="property-group">
                    <div class="property-label">Description</div>
                    <textarea class="property-input" id="propDescription" rows="3">\${activity.description}</textarea>
                </div>
                <div class="property-group">
                    <div class="property-label">Position</div>
                    <div style="display: flex; gap: 8px; flex: 1;">
                        <input type="number" class="property-input" id="propX" value="\${Math.round(activity.x)}" placeholder="X">
                        <input type="number" class="property-input" id="propY" value="\${Math.round(activity.y)}" placeholder="Y">
                    </div>
                </div>
            \`;
            
            // Add event listeners for user properties
            const addUserPropBtn = document.getElementById('addUserPropBtn');
            if (addUserPropBtn) {
                addUserPropBtn.addEventListener('click', () => {
                    activity.userProperties.push({ name: '', value: '' });
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            }
            
            document.querySelectorAll('.remove-user-prop').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const idx = parseInt(e.target.getAttribute('data-idx'));
                    activity.userProperties.splice(idx, 1);
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            document.querySelectorAll('#userPropsList input').forEach(input => {
                input.addEventListener('input', (e) => {
                    const idx = parseInt(e.target.getAttribute('data-idx'));
                    const field = e.target.getAttribute('data-field');
                    activity.userProperties[idx][field] = e.target.value;
                });
            });
            
            // Add event listeners to all config panel inputs to update activity object in real-time
            document.querySelectorAll('#configContent .property-input').forEach(input => {
                const key = input.getAttribute('data-key');
                if (!key) return;
                
                // Skip inputs that have their own specific handlers
                if (input.classList.contains('storedprocedure-lsprop-value')) return;
                // Skip web-secret fields (password, pfx, etc.) as they have their own handler
                if (input.classList.contains('web-secret-store') || input.classList.contains('web-secret-name') || input.classList.contains('web-secret-version')) return;
                // Skip additional-columns child inputs — they have their own ac- handlers
                if (input.classList.contains('ac-name-input') || input.classList.contains('ac-value-input')) return;
                // Skip copy-cmd default-values and additional-options inputs — they manage arrays/objects
                // and must NOT be overwritten with a plain string by this generic handler
                if (input.classList.contains('copy-cmd-dv-col-input') || input.classList.contains('copy-cmd-dv-val-input')) return;
                if (input.classList.contains('copy-cmd-ao-prop-input') || input.classList.contains('copy-cmd-ao-val-input')) return;
                
                if (input.type === 'checkbox') {
                    input.addEventListener('change', (e) => {
                        activity[key] = e.target.checked;
                        markAsDirty();
                        console.log('Updated ' + key + ':', activity[key]);

                        // Config-driven re-render: if this checkbox key is referenced by any conditional
                        // field, re-render that tab so dependents appear/hide immediately
                        const reRenderTab = reRenderKeyMap[key];
                        if (reRenderTab) {
                            showProperties(activity, reRenderTab);
                        }
                    });
                } else {
                    input.addEventListener('input', (e) => {
                        const value = e.target.value;
                        // Convert to appropriate type
                        if (input.type === 'number') {
                            activity[key] = value === '' ? undefined : (parseFloat(value) || 0);
                        } else {
                            activity[key] = value;
                        }
                        markAsDirty();
                        console.log('Updated ' + key + ':', activity[key]);
                        
                        // Re-render current tab if this field is referenced by a conditional
                        const reRenderTab = reRenderKeyMap[key];
                        if (reRenderTab) {
                            showProperties(activity, reRenderTab);
                        }
                        
                        // Mirror executorSize to driverSize for SynapseNotebook activities
                        if (key === 'executorSize' && activity.type === 'SynapseNotebook') {
                            activity.driverSize = value;
                            const driverSizeInput = document.querySelector('#configContent .property-input[data-key="driverSize"]');
                            if (driverSizeInput) {
                                driverSizeInput.value = value;
                            }
                        }
                    });
                }
            });
            
            // Add event listeners for dataset dropdowns to trigger dynamic field loading
            document.querySelectorAll('#configContent .dataset-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const key = select.getAttribute('data-key');
                    const datasetName = e.target.value;
                    activity[key] = datasetName;
                    markAsDirty();
                    console.log('Updated ' + key + ':', activity[key]);
                    
                    // Get dataset type and store it
                    if (datasetName && datasetContents[datasetName]) {
                        const datasetType = datasetContents[datasetName].properties?.type;
                        console.log('Dataset selected:', datasetName, 'Type:', datasetType);
                        
                        // Store dataset type in activity for later use
                        if (key === 'sourceDataset') {
                            activity._sourceDatasetType = datasetType;
                            activity._sourceLocationType = datasetContents[datasetName].properties?.typeProperties?.location?.type;
                            // If Prefix was selected but new dataset is ADLS (no prefix support), reset to filePathInDataset
                            if (activity.src_filePathType === 'prefix' && activity._sourceLocationType !== 'AzureBlobStorageLocation') {
                                activity.src_filePathType = 'filePathInDataset';
                            }
                        } else if (key === 'sinkDataset') {
                            activity._sinkDatasetType = datasetType;
                            activity._sinkLocationType = datasetContents[datasetName].properties?.typeProperties?.location?.type;
                        } else if (key === 'dataset' && activity.type === 'Delete') {
                            // Track location type for Delete (used for future conditional logic)
                            activity._datasetLocationType = datasetContents[datasetName].properties?.typeProperties?.location?.type;
                        }
                        
                        // Re-render the current tab to show dataset-specific fields
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            // Add event listeners for validation-dataset dropdowns
            document.querySelectorAll('#configContent .validation-dataset-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const key = select.getAttribute('data-key');
                    const datasetName = e.target.value;
                    
                    if (datasetName) {
                        // Store as reference object for Validation activity
                        activity[key] = {
                            referenceName: datasetName,
                            type: 'DatasetReference'
                        };
                        
                        // Store location type for conditional rendering of childItems
                        if (datasetContents[datasetName]) {
                            const locationType = datasetContents[datasetName].properties?.typeProperties?.location?.type;
                            activity._datasetLocationType = locationType;
                            console.log('Validation dataset selected:', datasetName, 'Location type:', locationType);
                            
                            // If the new dataset is NOT Blob/ADLS, remove childItems field
                            if (locationType !== 'AzureBlobStorageLocation' && locationType !== 'AzureBlobFSLocation') {
                                delete activity.childItems;
                                console.log('Validation dataset changed to non-storage type, removed childItems');
                            }
                        }
                    } else {
                        delete activity[key];
                        delete activity._datasetLocationType;
                        delete activity.childItems;
                    }
                    
                    markAsDirty();
                    console.log('Updated ' + key + ':', activity[key]);
                    
                    // Re-render the current tab to show/hide childItems field
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listeners for getmetadata-dataset dropdowns
            document.querySelectorAll('#configContent .getmetadata-dataset-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const key = select.getAttribute('data-key');
                    const datasetName = e.target.value;
                    
                    if (datasetName) {
                        // Store as string for GetMetadata activity
                        activity[key] = datasetName;
                        
                        // Store location type for conditional rendering of fields
                        if (datasetContents[datasetName]) {
                            const locationType = datasetContents[datasetName].properties?.typeProperties?.location?.type;
                            activity._datasetLocationType = locationType;
                            console.log('GetMetadata dataset selected:', datasetName, 'Location type:', locationType);
                            
                            // Reset field list when dataset changes
                            activity.fieldList = [];
                        }
                    } else {
                        delete activity[key];
                        delete activity._datasetLocationType;
                        activity.fieldList = [];
                    }
                    
                    markAsDirty();
                    console.log('Updated ' + key + ':', activity[key]);
                    
                    // Re-render the current tab to show dataset-specific fields
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listeners for lookup-dataset dropdowns
            document.querySelectorAll('#configContent .lookup-dataset-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const key = select.getAttribute('data-key');
                    const datasetName = e.target.value;
                    
                    if (datasetName) {
                        // Store as string for Lookup activity
                        activity[key] = datasetName;
                        
                        // Store dataset type for conditional rendering of fields
                        if (datasetContents[datasetName]) {
                            const datasetType = datasetContents[datasetName].properties?.type;
                            activity._datasetType = datasetType;
                            console.log('Lookup dataset selected:', datasetName, 'Type:', datasetType);
                            // Prefix is Blob Storage-only — reset if switching to an ADLS dataset
                            const newLocType = datasetContents[datasetName].properties?.typeProperties?.location?.type;
                            if (newLocType === 'AzureBlobFSLocation' && activity.filePathType === 'prefix') {
                                activity.filePathType = 'filePathInDataset';
                                delete activity.prefix;
                            }
                        }
                    } else {
                        delete activity[key];
                        delete activity._datasetType;
                    }
                    
                    markAsDirty();
                    console.log('Updated ' + key + ':', activity[key]);
                    
                    // Re-render the current tab to show dataset-specific fields
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listeners for GetMetadata field list
            document.querySelectorAll('.add-getmetadata-field-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.fieldList) activity.fieldList = [];
                    
                    // Set default value based on dataset type
                    const isStorageDataset = activity._datasetLocationType === 'AzureBlobFSLocation' || 
                                            activity._datasetLocationType === 'AzureBlobStorageLocation';
                    const defaultValue = isStorageDataset ? 'childItems' : 'columnCount';
                    
                    activity.fieldList.push({ type: 'predefined', value: defaultValue });
                    markAsDirty();
                    
                    // Re-render to show new field
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            document.querySelectorAll('.remove-getmetadata-field-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    if (activity.fieldList && activity.fieldList[index] !== undefined) {
                        activity.fieldList.splice(index, 1);
                        markAsDirty();
                        
                        // Re-render to update indices
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            document.querySelectorAll('.getmetadata-field-type').forEach(select => {
                select.addEventListener('change', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    const selectedType = e.target.value;
                    
                    if (!activity.fieldList) activity.fieldList = [];
                    
                    if (selectedType === '__dynamic__') {
                        // Switch to dynamic content mode
                        activity.fieldList[index] = { type: 'dynamic', value: '' };
                    } else if (selectedType) {
                        // Predefined option selected
                        activity.fieldList[index] = { type: 'predefined', value: selectedType };
                    } else {
                        // Empty selection
                        activity.fieldList[index] = { type: 'predefined', value: '' };
                    }
                    
                    markAsDirty();
                    
                    // Re-render to update readonly state on value input
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            document.querySelectorAll('.getmetadata-field-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    const value = e.target.value;
                    
                    if (!activity.fieldList) activity.fieldList = [];
                    if (activity.fieldList[index] && activity.fieldList[index].type === 'dynamic') {
                        activity.fieldList[index].value = value;
                        markAsDirty();
                    }
                });
            });
            
            // Add event listeners for Script activity linked service dropdown
            document.querySelectorAll('#configContent .script-linkedservice-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const key = select.getAttribute('data-key');
                    const linkedServiceName = e.target.value;
                    
                    if (linkedServiceName) {
                        // Store as reference object for Script activity
                        activity[key] = {
                            referenceName: linkedServiceName,
                            type: 'LinkedServiceReference'
                        };
                    } else {
                        delete activity[key];
                    }
                    
                    markAsDirty();
                    console.log('Updated ' + key + ':', activity[key]);
                });
            });
            
            // Add event listeners for Script activity - Add Script button
            document.querySelectorAll('.add-script-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.scripts) activity.scripts = [];
                    activity.scripts.push({ type: 'Query', text: '', parameters: [] });
                    markAsDirty();
                    
                    // Re-render to show new script
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listeners for Script activity - Remove Script button
            document.querySelectorAll('.remove-script-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const scriptIndex = parseInt(e.target.getAttribute('data-script-index'));
                    if (activity.scripts && activity.scripts[scriptIndex] !== undefined) {
                        activity.scripts.splice(scriptIndex, 1);
                        markAsDirty();
                        
                        // Re-render to update indices
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            // Add event listeners for Script activity - Script type radio buttons
            document.querySelectorAll('.script-type-radio').forEach(radio => {
                radio.addEventListener('change', (e) => {
                    const scriptIndex = parseInt(e.target.getAttribute('data-script-index'));
                    if (activity.scripts && activity.scripts[scriptIndex]) {
                        activity.scripts[scriptIndex].type = e.target.value;
                        markAsDirty();
                        console.log(\`Updated script \${scriptIndex} type:\`, activity.scripts[scriptIndex].type);
                    }
                });
            });
            
            // Add event listeners for Script activity - Script text area
            document.querySelectorAll('.script-text-area').forEach(textarea => {
                textarea.addEventListener('input', (e) => {
                    const scriptIndex = parseInt(e.target.getAttribute('data-script-index'));
                    if (activity.scripts && activity.scripts[scriptIndex]) {
                        activity.scripts[scriptIndex].text = e.target.value;
                        markAsDirty();
                    }
                });
            });
            
            // Add event listeners for Script activity - Add Parameter button
            document.querySelectorAll('.add-script-param-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const scriptIndex = parseInt(e.target.getAttribute('data-script-index'));
                    if (!activity.scripts) activity.scripts = [];
                    if (!activity.scripts[scriptIndex]) activity.scripts[scriptIndex] = { type: 'Query', text: '', parameters: [] };
                    if (!activity.scripts[scriptIndex].parameters) activity.scripts[scriptIndex].parameters = [];
                    
                    activity.scripts[scriptIndex].parameters.push({
                        name: '',
                        type: 'String',
                        value: '',
                        direction: 'Input'
                    });
                    markAsDirty();
                    
                    // Re-render to show new parameter
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listeners for Script activity - Remove Parameter button
            document.querySelectorAll('.remove-script-param-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const paramRow = e.target.closest('.script-param-row');
                    const scriptIndex = parseInt(paramRow.getAttribute('data-script-index'));
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.scripts && activity.scripts[scriptIndex] && activity.scripts[scriptIndex].parameters) {
                        activity.scripts[scriptIndex].parameters.splice(paramIndex, 1);
                        markAsDirty();
                        
                        // Re-render to update indices
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            // Add event listeners for namespace prefix pairs (XML datasets)
            document.querySelectorAll('.add-namespace-prefix-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.namespacePrefixPairs) {
                        activity.namespacePrefixPairs = {};
                    }
                    
                    // Add a new empty namespace pair with unique key
                    const newKey = \`URLName\${Object.keys(activity.namespacePrefixPairs).length + 1}\`;
                    activity.namespacePrefixPairs[newKey] = \`prefix\${Object.keys(activity.namespacePrefixPairs).length + 1}\`;
                    
                    markAsDirty();
                    console.log('Added new namespace prefix pair');
                    
                    // Re-render to show new pair
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Delete selected namespace prefix pairs
            document.querySelectorAll('.delete-namespace-prefix-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.namespacePrefixPairs) return;
                    
                    // Find all checked rows
                    const checkedRows = Array.from(document.querySelectorAll('.namespace-prefix-checkbox:checked'));
                    if (checkedRows.length === 0) {
                        console.log('No namespace prefix pairs selected for deletion');
                        return;
                    }
                    
                    // Get the URIs to delete
                    const urisToDelete = checkedRows.map(checkbox => {
                        const row = checkbox.closest('.namespace-prefix-row');
                        const uriInput = row.querySelector('.namespace-prefix-uri');
                        return uriInput.value;
                    });
                    
                    // Delete from activity
                    urisToDelete.forEach(uri => {
                        if (uri && activity.namespacePrefixPairs[uri] !== undefined) {
                            delete activity.namespacePrefixPairs[uri];
                        }
                    });
                    
                    markAsDirty();
                    console.log('Deleted namespace prefix pairs:', urisToDelete);
                    
                    // Re-render to update list
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listeners for namespace prefix URI and value fields
            document.querySelectorAll('.namespace-prefix-uri, .namespace-prefix-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const pairRow = e.target.closest('.namespace-prefix-row');
                    const pairIndex = parseInt(pairRow.getAttribute('data-pair-index'));
                    
                    if (activity.namespacePrefixPairs) {
                        const uris = Object.keys(activity.namespacePrefixPairs);
                        const oldUri = uris[pairIndex];
                        
                        if (oldUri !== undefined) {
                            if (e.target.classList.contains('namespace-prefix-uri')) {
                                const newUri = e.target.value;
                                const prefixValue = activity.namespacePrefixPairs[oldUri];
                                
                                // Update the URI (key)
                                delete activity.namespacePrefixPairs[oldUri];
                                activity.namespacePrefixPairs[newUri] = prefixValue;
                            } else if (e.target.classList.contains('namespace-prefix-value')) {
                                // Update the prefix value
                                activity.namespacePrefixPairs[oldUri] = e.target.value;
                            }
                            
                            markAsDirty();
                        }
                    }
                });
            });
            
            // Add event listeners for Script activity parameter fields
            document.querySelectorAll('.script-param-name, .script-param-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const paramRow = e.target.closest('.script-param-row');
                    const scriptIndex = parseInt(paramRow.getAttribute('data-script-index'));
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.scripts && activity.scripts[scriptIndex] && activity.scripts[scriptIndex].parameters && activity.scripts[scriptIndex].parameters[paramIndex]) {
                        const param = activity.scripts[scriptIndex].parameters[paramIndex];
                        if (e.target.classList.contains('script-param-name')) {
                            param.name = e.target.value;
                        } else if (e.target.classList.contains('script-param-value')) {
                            param.value = e.target.value;
                        }
                        markAsDirty();
                    }
                });
            });
            
            document.querySelectorAll('.script-param-type, .script-param-direction').forEach(select => {
                select.addEventListener('change', (e) => {
                    const paramRow = e.target.closest('.script-param-row');
                    const scriptIndex = parseInt(paramRow.getAttribute('data-script-index'));
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.scripts && activity.scripts[scriptIndex] && activity.scripts[scriptIndex].parameters && activity.scripts[scriptIndex].parameters[paramIndex]) {
                        const param = activity.scripts[scriptIndex].parameters[paramIndex];
                        if (e.target.classList.contains('script-param-type')) {
                            param.type = e.target.value;
                            
                            // If type is Byte[], restrict direction to Output only
                            if (e.target.value === 'Byte[]') {
                                const directionSelect = paramRow.querySelector('.script-param-direction');
                                if (directionSelect && directionSelect.value !== 'Output') {
                                    directionSelect.value = 'Output';
                                    param.direction = 'Output';
                                }
                            }
                        } else if (e.target.classList.contains('script-param-direction')) {
                            const typeSelect = paramRow.querySelector('.script-param-type');
                            const currentType = typeSelect ? typeSelect.value : param.type;
                            
                            // Prevent non-Output direction for Byte[] type
                            if (currentType === 'Byte[]' && e.target.value !== 'Output') {
                                e.target.value = 'Output';
                                return;
                            }
                            
                            param.direction = e.target.value;
                        }
                        markAsDirty();
                        
                        // Re-render to update size field visibility
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            document.querySelectorAll('.script-param-size').forEach(input => {
                input.addEventListener('input', (e) => {
                    const paramRow = e.target.closest('.script-param-row');
                    const scriptIndex = parseInt(paramRow.getAttribute('data-script-index'));
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.scripts && activity.scripts[scriptIndex] && activity.scripts[scriptIndex].parameters && activity.scripts[scriptIndex].parameters[paramIndex]) {
                        const value = parseInt(e.target.value);
                        activity.scripts[scriptIndex].parameters[paramIndex].size = isNaN(value) ? undefined : value;
                        markAsDirty();
                    }
                });
            });
            
            document.querySelectorAll('.script-param-null').forEach(checkbox => {
                checkbox.addEventListener('change', (e) => {
                    const paramRow = e.target.closest('.script-param-row');
                    const scriptIndex = parseInt(paramRow.getAttribute('data-script-index'));
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.scripts && activity.scripts[scriptIndex] && activity.scripts[scriptIndex].parameters && activity.scripts[scriptIndex].parameters[paramIndex]) {
                        const param = activity.scripts[scriptIndex].parameters[paramIndex];
                        if (e.target.checked) {
                            param.value = null;
                            // Disable value input
                            const valueInput = paramRow.querySelector('.script-param-value');
                            if (valueInput) {
                                valueInput.value = '';
                                valueInput.disabled = true;
                                valueInput.style.opacity = '0.5';
                            }
                        } else {
                            param.value = '';
                            // Enable value input
                            const valueInput = paramRow.querySelector('.script-param-value');
                            if (valueInput) {
                                valueInput.disabled = false;
                                valueInput.style.opacity = '1';
                            }
                        }
                        markAsDirty();
                    }
                });
            });
            
            // Add event listeners for Stored Procedure activity linked service dropdown
            document.querySelectorAll('#configContent .storedprocedure-linkedservice-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const key = select.getAttribute('data-key');
                    const linkedServiceName = e.target.value;
                    
                    if (linkedServiceName) {
                        // Store as reference object for Stored Procedure activity
                        activity[key] = {
                            referenceName: linkedServiceName,
                            type: 'LinkedServiceReference'
                        };
                        
                        // Determine linked service type
                        const linkedService = window.linkedServicesList?.find(ls => ls.name === linkedServiceName);
                        if (linkedService) {
                            activity._selectedLinkedServiceType = linkedService.type === 'AzureSqlDatabase' ? 'AzureSqlDatabase' : 'AzureSynapse';
                            console.log('Selected linked service type:', activity._selectedLinkedServiceType);
                        }
                    } else {
                        delete activity[key];
                        delete activity._selectedLinkedServiceType;
                        delete activity.linkedServiceProperties;
                    }
                    
                    markAsDirty();
                    console.log('Updated ' + key + ':', activity[key]);
                    
                    // Re-render to show/hide linked service properties field
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listener for Stored Procedure linked service properties value field
            document.querySelectorAll('.storedprocedure-lsprop-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const key = e.target.getAttribute('data-key');
                    const value = e.target.value;
                    
                    if (!activity.linkedServiceProperties) {
                        activity.linkedServiceProperties = {};
                    }
                    activity.linkedServiceProperties.DBName = value;
                    
                    markAsDirty();
                    console.log('Updated linked service property DBName:', value);
                });
            });
            
            // Add event listeners for Stored Procedure activity - Add Parameter button
            document.querySelectorAll('.add-storedprocedure-param-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.storedProcedureParameters) activity.storedProcedureParameters = {};
                    
                    // Use empty string as parameter name (will show placeholder)
                    let paramName = '';
                    let counter = 1;
                    // If empty name already exists, generate Name1, Name2, etc.
                    if (activity.storedProcedureParameters[''] !== undefined) {
                        paramName = 'Name' + counter;
                        while (activity.storedProcedureParameters[paramName] !== undefined) {
                            counter++;
                            paramName = 'Name' + counter;
                        }
                    }
                    
                    activity.storedProcedureParameters[paramName] = {
                        type: 'String',
                        value: ''
                    };
                    markAsDirty();
                    
                    // Re-render to show new parameter
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            // Add event listeners for Stored Procedure activity - Remove Parameter button
            document.querySelectorAll('.remove-storedprocedure-param-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const paramRow = e.target.closest('.storedprocedure-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.storedProcedureParameters) {
                        // Get the parameter name by index
                        const paramNames = Object.keys(activity.storedProcedureParameters);
                        if (paramNames[paramIndex]) {
                            delete activity.storedProcedureParameters[paramNames[paramIndex]];
                            markAsDirty();
                            
                            // Re-render to update indices
                            const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                            showProperties(activity, activeTab);
                        }
                    }
                });
            });
            
            // Add event listeners for Stored Procedure parameter name field
            document.querySelectorAll('.storedprocedure-param-name').forEach(input => {
                input.addEventListener('change', (e) => {
                    const paramRow = e.target.closest('.storedprocedure-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    const newName = e.target.value.trim();
                    
                    if (activity.storedProcedureParameters) {
                        const paramNames = Object.keys(activity.storedProcedureParameters);
                        const oldName = paramNames[paramIndex];
                        
                        // Only update if name changed
                        if (oldName !== undefined && newName !== oldName) {
                            // Check for duplicate name (only if new name is not empty)
                            if (newName && activity.storedProcedureParameters[newName]) {
                                // Duplicate name - show error and revert
                                alert('Parameter name already exists: ' + newName);
                                e.target.value = oldName;
                                return;
                            }
                            
                            // Rename parameter (preserve value and type)
                            const paramData = activity.storedProcedureParameters[oldName];
                            delete activity.storedProcedureParameters[oldName];
                            activity.storedProcedureParameters[newName] = paramData;
                            
                            markAsDirty();
                            console.log(\`Renamed parameter from "\${oldName}" to "\${newName}"\`);
                            
                            // Re-render to update parameter list
                            const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                            showProperties(activity, activeTab);
                        }
                    }
                });
            });
            
            // Add event listeners for Stored Procedure parameter type and value fields
            document.querySelectorAll('.storedprocedure-param-type, .storedprocedure-param-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const paramRow = e.target.closest('.storedprocedure-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.storedProcedureParameters) {
                        const paramNames = Object.keys(activity.storedProcedureParameters);
                        const paramName = paramNames[paramIndex];
                        
                        if (paramName && activity.storedProcedureParameters[paramName]) {
                            const param = activity.storedProcedureParameters[paramName];
                            
                            if (e.target.classList.contains('storedprocedure-param-type')) {
                                param.type = e.target.value;
                            } else if (e.target.classList.contains('storedprocedure-param-value')) {
                                param.value = e.target.value;
                            }
                            
                            markAsDirty();
                        }
                    }
                });
            });
            
            // Add event listeners for Stored Procedure parameter "Treat as null" checkbox
            document.querySelectorAll('.storedprocedure-param-null').forEach(checkbox => {
                checkbox.addEventListener('change', (e) => {
                    const paramRow = e.target.closest('.storedprocedure-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    
                    if (activity.storedProcedureParameters) {
                        const paramNames = Object.keys(activity.storedProcedureParameters);
                        const paramName = paramNames[paramIndex];
                        
                        if (paramName && activity.storedProcedureParameters[paramName]) {
                            const param = activity.storedProcedureParameters[paramName];
                            
                            if (e.target.checked) {
                                param.value = null;
                                // Disable value input
                                const valueInput = paramRow.querySelector('.storedprocedure-param-value');
                                if (valueInput) {
                                    valueInput.value = '';
                                    valueInput.disabled = true;
                                    valueInput.style.opacity = '0.5';
                                    valueInput.style.cursor = 'not-allowed';
                                }
                            } else {
                                param.value = '';
                                // Enable value input
                                const valueInput = paramRow.querySelector('.storedprocedure-param-value');
                                if (valueInput) {
                                    valueInput.disabled = false;
                                    valueInput.style.opacity = '1';
                                    valueInput.style.cursor = 'auto';
                                }
                            }
                            
                            markAsDirty();
                        }
                    }
                });
            });
            
            // Add event listeners for copy-sp-parameters (generic keyed SP params used by Copy activity sink)
            document.querySelectorAll('.add-copy-sp-param-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fieldKey = btn.getAttribute('data-key');
                    if (!activity[fieldKey] || typeof activity[fieldKey] !== 'object') activity[fieldKey] = {};
                    let paramName = '';
                    let counter = 1;
                    if (activity[fieldKey][''] !== undefined) {
                        paramName = 'Name' + counter;
                        while (activity[fieldKey][paramName] !== undefined) { counter++; paramName = 'Name' + counter; }
                    }
                    activity[fieldKey][paramName] = { type: 'String', value: '' };
                    markAsDirty();
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            document.querySelectorAll('.remove-copy-sp-param-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const fieldKey = e.target.getAttribute('data-field-key');
                    const paramRow = e.target.closest('.copy-sp-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    if (activity[fieldKey]) {
                        const paramNames = Object.keys(activity[fieldKey]);
                        if (paramNames[paramIndex] !== undefined) {
                            delete activity[fieldKey][paramNames[paramIndex]];
                            markAsDirty();
                            const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                            showProperties(activity, activeTab);
                        }
                    }
                });
            });
            document.querySelectorAll('.copy-sp-param-name').forEach(input => {
                input.addEventListener('change', (e) => {
                    const fieldKey = e.target.getAttribute('data-field-key');
                    const paramRow = e.target.closest('.copy-sp-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    const newName = e.target.value.trim();
                    if (activity[fieldKey]) {
                        const paramNames = Object.keys(activity[fieldKey]);
                        const oldName = paramNames[paramIndex];
                        if (oldName !== undefined && newName !== oldName) {
                            if (newName && activity[fieldKey][newName] !== undefined) {
                                // Duplicate — overwrite (last wins per spec)
                                delete activity[fieldKey][oldName];
                            } else {
                                const paramData = activity[fieldKey][oldName];
                                delete activity[fieldKey][oldName];
                                activity[fieldKey][newName] = paramData;
                            }
                            markAsDirty();
                            const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                            showProperties(activity, activeTab);
                        }
                    }
                });
            });
            document.querySelectorAll('.copy-sp-param-type, .copy-sp-param-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const fieldKey = e.target.getAttribute('data-field-key');
                    const paramRow = e.target.closest('.copy-sp-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    if (activity[fieldKey]) {
                        const paramNames = Object.keys(activity[fieldKey]);
                        const paramName = paramNames[paramIndex];
                        if (paramName !== undefined && activity[fieldKey][paramName]) {
                            if (e.target.classList.contains('copy-sp-param-type')) {
                                activity[fieldKey][paramName].type = e.target.value;
                            } else {
                                activity[fieldKey][paramName].value = e.target.value;
                            }
                            markAsDirty();
                        }
                    }
                });
            });
            document.querySelectorAll('.copy-sp-param-null').forEach(checkbox => {
                checkbox.addEventListener('change', (e) => {
                    const fieldKey = e.target.getAttribute('data-field-key');
                    const paramRow = e.target.closest('.copy-sp-param-row');
                    const paramIndex = parseInt(paramRow.getAttribute('data-param-index'));
                    if (activity[fieldKey]) {
                        const paramNames = Object.keys(activity[fieldKey]);
                        const paramName = paramNames[paramIndex];
                        if (paramName !== undefined && activity[fieldKey][paramName]) {
                            const param = activity[fieldKey][paramName];
                            if (e.target.checked) {
                                param.value = null;
                                const valueInput = paramRow.querySelector('.copy-sp-param-value');
                                if (valueInput) { valueInput.value = ''; valueInput.disabled = true; valueInput.style.opacity = '0.5'; valueInput.style.cursor = 'not-allowed'; }
                            } else {
                                param.value = '';
                                const valueInput = paramRow.querySelector('.copy-sp-param-value');
                                if (valueInput) { valueInput.disabled = false; valueInput.style.opacity = '1'; valueInput.style.cursor = 'auto'; }
                            }
                            markAsDirty();
                        }
                    }
                });
            });

            // Add event listeners for copy-cmd-default-values (SqlDW Copy command default values table)
            document.querySelectorAll('.add-copy-cmd-dv-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fieldKey = btn.getAttribute('data-key');
                    if (!Array.isArray(activity[fieldKey])) activity[fieldKey] = [];
                    activity[fieldKey].push({ columnName: '', defaultValue: '' });
                    markAsDirty();
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            document.querySelectorAll('.remove-copy-cmd-dv-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const fieldKey = e.target.getAttribute('data-key');
                    const idx = parseInt(e.target.getAttribute('data-index'));
                    if (Array.isArray(activity[fieldKey])) {
                        activity[fieldKey].splice(idx, 1);
                        markAsDirty();
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            document.querySelectorAll('.copy-cmd-dv-col-input, .copy-cmd-dv-val-input').forEach(input => {
                input.addEventListener('input', (e) => {
                    const fieldKey = e.target.getAttribute('data-key');
                    const idx = parseInt(e.target.getAttribute('data-index'));
                    if (Array.isArray(activity[fieldKey]) && activity[fieldKey][idx] !== undefined) {
                        if (e.target.classList.contains('copy-cmd-dv-col-input')) {
                            activity[fieldKey][idx].columnName = e.target.value;
                        } else {
                            activity[fieldKey][idx].defaultValue = e.target.value;
                        }
                        markAsDirty();
                    }
                });
            });

            // Add event listeners for copy-cmd-additional-options (SqlDW Copy command additional options)
            document.querySelectorAll('.add-copy-cmd-ao-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fieldKey = btn.getAttribute('data-key');
                    if (!activity[fieldKey] || typeof activity[fieldKey] !== 'object') activity[fieldKey] = {};
                    let propName = 'Property1';
                    let counter = 1;
                    while (activity[fieldKey][propName] !== undefined) { counter++; propName = 'Property' + counter; }
                    activity[fieldKey][propName] = '';
                    markAsDirty();
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            document.querySelectorAll('.remove-copy-cmd-ao-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const fieldKey = e.target.getAttribute('data-key');
                    const idx = parseInt(e.target.getAttribute('data-index'));
                    if (activity[fieldKey] && typeof activity[fieldKey] === 'object') {
                        const keys = Object.keys(activity[fieldKey]);
                        if (keys[idx] !== undefined) {
                            delete activity[fieldKey][keys[idx]];
                            markAsDirty();
                            const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                            showProperties(activity, activeTab);
                        }
                    }
                });
            });
            document.querySelectorAll('.copy-cmd-ao-prop-input').forEach(input => {
                input.addEventListener('change', (e) => {
                    const fieldKey = e.target.getAttribute('data-key');
                    const idx = parseInt(e.target.getAttribute('data-index'));
                    const newProp = e.target.value.trim();
                    if (activity[fieldKey] && typeof activity[fieldKey] === 'object') {
                        const keys = Object.keys(activity[fieldKey]);
                        const oldProp = keys[idx];
                        if (oldProp !== undefined && newProp !== oldProp) {
                            const val = activity[fieldKey][oldProp];
                            delete activity[fieldKey][oldProp];
                            activity[fieldKey][newProp] = val;
                            markAsDirty();
                            const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                            showProperties(activity, activeTab);
                        }
                    }
                });
            });
            document.querySelectorAll('.copy-cmd-ao-val-input').forEach(input => {
                input.addEventListener('input', (e) => {
                    const fieldKey = e.target.getAttribute('data-key');
                    const idx = parseInt(e.target.getAttribute('data-index'));
                    if (activity[fieldKey] && typeof activity[fieldKey] === 'object') {
                        const keys = Object.keys(activity[fieldKey]);
                        if (keys[idx] !== undefined) {
                            activity[fieldKey][keys[idx]] = e.target.value;
                            markAsDirty();
                        }
                    }
                });
            });

            // Add event listeners for string-list fields (e.g. upsert key columns)
            document.querySelectorAll('.add-string-list-item-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fieldKey = btn.getAttribute('data-key');
                    if (!Array.isArray(activity[fieldKey])) activity[fieldKey] = [];
                    activity[fieldKey].push('');
                    markAsDirty();
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            document.querySelectorAll('.remove-string-list-item-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const fieldKey = e.target.getAttribute('data-field-key');
                    const idx = parseInt(e.target.getAttribute('data-item-index'));
                    if (Array.isArray(activity[fieldKey])) {
                        activity[fieldKey].splice(idx, 1);
                        markAsDirty();
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            document.querySelectorAll('.string-list-item-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const fieldKey = e.target.getAttribute('data-field-key');
                    const idx = parseInt(e.target.getAttribute('data-item-index'));
                    if (Array.isArray(activity[fieldKey])) {
                        activity[fieldKey][idx] = e.target.value;
                        markAsDirty();
                    }
                });
            });

            // Additional columns field handlers (source additionalColumns)
            document.querySelectorAll('.add-additional-col-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fieldKey = btn.getAttribute('data-key');
                    if (!Array.isArray(activity[fieldKey])) activity[fieldKey] = [];
                    // Look up defaultRowValue from field config (source or sink)
                    const _fSrc = copyActivityConfig.datasetTypes?.[activity._sourceDatasetType]?.fields?.source || {};
                    const _fSnk = copyActivityConfig.datasetTypes?.[activity._sinkDatasetType]?.fields?.sink || {};
                    const _rowConf = _fSrc[fieldKey] || _fSnk[fieldKey] || {};
                    activity[fieldKey].push({ name: '', value: _rowConf.defaultRowValue || '' });
                    markAsDirty();
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            document.querySelectorAll('.remove-additional-col-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const fieldKey = btn.getAttribute('data-key');
                    const idx = parseInt(btn.getAttribute('data-index'));
                    if (Array.isArray(activity[fieldKey])) {
                        activity[fieldKey].splice(idx, 1);
                        markAsDirty();
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            document.querySelectorAll('.ac-name-input').forEach(input => {
                input.addEventListener('input', () => {
                    const fieldKey = input.getAttribute('data-key');
                    const idx = parseInt(input.getAttribute('data-index'));
                    if (Array.isArray(activity[fieldKey]) && activity[fieldKey][idx]) {
                        activity[fieldKey][idx].name = input.value;
                        markAsDirty();
                    }
                });
            });
            document.querySelectorAll('.ac-value-input').forEach(input => {
                input.addEventListener('input', () => {
                    const fieldKey = input.getAttribute('data-key');
                    const idx = parseInt(input.getAttribute('data-index'));
                    if (Array.isArray(activity[fieldKey]) && activity[fieldKey][idx]) {
                        activity[fieldKey][idx].value = input.value;
                        markAsDirty();
                    }
                });
            });

            // Add event listeners for Web activity - Authentication type dropdown
            document.querySelectorAll('#configContent select[data-key="authenticationType"]').forEach(select => {
                select.addEventListener('change', (e) => {
                    activity.authenticationType = e.target.value;
                    markAsDirty();
                    console.log('Updated authenticationType:', activity.authenticationType);
                    
                    // Re-render settings tab to show/hide conditional fields
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    if (activeTab === 'settings') {
                        showProperties(activity, 'settings');
                    }
                });
            });
            
            // Add event listeners for Web activity - Method dropdown
            document.querySelectorAll('#configContent select[data-key="method"]').forEach(select => {
                select.addEventListener('change', (e) => {
                    activity.method = e.target.value;
                    markAsDirty();
                    console.log('Updated method:', activity.method);
                    
                    // Re-render settings tab to show/hide body field
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    if (activeTab === 'settings') {
                        showProperties(activity, 'settings');
                    }
                });
            });
            
            // Add event listeners for Web activity - Secret fields (password, pfx, etc.)
            document.querySelectorAll('.web-secret-store, .web-secret-name, .web-secret-version').forEach(input => {
                input.addEventListener('input', (e) => {
                    const key = e.target.getAttribute('data-key');
                    if (!activity[key]) activity[key] = { type: 'AzureKeyVaultSecret', store: {}, secretName: '', secretVersion: 'latest' };
                    
                    if (e.target.classList.contains('web-secret-store')) {
                        if (!activity[key].store) activity[key].store = {};
                        activity[key].store.referenceName = e.target.value;
                        activity[key].store.type = 'LinkedServiceReference';
                    } else if (e.target.classList.contains('web-secret-name')) {
                        activity[key].secretName = e.target.value;
                    } else if (e.target.classList.contains('web-secret-version')) {
                        activity[key].secretVersion = e.target.value || 'latest';
                    }
                    
                    markAsDirty();
                    console.log('Updated ' + key + ':', activity[key]);
                });
            });
            
            // Add event listeners for Web activity - Headers
            document.querySelectorAll('.add-web-header-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.headers) activity.headers = [];
                    activity.headers.push({ name: '', value: '' });
                    markAsDirty();
                    
                    // Re-render settings tab to show new header
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            document.querySelectorAll('.remove-web-header-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    if (activity.headers && activity.headers[index] !== undefined) {
                        activity.headers.splice(index, 1);
                        markAsDirty();
                        
                        // Re-render settings tab to update indices
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            document.querySelectorAll('.web-header-name, .web-header-value').forEach(input => {
                input.addEventListener('input', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    if (!activity.headers) activity.headers = [];
                    if (!activity.headers[index]) activity.headers[index] = { name: '', value: '' };
                    
                    if (e.target.classList.contains('web-header-name')) {
                        activity.headers[index].name = e.target.value;
                    } else if (e.target.classList.contains('web-header-value')) {
                        activity.headers[index].value = e.target.value;
                    }
                    
                    markAsDirty();
                });
            });
            
            // Add event listeners for Web activity - Datasets
            document.querySelectorAll('.add-web-dataset-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.datasets) activity.datasets = [];
                    activity.datasets.push({ referenceName: '', type: 'DatasetReference' });
                    markAsDirty();
                    
                    // Re-render advanced tab to show new dataset
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            document.querySelectorAll('.remove-web-dataset-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    if (activity.datasets && activity.datasets[index] !== undefined) {
                        activity.datasets.splice(index, 1);
                        markAsDirty();
                        
                        // Re-render advanced tab to update indices
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            document.querySelectorAll('.web-dataset-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    if (!activity.datasets) activity.datasets = [];
                    if (!activity.datasets[index]) activity.datasets[index] = { referenceName: '', type: 'DatasetReference' };
                    
                    activity.datasets[index].referenceName = e.target.value;
                    markAsDirty();
                });
            });
            
            // Add event listeners for Web activity - Linked Services
            document.querySelectorAll('.add-web-linkedservice-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    if (!activity.linkedServices) activity.linkedServices = [];
                    activity.linkedServices.push({ referenceName: '', type: 'LinkedServiceReference' });
                    markAsDirty();
                    
                    // Re-render advanced tab to show new linked service
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(activity, activeTab);
                });
            });
            
            document.querySelectorAll('.remove-web-linkedservice-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    if (activity.linkedServices && activity.linkedServices[index] !== undefined) {
                        activity.linkedServices.splice(index, 1);
                        markAsDirty();
                        
                        // Re-render advanced tab to update indices
                        const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                        showProperties(activity, activeTab);
                    }
                });
            });
            
            document.querySelectorAll('.web-linkedservice-select').forEach(select => {
                select.addEventListener('change', (e) => {
                    const index = parseInt(e.target.getAttribute('data-index'));
                    if (!activity.linkedServices) activity.linkedServices = [];
                    if (!activity.linkedServices[index]) activity.linkedServices[index] = { referenceName: '', type: 'LinkedServiceReference' };
                    
                    activity.linkedServices[index].referenceName = e.target.value;
                    markAsDirty();
                });
            });
            
            // Build a config-driven map: { triggerKey -> tabId }
            // Scans all field configs for conditional/nestedConditional references so any new
            // conditional field added to a config automatically gets re-render support — no code change needed.
            const reRenderKeyMap = {};
            function _scanForConditionals(fieldsObj, tabId) {
                for (const prop of Object.values(fieldsObj || {})) {
                    if (prop.conditional?.field) reRenderKeyMap[prop.conditional.field] = tabId;
                    if (prop.nestedConditional?.field) reRenderKeyMap[prop.nestedConditional.field] = tabId;
                    if (Array.isArray(prop.conditionalAll)) {
                        for (const cond of prop.conditionalAll) {
                            if (cond.field) reRenderKeyMap[cond.field] = tabId;
                        }
                    }
                }
            }
            _scanForConditionals(schema?.commonProperties, 'general');
            _scanForConditionals(schema?.typeProperties, 'settings');
            _scanForConditionals(schema?.advancedProperties, 'advanced');
            // Lookup activity: dataset-specific fields also go to settings
            if (activity.type === 'Lookup' && activity._datasetType) {
                _scanForConditionals(datasetSchemas?.[activity._datasetType]?.lookupFields, 'settings');
            }
            // Copy activity: source and sink fields map to their respective tabs
            if (activity.type === 'Copy') {
                const _cpSrcFields = copyActivityConfig.datasetTypes?.[activity._sourceDatasetType]?.fields?.source || {};
                const _cpSnkFields = copyActivityConfig.datasetTypes?.[activity._sinkDatasetType]?.fields?.sink || {};
                _scanForConditionals(_cpSrcFields, 'source');
                _scanForConditionals(_cpSnkFields, 'sink');
            }

            // Add event listeners for radio buttons
            document.querySelectorAll('#configContent input[type="radio"]').forEach(radio => {
                radio.addEventListener('change', (e) => {
                    if (e.target.checked) {
                        const key = e.target.getAttribute('data-key');
                        activity[key] = e.target.value;
                        markAsDirty();
                        console.log('Updated ' + key + ':', activity[key]);

                        // Config-driven re-render: if this key is referenced by any conditional field, re-render that tab
                        const reRenderTab = reRenderKeyMap[key];
                        if (reRenderTab) {
                            showProperties(activity, reRenderTab);
                        }
                    }
                });
            });
            
            // Initialize array items containers for existing Array type parameters
            document.querySelectorAll('.array-items-container').forEach(container => {
                const kvPairGroup = container.closest('.kv-pair-group');
                if (kvPairGroup) {
                    const arrayContentStr = kvPairGroup.getAttribute('data-array-content');
                    if (arrayContentStr) {
                        try {
                            const arrayContent = JSON.parse(arrayContentStr);
                            renderArrayItems(container, arrayContent);
                        } catch (e) {
                            console.error('Failed to parse array content:', e);
                        }
                    }
                }
            });
            
            // Helper function to render array items
            function renderArrayItems(container, items) {
                container.innerHTML = \`
                    <div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-bottom: 6px;">Array Items</div>
                    <button class="add-array-item-btn" style="padding: 3px 6px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px; margin-bottom: 6px;">+ Add Item</button>
                    <div class="array-items-list" style="display: flex; flex-direction: column; gap: 6px;"></div>
                \`;
                
                const itemsList = container.querySelector('.array-items-list');
                items.forEach((item, index) => {
                    addArrayItemToList(itemsList, item, index);
                });
                
                // Add button listener
                container.querySelector('.add-array-item-btn').addEventListener('click', () => {
                    addArrayItemToList(itemsList, { type: 'String', content: '' }, items.length);
                    items.push({ type: 'String', content: '' });
                    updateArrayItemsInActivity();
                });
                
                function updateArrayItemsInActivity() {
                    const kvPairGroup = container.closest('.kv-pair-group');
                    const kvList = container.closest('.kv-list');
                    const fieldKey = kvList?.getAttribute('data-key');
                    if (fieldKey) {
                        updateActivityParameters(fieldKey);
                    }
                }
                
                function addArrayItemToList(list, item, index) {
                    const itemDiv = document.createElement('div');
                    itemDiv.style.cssText = 'display: flex; gap: 4px; align-items: center; padding: 4px; background: var(--vscode-editor-background); border-radius: 2px;';
                    itemDiv.innerHTML = \`
                        <input type="text" class="array-item-content" value="\${item.content || ''}" placeholder="Value" style="flex: 1; font-size: 11px; padding: 2px 4px; background: var(--vscode-input-background); color: var(--vscode-input-foreground); border: 1px solid var(--vscode-input-border);">
                        <button class="remove-array-item-btn" style="padding: 2px 6px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; font-size: 10px;">×</button>
                    \`;
                    list.appendChild(itemDiv);
                    
                    // Add event listeners
                    itemDiv.querySelector('.array-item-content').addEventListener('input', (e) => {
                        // All array items are String type
                        items[index].type = 'String';
                        items[index].content = e.target.value;
                        updateArrayItemsInActivity();
                    });
                    itemDiv.querySelector('.remove-array-item-btn').addEventListener('click', () => {
                        items.splice(index, 1);
                        itemDiv.remove();
                        updateArrayItemsInActivity();
                        // Re-render to fix indices
                        renderArrayItems(container, items);
                    });
                }
            }
            
            // Add event listeners for keyvalue add buttons
            document.querySelectorAll('.add-kv-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const key = e.target.getAttribute('data-key');
                    const kvList = document.querySelector(\`.kv-list[data-key="\${key}"]\`);
                    if (kvList) {
                        // Get the valueTypes from the schema for this field
                        const schema = ${JSON.stringify(activitySchemas)}[activity.type];
                        const prop = schema?.typeProperties?.[key];
                        const types = prop?.valueTypes || ['string', 'int', 'float', 'bool'];
                        const typeOptions = types.map(t => \`<option value="\${t}">\${t}</option>\`).join('');
                        
                        const kvPair = document.createElement('div');
                        kvPair.className = 'property-group kv-pair-group';
                        kvPair.style.marginBottom = '8px';
                        kvPair.style.display = 'flex';
                        kvPair.style.gap = '8px';
                        kvPair.style.alignItems = 'center';
                        kvPair.innerHTML = \`
                            <input type="text" class="property-input kv-key" placeholder="Key" style="flex: 1;">
                            <input type="text" class="property-input kv-value" placeholder="Value" style="flex: 1;">
                            <select class="property-input kv-type" style="flex: 0 0 100px;">\${typeOptions}</select>
                            <button class="remove-kv-btn" style="padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;">Remove</button>
                        \`;
                        kvList.appendChild(kvPair);
                        
                        // Add remove listener
                        kvPair.querySelector('.remove-kv-btn').addEventListener('click', () => {
                            kvPair.remove();
                            // Update activity parameters when removed
                            updateActivityParameters(key);
                        });
                        
                        // Add change listeners to update activity
                        const typeSelect = kvPair.querySelector('.kv-type');
                        typeSelect.addEventListener('change', () => {
                            const valueCell = kvPair.querySelector('.kv-value')?.parentElement || kvPair.children[1];
                            
                            // If Boolean is selected, replace value input with dropdown
                            if (typeSelect.value === 'Boolean') {
                                const valueInput = kvPair.querySelector('.kv-value');
                                if (valueInput && valueInput.tagName === 'INPUT') {
                                    const select = document.createElement('select');
                                    select.className = 'property-input kv-value';
                                    select.style.flex = '1';
                                    select.innerHTML = \`
                                        <option value="true">true</option>
                                        <option value="false" selected>false</option>
                                    \`;
                                    valueInput.replaceWith(select);
                                    select.addEventListener('change', () => updateActivityParameters(key));
                                }
                            }
                            // If Array is selected, replace value input with array editor
                            else if (typeSelect.value === 'Array') {
                                const valueInput = kvPair.querySelector('.kv-value');
                                if (valueInput && !valueInput.classList.contains('array-items-container')) {
                                    // Restructure the kvPair for Array layout
                                    kvPair.style.alignItems = 'flex-start';
                                    kvPair.setAttribute('data-array-content', JSON.stringify([]));
                                    
                                    const arrayContainer = document.createElement('div');
                                    arrayContainer.className = 'array-items-container';
                                    arrayContainer.style.cssText = 'flex: 1; border: 1px solid var(--vscode-input-border); border-radius: 3px; padding: 8px; background: var(--vscode-input-background);';
                                    
                                    // Move key input, type selector, and remove button to a new row
                                    const keyInput = kvPair.querySelector('.kv-key');
                                    const removeBtn = kvPair.querySelector('.remove-kv-btn');
                                    
                                    kvPair.innerHTML = '';
                                    const topRow = document.createElement('div');
                                    topRow.style.cssText = 'display: flex; gap: 8px; align-items: center; width: 100%; margin-bottom: 8px;';
                                    topRow.appendChild(keyInput);
                                    topRow.appendChild(typeSelect);
                                    topRow.appendChild(removeBtn);
                                    
                                    kvPair.style.flexDirection = 'column';
                                    kvPair.appendChild(topRow);
                                    kvPair.appendChild(arrayContainer);
                                    
                                    renderArrayItems(arrayContainer, []);
                                }
                            }
                            // If switched from Boolean/Array to other type, replace with text input
                            else {
                                const valueInput = kvPair.querySelector('.kv-value');
                                if (valueInput && (valueInput.tagName === 'SELECT' || valueInput.classList.contains('array-items-container'))) {
                                    // Reset to normal layout
                                    kvPair.style.alignItems = 'center';
                                    kvPair.style.flexDirection = 'row';
                                    
                                    const keyInput = kvPair.querySelector('.kv-key');
                                    const removeBtn = kvPair.querySelector('.remove-kv-btn');
                                    
                                    const textInput = document.createElement('input');
                                    textInput.type = 'text';
                                    textInput.className = 'property-input kv-value';
                                    textInput.placeholder = 'Value';
                                    textInput.style.flex = '1';
                                    textInput.value = valueInput.tagName === 'SELECT' ? valueInput.value : '';
                                    
                                    kvPair.innerHTML = '';
                                    kvPair.appendChild(keyInput);
                                    kvPair.appendChild(textInput);
                                    kvPair.appendChild(typeSelect);
                                    kvPair.appendChild(removeBtn);
                                    
                                    textInput.addEventListener('input', () => updateActivityParameters(key));
                                }
                            }
                            updateActivityParameters(key);
                        });
                        
                        kvPair.querySelectorAll('.kv-key, .kv-value').forEach(input => {
                            input.addEventListener('change', () => updateActivityParameters(key));
                            input.addEventListener('input', () => updateActivityParameters(key));
                        });
                    }
                });
            });
            
            // Function to update activity parameters from UI
            function updateActivityParameters(fieldKey) {
                const kvList = document.querySelector(\`.kv-list[data-key="\${fieldKey}"]\`);
                if (!kvList) return;
                
                const parameters = {};
                kvList.querySelectorAll('.kv-pair-group, .property-group').forEach(pair => {
                    const keyInput = pair.querySelector('.kv-key');
                    const typeSelect = pair.querySelector('.kv-type');
                    
                    if (keyInput && typeSelect) {
                        const key = keyInput.value.trim();
                        const type = typeSelect.value;
                        
                        if (key) {
                            // Handle Array type differently
                            if (type === 'Array') {
                                const arrayContainer = pair.querySelector('.array-items-container');
                                if (arrayContainer) {
                                    const arrayItems = [];
                                    arrayContainer.querySelectorAll('.array-items-list > div').forEach(itemDiv => {
                                        const itemContent = itemDiv.querySelector('.array-item-content')?.value;
                                        if (itemContent !== undefined) {
                                            // All array items are String type
                                            arrayItems.push({
                                                type: 'String',
                                                content: itemContent || ''
                                            });
                                        }
                                    });
                                    parameters[key] = {
                                        type: type,
                                        content: arrayItems
                                    };
                                }
                            } else {
                                // Handle non-Array types
                                const valueInput = pair.querySelector('.kv-value');
                                if (valueInput) {
                                    const value = valueInput.value.trim();
                                    parameters[key] = {
                                        value: value,
                                        type: type
                                    };
                                }
                            }
                        }
                    }
                });
                
                // Store in activity object
                activity[fieldKey] = Object.keys(parameters).length > 0 ? parameters : undefined;
                console.log('Updated', fieldKey + ':', activity[fieldKey]);
                markAsDirty();
            }
            
            // Add change listeners to existing parameter inputs
            document.querySelectorAll('.kv-list').forEach(kvList => {
                const fieldKey = kvList.getAttribute('data-key');
                kvList.querySelectorAll('.kv-pair-group, .property-group').forEach(pair => {
                    pair.querySelectorAll('.kv-key, .kv-value, .kv-type').forEach(input => {
                        input.addEventListener('change', () => {
                            // If type changed to Boolean, replace value input with dropdown
                            if (input.classList.contains('kv-type') && input.value === 'Boolean') {
                                const valueInput = pair.querySelector('.kv-value');
                                if (valueInput && valueInput.tagName === 'INPUT' && !valueInput.classList.contains('array-items-container')) {
                                    const currentValue = valueInput.value;
                                    const select = document.createElement('select');
                                    select.className = 'property-input kv-value';
                                    select.style.flex = '1';
                                    select.innerHTML = \`
                                        <option value="true" \${currentValue === 'true' ? 'selected' : ''}>true</option>
                                        <option value="false" \${currentValue === 'false' || !currentValue ? 'selected' : ''}>false</option>
                                    \`;
                                    valueInput.replaceWith(select);
                                    select.addEventListener('change', () => updateActivityParameters(fieldKey));
                                }
                            }
                            // If type changed to Array, replace value input with array editor
                            else if (input.classList.contains('kv-type') && input.value === 'Array') {
                                const valueInput = pair.querySelector('.kv-value');
                                if (valueInput && !valueInput.classList.contains('array-items-container')) {
                                    // Restructure the kvPair for Array layout
                                    pair.style.alignItems = 'flex-start';
                                    pair.style.flexDirection = 'column';
                                    pair.setAttribute('data-array-content', JSON.stringify([]));
                                    
                                    const arrayContainer = document.createElement('div');
                                    arrayContainer.className = 'array-items-container';
                                    arrayContainer.style.cssText = 'flex: 1; border: 1px solid var(--vscode-input-border); border-radius: 3px; padding: 8px; background: var(--vscode-input-background); width: 100%;';
                                    
                                    // Move key input, type selector, and remove button to a new row
                                    const keyInput = pair.querySelector('.kv-key');
                                    const typeSelect = pair.querySelector('.kv-type');
                                    const removeBtn = pair.querySelector('.remove-kv-btn');
                                    
                                    pair.innerHTML = '';
                                    const topRow = document.createElement('div');
                                    topRow.style.cssText = 'display: flex; gap: 8px; align-items: center; width: 100%;';
                                    topRow.appendChild(keyInput);
                                    topRow.appendChild(typeSelect);
                                    topRow.appendChild(removeBtn);
                                    
                                    pair.appendChild(topRow);
                                    pair.appendChild(arrayContainer);
                                    
                                    renderArrayItems(arrayContainer, []);
                                }
                            }
                            // If type changed from Boolean/Array to something else, replace with text input
                            else if (input.classList.contains('kv-type') && input.value !== 'Boolean' && input.value !== 'Array') {
                                const valueInput = pair.querySelector('.kv-value');
                                const arrayContainer = pair.querySelector('.array-items-container');
                                if ((valueInput && valueInput.tagName === 'SELECT') || arrayContainer) {
                                    // Reset to normal layout
                                    pair.style.alignItems = 'center';
                                    pair.style.flexDirection = 'row';
                                    
                                    const keyInput = pair.querySelector('.kv-key');
                                    const typeSelect = pair.querySelector('.kv-type');
                                    const removeBtn = pair.querySelector('.remove-kv-btn');
                                    
                                    const textInput = document.createElement('input');
                                    textInput.type = 'text';
                                    textInput.className = 'property-input kv-value';
                                    textInput.placeholder = 'Value';
                                    textInput.style.flex = '1';
                                    textInput.value = valueInput && valueInput.tagName === 'SELECT' ? valueInput.value : '';
                                    
                                    pair.innerHTML = '';
                                    pair.appendChild(keyInput);
                                    pair.appendChild(textInput);
                                    pair.appendChild(typeSelect);
                                    pair.appendChild(removeBtn);
                                    
                                    textInput.addEventListener('input', () => updateActivityParameters(fieldKey));
                                }
                            }
                            updateActivityParameters(fieldKey);
                        });
                        input.addEventListener('input', () => updateActivityParameters(fieldKey));
                    });
                    
                    pair.querySelector('.remove-kv-btn')?.addEventListener('click', () => {
                        pair.remove();
                        updateActivityParameters(fieldKey);
                    });
                });
            });
            
            // Add tab click handlers
            document.querySelectorAll('.activity-tab').forEach(tab => {
                tab.addEventListener('click', () => {
                    document.querySelectorAll('.config-tab').forEach(t => {
                        t.classList.remove('active');
                        t.style.color = 'var(--vscode-tab-inactiveForeground)';
                        t.style.borderBottom = 'none';
                    });
                    document.querySelectorAll('.activity-pane').forEach(p => {
                        p.classList.remove('active');
                        p.style.display = 'none';
                    });
                    
                    tab.classList.add('active');
                    tab.style.color = 'var(--vscode-tab-activeForeground)';
                    tab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
                    const tabName = tab.getAttribute('data-tab');
                    const pane = document.getElementById(\`tab-\${tabName}\`);
                    if (pane) {
                        pane.classList.add('active');
                        pane.style.display = '';
                    }
                });
            });

            // Right sidebar - basic properties
            rightPanel.innerHTML = \`
                <div class="property-group">
                    <div class="property-label">Name</div>
                    <input type="text" class="property-input" id="propName" value="\${activity.name}">
                </div>
                <div class="property-group">
                    <div class="property-label">Type</div>
                    <input type="text" class="property-input" value="\${activity.type}" readonly>
                </div>
                <div class="property-group">
                    <div class="property-label">Description</div>
                    <textarea class="property-input" id="propDescription" rows="3">\${activity.description}</textarea>
                </div>
                <div class="property-group">
                    <div class="property-label">Position</div>
                    <div style="display: flex; gap: 8px; flex: 1;">
                        <input type="number" class="property-input" id="propX" value="\${Math.round(activity.x)}" placeholder="X">
                        <input type="number" class="property-input" id="propY" value="\${Math.round(activity.y)}" placeholder="Y">
                    </div>
                </div>
            \`;

            document.getElementById('propName').addEventListener('input', (e) => {
                activity.updateName(e.target.value);
                markAsDirty();
                draw();
            });

            document.getElementById('propDescription').addEventListener('input', (e) => {
                activity.description = e.target.value;
                markAsDirty();
            });
            
            // Expression field for IfCondition activities
            const propExpression = document.getElementById('propExpression');
            if (propExpression) {
                propExpression.addEventListener('input', (e) => {
                    activity.expression = e.target.value;
                    markAsDirty();
                });
            }

            // On expression field for Switch activities
            const propOn = document.getElementById('propOn');
            if (propOn) {
                propOn.addEventListener('input', (e) => {
                    activity.on = e.target.value;
                    markAsDirty();
                });
            }

            // Expression + timeout for Until activities
            const propUntilExpression = document.getElementById('propUntilExpression');
            if (propUntilExpression) {
                propUntilExpression.addEventListener('input', (e) => {
                    activity.expression = e.target.value;
                    markAsDirty();
                });
            }
            const propUntilTimeout = document.getElementById('propUntilTimeout');
            if (propUntilTimeout) {
                propUntilTimeout.addEventListener('input', (e) => {
                    activity.timeout = e.target.value;
                    markAsDirty();
                });
            }
            
            document.getElementById('propX').addEventListener('input', (e) => {
                const x = parseInt(e.target.value) || 0;
                activity.updatePosition(x, activity.y);
                markAsDirty();
                draw();
            });
            
            document.getElementById('propY').addEventListener('input', (e) => {
                const y = parseInt(e.target.value) || 0;
                activity.updatePosition(activity.x, y);
                markAsDirty();
                draw();
            });
        }

        // Tab switching
        document.querySelectorAll('.config-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                // Remove active class and styles from all tabs
                document.querySelectorAll('.config-tab').forEach(t => {
                    t.classList.remove('active');
                    t.style.color = 'var(--vscode-tab-inactiveForeground)';
                    t.style.borderBottom = 'none';
                });
                document.querySelectorAll('.config-tab-pane').forEach(p => {
                    p.classList.remove('active');
                    p.style.display = 'none';
                });
                
                // Add active class and styles to clicked tab
                tab.classList.add('active');
                tab.style.color = 'var(--vscode-tab-activeForeground)';
                tab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
                const tabName = tab.getAttribute('data-tab');
                const pane = document.getElementById(\`tab-\${tabName}\`);
                if (pane) {
                    pane.classList.add('active');
                    pane.style.display = 'block';
                }
                
                // Re-render pipeline properties when switching to their tabs
                if (tabName === 'parameters') {
                    renderPipelineParameters();
                } else if (tabName === 'pipeline-variables') {
                    renderPipelineVariables();
                }
            });
        });

        // Toolbar buttons
        document.getElementById('saveBtn').addEventListener('click', () => {
            // If we're inside a branch editor, temporarily save branch data to parent
            if (editingContext) {
                // Save current branch activities to parent activity
                const branchData = activities.map(a => {
                    const cleaned = {
                        name: a.name,
                        type: a.type,
                        x: a.x,
                        y: a.y,
                        dependsOn: connections
                            .filter(c => c.to === a)
                            .map(c => ({
                                activity: c.from.name,
                                dependencyConditions: [c.condition || 'Succeeded']
                            })),
                        userProperties: a.userProperties || []
                    };
                    
                    if (a.description) cleaned.description = a.description;
                    if (a.state) cleaned.state = a.state;
                    
                    const { typeProperties: _tp2, activityProps: _ap2 } = buildNestedActivityTypeProperties(a);
                    cleaned.typeProperties = _tp2;
                    Object.assign(cleaned, _ap2);
                    return cleaned;
                });
                
                // Update parent activity's branch
                if (editingContext.branch === 'true') {
                    editingContext.parentActivity.ifTrueActivities = branchData;
                } else if (editingContext.branch === 'false') {
                    editingContext.parentActivity.ifFalseActivities = branchData;
                } else if (editingContext.branch === 'activities') {
                    editingContext.parentActivity.activities = branchData;
                } else if (editingContext.branch === 'default') {
                    editingContext.parentActivity.defaultActivities = branchData;
                } else if (editingContext.branch && editingContext.branch.startsWith('case:')) {
                    const _caseIdx = parseInt(editingContext.branch.split(':')[1]);
                    if (editingContext.parentActivity.cases[_caseIdx]) {
                        editingContext.parentActivity.cases[_caseIdx].activities = branchData;
                    }
                }
                
                // Walk up the context stack: serialize each ancestor canvas into its parent activity.
                // When N levels deep (e.g. IfCondition True inside ForEach inside main pipeline):
                //   - editingContext.savedState.activities = ForEach body canvas [IfCondition_obj]
                //   - editingContextStack[0].savedState.activities = main pipeline canvas [ForEach_obj]
                // We need to serialize the ForEach body into ForEach_obj.activities so that
                // buildPipelineDataForSave can read it correctly.
                for (let _i = editingContextStack.length - 1; _i >= 0; _i--) {
                    // The canvas for this ancestor level comes from the *child* context's savedState
                    const _levelCanvasCtx = (_i === editingContextStack.length - 1)
                        ? editingContext
                        : editingContextStack[_i + 1];
                    const _levelActs = _levelCanvasCtx.savedState.activities;
                    const _levelConns = _levelCanvasCtx.savedState.connections;
                    const _ctx = editingContextStack[_i];
                    
                    const _levelBranchData = _levelActs.map(_a3 => {
                        const _c3 = {
                            name: _a3.name,
                            type: _a3.type,
                            x: _a3.x,
                            y: _a3.y,
                            dependsOn: _levelConns
                                .filter(_c => _c.to === _a3)
                                .map(_c => ({
                                    activity: _c.from.name,
                                    dependencyConditions: [_c.condition || 'Succeeded']
                                })),
                            userProperties: _a3.userProperties || []
                        };
                        if (_a3.description) _c3.description = _a3.description;
                        if (_a3.state) _c3.state = _a3.state;
                        const { typeProperties: _tp3, activityProps: _ap3 } = buildNestedActivityTypeProperties(_a3);
                        _c3.typeProperties = _tp3;
                        Object.assign(_c3, _ap3);
                        return _c3;
                    });
                    
                    if (_ctx.branch === 'true') _ctx.parentActivity.ifTrueActivities = _levelBranchData;
                    else if (_ctx.branch === 'false') _ctx.parentActivity.ifFalseActivities = _levelBranchData;
                    else if (_ctx.branch === 'activities') _ctx.parentActivity.activities = _levelBranchData;
                    else if (_ctx.branch === 'default') _ctx.parentActivity.defaultActivities = _levelBranchData;
                    else if (_ctx.branch && _ctx.branch.startsWith('case:')) {
                        const _stackCaseIdx = parseInt(_ctx.branch.split(':')[1]);
                        if (_ctx.parentActivity.cases[_stackCaseIdx]) {
                            _ctx.parentActivity.cases[_stackCaseIdx].activities = _levelBranchData;
                        }
                    }
                }
                
                // Validate current branch: IfCondition must have an expression; Switch must have an on expression
                for (const _a of activities) {
                    if (_a.type === 'IfCondition' && (!_a.expression || _a.expression.trim() === '')) {
                        vscode.postMessage({ type: 'error', text: 'If Condition "' + _a.name + '" requires an expression.' });
                        return;
                    }
                    if (_a.type === 'Switch' && (!_a.on || _a.on.trim() === '')) {
                        vscode.postMessage({ type: 'error', text: 'Switch "' + _a.name + '" requires an On Expression.' });
                        return;
                    }
                    if (_a.type === 'Switch' && (!_a.cases || _a.cases.length === 0)) {
                        vscode.postMessage({ type: 'error', text: 'Switch "' + _a.name + '" must have at least one case.' });
                        return;
                    }
                    if (_a.type === 'Until' && (!_a.expression || _a.expression.trim() === '')) {
                        vscode.postMessage({ type: 'error', text: 'Until "' + _a.name + '" requires an expression.' });
                        return;
                    }
                }
                
                // Use root saved state (outermost ancestor) as the main pipeline for save
                // When 2+ levels deep (e.g. IfCondition inside ForEach), stack[0] holds the true root
                const rootCtx = editingContextStack.length > 0 ? editingContextStack[0] : editingContext;
                
                // Temporarily switch to main pipeline context for validation and save
                const savedActivities = activities;
                const savedConnections = connections;
                activities = rootCtx.savedState.activities;
                connections = rootCtx.savedState.connections;
                
                // Validate main pipeline activities
                const validation = validateActivities();
                if (!validation.valid) {
                    // Restore branch context
                    activities = savedActivities;
                    connections = savedConnections;
                    vscode.postMessage({ type: 'error', text: validation.message });
                    return;
                }
                
                // Build save data from main pipeline.
                // Use try/finally so that the branch context (activities, connections) is
                // ALWAYS restored even when buildPipelineDataForSave throws a validation error.
                let saveData;
                try {
                    saveData = buildPipelineDataForSave(getPipelineNameFromPath(), false);
                } catch (_saveErr) {
                    // Validation error was already reported inside buildPipelineDataForSave
                    // via vscode.postMessage({ type: 'validationError', ... }).
                    return;
                } finally {
                    // Always restore branch context so subsequent saves work correctly.
                    activities = savedActivities;
                    connections = savedConnections;
                }
                
                console.log('[Webview] Sending save message (from branch) with filePath:', currentFilePath);
                vscode.postMessage({ 
                    type: 'save', 
                    data: saveData,
                    filePath: currentFilePath 
                });
                return;
            }
            
            // Normal save from main pipeline
            const validation = validateActivities();
            if (!validation.valid) {
                vscode.postMessage({ type: 'error', text: validation.message });
                return;
            }
            
            const data = buildPipelineDataForSave(getPipelineNameFromPath(), false);
            
            console.log('[Webview] Sending save message with filePath:', currentFilePath);
            vscode.postMessage({ 
                type: 'save', 
                data: data,
                filePath: currentFilePath 
            });
        });

        document.getElementById('clearBtn').addEventListener('click', () => {
            if (confirm('Clear all activities?')) {
                // Remove all activity DOM elements
                activities.forEach(a => a.remove());
                activities = [];
                connections = [];
                selectedActivity = null;
                markAsDirty();
                showProperties(null);
                draw();
            }
        });

        document.getElementById('zoomInBtn').addEventListener('click', () => {
            scale *= 1.2;
            ctx.scale(1.2, 1.2);
            draw();
        });

        document.getElementById('zoomOutBtn').addEventListener('click', () => {
            scale /= 1.2;
            ctx.scale(1 / 1.2, 1 / 1.2);
            draw();
        });

        document.getElementById('fitBtn').addEventListener('click', () => {
            scale = 1;
            ctx.setTransform(1, 0, 0, 1, 0, 0);
            draw();
        });

        // Handle messages from extension
        window.addEventListener('message', event => {
            const message = event.data;
            
            if (message.type === 'initSchemas') {
                datasetSchemas = message.datasetSchemas;
                datasetList = message.datasetList || [];
                datasetContents = message.datasetContents || {};
                pipelineList = message.pipelineList || [];
                window.linkedServicesList = message.linkedServicesList || [];
                copyActivityConfig = message.copyActivityConfig || {};
                console.log('Dataset schemas loaded:', Object.keys(datasetSchemas));
                console.log('Dataset list loaded:', datasetList);
                console.log('Dataset contents loaded:', Object.keys(datasetContents).length, 'datasets');
                console.log('Pipeline list loaded:', pipelineList);
                console.log('Linked services list loaded:', window.linkedServicesList);
                console.log('Copy activity config loaded:', Object.keys(copyActivityConfig.datasetTypes || {}).length, 'dataset types');
            } else if (message.type === 'addActivity') {
                const canvasWrapper = document.getElementById('canvasWrapper');
                const activity = new Activity(message.activityType, 100, 100, canvasWrapper);
                activities.push(activity);
                markAsDirty();
                draw();
            } else if (message.type === 'loadPipeline') {
                currentFilePath = message.filePath || null; // Store the file path
                loadPipelineFromJson(message.data);
            } else if (message.type === 'saveCompleted') {
                // Clear dirty state after successful save
                clearDirty();
                // Strip empty entries from noEmpty/filterEmpty arrays on all activities so UI reflects clean state
                activities.forEach(a => {
                    if (a.type === 'Copy') {
                        if (a._sinkDatasetType) {
                            const _snkConf = copyActivityConfig.datasetTypes?.[a._sinkDatasetType];
                            const _snkFields = _snkConf?.fields?.sink || {};
                            for (const [_fk, _fc] of Object.entries(_snkFields)) {
                                if (_fc.noEmpty && Array.isArray(a[_fk])) {
                                    a[_fk] = a[_fk].filter(s => typeof s === 'string' ? s.trim() !== '' : s !== null && s !== undefined);
                                }
                                if (_fc.filterEmpty && Array.isArray(a[_fk])) {
                                    a[_fk] = a[_fk].filter(item => item[_fc.filterEmpty] && String(item[_fc.filterEmpty]).trim() !== '');
                                }
                            }
                        }
                        if (a._sourceDatasetType) {
                            const _srcConf = copyActivityConfig.datasetTypes?.[a._sourceDatasetType];
                            const _srcFields = _srcConf?.fields?.source || {};
                            for (const [_fk, _fc] of Object.entries(_srcFields)) {
                                if (_fc.filterEmpty && Array.isArray(a[_fk])) {
                                    a[_fk] = a[_fk].filter(item => item[_fc.filterEmpty] && String(item[_fc.filterEmpty]).trim() !== '');
                                }
                            }
                        }
                    }
                });
                // Re-render current activity if one is selected
                if (selectedActivity) {
                    const activeTab = document.querySelector('.activity-tab.active')?.getAttribute('data-tab');
                    showProperties(selectedActivity, activeTab);
                }
            }
        });

        function loadPipelineFromJson(pipelineJson) {
            try {
                // Clear existing
                activities.forEach(a => a.remove());
                activities = [];
                connections = [];
                
                // Extract activities from Synapse format
                const pipelineActivities = pipelineJson.properties?.activities || pipelineJson.activities || [];
                const pipelineVariables = pipelineJson.properties?.variables || {};
                const pipelineParameters = pipelineJson.properties?.parameters || {};
                const pipelineConcurrency = pipelineJson.properties?.concurrency || 1;
                console.log('[Webview] Loading', pipelineActivities.length, 'activities');
                console.log('[Webview] Pipeline variables:', pipelineVariables);
                console.log('[Webview] Pipeline parameters:', pipelineParameters);
                console.log('[Webview] Pipeline concurrency:', pipelineConcurrency);
                
                // Load pipeline-level properties
                pipelineData.variables = pipelineVariables;
                pipelineData.parameters = pipelineParameters;
                pipelineData.concurrency = pipelineConcurrency;
                
                // Create activities first
                const canvasWrapper = document.getElementById('canvasWrapper');
                const activityMap = new Map();
                
                pipelineActivities.forEach((activityData, index) => {
                    const x = 100 + (index % 5) * 200;
                    const y = 100 + Math.floor(index / 5) * 150;
                    
                    const activity = new Activity(activityData.type, x, y, canvasWrapper);
                    activity.name = activityData.name;
                    activity.description = activityData.description || '';
                    
                    // Load policy properties
                    if (activityData.policy) {
                        activity.timeout = activityData.policy.timeout;
                        activity.retry = activityData.policy.retry;
                        activity.retryIntervalInSeconds = activityData.policy.retryIntervalInSeconds;
                        activity.secureOutput = activityData.policy.secureOutput;
                        activity.secureInput = activityData.policy.secureInput;
                    }
                    
                    // Load state (for Copy activity)
                    if (activityData.state) {
                        activity.state = activityData.state;
                    }
                    if (activityData.onInactiveMarkAs) {
                        activity.onInactiveMarkAs = activityData.onInactiveMarkAs;
                    }
                    
                    activity.userProperties = activityData.userProperties || [];
                    
                    // Load inputs/outputs (for Copy activity)
                    if (activityData.inputs) {
                        activity.inputs = activityData.inputs;
                    }
                    if (activityData.outputs) {
                        activity.outputs = activityData.outputs;
                    }
                    
                    // Copy all typeProperties to activity object
                    if (activityData.typeProperties) {
                        // Handle special case: conf object for Spark settings
                        if (activityData.typeProperties.conf) {
                            const conf = activityData.typeProperties.conf;
                            if (conf['spark.dynamicAllocation.enabled'] !== undefined) {
                                // Convert boolean to 'Enabled'/'Disabled' string
                                activity.dynamicAllocation = conf['spark.dynamicAllocation.enabled'] ? 'Enabled' : 'Disabled';
                            }
                            if (conf['spark.dynamicAllocation.minExecutors'] !== undefined) {
                                activity.minExecutors = conf['spark.dynamicAllocation.minExecutors'];
                            }
                            if (conf['spark.dynamicAllocation.maxExecutors'] !== undefined) {
                                activity.maxExecutors = conf['spark.dynamicAllocation.maxExecutors'];
                            }
                            // Don't copy the raw conf object
                            delete activityData.typeProperties.conf;
                        }
                        
                        // Convert notebook and sparkPool reference objects to strings for editing
                        if (activityData.typeProperties.notebook && typeof activityData.typeProperties.notebook === 'object') {
                            const notebookRef = activityData.typeProperties.notebook.referenceName;
                            if (typeof notebookRef === 'object' && notebookRef.value) {
                                // Expression format: { referenceName: { value: "name", type: "Expression" } }
                                activity.notebook = notebookRef.value;
                            } else if (typeof notebookRef === 'string') {
                                // Direct string format: { referenceName: "name" }
                                activity.notebook = notebookRef;
                            } else {
                                activity.notebook = '';
                            }
                        }
                        if (activityData.typeProperties.sparkPool && typeof activityData.typeProperties.sparkPool === 'object') {
                            const sparkPoolRef = activityData.typeProperties.sparkPool.referenceName;
                            if (typeof sparkPoolRef === 'object' && sparkPoolRef.value) {
                                // Expression format: { referenceName: { value: "name", type: "Expression" } }
                                activity.sparkPool = sparkPoolRef.value;
                            } else if (typeof sparkPoolRef === 'string') {
                                // Direct string format: { referenceName: "name" }
                                activity.sparkPool = sparkPoolRef;
                            } else {
                                activity.sparkPool = '';
                            }
                        }
                        
                        // Handle Copy activity source/sink nested structures
                        if (activityData.type === 'Copy') {
                            const tp = activityData.typeProperties || {};

                            console.log('[Load] Copy activity detected:', activityData.name);

                            // ── Parse source dataset reference ──────────────────────────────────────────
                            if (activityData.inputs && activityData.inputs.length > 0) {
                                const ref = activityData.inputs[0];
                                activity.sourceDataset = (typeof ref === 'object' && ref.referenceName) ? ref.referenceName : ref;
                                console.log('[Load] sourceDataset:', activity.sourceDataset);
                                if (activity.sourceDataset && datasetContents[activity.sourceDataset]) {
                                    const ds = datasetContents[activity.sourceDataset].properties || {};
                                    activity._sourceDatasetType = ds.type;
                                    activity._sourceLocationType = ds.typeProperties && ds.typeProperties.location && ds.typeProperties.location.type;
                                    console.log('[Load] Source type:', activity._sourceDatasetType, 'loc:', activity._sourceLocationType);
                                }
                            }

                            // ── Parse sink dataset reference ────────────────────────────────────────────
                            if (activityData.outputs && activityData.outputs.length > 0) {
                                const ref = activityData.outputs[0];
                                activity.sinkDataset = (typeof ref === 'object' && ref.referenceName) ? ref.referenceName : ref;
                                console.log('[Load] sinkDataset:', activity.sinkDataset);
                                if (activity.sinkDataset && datasetContents[activity.sinkDataset]) {
                                    const ds = datasetContents[activity.sinkDataset].properties || {};
                                    activity._sinkDatasetType = ds.type;
                                    activity._sinkLocationType = ds.typeProperties && ds.typeProperties.location && ds.typeProperties.location.type;
                                    console.log('[Load] Sink type:', activity._sinkDatasetType, 'loc:', activity._sinkLocationType);
                                }
                            }

                            // ── Resolve source/sink JSON objects (handle double-nesting edge case) ──────
                            let sourceObj = null;
                            let sinkObj = null;
                            if (tp.typeProperties && (tp.typeProperties.source || tp.typeProperties.sink)) {
                                console.log('[Load] Found nested typeProperties, using deeper level');
                                sourceObj = tp.typeProperties.source || null;
                                sinkObj   = tp.typeProperties.sink   || null;
                            } else {
                                sourceObj = tp.source || null;
                                sinkObj   = tp.sink   || null;
                            }

                            // ── Flatten source into src_ prefixed fields using config ─────────────────
                            if (sourceObj) {
                                activity._sourceObject = sourceObj;
                                console.log('[Load] Source object type:', sourceObj.type);
                                const _srcConf = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[activity._sourceDatasetType];
                                if (_srcConf && _srcConf.fields && _srcConf.fields.source) {
                                    // Config-driven: read values using jsonPath and store with src_ prefix
                                    for (const [_fk, _fc] of Object.entries(_srcConf.fields.source)) {
                                        if (!_fc.jsonPath) continue;
                                        const _v = _getValueByPath(sourceObj, _fc.jsonPath);
                                        if (_v !== undefined) {
                                            // Normalize filterEmpty fields (e.g. additional-columns): must be arrays;
                                            // old JSON may store a plain string — treat that as empty
                                            activity[_fk] = (_fc.filterEmpty && !Array.isArray(_v)) ? [] : _v;
                                        } else if (_fc.default !== undefined) {
                                            activity[_fk] = _fc.default;
                                        }
                                    }
                                    console.log('[Load] Config-parsed source fields for', activity._sourceDatasetType);
                                }
                                // Always keep _sourceObject as fallback for unknown types / extension save
                            }

                            // ── Flatten sink into snk_ prefixed fields using config ───────────────────
                            if (sinkObj) {
                                activity._sinkObject = sinkObj;
                                console.log('[Load] Sink object type:', sinkObj.type);
                                const _snkConf = copyActivityConfig.datasetTypes && copyActivityConfig.datasetTypes[activity._sinkDatasetType];
                                if (_snkConf && _snkConf.fields && _snkConf.fields.sink) {
                                    for (const [_fk, _fc] of Object.entries(_snkConf.fields.sink)) {
                                        if (!_fc.jsonPath) continue;
                                        const _v = _getValueByPath(sinkObj, _fc.jsonPath);
                                        if (_v !== undefined) {
                                            // Coerce corrupt saved values back to the correct type
                                            if (_fc.type === 'copy-cmd-additional-options') {
                                                activity[_fk] = (_v && typeof _v === 'object' && !Array.isArray(_v)) ? _v : {};
                                            } else if (_fc.type === 'copy-cmd-default-values') {
                                                activity[_fk] = Array.isArray(_v) ? _v : [];
                                            } else {
                                                activity[_fk] = (_fc.filterEmpty && !Array.isArray(_v)) ? [] : _v;
                                            }
                                        } else if (_fc.filterEmpty !== undefined) {
                                            activity[_fk] = [];
                                        } else if (_fc.default !== undefined) {
                                            activity[_fk] = _fc.default;
                                        }
                                    }
                                    console.log('[Load] Config-parsed sink fields for', activity._sinkDatasetType);
                                }
                            }

                            // ── Advanced / top-level typeProperties fields ────────────────────────────
                            const _advFields = copyActivityConfig.advancedFields || {};
                            for (const [_fk, _fc] of Object.entries(_advFields)) {
                                const _v = _getValueByPath(tp, _fc.jsonPath || _fk);
                                if (_v !== undefined) activity[_fk] = _v;
                            }
                            // Also preserve staging, translator for pass-through
                            if (tp.enableStaging !== undefined) activity.enableStaging = tp.enableStaging;
                            if (tp.stagingSettings)             activity.stagingSettings = tp.stagingSettings;
                            if (tp.logSettings)                 activity.logSettings = tp.logSettings;
                            if (tp.translator)                  activity.translator = tp.translator;

                        } else if (activityData.type === 'SetVariable') {
                            // Handle SetVariable specific loading
                            const tp = activityData.typeProperties;
                            
                            // Check if this is a pipeline return value
                            if (tp.setSystemVariable && tp.variableName === 'pipelineReturnValue' && Array.isArray(tp.value)) {
                                activity.variableType = 'Pipeline return value';
                                activity.returnValues = {};
                                
                                // Parse the return values array
                                tp.value.forEach(item => {
                                    if (item.key && item.value) {
                                        const returnValue = {
                                            type: item.value.type
                                        };
                                        
                                        // Handle different content types
                                        if (item.value.type === 'Null') {
                                            returnValue.value = '';
                                        } else if (item.value.type === 'Array') {
                                            returnValue.content = item.value.content || [];
                                        } else {
                                            returnValue.value = item.value.content !== undefined ? String(item.value.content) : '';
                                        }
                                        
                                        activity.returnValues[item.key] = returnValue;
                                    }
                                });
                            } else {
                                // Pipeline variable
                                activity.variableType = 'Pipeline variable';
                                activity.variableName = tp.variableName;
                                activity.value = tp.value;
                                
                                // Try to determine the variable type from the variables section
                                if (pipelineVariables && pipelineVariables[tp.variableName]) {
                                    const varType = pipelineVariables[tp.variableName].type;
                                    // Map Azure type to UI type
                                    const typeMap = {
                                        'String': 'String',
                                        'Boolean': 'Boolean',
                                        'Array': 'Array',
                                        'Integer': 'Integer'
                                    };
                                    activity.pipelineVariableType = typeMap[varType] || 'String';
                                } else {
                                    activity.pipelineVariableType = 'String';
                                }
                            }
                        } else if (activityData.type === 'Filter') {
                            const tp = activityData.typeProperties || {};
                            const { items, condition, ...rest } = tp;
                            Object.assign(activity, rest);
                            if (items && typeof items === 'object' && items.value !== undefined) {
                                activity.items = items.value;
                            } else if (items !== undefined) {
                                activity.items = items;
                            }
                            if (condition && typeof condition === 'object' && condition.value !== undefined) {
                                activity.condition = condition.value;
                            } else if (condition !== undefined) {
                                activity.condition = condition;
                            }
                        } else if (activityData.type === 'Delete') {
                            // Handle Delete activity - parse storeSettings structure
                            const tp = activityData.typeProperties || {};
                            
                            // Extract dataset reference
                            if (tp.dataset && tp.dataset.referenceName) {
                                activity.dataset = tp.dataset.referenceName;
                            }
                            
                            // Parse storeSettings
                            if (tp.storeSettings) {
                                const ss = tp.storeSettings;
                                
                                // Determine file path type based on what fields are present
                                if (ss.fileListPath) {
                                    activity.filePathType = 'listOfFiles';
                                    activity.fileListPath = ss.fileListPath;
                                } else if (ss.prefix) {
                                    activity.filePathType = 'prefix';
                                    activity.prefix = ss.prefix;
                                } else if (ss.wildcardFileName || ss.wildcardFolderPath) {
                                    activity.filePathType = 'wildcardFilePath';
                                    if (ss.wildcardFolderPath) activity.wildcardFolderPath = ss.wildcardFolderPath;
                                    if (ss.wildcardFileName) activity.wildcardFileName = ss.wildcardFileName;
                                } else {
                                    activity.filePathType = 'filePathInDataset';
                                }
                                
                                // Extract other settings
                                if (ss.maxConcurrentConnections !== undefined) {
                                    activity.maxConcurrentConnections = ss.maxConcurrentConnections;
                                }
                                if (ss.recursive !== undefined) {
                                    activity.recursive = ss.recursive;
                                }
                                // Convert ISO datetime to datetime-local format
                                if (ss.modifiedDatetimeStart) {
                                    const startDate = new Date(ss.modifiedDatetimeStart);
                                    // Format as YYYY-MM-DDTHH:mm:ss for datetime-local input
                                    activity.modifiedDatetimeStart = startDate.toISOString().slice(0, 19);
                                }
                                if (ss.modifiedDatetimeEnd) {
                                    const endDate = new Date(ss.modifiedDatetimeEnd);
                                    // Format as YYYY-MM-DDTHH:mm:ss for datetime-local input
                                    activity.modifiedDatetimeEnd = endDate.toISOString().slice(0, 19);
                                }
                            }
                        } else if (activityData.type === 'ExecutePipeline') {
                            // Handle ExecutePipeline activity - parse pipeline reference
                            const tp = activityData.typeProperties || {};
                            
                            // Extract pipeline reference
                            if (tp.pipeline && tp.pipeline.referenceName) {
                                activity.pipeline = tp.pipeline.referenceName;
                            }
                            
                            // Extract waitOnCompletion (default to true if not specified)
                            if (tp.waitOnCompletion !== undefined) {
                                activity.waitOnCompletion = tp.waitOnCompletion;
                            } else {
                                activity.waitOnCompletion = true;
                            }
                        } else if (activityData.type === 'Validation') {
                            // Handle Validation activity - parse dataset reference and childItems
                            const tp = activityData.typeProperties || {};
                            
                            // Extract dataset reference (keep as object for proper saving)
                            if (tp.dataset) {
                                activity.dataset = tp.dataset;
                                
                                // Store location type for conditional rendering
                                if (tp.dataset.referenceName && datasetContents[tp.dataset.referenceName]) {
                                    const locationType = datasetContents[tp.dataset.referenceName].properties?.typeProperties?.location?.type;
                                    activity._datasetLocationType = locationType;
                                    console.log('[Load] Validation dataset location type:', locationType);
                                }
                            }
                            
                            // Extract timeout
                            if (tp.timeout !== undefined) {
                                activity.timeout = tp.timeout;
                            }
                            
                            // Extract sleep
                            if (tp.sleep !== undefined) {
                                activity.sleep = tp.sleep;
                            }
                            
                            // Extract childItems - convert boolean to string, or set to "ignore" if not present
                            if (tp.childItems !== undefined) {
                                activity.childItems = String(tp.childItems); // Convert true/false to "true"/"false"
                            } else {
                                activity.childItems = 'ignore'; // Default to ignore if not present
                            }
                        } else if (activityData.type === 'GetMetadata') {
                            // Handle GetMetadata activity - parse dataset reference and field list
                            const tp = activityData.typeProperties || {};
                            
                            // Extract dataset reference
                            if (tp.dataset && tp.dataset.referenceName) {
                                activity.dataset = tp.dataset.referenceName;
                                
                                // Store location type for conditional rendering
                                if (datasetContents[tp.dataset.referenceName]) {
                                    const locationType = datasetContents[tp.dataset.referenceName].properties?.typeProperties?.location?.type;
                                    activity._datasetLocationType = locationType;
                                    console.log('[Load] GetMetadata dataset location type:', locationType);
                                }
                            }
                            
                            // Extract field list - convert from array format to internal format
                            if (tp.fieldList && Array.isArray(tp.fieldList)) {
                                activity.fieldList = tp.fieldList.map(field => {
                                    if (typeof field === 'string') {
                                        // Simple string field like "childItems", "exists"
                                        return { type: 'predefined', value: field };
                                    } else if (typeof field === 'object' && field.type === 'Expression') {
                                        // Dynamic expression field
                                        return { type: 'dynamic', value: field.value };
                                    }
                                    return field;
                                });
                            } else {
                                activity.fieldList = [];
                            }
                            
                            // Parse storeSettings for datetime filters (Blob/ADLS only)
                            if (tp.storeSettings) {
                                const ss = tp.storeSettings;
                                
                                // Convert ISO datetime to datetime-local format
                                if (ss.modifiedDatetimeStart) {
                                    const startDate = new Date(ss.modifiedDatetimeStart);
                                    activity.modifiedDatetimeStart = startDate.toISOString().slice(0, 19);
                                }
                                if (ss.modifiedDatetimeEnd) {
                                    const endDate = new Date(ss.modifiedDatetimeEnd);
                                    activity.modifiedDatetimeEnd = endDate.toISOString().slice(0, 19);
                                }
                            }
                            
                            // Parse formatSettings for skipLineCount (Blob/ADLS only)
                            if (tp.formatSettings && tp.formatSettings.skipLineCount !== undefined) {
                                activity.skipLineCount = tp.formatSettings.skipLineCount;
                            }
                        } else if (activityData.type === 'Lookup') {
                            // Handle Lookup activity - parse dataset reference and source configuration
                            const tp = activityData.typeProperties || {};
                            
                            // Extract dataset reference
                            if (tp.dataset && tp.dataset.referenceName) {
                                activity.dataset = tp.dataset.referenceName;
                                
                                // Store dataset type for conditional rendering
                                if (datasetContents[tp.dataset.referenceName]) {
                                    const datasetType = datasetContents[tp.dataset.referenceName].properties?.type;
                                    activity._datasetType = datasetType;
                                    console.log('[Load] Lookup dataset type:', datasetType);
                                }
                            }
                            
                            // Extract firstRowOnly (default to true if not specified)
                            if (tp.firstRowOnly !== undefined) {
                                activity.firstRowOnly = tp.firstRowOnly;
                            } else {
                                activity.firstRowOnly = true;
                            }
                            
                            // Parse source configuration
                            if (tp.source) {
                                const source = tp.source;
                                
                                // For SQL sources
                                if (source.type === 'AzureSqlSource' || source.type === 'SqlDWSource') {
                                    // Determine useQuery based on what's present
                                    if (source.sqlReaderQuery) {
                                        activity.useQuery = 'Query';
                                        activity.sqlReaderQuery = source.sqlReaderQuery;
                                    } else if (source.sqlReaderStoredProcedureName) {
                                        activity.useQuery = 'Stored procedure';
                                        activity.sqlReaderStoredProcedureName = source.sqlReaderStoredProcedureName;
                                    } else {
                                        activity.useQuery = 'Table';
                                    }
                                    
                                    // Extract query timeout - convert from HH:MM:SS to minutes
                                    if (source.queryTimeout !== undefined) {
                                        const timeStr = source.queryTimeout;
                                        if (typeof timeStr === 'string' && timeStr.includes(':')) {
                                            const parts = timeStr.split(':');
                                            const hours = parseInt(parts[0]) || 0;
                                            const minutes = parseInt(parts[1]) || 0;
                                            activity.queryTimeout = (hours * 60) + minutes;
                                        } else {
                                            activity.queryTimeout = parseInt(source.queryTimeout) || 120;
                                        }
                                    }
                                    
                                    // Extract isolation level
                                    if (source.isolationLevel) {
                                        activity.isolationLevel = source.isolationLevel;
                                    }
                                    
                                    // Extract partition option - handle both string and nested object format
                                    if (source.partitionOption) {
                                        const partitionValue = typeof source.partitionOption === 'string' 
                                            ? source.partitionOption 
                                            : source.partitionOption.partitionOption;
                                        
                                        // Set the appropriate partition option field based on useQuery
                                        if (activity.useQuery === 'Table') {
                                            activity.partitionOption = partitionValue;
                                        } else if (activity.useQuery === 'Query') {
                                            activity.partitionOptionQuery = partitionValue;
                                        } else if (activity.useQuery === 'Stored procedure') {
                                            activity.partitionOptionStoredProc = partitionValue;
                                        }
                                    }
                                    
                                    // Extract partition settings (for DynamicRange)
                                    if (source.partitionSettings) {
                                        const ps = source.partitionSettings;
                                        if (activity.useQuery === 'Table') {
                                            if (ps.partitionColumnName) activity.partitionColumnName = ps.partitionColumnName;
                                            if (ps.partitionUpperBound) activity.partitionUpperBound = ps.partitionUpperBound;
                                            if (ps.partitionLowerBound) activity.partitionLowerBound = ps.partitionLowerBound;
                                        } else if (activity.useQuery === 'Query') {
                                            if (ps.partitionColumnName) activity.partitionColumnNameQuery = ps.partitionColumnName;
                                            if (ps.partitionUpperBound) activity.partitionUpperBoundQuery = ps.partitionUpperBound;
                                            if (ps.partitionLowerBound) activity.partitionLowerBoundQuery = ps.partitionLowerBound;
                                        }
                                    }
                                    
                                    // Extract stored procedure parameters
                                    if (source.storedProcedureParameters) {
                                        activity.storedProcedureParameters = source.storedProcedureParameters;
                                    }
                                }
                                // For storage sources (DelimitedText, Parquet, Json, etc.)
                                else if (source.storeSettings) {
                                    const ss = source.storeSettings;
                                    
                                    // Determine file path type (simplified for Lookup)
                                    activity.filePathType = 'filePathInDataset';
                                    
                                    // Extract modified datetime filters
                                    if (ss.modifiedDatetimeStart) {
                                        const startDate = new Date(ss.modifiedDatetimeStart);
                                        activity.modifiedDatetimeStart = startDate.toISOString().slice(0, 19);
                                    }
                                    if (ss.modifiedDatetimeEnd) {
                                        const endDate = new Date(ss.modifiedDatetimeEnd);
                                        activity.modifiedDatetimeEnd = endDate.toISOString().slice(0, 19);
                                    }
                                    
                                    // Extract recursive flag
                                    if (ss.recursive !== undefined) {
                                        activity.recursive = ss.recursive;
                                    }
                                    
                                    // Extract partition discovery
                                    if (ss.enablePartitionDiscovery !== undefined) {
                                        activity.enablePartitionDiscovery = ss.enablePartitionDiscovery;
                                    }
                                    
                                    // Extract max concurrent connections
                                    if (ss.maxConcurrentConnections !== undefined) {
                                        activity.maxConcurrentConnections = ss.maxConcurrentConnections;
                                    }
                                    
                                    // Extract skipLineCount from formatSettings
                                    if (source.formatSettings && source.formatSettings.skipLineCount !== undefined) {
                                        activity.skipLineCount = source.formatSettings.skipLineCount;
                                    }
                                    
                                    // Extract XML-specific formatSettings
                                    if (source.formatSettings && source.formatSettings.type === 'XmlReadSettings') {
                                        const fs = source.formatSettings;
                                        
                                        // Extract validation mode
                                        if (fs.validationMode) {
                                            activity.validationMode = fs.validationMode;
                                        }
                                        
                                        // Extract detectDataType
                                        if (fs.detectDataType !== undefined) {
                                            activity.detectDataType = fs.detectDataType;
                                        }
                                        
                                        // Extract namespaces
                                        if (fs.namespaces !== undefined) {
                                            activity.namespaces = fs.namespaces;
                                        }
                                        
                                        // Extract namespace prefix pairs
                                        if (fs.namespacePrefixes) {
                                            activity.namespacePrefixPairs = fs.namespacePrefixes;
                                        }
                                    }
                                }
                                // For HTTP sources
                                else if (source.type === 'HttpSource') {
                                    // Extract request method (default to GET)
                                    activity.requestMethod = 'GET';
                                    
                                    // Extract request timeout
                                    if (source.httpRequestTimeout) {
                                        activity.requestTimeout = source.httpRequestTimeout;
                                    }
                                    
                                    // Extract max concurrent connections
                                    if (source.maxConcurrentConnections !== undefined) {
                                        activity.maxConcurrentConnections = source.maxConcurrentConnections;
                                    }
                                }
                            }
                        } else if (activityData.type === 'Script') {
                            // Handle Script activity - parse linkedServiceName and scripts array
                            const tp = activityData.typeProperties || {};
                            
                            // Parse linkedServiceName
                            if (activityData.linkedServiceName) {
                                activity.linkedServiceName = activityData.linkedServiceName;
                            }
                            
                            // Parse scripts array
                            if (tp.scripts && Array.isArray(tp.scripts)) {
                                activity.scripts = tp.scripts.map(script => ({
                                    type: script.type || 'Query',
                                    text: script.text || '',
                                    parameters: script.parameters ? script.parameters.map(param => ({
                                        name: param.name || '',
                                        type: param.type || 'String',
                                        value: param.value,
                                        direction: param.direction || 'Input',
                                        size: param.size
                                    })) : []
                                }));
                            } else {
                                activity.scripts = [{ type: 'Query', text: '', parameters: [] }];
                            }
                            
                            // Parse scriptBlockExecutionTimeout
                            if (tp.scriptBlockExecutionTimeout) {
                                activity.scriptBlockExecutionTimeout = tp.scriptBlockExecutionTimeout;
                            }
                        } else if (activityData.type === 'SqlServerStoredProcedure') {
                            // Handle SqlServerStoredProcedure activity - parse linkedServiceName, storedProcedureName, and storedProcedureParameters
                            const tp = activityData.typeProperties || {};
                            
                            // Parse linkedServiceName
                            if (activityData.linkedServiceName) {
                                activity.linkedServiceName = activityData.linkedServiceName;
                                
                                // Determine linked service type from the reference name
                                const linkedServiceRefName = activityData.linkedServiceName.referenceName;
                                const linkedService = window.linkedServicesList?.find(ls => ls.name === linkedServiceRefName);
                                if (linkedService) {
                                    activity._selectedLinkedServiceType = linkedService.type === 'AzureSqlDatabase' ? 'AzureSqlDatabase' : 'AzureSynapse';
                                } else {
                                    // Fallback: check if it has parameters (indicates Synapse)
                                    activity._selectedLinkedServiceType = activityData.linkedServiceName.parameters ? 'AzureSynapse' : 'AzureSqlDatabase';
                                }
                                
                                // Parse linked service properties (for Azure Synapse Analytics)
                                if (activityData.linkedServiceName.parameters && activityData.linkedServiceName.parameters.DBName) {
                                    activity.linkedServiceProperties = {
                                        DBName: activityData.linkedServiceName.parameters.DBName
                                    };
                                }
                            }
                            
                            // Parse storedProcedureName
                            if (tp.storedProcedureName) {
                                activity.storedProcedureName = tp.storedProcedureName;
                            }
                            
                            // Parse storedProcedureParameters
                            if (tp.storedProcedureParameters && typeof tp.storedProcedureParameters === 'object') {
                                activity.storedProcedureParameters = {};
                                for (const [paramName, paramData] of Object.entries(tp.storedProcedureParameters)) {
                                    activity.storedProcedureParameters[paramName] = {
                                        type: paramData.type || 'String',
                                        value: paramData.value
                                    };
                                }
                            }
                        } else if (activityData.type === 'IfCondition') {
                            // Handle IfCondition activities
                            const tp = activityData.typeProperties;
                            
                            // Convert expression object to string
                            if (tp.expression && typeof tp.expression === 'object') {
                                activity.expression = tp.expression.value || '';
                            } else if (typeof tp.expression === 'string') {
                                activity.expression = tp.expression;
                            } else {
                                activity.expression = '';
                            }
                            
                            // Load nested activities for True branch
                            if (tp.ifTrueActivities && Array.isArray(tp.ifTrueActivities)) {
                                activity.ifTrueActivities = tp.ifTrueActivities;
                            } else {
                                activity.ifTrueActivities = [];
                            }
                            
                            // Load nested activities for False branch
                            if (tp.ifFalseActivities && Array.isArray(tp.ifFalseActivities)) {
                                activity.ifFalseActivities = tp.ifFalseActivities;
                            } else {
                                activity.ifFalseActivities = [];
                            }
                            
                            // Don't copy the object-based expression, container flags, or nested arrays
                            const excludeProps = ['expression', 'ifTrueActivities', 'ifFalseActivities'];
                            const cleanedProps = {};
                            for (const key in tp) {
                                if (!excludeProps.includes(key)) {
                                    cleanedProps[key] = tp[key];
                                }
                            }
                            Object.assign(activity, cleanedProps);
                        } else if (activityData.type === 'ForEach') {
                            // Handle ForEach activities
                            const tp = activityData.typeProperties;
                            
                            // Convert items expression object to string
                            if (tp.items && typeof tp.items === 'object') {
                                activity.items = tp.items.value || '';
                            } else if (typeof tp.items === 'string') {
                                activity.items = tp.items;
                            } else {
                                activity.items = '';
                            }
                            
                            // Load isSequential
                            activity.isSequential = tp.isSequential === true;
                            
                            // Load batchCount if specified
                            if (tp.batchCount !== undefined) {
                                activity.batchCount = tp.batchCount;
                            }
                            
                            // Load nested activities
                            if (tp.activities && Array.isArray(tp.activities)) {
                                activity.activities = tp.activities;
                            } else {
                                activity.activities = [];
                            }
                            
                            // Don't copy the items expression object or nested arrays
                            const excludePropsForEach = ['items', 'activities', 'isSequential', 'batchCount'];
                            const cleanedPropsForEach = {};
                            for (const key in tp) {
                                if (!excludePropsForEach.includes(key)) {
                                    cleanedPropsForEach[key] = tp[key];
                                }
                            }
                            Object.assign(activity, cleanedPropsForEach);
                        } else if (activityData.type === 'Until') {
                            // Handle Until activities
                            const tp = activityData.typeProperties;
                            // Convert expression object to string
                            if (tp.expression && typeof tp.expression === 'object') {
                                activity.expression = tp.expression.value || '';
                            } else {
                                activity.expression = tp.expression || '';
                            }
                            if (tp.timeout) activity.timeout = tp.timeout;
                            activity.activities = Array.isArray(tp.activities) ? tp.activities : [];
                        } else if (activityData.type === 'Switch') {
                            // Handle Switch activities
                            const tp = activityData.typeProperties;

                            // Convert on expression object to string
                            if (tp.on && typeof tp.on === 'object') {
                                activity.on = tp.on.value || '';
                            } else if (typeof tp.on === 'string') {
                                activity.on = tp.on;
                            } else {
                                activity.on = '';
                            }

                            // Load cases array: [{value, activities}]
                            if (tp.cases && Array.isArray(tp.cases)) {
                                activity.cases = tp.cases.map(c => ({
                                    value: c.value || '',
                                    activities: Array.isArray(c.activities) ? c.activities : []
                                }));
                            } else {
                                activity.cases = [];
                            }

                            // Load defaultActivities
                            if (tp.defaultActivities && Array.isArray(tp.defaultActivities)) {
                                activity.defaultActivities = tp.defaultActivities;
                            } else {
                                activity.defaultActivities = [];
                            }

                            // Copy remaining typeProperties (excluding the ones we handle manually)
                            const excludePropsSwitch = ['on', 'cases', 'defaultActivities'];
                            const cleanedPropsSwitch = {};
                            for (const key in tp) {
                                if (!excludePropsSwitch.includes(key)) {
                                    cleanedPropsSwitch[key] = tp[key];
                                }
                            }
                            Object.assign(activity, cleanedPropsSwitch);
                        } else {
                            Object.assign(activity, activityData.typeProperties);
                        }
                    }
                    
                    activities.push(activity);
                    activityMap.set(activityData.name, activity);
                });
                
                // Create connections based on dependsOn
                pipelineActivities.forEach((activityData) => {
                    if (activityData.dependsOn && activityData.dependsOn.length > 0) {
                        const toActivity = activityMap.get(activityData.name);
                        if (toActivity) {
                            activityData.dependsOn.forEach(dep => {
                                const fromActivity = activityMap.get(dep.activity);
                                if (fromActivity) {
                                    const condition = dep.dependencyConditions?.[0] || 'Succeeded';
                                    const connection = new Connection(fromActivity, toActivity, condition);
                                    connections.push(connection);
                                }
                            });
                        }
                    }
                });
                
                draw();
                showProperties(null);
                console.log(\`Loaded \${activities.length} activities from pipeline JSON\`);
                // Clear dirty state after loading
                clearDirty();
            } catch (error) {
                console.error('Error loading pipeline:', error);
            }
        }

        // Pipeline-level properties functions
        function renderPipelineParameters() {
            const parametersList = document.getElementById('parametersList');
            if (!parametersList) return;
            
            parametersList.innerHTML = '';
            
            for (const [paramName, paramData] of Object.entries(pipelineData.parameters)) {
                const paramDiv = document.createElement('div');
                paramDiv.className = 'property-group kv-pair-group';
                paramDiv.style.cssText = 'margin-bottom: 8px; display: flex; gap: 8px; align-items: center;';
                
                // Key input
                const keyInput = document.createElement('input');
                keyInput.type = 'text';
                keyInput.className = 'property-input param-key';
                keyInput.value = paramData._displayName !== undefined ? paramData._displayName : paramName;
                keyInput.placeholder = 'Name';
                keyInput.style.flex = '1';
                keyInput.setAttribute('data-original-name', paramName);
                
                // Default value input
                const valueInput = document.createElement('input');
                valueInput.type = 'text';
                valueInput.className = 'property-input param-default';
                valueInput.value = paramData.defaultValue || '';
                valueInput.placeholder = 'Value';
                valueInput.style.flex = '1';
                valueInput.setAttribute('data-param', paramName);
                
                // Type select
                const typeSelect = document.createElement('select');
                typeSelect.className = 'property-input param-type';
                typeSelect.style.cssText = 'flex: 0 0 100px;';
                typeSelect.setAttribute('data-param', paramName);
                
                const types = ['string', 'int', 'float', 'bool', 'array', 'object'];
                types.forEach(t => {
                    const option = document.createElement('option');
                    option.value = t;
                    option.textContent = t.charAt(0).toUpperCase() + t.slice(1);
                    if (t === paramData.type) option.selected = true;
                    typeSelect.appendChild(option);
                });
                
                // Remove button
                const removeBtn = document.createElement('button');
                removeBtn.className = 'remove-param-btn';
                removeBtn.textContent = 'Remove';
                removeBtn.style.cssText = 'padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;';
                removeBtn.setAttribute('data-param', paramName);
                
                paramDiv.appendChild(keyInput);
                paramDiv.appendChild(valueInput);
                paramDiv.appendChild(typeSelect);
                paramDiv.appendChild(removeBtn);
                
                parametersList.appendChild(paramDiv);
            }
            
            // Add event listeners for remove buttons
            document.querySelectorAll('.remove-param-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const paramName = e.target.getAttribute('data-param');
                    delete pipelineData.parameters[paramName];
                    renderPipelineParameters();
                    markAsDirty();
                });
            });
            
            // Add event listeners for type changes
            document.querySelectorAll('.param-type').forEach(select => {
                select.addEventListener('change', (e) => {
                    const paramName = e.target.getAttribute('data-param');
                    pipelineData.parameters[paramName].type = e.target.value;
                    markAsDirty();
                });
            });
            
            // Add event listeners for default value changes
            document.querySelectorAll('.param-default').forEach(input => {
                input.addEventListener('input', (e) => {
                    const paramName = e.target.getAttribute('data-param');
                    pipelineData.parameters[paramName].defaultValue = e.target.value;
                    markAsDirty();
                });
            });
            
            // Add event listeners for key name changes (rename)
            document.querySelectorAll('.param-key').forEach(input => {
                input.addEventListener('blur', (e) => {
                    const originalName = e.target.getAttribute('data-original-name');
                    const newName = e.target.value.trim();
                    
                    if (newName && newName !== originalName) {
                        // Check if it's a temp key or actual rename
                        const isTempKey = originalName.startsWith('_temp_param_');
                        
                        if (pipelineData.parameters[newName] && !isTempKey) {
                            alert('A parameter with this name already exists!');
                            e.target.value = pipelineData.parameters[originalName]._displayName || originalName;
                        } else {
                            // Create entry with new name
                            const paramData = { ...pipelineData.parameters[originalName] };
                            delete paramData._isNew;
                            delete paramData._displayName;
                            
                            pipelineData.parameters[newName] = paramData;
                            delete pipelineData.parameters[originalName];
                            markAsDirty();
                            renderPipelineParameters();
                        }
                    } else if (!newName && originalName.startsWith('_temp_param_')) {
                        // Update display name for temp keys
                        pipelineData.parameters[originalName]._displayName = '';
                    } else if (!newName) {
                        e.target.value = pipelineData.parameters[originalName]._displayName || originalName;
                    }
                });
            });
        }
        
        function renderPipelineVariables() {
            const variablesList = document.getElementById('variablesList');
            if (!variablesList) return;
            
            variablesList.innerHTML = '';
            
            for (const [varName, varData] of Object.entries(pipelineData.variables)) {
                const varDiv = document.createElement('div');
                varDiv.className = 'property-group kv-pair-group';
                varDiv.style.cssText = 'margin-bottom: 8px; display: flex; gap: 8px; align-items: center;';
                
                // Key input
                const keyInput = document.createElement('input');
                keyInput.type = 'text';
                keyInput.className = 'property-input var-key';
                keyInput.value = varData._displayName !== undefined ? varData._displayName : varName;
                keyInput.placeholder = 'Name';
                keyInput.style.flex = '1';
                keyInput.setAttribute('data-original-name', varName);
                
                // Default value input
                const valueInput = document.createElement('input');
                valueInput.type = 'text';
                valueInput.className = 'property-input var-default';
                valueInput.value = varData.defaultValue || '';
                valueInput.placeholder = 'Value';
                valueInput.style.flex = '1';
                valueInput.setAttribute('data-var', varName);
                
                // Type select
                const typeSelect = document.createElement('select');
                typeSelect.className = 'property-input var-type';
                typeSelect.style.cssText = 'flex: 0 0 100px;';
                typeSelect.setAttribute('data-var', varName);
                
                const types = ['String', 'Boolean', 'Array', 'Integer'];
                types.forEach(t => {
                    const option = document.createElement('option');
                    option.value = t;
                    option.textContent = t;
                    if (t === varData.type) option.selected = true;
                    typeSelect.appendChild(option);
                });
                
                // Remove button
                const removeBtn = document.createElement('button');
                removeBtn.className = 'remove-var-btn';
                removeBtn.textContent = 'Remove';
                removeBtn.style.cssText = 'padding: 6px 12px; background: var(--vscode-button-secondaryBackground); color: var(--vscode-button-secondaryForeground); border: none; cursor: pointer; border-radius: 2px; flex-shrink: 0;';
                removeBtn.setAttribute('data-var', varName);
                
                varDiv.appendChild(keyInput);
                varDiv.appendChild(valueInput);
                varDiv.appendChild(typeSelect);
                varDiv.appendChild(removeBtn);
                
                variablesList.appendChild(varDiv);
            }
            
            // Add event listeners for remove buttons
            document.querySelectorAll('.remove-var-btn').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const varName = e.target.getAttribute('data-var');
                    delete pipelineData.variables[varName];
                    renderPipelineVariables();
                    markAsDirty();
                });
            });
            
            // Add event listeners for type changes
            document.querySelectorAll('.var-type').forEach(select => {
                select.addEventListener('change', (e) => {
                    const varName = e.target.getAttribute('data-var');
                    pipelineData.variables[varName].type = e.target.value;
                    markAsDirty();
                });
            });
            
            // Add event listeners for default value changes
            document.querySelectorAll('.var-default').forEach(input => {
                input.addEventListener('input', (e) => {
                    const varName = e.target.getAttribute('data-var');
                    pipelineData.variables[varName].defaultValue = e.target.value;
                    markAsDirty();
                });
            });
            
            // Add event listeners for key name changes (rename)
            document.querySelectorAll('.var-key').forEach(input => {
                input.addEventListener('blur', (e) => {
                    const originalName = e.target.getAttribute('data-original-name');
                    const newName = e.target.value.trim();
                    
                    if (newName && newName !== originalName) {
                        // Check if it's a temp key or actual rename
                        const isTempKey = originalName.startsWith('_temp_var_');
                        
                        if (pipelineData.variables[newName] && !isTempKey) {
                            alert('A variable with this name already exists!');
                            e.target.value = pipelineData.variables[originalName]._displayName || originalName;
                        } else {
                            // Create entry with new name
                            const varData = { ...pipelineData.variables[originalName] };
                            delete varData._isNew;
                            delete varData._displayName;
                            
                            pipelineData.variables[newName] = varData;
                            delete pipelineData.variables[originalName];
                            markAsDirty();
                            renderPipelineVariables();
                        }
                    } else if (!newName && originalName.startsWith('_temp_var_')) {
                        // Update display name for temp keys
                        pipelineData.variables[originalName]._displayName = '';
                    } else if (!newName) {
                        e.target.value = pipelineData.variables[originalName]._displayName || originalName;
                    }
                });
            });
        }
        
        // Add parameter button handler
        const addParameterBtn = document.getElementById('addParameterBtn');
        if (addParameterBtn) {
            addParameterBtn.addEventListener('click', () => {
                // Generate temporary unique internal key
                const tempKey = '_temp_param_' + Date.now();
                
                pipelineData.parameters[tempKey] = {
                    type: 'string',
                    defaultValue: '',
                    _isNew: true,
                    _displayName: ''
                };
                renderPipelineParameters();
                markAsDirty();
            });
        }
        
        // Add variable button handler
        const addVariableBtn = document.getElementById('addVariableBtn');
        if (addVariableBtn) {
            addVariableBtn.addEventListener('click', () => {
                // Generate temporary unique internal key
                const tempKey = '_temp_var_' + Date.now();
                
                pipelineData.variables[tempKey] = {
                    type: 'String',
                    defaultValue: '',
                    _isNew: true,
                    _displayName: ''
                };
                renderPipelineVariables();
                markAsDirty();
            });
        }
        
        // Concurrency input handler
        const concurrencyInput = document.getElementById('concurrencyInput');
        if (concurrencyInput) {
            concurrencyInput.value = pipelineData.concurrency || 1;
            concurrencyInput.addEventListener('input', (e) => {
                const value = parseInt(e.target.value) || 1;
                pipelineData.concurrency = Math.max(1, value);
                markAsDirty();
            });
        }
        
        // Initialize pipeline properties display
        renderPipelineParameters();
        renderPipelineVariables();

        // Debug hook — accessible from VS Code DevTools webview console
        window._debug = {
            buildPipelineDataForSave,
            buildNestedActivityTypeProperties,
            getActivities: () => activities,
            getConnections: () => connections,
            setActivities: (a) => { activities = a; },
            setConnections: (c) => { connections = c; },
            runTest: (testActivities) => {
                const prev = activities, prevC = connections;
                activities = testActivities; connections = [];
                const result = buildPipelineDataForSave(getPipelineNameFromPath(), false);
                activities = prev; connections = prevC;
                return result;
            }
        };
        console.log('[_debug] window._debug exposed — call window._debug.runTest([...]) to generate JSON without UI interaction');

        // Initial draw
        draw();
    </script>
</body>
</html>`;
	}
}

module.exports = {
	PipelineEditorProvider
};