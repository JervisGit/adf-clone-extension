const vscode = require('vscode');

class TriggerEditorProvider {
	static panels = new Map(); // Map<filePath, panel>
	static dirtyStates = new Map(); // Map<filePath, isDirty>

	constructor(context) {
		this.context = context;
	}

	markPanelAsDirty(filePath, isDirty) {
		const panel = TriggerEditorProvider.panels.get(filePath);
		if (panel) {
			TriggerEditorProvider.dirtyStates.set(filePath, isDirty);
		}
	}

	createOrShow(filePath = null) {
		const column = vscode.window.activeTextEditor
			? vscode.window.activeTextEditor.viewColumn
			: undefined;

		// If opening a specific file, check if panel already exists and is not disposed
		if (filePath && TriggerEditorProvider.panels.has(filePath)) {
			const panel = TriggerEditorProvider.panels.get(filePath);
			// Check if panel is still valid (not disposed)
			try {
				panel.reveal(column);
				return panel;
			} catch {
				// Panel was disposed, remove it from the map
				TriggerEditorProvider.panels.delete(filePath);
				TriggerEditorProvider.dirtyStates.delete(filePath);
			}
		}

		// Create title from filename if provided
		const path = require('path');
		const title = filePath 
			? path.basename(filePath, '.json')
			: 'Synapse Trigger Editor';

		// Create a new panel
		const panel = vscode.window.createWebviewPanel(
			'adfTriggerEditor',
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
			TriggerEditorProvider.panels.set(filePath, panel);
			TriggerEditorProvider.dirtyStates.set(filePath, false);
		}

		// Set the webview's initial html content
		panel.webview.html = this.getHtmlContent(panel.webview);

		// Send initial data after webview loads
		setImmediate(() => {
			// Load trigger data if file path provided
			if (filePath) {
				const fs = require('fs');
				try {
					const triggerContent = fs.readFileSync(filePath, 'utf8');
					const triggerData = JSON.parse(triggerContent);
					panel.webview.postMessage({
						command: 'loadTriggerData',
						data: triggerData,
						filePath: filePath
					});
				} catch (error) {
					vscode.window.showErrorMessage(`Failed to load trigger: ${error.message}`);
				}
			}

			// Load available pipelines
			this.loadAvailablePipelines(panel);
		});

		// Handle messages from the webview
		panel.webview.onDidReceiveMessage(
			async message => {
				switch (message.command) {
					case 'saveTrigger':
						if (message.filePath) {
							await this.saveTriggerToWorkspace(message.data, message.filePath);
							this.markPanelAsDirty(message.filePath, false);
						} else {
							// Prompt for file name if new trigger
							const name = message.data.name || 'NewTrigger';
							const workspaceFolders = vscode.workspace.workspaceFolders;
							if (workspaceFolders) {
								const fs = require('fs');
								const path = require('path');
								const triggerFolder = path.join(workspaceFolders[0].uri.fsPath, 'trigger');
								
								// Create trigger folder if it doesn't exist
								if (!fs.existsSync(triggerFolder)) {
									fs.mkdirSync(triggerFolder, { recursive: true });
								}
								
								const newFilePath = path.join(triggerFolder, `${name}.json`);
								await this.saveTriggerToWorkspace(message.data, newFilePath);
								
								// Update panel to track this file
								TriggerEditorProvider.panels.delete(null);
								TriggerEditorProvider.panels.set(newFilePath, panel);
								panel.title = name;
								
								panel.webview.postMessage({
									command: 'updateFilePath',
									filePath: newFilePath
								});
							}
						}
						break;
					case 'triggerModified':
						if (message.filePath) {
							this.markPanelAsDirty(message.filePath, true);
						}
						break;
					case 'showError':
						vscode.window.showErrorMessage(message.message);
						break;
				}
			},
			undefined,
			this.context.subscriptions
		);

		// Reset when the current panel is closed
		panel.onDidDispose(
			async () => {
				if (filePath) {
					const isDirty = TriggerEditorProvider.dirtyStates.get(filePath);
					if (isDirty) {
						const answer = await vscode.window.showWarningMessage(
							'You have unsaved changes. Do you want to save before closing?',
							'Save',
							'Don\'t Save'
						);
						
						if (answer === 'Save') {
							// Trigger save command - data will be sent from webview
							panel.webview.postMessage({ command: 'saveBeforeClose' });
						}
					}
					
					TriggerEditorProvider.panels.delete(filePath);
					TriggerEditorProvider.dirtyStates.delete(filePath);
				} else {
					// Remove any panel without a file path
					for (const [key, value] of TriggerEditorProvider.panels.entries()) {
						if (value === panel) {
							TriggerEditorProvider.panels.delete(key);
							TriggerEditorProvider.dirtyStates.delete(key);
							break;
						}
					}
				}
			},
			null,
			this.context.subscriptions
		);
		
		return panel;
	}

	loadTriggerFile(filePath) {
		this.createOrShow(filePath);
	}

	async loadAvailablePipelines(panel) {
		const workspaceFolders = vscode.workspace.workspaceFolders;
		if (!workspaceFolders) return;

		const fs = require('fs');
		const path = require('path');
		const pipelineFolder = path.join(workspaceFolders[0].uri.fsPath, 'pipeline');
		
		const pipelines = [];
		
		if (fs.existsSync(pipelineFolder)) {
			const files = fs.readdirSync(pipelineFolder);
			for (const file of files) {
				if (file.endsWith('.json')) {
					try {
						const filePath = path.join(pipelineFolder, file);
						const content = fs.readFileSync(filePath, 'utf8');
						const pipelineData = JSON.parse(content);
						pipelines.push({
							name: pipelineData.name || path.basename(file, '.json'),
							filePath: filePath
						});
					} catch (error) {
						console.error(`Error reading pipeline ${file}:`, error);
					}
				}
			}
		}

		panel.webview.postMessage({
			command: 'loadPipelines',
			pipelines: pipelines
		});
	}

	async saveTriggerToWorkspace(triggerData, filePath) {
		const fs = require('fs');
		const path = require('path');
		
		console.log('[Extension] saveTriggerToWorkspace called');
		console.log('[Extension] Trigger data:', JSON.stringify(triggerData, null, 2));
		console.log('[Extension] File path:', filePath);
		
		try {
			// Ensure directory exists
			const dir = path.dirname(filePath);
			if (!fs.existsSync(dir)) {
				fs.mkdirSync(dir, { recursive: true });
			}

			// Write trigger file
			fs.writeFileSync(filePath, JSON.stringify(triggerData, null, 2));
			
			vscode.window.showInformationMessage(`Trigger saved to ${path.basename(filePath)}`);
			console.log('[Extension] Trigger saved successfully');
			
		} catch (error) {
			vscode.window.showErrorMessage(`Failed to save trigger: ${error.message}`);
			console.error('[Extension] Error saving trigger:', error);
		}
	}

	getHtmlContent() {
		return `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>Trigger Editor</title>
	<style>
		* {
			box-sizing: border-box;
			margin: 0;
			padding: 0;
		}

		body {
			font-family: var(--vscode-font-family);
			color: var(--vscode-foreground);
			background-color: var(--vscode-editor-background);
			padding: 20px;
			overflow-y: auto;
		}

		.editor-container {
			max-width: 900px;
			margin: 0 auto;
			background-color: var(--vscode-editor-background);
		}

		.form-section {
			background-color: var(--vscode-editor-background);
			border: 1px solid var(--vscode-panel-border);
			border-radius: 4px;
			padding: 20px;
			margin-bottom: 20px;
		}

		.form-group {
			margin-bottom: 16px;
		}

		.form-group label {
			display: block;
			margin-bottom: 6px;
			font-weight: 500;
			font-size: 13px;
		}

		.form-group label .required {
			color: var(--vscode-errorForeground);
			margin-left: 2px;
		}

		.form-group input[type="text"],
		.form-group input[type="number"],
		.form-group input[type="datetime-local"],
		.form-group select,
		.form-group textarea {
			width: 100%;
			padding: 6px 8px;
			background-color: var(--vscode-input-background);
			color: var(--vscode-input-foreground);
			border: 1px solid var(--vscode-input-border);
			border-radius: 2px;
			font-size: 13px;
			font-family: var(--vscode-font-family);
		}

		.form-group input[readonly] {
			background-color: var(--vscode-input-background);
			opacity: 0.6;
			cursor: not-allowed;
		}

		.form-group textarea {
			resize: vertical;
			min-height: 80px;
		}

		.form-group input:focus,
		.form-group select:focus,
		.form-group textarea:focus {
			outline: 1px solid var(--vscode-focusBorder);
			border-color: var(--vscode-focusBorder);
		}

		.form-row {
			display: flex;
			gap: 12px;
		}

		.form-row .form-group {
			flex: 1;
		}

		.radio-group {
			display: flex;
			gap: 20px;
			padding: 8px 0;
		}

		.radio-group label {
			display: flex;
			align-items: center;
			gap: 6px;
			font-weight: normal;
			margin: 0;
		}

		.radio-group input[type="radio"] {
			margin: 0;
		}

		.button-group {
			display: flex;
			gap: 8px;
			margin-top: 20px;
		}

		button {
			padding: 6px 14px;
			background-color: var(--vscode-button-background);
			color: var(--vscode-button-foreground);
			border: none;
			border-radius: 2px;
			cursor: pointer;
			font-size: 13px;
		}

		button:hover {
			background-color: var(--vscode-button-hoverBackground);
		}

		button.secondary {
			background-color: var(--vscode-button-secondaryBackground);
			color: var(--vscode-button-secondaryForeground);
		}

		button.secondary:hover {
			background-color: var(--vscode-button-secondaryHoverBackground);
		}

		.annotations-table {
			width: 100%;
			border-collapse: collapse;
			margin-top: 8px;
		}

		.annotations-table th {
			background-color: var(--vscode-editor-background);
			border: 1px solid var(--vscode-panel-border);
			padding: 8px;
			text-align: left;
			font-weight: 500;
			font-size: 13px;
		}

		.annotations-table td {
			border: 1px solid var(--vscode-panel-border);
			padding: 6px 8px;
		}

		.annotations-table td input {
			width: 100%;
			padding: 4px 6px;
			background-color: var(--vscode-input-background);
			color: var(--vscode-input-foreground);
			border: 1px solid var(--vscode-input-border);
			border-radius: 2px;
			font-size: 13px;
		}

		.annotations-table .action-cell {
			width: 60px;
			text-align: center;
		}

		.delete-btn {
			background-color: transparent;
			color: var(--vscode-errorForeground);
			padding: 2px 8px;
			font-size: 12px;
		}

		.delete-btn:hover {
			background-color: var(--vscode-errorBackground);
		}

		.annotations-table select {
			width: 100%;
			padding: 4px 6px;
			background-color: var(--vscode-input-background);
			color: var(--vscode-input-foreground);
			border: 1px solid var(--vscode-input-border);
			border-radius: 2px;
			font-size: 13px;
		}

		.section-title {
			font-size: 14px;
			font-weight: 600;
			margin-bottom: 12px;
			color: var(--vscode-foreground);
		}

		.info-icon {
			display: inline-block;
			width: 14px;
			height: 14px;
			line-height: 14px;
			text-align: center;
			border-radius: 50%;
			background-color: var(--vscode-badge-background);
			color: var(--vscode-badge-foreground);
			font-size: 10px;
			margin-left: 4px;
			cursor: help;
		}

		.checkbox-group {
			display: flex;
			align-items: center;
			gap: 8px;
			padding: 8px 0;
		}

		.checkbox-group input[type="checkbox"] {
			margin: 0;
		}

		.checkbox-group label {
			margin: 0;
			font-weight: normal;
		}

		.month-days-grid {
			display: grid;
			grid-template-columns: repeat(7, 1fr);
			gap: 8px;
			margin-top: 8px;
		}

		.month-day-cell {
			position: relative;
		}

		.month-day-cell input[type="checkbox"] {
			position: absolute;
			opacity: 0;
			cursor: pointer;
		}

		.month-day-label {
			display: flex;
			align-items: center;
			justify-content: center;
			height: 36px;
			border: 1px solid var(--vscode-input-border);
			background-color: var(--vscode-input-background);
			color: var(--vscode-input-foreground);
			cursor: pointer;
			border-radius: 2px;
			font-size: 13px;
			transition: all 0.2s;
		}

		.month-day-cell input[type="checkbox"]:checked + .month-day-label {
			background-color: var(--vscode-button-background);
			color: var(--vscode-button-foreground);
			border-color: var(--vscode-button-background);
		}

		.month-day-label:hover {
			border-color: var(--vscode-focusBorder);
		}

		.recurrence-row {
			display: flex;
			gap: 8px;
			align-items: center;
			margin-bottom: 8px;
		}

		.recurrence-row select {
			flex: 1;
			padding: 6px 8px;
			background-color: var(--vscode-input-background);
			color: var(--vscode-input-foreground);
			border: 1px solid var(--vscode-input-border);
			border-radius: 2px;
			font-size: 13px;
		}

		.recurrence-delete-btn {
			padding: 6px 10px;
			background-color: transparent;
			color: var(--vscode-errorForeground);
			border: 1px solid var(--vscode-input-border);
			border-radius: 2px;
			cursor: pointer;
			font-size: 13px;
			flex-shrink: 0;
		}

		.recurrence-delete-btn:hover {
			background-color: var(--vscode-errorBackground);
			border-color: var(--vscode-errorForeground);
		}

		.add-recurrence-btn {
			display: inline-flex;
			align-items: center;
			gap: 4px;
			padding: 6px 12px;
			margin-top: 8px;
			background-color: transparent;
			color: var(--vscode-textLink-foreground);
			border: none;
			cursor: pointer;
			font-size: 13px;
		}

		.add-recurrence-btn:hover {
			color: var(--vscode-textLink-activeForeground);
		}
	</style>
</head>
<body>
	<div class="editor-container">
		<div class="form-section">
			<div class="form-group">
				<label for="triggerName">Name <span class="required">*</span></label>
				<input type="text" id="triggerName" placeholder="Trigger 1" required>
			</div>

			<div class="form-group">
				<label for="triggerDescription">Description</label>
				<textarea id="triggerDescription" placeholder="Enter trigger description"></textarea>
			</div>

			<div class="form-group">
				<label for="triggerType">Type <span class="required">*</span></label>
				<select id="triggerType">
					<option value="ScheduleTrigger">Schedule</option>
					<option value="TumblingWindowTrigger">Tumbling window</option>
					<option value="BlobEventsTrigger">Storage events</option>
				</select>
			</div>

			<div id="scheduleFields" style="display: block;">
				<div class="form-group">
					<label for="startDate">Start date <span class="required">*</span> <span class="info-icon" title="The date and time when the trigger starts">?</span></label>
					<input type="datetime-local" id="startDate" required>
				</div>

				<div class="form-group">
					<label for="timeZone">Time zone <span class="required">*</span></label>
					<input type="text" id="timeZone" value="Kuala Lumpur, Singapore (UTC+8)" readonly>
				</div>

				<div class="form-group">
					<label>Recurrence <span class="required">*</span> <span class="info-icon" title="How often the trigger runs">?</span></label>
				<div class="form-row">
					<div class="form-group" style="flex: 0 0 150px;">
						<label for="recurrenceInterval">Every</label>
						<input type="number" id="recurrenceInterval" min="1" value="15" required>
					</div>
					<div class="form-group">
						<label for="recurrenceFrequency">&nbsp;</label>
						<select id="recurrenceFrequency">
							<option value="Minute">Minute(s)</option>
							<option value="Hour">Hour(s)</option>
							<option value="Day">Day(s)</option>
							<option value="Week">Week(s)</option>
							<option value="Month">Month(s)</option>
						</select>
					</div>
				</div>
			</div>

			<div class="form-group">
				<div class="checkbox-group">
					<input type="checkbox" id="specifyEndDate">
					<label for="specifyEndDate">Specify an end date</label>
				</div>
			</div>

			<div class="form-group" id="endDateGroup" style="display: none;">
				<label for="endDate">End date</label>
				<input type="datetime-local" id="endDate">
			</div>

			<div id="advancedRecurrenceSection" style="display: none; margin-top: 16px;">
				<div class="form-group">
					<div style="font-weight: 500; margin-bottom: 12px; cursor: pointer; user-select: none;" id="advancedRecurrenceToggle">
						<span id="advancedRecurrenceArrow">▶</span> Advanced recurrence options
					</div>
					<div id="advancedRecurrenceContent" style="display: none; margin-left: 16px;">
						<div class="form-group" id="weekDaysGroup" style="display: none;">
							<label>Run on these days</label>
							<div style="display: flex; gap: 12px; padding: 8px 0; flex-wrap: wrap;">
								<label style="display: flex; align-items: center; gap: 4px; margin: 0; font-weight: normal;">
									<input type="checkbox" class="weekday-checkbox" value="Sunday"> Sun
								</label>
								<label style="display: flex; align-items: center; gap: 4px; margin: 0; font-weight: normal;">
									<input type="checkbox" class="weekday-checkbox" value="Monday"> Mon
								</label>
								<label style="display: flex; align-items: center; gap: 4px; margin: 0; font-weight: normal;">
									<input type="checkbox" class="weekday-checkbox" value="Tuesday"> Tue
								</label>
								<label style="display: flex; align-items: center; gap: 4px; margin: 0; font-weight: normal;">
									<input type="checkbox" class="weekday-checkbox" value="Wednesday"> Wed
								</label>
								<label style="display: flex; align-items: center; gap: 4px; margin: 0; font-weight: normal;">
									<input type="checkbox" class="weekday-checkbox" value="Thursday"> Thu
								</label>
								<label style="display: flex; align-items: center; gap: 4px; margin: 0; font-weight: normal;">
									<input type="checkbox" class="weekday-checkbox" value="Friday"> Fri
								</label>
								<label style="display: flex; align-items: center; gap: 4px; margin: 0; font-weight: normal;">
									<input type="checkbox" class="weekday-checkbox" value="Saturday"> Sat
								</label>
							</div>
						</div>
						<div class="form-group" id="monthScheduleGroup" style="display: none;">
							<div class="radio-group" style="padding: 0 0 12px 0;">
								<label>
									<input type="radio" name="monthScheduleType" value="monthDays" checked> Month days
								</label>
								<label>
									<input type="radio" name="monthScheduleType" value="weekDays"> Week days
								</label>
							</div>
							<div id="monthDaysSelection">
								<label>Select day(s) of the month to execute</label>
								<div class="month-days-grid" id="monthDaysGrid">
									<!-- Days 1-31 will be generated here -->
								</div>
							</div>
							<div id="monthWeekDaysSelection" style="display: none;">
								<label>Recur every</label>
								<div id="monthlyOccurrencesContainer">
									<!-- Recurrence rows will be added here -->
								</div>
								<button class="add-recurrence-btn" id="addMonthlyOccurrenceBtn">
									<span>+</span> Add new recurrence
								</button>
							</div>
						</div>
						<div class="form-group">
							<label>Execute at these times <span class="info-icon" title="Hours must be in the range 0-23 and minutes in the range 0-59. The time specified follows the timezone setting above.">?</span></label>
							<div class="form-row">
								<div class="form-group">
									<label for="scheduleHours">Hours</label>
									<input type="text" id="scheduleHours" placeholder="0,6,12,18" title="Enter hours (0-23) separated by commas">
								</div>
								<div class="form-group">
									<label for="scheduleMinutes">Minutes</label>
									<input type="text" id="scheduleMinutes" placeholder="0,15,30,45" title="Enter minutes (0-59) separated by commas">
								</div>
							</div>
							<div style="font-size: 11px; color: var(--vscode-descriptionForeground); margin-top: 4px;">
								<div id="scheduleExecutionTimes" style="margin-top: 4px;"></div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
		</div>

		<div id="blobEventsFields" style="display: none;">
			<div class="form-group">
				<label for="azureSubscription">Azure subscription <span class="required">*</span></label>
				<input type="text" id="azureSubscription" placeholder="Enter subscription ID">
			</div>

			<div class="form-group">
				<label for="resourceGroup">Resource group <span class="required">*</span></label>
				<input type="text" id="resourceGroup" placeholder="Enter resource group name">
			</div>

			<div class="form-group">
				<label for="storageAccountName">Storage account name <span class="required">*</span></label>
				<input type="text" id="storageAccountName" placeholder="Enter storage account name">
			</div>

			<div class="form-group">
				<label for="containerName">Container name <span class="required">*</span></label>
				<input type="text" id="containerName" placeholder="Enter container name">
			</div>

			<div class="form-group">
				<label for="blobPathBeginsWith">Blob path begins with</label>
				<input type="text" id="blobPathBeginsWith" placeholder="Enter blob path prefix">
			</div>

			<div class="form-group">
				<label for="blobPathEndsWith">Blob path ends with</label>
				<input type="text" id="blobPathEndsWith" placeholder="Enter blob path suffix">
			</div>

			<div class="form-group">
				<label>Event <span class="required">*</span></label>
				<div class="checkbox-group">
					<input type="checkbox" id="eventBlobCreated" value="Microsoft.Storage.BlobCreated">
					<label for="eventBlobCreated">Blob created</label>
				</div>
				<div class="checkbox-group">
					<input type="checkbox" id="eventBlobDeleted" value="Microsoft.Storage.BlobDeleted">
					<label for="eventBlobDeleted">Blob deleted</label>
				</div>
			</div>

			<div class="form-group">
				<label>Ignore empty blobs <span class="required">*</span></label>
				<div class="radio-group">
					<label>
						<input type="radio" name="ignoreEmptyBlobs" value="true" checked> Yes
					</label>
					<label>
						<input type="radio" name="ignoreEmptyBlobs" value="false"> No
					</label>
				</div>
			</div>
		</div>

		<div id="tumblingWindowFields" style="display: none;">
			<div class="form-group">
				<label for="tumblingStartDate">Start date (UTC) <span class="required">*</span></label>
				<input type="datetime-local" id="tumblingStartDate" required>
			</div>

			<div class="form-group">
				<label>Recurrence <span class="required">*</span></label>
				<div class="form-row">
					<div class="form-group" style="flex: 0 0 150px;">
						<label for="tumblingInterval">Every</label>
						<input type="number" id="tumblingInterval" min="1" value="15" required>
					</div>
					<div class="form-group">
						<label for="tumblingFrequency">&nbsp;</label>
						<select id="tumblingFrequency">
							<option value="Minute">Minute(s)</option>
							<option value="Hour">Hour(s)</option>
							<option value="Day">Day(s)</option>
							<option value="Week">Week(s)</option>
							<option value="Month">Month(s)</option>
						</select>
					</div>
				</div>
			</div>

			<div class="form-group">
				<div class="checkbox-group">
					<input type="checkbox" id="tumblingSpecifyEndDate">
					<label for="tumblingSpecifyEndDate">Specify an end date</label>
				</div>
			</div>

			<div class="form-group" id="tumblingEndDateGroup" style="display: none;">
				<label for="tumblingEndDate">End date (UTC)</label>
				<input type="datetime-local" id="tumblingEndDate">
			</div>

			<div class="form-group">
				<div style="font-weight: 500; margin-bottom: 12px; cursor: pointer; user-select: none;" id="advancedToggle">
					<span id="advancedArrow">▶</span> Advanced
				</div>
				<div id="advancedContent" style="display: none; margin-left: 16px;">
					<div class="form-group">
						<label for="delay">Delay</label>
						<input type="text" id="delay" value="00:00:00" placeholder="00:00:00">
					</div>

					<div class="form-group">
						<label for="maxConcurrency">Max concurrency <span class="required">*</span></label>
						<input type="number" id="maxConcurrency" min="1" value="50" required>
					</div>

					<div class="form-group">
						<label for="retryCount">Retry policy: count</label>
						<input type="number" id="retryCount" min="0" value="0">
					</div>

					<div class="form-group">
						<label for="retryIntervalInSeconds">Retry policy: interval in seconds</label>
						<input type="number" id="retryIntervalInSeconds" min="1" value="30">
					</div>

					<div class="form-group">
						<label>Add dependencies</label>
						<button class="secondary" id="addDependencyBtn" style="margin-bottom: 8px;">+ New</button>
						<button class="secondary" id="deleteDependencyBtn" style="margin-bottom: 8px; margin-left: 8px;">🗑 Delete</button>
						<table class="annotations-table" id="dependenciesTable">
							<thead>
								<tr>
									<th style="width: 30px;"></th>
									<th>Trigger</th>
									<th>Offset</th>
									<th>Window size</th>
								</tr>
							</thead>
							<tbody id="dependenciesBody">
								<!-- Dependencies will be added here -->
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>

		<div class="form-section">
			<div class="section-title">Annotations</div>
			<table class="annotations-table" id="annotationsTable">
				<thead>
					<tr>
						<th>Name</th>
						<th class="action-cell">Action</th>
					</tr>
				</thead>
				<tbody id="annotationsBody">
					<!-- Annotations will be added here -->
				</tbody>
			</table>
			<button class="secondary" id="addAnnotationBtn" style="margin-top: 8px;">+ New</button>
		</div>

		<div class="form-section">
			<div class="section-title">Pipelines to Trigger</div>
			<table class="annotations-table" id="pipelinesTable">
				<thead>
					<tr>
						<th>Pipeline</th>
						<th class="action-cell">Action</th>
					</tr>
				</thead>
				<tbody id="pipelinesBody">
					<!-- Pipeline dropdowns will be added here -->
				</tbody>
			</table>
			<button class="secondary" id="addPipelineBtn" style="margin-top: 8px;">+ Add Pipeline</button>
		</div>

		<div class="form-section">
			<div class="form-group">
				<label>Status <span class="info-icon" title="Whether the trigger is currently active">?</span></label>
				<div class="radio-group">
					<label>
						<input type="radio" name="status" value="Started"> Started
					</label>
					<label>
						<input type="radio" name="status" value="Stopped" checked> Stopped
					</label>
				</div>
			</div>
		</div>

		<div class="button-group">
			<button id="saveBtn">Commit</button>
			<button class="secondary" id="cancelBtn">Cancel</button>
		</div>
	</div>

	<script>
		const vscode = acquireVsCodeApi();
		let currentFilePath = null;
		let availablePipelines = [];
		let annotationIdCounter = 0;
		let monthlyOccurrenceIdCounter = 0;

		// Initialize
		document.addEventListener('DOMContentLoaded', () => {
			initializeEventListeners();
			setDefaultStartDate();
			setDefaultTumblingStartDate();
			initializeMonthDaysGrid();
			// Add initial monthly occurrence row
			addMonthlyOccurrenceRow();
		});

		function initializeEventListeners() {
			// Save button
			document.getElementById('saveBtn').addEventListener('click', saveTrigger);

			// Cancel button
			document.getElementById('cancelBtn').addEventListener('click', () => {
				// Could ask for confirmation if there are unsaved changes
			});

			// Add annotation button
		document.getElementById('addAnnotationBtn').addEventListener('click', (e) => {
			e.preventDefault();
			addAnnotationRow('');
		});

			// Add pipeline button
			document.getElementById('addPipelineBtn').addEventListener('click', (e) => {
				e.preventDefault();
				addPipelineRow();
			});

			// Specify end date checkbox
			document.getElementById('specifyEndDate').addEventListener('change', (e) => {
				document.getElementById('endDateGroup').style.display = e.target.checked ? 'block' : 'none';
			});

			// Trigger type and frequency change
			document.getElementById('triggerType').addEventListener('change', updateAdvancedRecurrenceVisibility);
			document.getElementById('recurrenceFrequency').addEventListener('change', updateAdvancedRecurrenceVisibility);

			// Advanced recurrence toggle
			document.getElementById('advancedRecurrenceToggle').addEventListener('click', () => {
				const content = document.getElementById('advancedRecurrenceContent');
				const arrow = document.getElementById('advancedRecurrenceArrow');
				if (content.style.display === 'none') {
					content.style.display = 'block';
					arrow.textContent = '▼';
				} else {
					content.style.display = 'none';
					arrow.textContent = '▶';
				}
			});

			// Schedule hours and minutes change
			document.getElementById('scheduleHours').addEventListener('input', updateScheduleExecutionTimes);
			document.getElementById('scheduleMinutes').addEventListener('input', updateScheduleExecutionTimes);

			// Weekday checkboxes change
			document.querySelectorAll('.weekday-checkbox').forEach(checkbox => {
				checkbox.addEventListener('change', () => {
					vscode.postMessage({
						command: 'triggerModified',
						filePath: currentFilePath
					});
				});
			});

			// Month schedule type radio buttons
			document.querySelectorAll('input[name="monthScheduleType"]').forEach(radio => {
				radio.addEventListener('change', (e) => {
					if (e.target.value === 'monthDays') {
						document.getElementById('monthDaysSelection').style.display = 'block';
						document.getElementById('monthWeekDaysSelection').style.display = 'none';
					} else {
						document.getElementById('monthDaysSelection').style.display = 'none';
						document.getElementById('monthWeekDaysSelection').style.display = 'block';
					}
					vscode.postMessage({
						command: 'triggerModified',
						filePath: currentFilePath
					});
				});
			});

			// Add monthly occurrence button
			document.getElementById('addMonthlyOccurrenceBtn').addEventListener('click', (e) => {
				e.preventDefault();
				addMonthlyOccurrenceRow();
			});

			// Tumbling window specify end date checkbox
			document.getElementById('tumblingSpecifyEndDate').addEventListener('change', (e) => {
				document.getElementById('tumblingEndDateGroup').style.display = e.target.checked ? 'block' : 'none';
			});

			// Tumbling window advanced toggle
			document.getElementById('advancedToggle').addEventListener('click', () => {
				const content = document.getElementById('advancedContent');
				const arrow = document.getElementById('advancedArrow');
				if (content.style.display === 'none') {
					content.style.display = 'block';
					arrow.textContent = '▼';
				} else {
					content.style.display = 'none';
					arrow.textContent = '▶';
				}
			});

			// Add/delete dependency buttons
			document.getElementById('addDependencyBtn').addEventListener('click', (e) => {
				e.preventDefault();
				addDependencyRow();
			});

			document.getElementById('deleteDependencyBtn').addEventListener('click', (e) => {
				e.preventDefault();
				deleteSelectedDependencies();
			});

			// Track changes to mark as dirty
			const inputs = document.querySelectorAll('input, select, textarea');
			inputs.forEach(input => {
				input.addEventListener('change', () => {
					vscode.postMessage({
						command: 'triggerModified',
						filePath: currentFilePath
					});
				});
			});
		}

		function updateAdvancedRecurrenceVisibility() {
			const triggerType = document.getElementById('triggerType').value;
			const frequency = document.getElementById('recurrenceFrequency').value;
			const section = document.getElementById('advancedRecurrenceSection');
			const weekDaysGroup = document.getElementById('weekDaysGroup');
			const monthScheduleGroup = document.getElementById('monthScheduleGroup');
			const scheduleFields = document.getElementById('scheduleFields');
			const blobEventsFields = document.getElementById('blobEventsFields');
		const tumblingWindowFields = document.getElementById('tumblingWindowFields');
		
		// Show/hide fields based on trigger type
		if (triggerType === 'BlobEventsTrigger') {
			scheduleFields.style.display = 'none';
			blobEventsFields.style.display = 'block';
			tumblingWindowFields.style.display = 'none';
			section.style.display = 'none';
		} else if (triggerType === 'TumblingWindowTrigger') {
			scheduleFields.style.display = 'none';
			blobEventsFields.style.display = 'none';
			tumblingWindowFields.style.display = 'block';
			section.style.display = 'none';
		} else {
			scheduleFields.style.display = 'block';
			blobEventsFields.style.display = 'none';
			tumblingWindowFields.style.display = 'none';
		
		// Handle advanced recurrence for Schedule trigger
		if (triggerType === 'ScheduleTrigger' && (frequency === 'Day' || frequency === 'Week' || frequency === 'Month')) {
			section.style.display = 'block';
			
			// Show appropriate group based on frequency
			if (frequency === 'Week') {
				weekDaysGroup.style.display = 'block';
				monthScheduleGroup.style.display = 'none';
			} else if (frequency === 'Month') {
				weekDaysGroup.style.display = 'none';
				monthScheduleGroup.style.display = 'block';
			} else {
				weekDaysGroup.style.display = 'none';
				monthScheduleGroup.style.display = 'none';
			}
		} else {
			section.style.display = 'none';
		}
	}
}

		function initializeMonthDaysGrid() {
			const grid = document.getElementById('monthDaysGrid');
			
			// Create checkboxes for days 1-31
			for (let day = 1; day <= 31; day++) {
				const cell = document.createElement('div');
				cell.className = 'month-day-cell';
				
				const checkbox = document.createElement('input');
				checkbox.type = 'checkbox';
				checkbox.className = 'month-day-checkbox';
				checkbox.value = day;
				checkbox.id = 'monthDay' + day;
				
				const label = document.createElement('label');
				label.className = 'month-day-label';
				label.htmlFor = 'monthDay' + day;
				label.textContent = day;
				
				cell.appendChild(checkbox);
				cell.appendChild(label);
				grid.appendChild(cell);
				
				// Add change listener
				checkbox.addEventListener('change', () => {
					vscode.postMessage({
						command: 'triggerModified',
						filePath: currentFilePath
					});
				});
			}
			
			// Add "Last" day cell
			const lastCell = document.createElement('div');
			lastCell.className = 'month-day-cell';
			
			const lastCheckbox = document.createElement('input');
			lastCheckbox.type = 'checkbox';
			lastCheckbox.className = 'month-day-checkbox';
			lastCheckbox.value = -1;  // -1 represents "Last" day
			lastCheckbox.id = 'monthDayLast';
			
			const lastLabel = document.createElement('label');
			lastLabel.className = 'month-day-label';
			lastLabel.htmlFor = 'monthDayLast';
			lastLabel.textContent = 'Last';
			
			lastCell.appendChild(lastCheckbox);
			lastCell.appendChild(lastLabel);
			grid.appendChild(lastCell);
			
			// Add change listener for Last
			lastCheckbox.addEventListener('change', () => {
				vscode.postMessage({
					command: 'triggerModified',
					filePath: currentFilePath
				});
			});
		}

		function addMonthlyOccurrenceRow(occurrence = 1, day = 'Sunday') {
			const container = document.getElementById('monthlyOccurrencesContainer');
			const row = document.createElement('div');
			row.className = 'recurrence-row';
			const id = monthlyOccurrenceIdCounter++;
			row.dataset.id = id;
			
			// Occurrence dropdown
			const occurrenceSelect = document.createElement('select');
			occurrenceSelect.className = 'occurrence-select';
			const occurrences = [
				{ value: 1, label: 'First' },
				{ value: 2, label: 'Second' },
				{ value: 3, label: 'Third' },
				{ value: 4, label: 'Fourth' },
				{ value: 5, label: 'Fifth' },
				{ value: -1, label: 'Last' }
			];
			occurrences.forEach(occ => {
				const option = document.createElement('option');
				option.value = occ.value;
				option.textContent = occ.label;
				if (occ.value === occurrence) {
					option.selected = true;
				}
				occurrenceSelect.appendChild(option);
			});
			
			// Day dropdown
			const daySelect = document.createElement('select');
			daySelect.className = 'day-select';
			const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
			days.forEach(d => {
				const option = document.createElement('option');
				option.value = d;
				option.textContent = d;
				if (d === day) {
					option.selected = true;
				}
				daySelect.appendChild(option);
			});
			
			// Delete button
			const deleteBtn = document.createElement('button');
			deleteBtn.className = 'recurrence-delete-btn';
			deleteBtn.innerHTML = '🗑️';
			deleteBtn.title = 'Delete';
			deleteBtn.addEventListener('click', (e) => {
				e.preventDefault();
				row.remove();
				vscode.postMessage({
					command: 'triggerModified',
					filePath: currentFilePath
				});
			});
			
			row.appendChild(occurrenceSelect);
			row.appendChild(daySelect);
			row.appendChild(deleteBtn);
			container.appendChild(row);
			
			// Add change listeners
			occurrenceSelect.addEventListener('change', () => {
				vscode.postMessage({
					command: 'triggerModified',
					filePath: currentFilePath
				});
			});
			
			daySelect.addEventListener('change', () => {
				vscode.postMessage({
					command: 'triggerModified',
					filePath: currentFilePath
				});
			});
		}

		function addDependencyRow(triggerName = '', offset = '', windowSize = '') {
			const tbody = document.getElementById('dependenciesBody');
			const row = document.createElement('tr');
			
			const checkboxCell = document.createElement('td');
			const checkbox = document.createElement('input');
			checkbox.type = 'checkbox';
			checkbox.className = 'dependency-checkbox';
			checkboxCell.appendChild(checkbox);
			
			const triggerCell = document.createElement('td');
			const triggerInput = document.createElement('input');
			triggerInput.type = 'text';
			triggerInput.className = 'trigger-input';
			triggerInput.placeholder = 'Trigger 5';
			triggerInput.value = triggerName;
			triggerCell.appendChild(triggerInput);
			
			const offsetCell = document.createElement('td');
			const offsetInput = document.createElement('input');
			offsetInput.type = 'text';
			offsetInput.className = 'offset-input';
			offsetInput.placeholder = '-0.00:16:00';
			offsetInput.value = offset;
			offsetCell.appendChild(offsetInput);
			
			const windowSizeCell = document.createElement('td');
			const windowSizeInput = document.createElement('input');
			windowSizeInput.type = 'text';
			windowSizeInput.className = 'window-size-input';
			windowSizeInput.placeholder = '0.00:00:00';
			windowSizeInput.value = windowSize;
			windowSizeCell.appendChild(windowSizeInput);
			
			row.appendChild(checkboxCell);
			row.appendChild(triggerCell);
			row.appendChild(offsetCell);
			row.appendChild(windowSizeCell);
			tbody.appendChild(row);
			
			// Add change listeners
			[triggerInput, offsetInput, windowSizeInput].forEach(el => {
				el.addEventListener('input', () => {
					vscode.postMessage({
						command: 'triggerModified',
						filePath: currentFilePath
					});
				});
			});
		}

		function deleteSelectedDependencies() {
			const checkboxes = document.querySelectorAll('.dependency-checkbox:checked');
			checkboxes.forEach(checkbox => {
				checkbox.closest('tr').remove();
			});
			
			vscode.postMessage({
				command: 'triggerModified',
				filePath: currentFilePath
			});
		}

		function parseNumberList(input, min, max) {
			if (!input || input.trim() === '') return [];
			
			const numbers = input.split(',')
				.map(s => s.trim())
				.filter(s => s !== '')
				.map(s => parseInt(s))
				.filter(n => !isNaN(n) && n >= min && n <= max);
			
			return [...new Set(numbers)].sort((a, b) => a - b);
		}

		function validateNumberList(input, min, max) {
			if (!input || input.trim() === '') return [];
			
			const invalidValues = [];
			const parts = input.split(',').map(s => s.trim()).filter(s => s !== '');
			
			parts.forEach(part => {
				const num = parseInt(part);
				if (isNaN(num)) {
					invalidValues.push(part);
				} else if (num < min || num > max) {
					invalidValues.push(part);
				}
			});
			
			return invalidValues;
		}

		function updateScheduleExecutionTimes() {
			const hoursInput = document.getElementById('scheduleHours').value;
			const minutesInput = document.getElementById('scheduleMinutes').value;
			const display = document.getElementById('scheduleExecutionTimes');
			
			const hours = parseNumberList(hoursInput, 0, 23);
			const minutes = parseNumberList(minutesInput, 0, 59);
			
			if (hours.length === 0 || minutes.length === 0) {
				display.textContent = '';
				return;
			}
			
			const times = [];
			hours.forEach(h => {
				minutes.forEach(m => {
					const hourStr = String(h).padStart(2, '0');
					const minStr = String(m).padStart(2, '0');
					times.push(\`\${hourStr}:\${minStr}\`);
				});
			});
			
			display.textContent = 'Schedule execution times: ' + times.join(', ');
		}

		function setDefaultStartDate() {
			const now = new Date();
			const year = now.getFullYear();
			const month = String(now.getMonth() + 1).padStart(2, '0');
			const day = String(now.getDate()).padStart(2, '0');
			const hours = String(now.getHours()).padStart(2, '0');
			const minutes = String(now.getMinutes()).padStart(2, '0');
			
			document.getElementById('startDate').value = \`\${year}-\${month}-\${day}T\${hours}:\${minutes}\`;
		}

		function setDefaultTumblingStartDate() {
			const now = new Date();
			const year = now.getFullYear();
			const month = String(now.getMonth() + 1).padStart(2, '0');
			const day = String(now.getDate()).padStart(2, '0');
			const hours = String(now.getHours()).padStart(2, '0');
			const minutes = String(now.getMinutes()).padStart(2, '0');
			
			document.getElementById('tumblingStartDate').value = \`\${year}-\${month}-\${day}T\${hours}:\${minutes}\`;
		}

		function addAnnotationRow(name = '') {
			const tbody = document.getElementById('annotationsBody');
			const row = tbody.insertRow();
			const id = annotationIdCounter++;
			
			row.innerHTML = \`
				<td><input type="text" class="annotation-name" value="\${name}" placeholder="Enter annotation name"></td>
				<td class="action-cell">
					<button class="delete-btn" onclick="deleteAnnotationRow(this)">Delete</button>
				</td>
			\`;

			// Add change listener to new input
			row.querySelector('.annotation-name').addEventListener('change', () => {
				vscode.postMessage({
					command: 'triggerModified',
					filePath: currentFilePath
				});
			});
		}

		function deleteAnnotationRow(btn) {
			btn.closest('tr').remove();
			vscode.postMessage({
				command: 'triggerModified',
				filePath: currentFilePath
			});
		}

		function addPipelineRow(selectedPipeline = '') {
			const tbody = document.getElementById('pipelinesBody');
			const row = tbody.insertRow();
			
			// Create select element with available pipelines
			const selectCell = row.insertCell(0);
			const select = document.createElement('select');
			select.className = 'pipeline-select';
			
			// Add default option
			const defaultOption = document.createElement('option');
			defaultOption.value = '';
			defaultOption.textContent = '-- Select Pipeline --';
			select.appendChild(defaultOption);
			
			// Add pipeline options
			availablePipelines.forEach(pipeline => {
				const option = document.createElement('option');
				option.value = pipeline.name;
				option.textContent = pipeline.name;
				if (pipeline.name === selectedPipeline) {
					option.selected = true;
				}
				select.appendChild(option);
			});
			
			selectCell.appendChild(select);
			
			// Add delete button
			const actionCell = row.insertCell(1);
			actionCell.className = 'action-cell';
			actionCell.innerHTML = '<button class="delete-btn" onclick="deletePipelineRow(this)">Delete</button>';
			
			// Add change listener
			select.addEventListener('change', () => {
				// Check for duplicates
				const allSelects = document.querySelectorAll('.pipeline-select');
				const selectedValues = Array.from(allSelects)
					.map(s => s.value)
					.filter(v => v !== '');
				
				const hasDuplicates = selectedValues.length !== new Set(selectedValues).size;
				
				if (hasDuplicates && select.value !== '') {
					alert('This pipeline is already selected. Please choose a different pipeline.');
					select.value = '';
					return;
				}
				
				vscode.postMessage({
					command: 'triggerModified',
					filePath: currentFilePath
				});
			});
		}

		function deletePipelineRow(btn) {
			btn.closest('tr').remove();
			vscode.postMessage({
				command: 'triggerModified',
				filePath: currentFilePath
			});
		}

		function loadPipelines(pipelines) {
			availablePipelines = pipelines;
			// Pipelines are now loaded and will be available for dropdown rows
		}

		function loadTriggerData(triggerData, filePath) {
			currentFilePath = filePath;

			// Basic fields
			document.getElementById('triggerName').value = triggerData.name || '';
			document.getElementById('triggerDescription').value = triggerData.properties.description || '';
			document.getElementById('triggerType').value = triggerData.properties.type || 'ScheduleTrigger';

			// Recurrence
			const recurrence = triggerData.properties.typeProperties?.recurrence || {};
			document.getElementById('recurrenceInterval').value = recurrence.interval || 1;
			document.getElementById('recurrenceFrequency').value = recurrence.frequency || 'Minute';
			
			// Start date/time
			if (recurrence.startTime) {
				// Parse datetime string without timezone conversion
				// Format should be "YYYY-MM-DDTHH:mm:ss" or "YYYY-MM-DDTHH:mm:ssZ"
				const startTimeStr = recurrence.startTime.replace('Z', '').slice(0, 16); // Get YYYY-MM-DDTHH:mm
				document.getElementById('startDate').value = startTimeStr;
			}

			// End date/time
			if (recurrence.endTime) {
				document.getElementById('specifyEndDate').checked = true;
				document.getElementById('endDateGroup').style.display = 'block';
				const endTimeStr = recurrence.endTime.replace('Z', '').slice(0, 16); // Get YYYY-MM-DDTHH:mm
				document.getElementById('endDate').value = endTimeStr;
			}

			// Time zone
			const timeZone = recurrence.timeZone || 'Singapore Standard Time';
			// Always show the display format in the UI
			document.getElementById('timeZone').value = 'Kuala Lumpur, Singapore (UTC+8)';

			// Status
			const status = triggerData.properties.runtimeState || 'Stopped';
			document.querySelector(\`input[name="status"][value="\${status}"]\`).checked = true;

			// Annotations
			const annotations = triggerData.properties.annotations || [];
			annotations.forEach(annotation => {
				addAnnotationRow(annotation);
			});

			// Pipelines
			const pipelines = triggerData.properties.pipelines || [];
			pipelines.forEach(pipeline => {
				const pipelineName = pipeline.pipelineReference?.referenceName;
				if (pipelineName) {
					addPipelineRow(pipelineName);
				}
			});

			// Schedule (advanced recurrence)
			const schedule = recurrence.schedule;
			if (schedule) {
				if (schedule.hours && Array.isArray(schedule.hours)) {
					document.getElementById('scheduleHours').value = schedule.hours.join(',');
				}
				if (schedule.minutes && Array.isArray(schedule.minutes)) {
					document.getElementById('scheduleMinutes').value = schedule.minutes.join(',');
				}
				if (schedule.weekDays && Array.isArray(schedule.weekDays)) {
					document.querySelectorAll('.weekday-checkbox').forEach(checkbox => {
						if (schedule.weekDays.includes(checkbox.value)) {
							checkbox.checked = true;
						}
					});
				}
				if (schedule.monthDays && Array.isArray(schedule.monthDays)) {
					document.querySelectorAll('.month-day-checkbox').forEach(checkbox => {
						const dayValue = parseInt(checkbox.value);
						if (schedule.monthDays.includes(dayValue)) {
							checkbox.checked = true;
						}
					});
				}
				if (schedule.monthlyOccurrences && Array.isArray(schedule.monthlyOccurrences)) {
					// Clear default row
					document.getElementById('monthlyOccurrencesContainer').innerHTML = '';
					// Add loaded occurrences
					schedule.monthlyOccurrences.forEach(occ => {
						addMonthlyOccurrenceRow(occ.occurrence, occ.day);
					});
					// Switch to week days mode
					document.querySelector('input[name="monthScheduleType"][value="weekDays"]').checked = true;
					document.getElementById('monthDaysSelection').style.display = 'none';
					document.getElementById('monthWeekDaysSelection').style.display = 'block';
				}
				updateScheduleExecutionTimes();
				
				// Expand advanced recurrence section if schedule data exists
				document.getElementById('advancedRecurrenceContent').style.display = 'block';
				document.getElementById('advancedRecurrenceArrow').textContent = '▼';
			}

			// BlobEventsTrigger fields
			if (triggerData.properties.type === 'BlobEventsTrigger') {
				const typeProps = triggerData.properties.typeProperties || {};
				
				// Parse scope to extract subscription, resource group, and storage account
				if (typeProps.scope) {
					const scopeParts = typeProps.scope.split('/');
					const subscriptionIndex = scopeParts.indexOf('subscriptions');
					if (subscriptionIndex >= 0 && subscriptionIndex + 1 < scopeParts.length) {
						document.getElementById('azureSubscription').value = scopeParts[subscriptionIndex + 1];
					}
					const resourceGroupIndex = scopeParts.indexOf('resourceGroups');
					if (resourceGroupIndex >= 0 && resourceGroupIndex + 1 < scopeParts.length) {
						document.getElementById('resourceGroup').value = scopeParts[resourceGroupIndex + 1];
					}
					const storageAccountIndex = scopeParts.indexOf('storageAccounts');
					if (storageAccountIndex >= 0 && storageAccountIndex + 1 < scopeParts.length) {
						document.getElementById('storageAccountName').value = scopeParts[storageAccountIndex + 1];
					}
				}
				
				// Parse container name from blobPathBeginsWith
				if (typeProps.blobPathBeginsWith) {
					const pathParts = typeProps.blobPathBeginsWith.split('/').filter(p => p);
					if (pathParts.length > 0) {
						document.getElementById('containerName').value = pathParts[0];
					}
					document.getElementById('blobPathBeginsWith').value = typeProps.blobPathBeginsWith;
				}
				
				if (typeProps.blobPathEndsWith) {
					document.getElementById('blobPathEndsWith').value = typeProps.blobPathEndsWith;
				}
				
				// Events
				if (typeProps.events && Array.isArray(typeProps.events)) {
					if (typeProps.events.includes('Microsoft.Storage.BlobCreated')) {
						document.getElementById('eventBlobCreated').checked = true;
					}
					if (typeProps.events.includes('Microsoft.Storage.BlobDeleted')) {
						document.getElementById('eventBlobDeleted').checked = true;
					}
				}
				
				// Ignore empty blobs
				if (typeProps.ignoreEmptyBlobs !== undefined) {
					const value = typeProps.ignoreEmptyBlobs.toString();
					document.querySelector(\`input[name="ignoreEmptyBlobs"][value="\${value}"]\`).checked = true;
				}
			}

		// TumblingWindowTrigger fields
		if (triggerData.properties.type === 'TumblingWindowTrigger') {
			const typeProps = triggerData.properties.typeProperties || {};
			
			// Start date/time
			if (typeProps.startTime) {
				const startTimeStr = typeProps.startTime.replace('Z', '').slice(0, 16);
				document.getElementById('tumblingStartDate').value = startTimeStr;
			}
			
			// End date/time
			if (typeProps.endTime) {
				document.getElementById('tumblingSpecifyEndDate').checked = true;
				document.getElementById('tumblingEndDateGroup').style.display = 'block';
				const endTimeStr = typeProps.endTime.replace('Z', '').slice(0, 16);
				document.getElementById('tumblingEndDate').value = endTimeStr;
			}
			
			// Frequency and interval
			if (typeProps.frequency) {
				document.getElementById('tumblingFrequency').value = typeProps.frequency;
			}
			if (typeProps.interval) {
				document.getElementById('tumblingInterval').value = typeProps.interval;
			}
			
			// Delay
			if (typeProps.delay) {
				document.getElementById('delay').value = typeProps.delay;
			}
			
			// Max concurrency
			if (typeProps.maxConcurrency !== undefined) {
				document.getElementById('maxConcurrency').value = typeProps.maxConcurrency;
			}
			
			// Retry policy
			if (typeProps.retryPolicy) {
				if (typeProps.retryPolicy.count !== undefined) {
					document.getElementById('retryCount').value = typeProps.retryPolicy.count;
				}
				if (typeProps.retryPolicy.intervalInSeconds !== undefined) {
					document.getElementById('retryIntervalInSeconds').value = typeProps.retryPolicy.intervalInSeconds;
				}
			}
			
			// Dependencies
			if (typeProps.dependsOn && Array.isArray(typeProps.dependsOn)) {
				typeProps.dependsOn.forEach(dep => {
					if (dep.type === 'TumblingWindowTriggerDependencyReference') {
						const triggerName = dep.referenceTrigger?.referenceName || '';
						const offset = dep.offset || '';
						const windowSize = dep.size || '';
						addDependencyRow(triggerName, offset, windowSize);
					}
				});
				
				// Expand advanced section if dependencies exist
				if (typeProps.dependsOn.length > 0) {
					document.getElementById('advancedContent').style.display = 'block';
					document.getElementById('advancedArrow').textContent = '▼';
				}
			}
		}

			// Update visibility based on current values
			updateAdvancedRecurrenceVisibility();
		}

		function saveTrigger() {
			const name = document.getElementById('triggerName').value.trim();

			if (!name) {
				vscode.postMessage({
					command: 'showError',
					message: 'Please enter a trigger name'
				});
				return;
			}

			const triggerType = document.getElementById('triggerType').value;

			// Build trigger object
			const triggerData = {
				name: name,
				properties: {
					description: document.getElementById('triggerDescription').value.trim(),
					annotations: [],
					runtimeState: document.querySelector('input[name="status"]:checked').value,
					pipelines: [],
					type: triggerType,
					typeProperties: {}
				}
			};

			// Handle BlobEventsTrigger
			if (triggerType === 'BlobEventsTrigger') {
				const azureSubscription = document.getElementById('azureSubscription').value.trim();
				if (!azureSubscription) {
					vscode.postMessage({
						command: 'showError',
						message: 'Please enter an Azure subscription ID'
					});
					return;
				}

				const resourceGroup = document.getElementById('resourceGroup').value.trim();
				if (!resourceGroup) {
					vscode.postMessage({
						command: 'showError',
						message: 'Please enter a resource group name'
					});
					return;
				}

				const storageAccountName = document.getElementById('storageAccountName').value.trim();
				if (!storageAccountName) {
					vscode.postMessage({
						command: 'showError',
						message: 'Please enter a storage account name'
					});
					return;
				}

				const containerName = document.getElementById('containerName').value.trim();
				if (!containerName) {
					vscode.postMessage({
						command: 'showError',
						message: 'Please enter a container name'
					});
					return;
				}

				// Collect events
				const events = [];
				if (document.getElementById('eventBlobCreated').checked) {
					events.push('Microsoft.Storage.BlobCreated');
				}
				if (document.getElementById('eventBlobDeleted').checked) {
					events.push('Microsoft.Storage.BlobDeleted');
				}
				
				if (events.length === 0) {
					vscode.postMessage({
						command: 'showError',
						message: 'Please select at least one event'
					});
					return;
				}

				// Build scope
				const scope = '/subscriptions/' + azureSubscription + '/resourceGroups/' + resourceGroup + '/providers/Microsoft.Storage/storageAccounts/' + storageAccountName;

				// Build blob path
				const blobPathBeginsWith = document.getElementById('blobPathBeginsWith').value.trim();
				const blobPathEndsWith = document.getElementById('blobPathEndsWith').value.trim();

				triggerData.properties.typeProperties = {
					blobPathBeginsWith: blobPathBeginsWith || (containerName ? '/' + containerName + '/blobs/' : ''),
					blobPathEndsWith: blobPathEndsWith,
					ignoreEmptyBlobs: document.querySelector('input[name="ignoreEmptyBlobs"]:checked').value === 'true',
					scope: scope,
					events: events
				};
			} else if (triggerType === 'TumblingWindowTrigger') {
				// Handle TumblingWindowTrigger
				const startDate = document.getElementById('tumblingStartDate').value;
				if (!startDate) {
					vscode.postMessage({
						command: 'showError',
						message: 'Please select a start date'
					});
					return;
				}

				const frequency = document.getElementById('tumblingFrequency').value;
				const interval = parseInt(document.getElementById('tumblingInterval').value);
				
				// Validate interval
				const intervalRanges = {
					'Minute': { min: 1, max: 720000 },
					'Hour': { min: 1, max: 12000 },
					'Day': { min: 1, max: 500 },
					'Week': { min: 1, max: 71 },
					'Month': { min: 1, max: 16 }
				};
				
				if (isNaN(interval) || interval < intervalRanges[frequency].min || interval > intervalRanges[frequency].max) {
					vscode.postMessage({
						command: 'showError',
						message: 'Interval must be between ' + intervalRanges[frequency].min + ' and ' + intervalRanges[frequency].max + ' for ' + frequency + ' frequency.'
					});
					return;
				}

				triggerData.properties.typeProperties = {
					frequency: frequency,
					interval: interval,
					startTime: startDate + ':00Z',
					delay: document.getElementById('delay').value.trim() || '00:00:00',
					maxConcurrency: parseInt(document.getElementById('maxConcurrency').value) || 50,
					retryPolicy: {
						intervalInSeconds: parseInt(document.getElementById('retryIntervalInSeconds').value) || 30
					},
					dependsOn: []
				};

				// Add end time if specified
				if (document.getElementById('tumblingSpecifyEndDate').checked) {
					const endDate = document.getElementById('tumblingEndDate').value;
					if (endDate) {
						triggerData.properties.typeProperties.endTime = endDate + ':00Z';
					}
				}
				
				// Add retry count if specified
				const retryCount = parseInt(document.getElementById('retryCount').value);
				if (retryCount > 0) {
					triggerData.properties.typeProperties.retryPolicy.count = retryCount;
				}
				
				// Add dependencies
				const dependencyRows = document.querySelectorAll('#dependenciesBody tr');
				dependencyRows.forEach(row => {
					const triggerInput = row.querySelector('.trigger-input');
					const offsetInput =  row.querySelector('.offset-input');
					const windowSizeInput = row.querySelector('.window-size-input');
					
					if (triggerInput && triggerInput.value.trim()) {
						const dependency = {
							type: 'TumblingWindowTriggerDependencyReference',
							offset: offsetInput.value.trim() || '0.00:00:00',
							referenceTrigger: {
								referenceName: triggerInput.value.trim(),
								type: 'TriggerReference'
							}
						};
						
						// Add size if provided
						if (windowSizeInput.value.trim()) {
							dependency.size = windowSizeInput.value.trim();
						}
						
						triggerData.properties.typeProperties.dependsOn.push(dependency);
					}
				});
			} else {
				// Handle ScheduleTrigger and other types
				const startDate = document.getElementById('startDate').value;
				if (!startDate) {
					vscode.postMessage({
						command: 'showError',
						message: 'Please select a start date'
					});
					return;
				}

				// Validate interval based on frequency
				const frequency = document.getElementById('recurrenceFrequency').value;
				const interval = parseInt(document.getElementById('recurrenceInterval').value);
				
				const intervalRanges = {
					'Minute': { min: 1, max: 720000 },
					'Hour': { min: 1, max: 12000 },
					'Day': { min: 1, max: 500 },
					'Week': { min: 1, max: 71 },
					'Month': { min: 1, max: 16 }
				};
				
				if (isNaN(interval) || interval < intervalRanges[frequency].min || interval > intervalRanges[frequency].max) {
					vscode.postMessage({
						command: 'showError',
						message: 'Interval must be between ' + intervalRanges[frequency].min + ' and ' + intervalRanges[frequency].max + ' for ' + frequency + ' frequency.'
					});
					return;
				}

				triggerData.properties.typeProperties = {
					recurrence: {
						frequency: document.getElementById('recurrenceFrequency').value,
						interval: parseInt(document.getElementById('recurrenceInterval').value) || 1,
						startTime: startDate.replace('T', 'T') + ':00', // Format: YYYY-MM-DDTHH:mm:ss
						timeZone: "Singapore Standard Time"
					}
				};

				// Add end time if specified
				if (document.getElementById('specifyEndDate').checked) {
					const endDate = document.getElementById('endDate').value;
					if (endDate) {
						triggerData.properties.typeProperties.recurrence.endTime = endDate.replace('T', 'T') + ':00';
					}
				}

				// Add schedule if trigger is Schedule type and frequency is Day, Week, or Month
				if (triggerType === 'ScheduleTrigger' && (frequency === 'Day' || frequency === 'Week' || frequency === 'Month')) {
					const hoursInput = document.getElementById('scheduleHours').value.trim();
					const minutesInput = document.getElementById('scheduleMinutes').value.trim();
				
					// Validate hours input
					if (hoursInput) {
						const invalidHours = validateNumberList(hoursInput, 0, 23);
						if (invalidHours.length > 0) {
							vscode.postMessage({
								command: 'showError',
								message: 'Invalid hour values: ' + invalidHours.join(', ') + '. Hours must be in the range 0-23.'
							});
							return;
						}
					}
				
				// Validate minutes input
				if (minutesInput) {
					const invalidMinutes = validateNumberList(minutesInput, 0, 59);
					if (invalidMinutes.length > 0) {
						vscode.postMessage({
							command: 'showError',
							message: 'Invalid minute values: ' + invalidMinutes.join(', ') + '. Minutes must be in the range 0-59.'
						});
						return;
					}
				}
				
				const hours = parseNumberList(hoursInput, 0, 23);
				const minutes = parseNumberList(minutesInput, 0, 59);
				const selectedWeekDays = Array.from(document.querySelectorAll('.weekday-checkbox:checked')).map(cb => cb.value);
				const selectedMonthDays = Array.from(document.querySelectorAll('.month-day-checkbox:checked')).map(cb => parseInt(cb.value));
				
				// Get monthly occurrences (for Week days option)
				const monthlyOccurrences = [];
				if (frequency === 'Month') {
					const monthScheduleType = document.querySelector('input[name="monthScheduleType"]:checked').value;
					if (monthScheduleType === 'weekDays') {
						const rows = document.querySelectorAll('#monthlyOccurrencesContainer .recurrence-row');
						rows.forEach(row => {
							const occurrenceSelect = row.querySelector('.occurrence-select');
							const daySelect = row.querySelector('.day-select');
							if (occurrenceSelect && daySelect) {
								monthlyOccurrences.push({
									day: daySelect.value,
									occurrence: parseInt(occurrenceSelect.value)
								});
							}
						});
					}
				}
				
				if (hours.length > 0 || minutes.length > 0 || selectedWeekDays.length > 0 || selectedMonthDays.length > 0 || monthlyOccurrences.length > 0) {
					triggerData.properties.typeProperties.recurrence.schedule = {};
					
					if (hours.length > 0) {
						triggerData.properties.typeProperties.recurrence.schedule.hours = hours;
					}
					
					if (minutes.length > 0) {
						triggerData.properties.typeProperties.recurrence.schedule.minutes = minutes;
					}
					
					if (frequency === 'Week' && selectedWeekDays.length > 0) {
						triggerData.properties.typeProperties.recurrence.schedule.weekDays = selectedWeekDays;
					}
					
					if (frequency === 'Month') {
						if (selectedMonthDays.length > 0) {
							triggerData.properties.typeProperties.recurrence.schedule.monthDays = selectedMonthDays;
						} else if (monthlyOccurrences.length > 0) {
							triggerData.properties.typeProperties.recurrence.schedule.monthlyOccurrences = monthlyOccurrences;
						}
					}
				}
				}
			}

			// Collect annotations
			const annotationInputs = document.querySelectorAll('.annotation-name');
			annotationInputs.forEach(input => {
				const value = input.value.trim();
				if (value) {
					triggerData.properties.annotations.push(value);
				}
			});

			// Collect selected pipelines
			const pipelineSelects = document.querySelectorAll('.pipeline-select');
			pipelineSelects.forEach(select => {
				const pipelineName = select.value;
				if (pipelineName) {
					triggerData.properties.pipelines.push({
						pipelineReference: {
							referenceName: pipelineName,
							type: "PipelineReference"
						}
					});
				}
			});

			// Send save message
			vscode.postMessage({
				command: 'saveTrigger',
				data: triggerData,
				filePath: currentFilePath
			});
		}

		// Handle messages from extension
		window.addEventListener('message', event => {
			const message = event.data;
			switch (message.command) {
				case 'loadTriggerData':
					loadTriggerData(message.data, message.filePath);
					break;
				case 'loadPipelines':
					loadPipelines(message.pipelines);
					break;
				case 'updateFilePath':
					currentFilePath = message.filePath;
					break;
				case 'saveBeforeClose':
					saveTrigger();
					break;
			}
		});
	</script>
</body>
</html>`;
	}
}

module.exports = {
	TriggerEditorProvider
};
