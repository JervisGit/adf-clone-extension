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
					<option value="CustomEventsTrigger">Custom events</option>
				</select>
			</div>

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

		// Initialize
		document.addEventListener('DOMContentLoaded', () => {
			initializeEventListeners();
			setDefaultStartDate();
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
			
			if (triggerType === 'ScheduleTrigger' && (frequency === 'Day' || frequency === 'Week')) {
				section.style.display = 'block';
				
				// Show weekDays only for Week frequency
				if (frequency === 'Week') {
					weekDaysGroup.style.display = 'block';
				} else {
					weekDaysGroup.style.display = 'none';
				}
			} else {
				section.style.display = 'none';
			}
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
				updateScheduleExecutionTimes();
				
				// Expand advanced recurrence section if schedule data exists
				document.getElementById('advancedRecurrenceContent').style.display = 'block';
				document.getElementById('advancedRecurrenceArrow').textContent = '▼';
			}

			// Update visibility of advanced recurrence section
			updateAdvancedRecurrenceVisibility();
		}

		function saveTrigger() {
			// Collect form data
			const name = document.getElementById('triggerName').value.trim();
			if (!name) {
				vscode.postMessage({
					command: 'showError',
					message: 'Please enter a trigger name'
				});
				return;
			}

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

			// Build trigger object
			const triggerData = {
				name: name,
				properties: {
					description: document.getElementById('triggerDescription').value.trim(),
					annotations: [],
					runtimeState: document.querySelector('input[name="status"]:checked').value,
					pipelines: [],
					type: document.getElementById('triggerType').value,
					typeProperties: {
						recurrence: {
							frequency: document.getElementById('recurrenceFrequency').value,
							interval: parseInt(document.getElementById('recurrenceInterval').value) || 1,
							startTime: startDate.replace('T', 'T') + ':00', // Format: YYYY-MM-DDTHH:mm:ss
							timeZone: "Singapore Standard Time"
						}
					}
				}
			};

			// Add end time if specified
			if (document.getElementById('specifyEndDate').checked) {
				const endDate = document.getElementById('endDate').value;
				if (endDate) {
					triggerData.properties.typeProperties.recurrence.endTime = endDate.replace('T', 'T') + ':00';
				}
			}

			// Add schedule if trigger is Schedule type and frequency is Day or Week
			const triggerType = document.getElementById('triggerType').value;
			if (triggerType === 'ScheduleTrigger' && (frequency === 'Day' || frequency === 'Week')) {
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
				
				if (hours.length > 0 || minutes.length > 0 || selectedWeekDays.length > 0) {
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
