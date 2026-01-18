const vscode = require('vscode');
const activitiesConfig = require('./activities-config-verified.json');

class PipelineEditorProvider {
	static currentPanel;

	constructor(context) {
		this.context = context;
	}

	createOrShow() {
		const column = vscode.window.activeTextEditor
			? vscode.window.activeTextEditor.viewColumn
			: undefined;

		// If we already have a panel, show it
		if (PipelineEditorProvider.currentPanel) {
			PipelineEditorProvider.currentPanel.reveal(column);
			return;
		}

		// Otherwise, create a new panel
		const panel = vscode.window.createWebviewPanel(
			'adfPipelineEditor',
			'Pipeline Editor',
			column || vscode.ViewColumn.One,
			{
				enableScripts: true,
				retainContextWhenHidden: true,
				localResourceRoots: [
					vscode.Uri.joinPath(this.context.extensionUri, 'media')
				]
			}
		);

		PipelineEditorProvider.currentPanel = panel;

		// Set the webview's initial html content
		panel.webview.html = this.getHtmlContent(panel.webview);

		// Handle messages from the webview
		panel.webview.onDidReceiveMessage(
			message => {
				switch (message.type) {
					case 'alert':
						vscode.window.showInformationMessage(message.text);
						break;
					case 'save':
						this.savePipeline(message.data);
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
			() => {
				PipelineEditorProvider.currentPanel = undefined;
			},
			null,
			this.context.subscriptions
		);
	}

	addActivity(activityType) {
		if (PipelineEditorProvider.currentPanel) {
			PipelineEditorProvider.currentPanel.webview.postMessage({
				type: 'addActivity',
				activityType: activityType
			});
		} else {
			vscode.window.showWarningMessage('Please open the pipeline editor first');
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
            font-size: 10px;
            transition: transform 0.2s ease;
            display: inline-block;
        }

        .activity-group.collapsed .category-arrow {
            transform: rotate(-90deg);
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
            background: #f3f2f1 !important;
            border-top: 2px solid #0078d4 !important;
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
            background: #e1dfdd !important;
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
        }

        .property-label {
            font-size: 12px;
            font-weight: 600;
            margin-bottom: 6px;
            color: var(--vscode-descriptionForeground);
        }

        .property-input {
            width: 100%;
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

        .empty-state {
            padding: 24px;
            text-align: center;
            color: var(--vscode-descriptionForeground);
            font-size: 13px;
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
                <button class="toolbar-button" id="saveBtn">ðŸ’¾ Save</button>
                <button class="toolbar-button" id="clearBtn">ðŸ—‘ï¸ Clear</button>
                <button class="toolbar-button" id="zoomInBtn">ðŸ”+ Zoom In</button>
                <button class="toolbar-button" id="zoomOutBtn">ðŸ”- Zoom Out</button>
                <button class="toolbar-button" id="fitBtn">â¬œ Fit to Screen</button>
                <div class="toolbar-spacer"></div>
                <button class="expand-properties-btn" id="expandPropertiesBtn" onclick="toggleProperties()">« Properties</button>
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
        <div class="config-tabs" style="display: flex; background: var(--vscode-editorGroupHeader-tabsBackground); padding: 0 16px; height: 40px; align-items: center; gap: 4px; border-bottom: 1px solid var(--vscode-panel-border);">
            <button class="config-tab active" data-tab="general" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-activeForeground); border-bottom: 2px solid var(--vscode-focusBorder);">General</button>
            <button class="config-tab" data-tab="settings" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Settings</button>
            <button class="config-tab" data-tab="user-properties" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">User Properties</button>
            <button class="config-tab" data-tab="variables" style="padding: 8px 16px; border: none; background: transparent; cursor: pointer; color: var(--vscode-tab-inactiveForeground);">Variables</button>
            <button class="config-collapse-btn" id="configCollapseBtn" onclick="toggleConfig()" title="Collapse Configuration Panel">▼</button>
        </div>
        <div class="config-content" id="configContent" style="flex: 1; overflow-y: auto; padding: 16px; background: var(--vscode-editor-background);">
            <div class="config-tab-pane active" id="tab-general">
                <div id="generalContent" class="empty-state">Select an activity to configure</div>
            </div>
            <div class="config-tab-pane" id="tab-settings">
                <div class="empty-state">Activity-specific settings will appear here</div>
            </div>
            <div class="config-tab-pane" id="tab-user-properties">
                <div class="empty-state">User properties will appear here</div>
            </div>
            <div class="config-tab-pane" id="tab-variables">
                <div class="empty-state">Variables will appear here</div>
            </div>
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
            btn.textContent = panel.classList.contains('minimized') ? '▲' : '▼';
        }
        
        // Canvas state
        let canvas = document.getElementById('canvas');
        let ctx = canvas.getContext('2d');
        console.log('Canvas:', canvas);
        console.log('Canvas context:', ctx);
        let activities = [];
        let connections = [];
        let selectedActivity = null;
        let draggedActivity = null;
        let connectionStart = null;
        let isDragging = false;
        let dragOffset = { x: 0, y: 0 };
        let scale = 1;
        let panOffset = { x: 0, y: 0 };
        let isPanning = false;
        let panStart = { x: 0, y: 0 };
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
                this.name = type;
                this.description = '';
                this.color = this.getColorForType(type);
                this.container = container;
                this.element = null;
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
                    'Wait': '#00bcf2',
                    'WebActivity': '#8661c5',
                    'StoredProcedure': '#847545'
                };
                return colors[type] || '#0078d4';
            }

            createDOMElement() {
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
                deleteBtn.innerHTML = '🗑️';
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
                copyBtn.innerHTML = '📋';
                copyBtn.title = 'Copy';
                copyBtn.onclick = (e) => e.stopPropagation();
                
                const infoBtn = document.createElement('button');
                infoBtn.className = 'action-icon-btn info';
                infoBtn.innerHTML = 'ℹ';
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
                    'Wait': 'Wait',
                    'WebActivity': 'Web Activity',
                    'StoredProcedure': 'Stored Procedure'
                };
                return labels[this.type] || this.type;
            }

            getIcon() {
                const icons = {
                    'Copy': '📋',
                    'Delete': 'ðŸ—‘ï¸',
                    'Dataflow': '🔄',
                    'Notebook': 'ðŸ““',
                    'ForEach': 'ðŸ”',
                    'IfCondition': 'â“',
                    'Wait': '⏱️',
                    'WebActivity': '🌐',
                    'StoredProcedure': 'ðŸ’¾'
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
            console.log('Activities count:', activities.length);
            draw();
        });

        // Canvas mousedown - deselect when clicking empty space
        document.getElementById('canvasWrapper').addEventListener('mousedown', (e) => {
            // Only handle if clicking directly on the wrapper (not on an activity or canvas)
            if (e.target.id === 'canvasWrapper') {
                selectedActivity = null;
                // Deselect all activities
                activities.forEach(a => a.setSelected(false));
                showProperties(null);
                draw();
            }
        });

        document.addEventListener('mousemove', (e) => {
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
                <button class="toolbar-button" data-condition="Succeeded" style="width: 100%; margin: 2px 0; background: #00a86b; color: white;">âœ“ Succeeded</button>
                <button class="toolbar-button" data-condition="Failed" style="width: 100%; margin: 2px 0; background: #d13438; color: white;">âœ— Failed</button>
                <button class="toolbar-button" data-condition="Completed" style="width: 100%; margin: 2px 0; background: #0078d4; color: white;">âŠ™ Completed</button>
                <button class="toolbar-button" data-condition="Skipped" style="width: 100%; margin: 2px 0; background: #ffa500; color: white;">âŠ˜ Skipped</button>
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
                    document.body.removeChild(dialog);
                    draw();
                });
            });
            
            // Close on click outside
            setTimeout(() => {
                const closeHandler = (e) => {
                    if (!dialog.contains(e.target)) {
                        document.body.removeChild(dialog);
                        document.removeEventListener('click', closeHandler);
                    }
                };
                document.addEventListener('click', closeHandler);
            }, 100);
        }

        // Configuration panel
        function showProperties(activity) {
            const rightPanel = document.getElementById('propertiesContent');
            const bottomPanel = document.getElementById('generalContent');
            
            if (!activity) {
                rightPanel.innerHTML = '<div class="empty-state">Select an activity to view its properties</div>';
                bottomPanel.innerHTML = '<div class="empty-state">Select an activity to configure</div>';
                return;
            }

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
                    <div style="display: flex; gap: 8px;">
                        <input type="number" class="property-input" id="propX" value="\${Math.round(activity.x)}" placeholder="X" style="flex: 1;">
                        <input type="number" class="property-input" id="propY" value="\${Math.round(activity.y)}" placeholder="Y" style="flex: 1;">
                    </div>
                </div>
            \`;

            // Bottom panel - detailed configuration
            bottomPanel.innerHTML = \`
                <div class="property-group">
                    <div class="property-label">Activity Name</div>
                    <input type="text" class="property-input" value="\${activity.name}" readonly>
                </div>
                <div class="property-group">
                    <div class="property-label">Activity Type</div>
                    <input type="text" class="property-input" value="\${activity.type}" readonly>
                </div>
                <div style="margin-top: 16px; padding: 12px; background: var(--vscode-textBlockQuote-background); border-left: 3px solid var(--vscode-textBlockQuote-border); border-radius: 4px;">
                    <strong>Activity Configuration</strong>
                    <p style="margin: 8px 0 0 0; color: var(--vscode-descriptionForeground); font-size: 12px;">
                        Activity-specific settings will be available here based on the activity type.
                    </p>
                </div>
            \`;

            document.getElementById('propName').addEventListener('input', (e) => {
                activity.updateName(e.target.value);
                draw();
            });

            document.getElementById('propDescription').addEventListener('input', (e) => {
                activity.description = e.target.value;
            });
            
            document.getElementById('propX').addEventListener('input', (e) => {
                const x = parseInt(e.target.value) || 0;
                activity.updatePosition(x, activity.y);
                draw();
            });
            
            document.getElementById('propY').addEventListener('input', (e) => {
                const y = parseInt(e.target.value) || 0;
                activity.updatePosition(activity.x, y);
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
                document.querySelectorAll('.config-tab-pane').forEach(p => p.classList.remove('active'));
                
                // Add active class and styles to clicked tab
                tab.classList.add('active');
                tab.style.color = 'var(--vscode-tab-activeForeground)';
                tab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
                const tabName = tab.getAttribute('data-tab');
                document.getElementById(\`tab-\${tabName}\`).classList.add('active');
            });
        });

        // Toolbar buttons
        document.getElementById('saveBtn').addEventListener('click', () => {
            const data = {
                activities: activities.map(a => ({
                    id: a.id,
                    type: a.type,
                    name: a.name,
                    x: a.x,
                    y: a.y
                })),
                connections: connections.map(c => ({
                    from: c.from.id,
                    to: c.to.id
                }))
            };
            vscode.postMessage({ type: 'save', data: data });
        });

        document.getElementById('clearBtn').addEventListener('click', () => {
            if (confirm('Clear all activities?')) {
                // Remove all activity DOM elements
                activities.forEach(a => a.remove());
                activities = [];
                connections = [];
                selectedActivity = null;
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
            
            if (message.type === 'addActivity') {
                const canvasWrapper = document.getElementById('canvasWrapper');
                const activity = new Activity(message.activityType, 100, 100, canvasWrapper);
                activities.push(activity);
                draw();
            }
        });

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
