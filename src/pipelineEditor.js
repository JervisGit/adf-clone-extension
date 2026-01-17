const vscode = require('vscode');

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
            overflow-y: auto;
            flex-shrink: 0;
        }

        .sidebar-header {
            padding: 16px;
            border-bottom: 1px solid var(--vscode-panel-border);
            font-size: 14px;
            font-weight: 600;
        }

        .activity-group {
            padding: 12px;
        }

        .activity-group-title {
            font-size: 12px;
            font-weight: 600;
            margin-bottom: 8px;
            color: var(--vscode-descriptionForeground);
            text-transform: uppercase;
        }

        .activity-item {
            padding: 10px 12px;
            margin: 4px 0;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
            background: var(--vscode-button-secondaryBackground);
            border: 1px solid var(--vscode-button-border);
            transition: all 0.2s;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .activity-item:hover {
            background: var(--vscode-button-secondaryHoverBackground);
            transform: translateX(2px);
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
            display: block;
            cursor: default;
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
            flex-shrink: 0;
        }

        .properties-header {
            padding: 16px;
            border-bottom: 1px solid var(--vscode-panel-border);
            font-size: 14px;
            font-weight: 600;
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
            
            <div class="activity-group">
                <div class="activity-group-title">Data Movement</div>
                <div class="activity-item" draggable="true" data-type="Copy">
                    <div class="activity-icon">📋</div>
                    <span>Copy Data</span>
                </div>
                <div class="activity-item" draggable="true" data-type="Delete">
                    <div class="activity-icon">ðŸ—‘ï¸</div>
                    <span>Delete</span>
                </div>
            </div>

            <div class="activity-group">
                <div class="activity-group-title">Data Transformation</div>
                <div class="activity-item" draggable="true" data-type="Dataflow">
                    <div class="activity-icon">🔄</div>
                    <span>Data Flow</span>
                </div>
                <div class="activity-item" draggable="true" data-type="Notebook">
                    <div class="activity-icon">ðŸ““</div>
                    <span>Notebook</span>
                </div>
            </div>

            <div class="activity-group">
                <div class="activity-group-title">Control Flow</div>
                <div class="activity-item" draggable="true" data-type="ForEach">
                    <div class="activity-icon">ðŸ”</div>
                    <span>For Each</span>
                </div>
                <div class="activity-item" draggable="true" data-type="IfCondition">
                    <div class="activity-icon">â“</div>
                    <span>If Condition</span>
                </div>
                <div class="activity-item" draggable="true" data-type="Wait">
                    <div class="activity-icon">⏱️</div>
                    <span>Wait</span>
                </div>
            </div>

            <div class="activity-group">
                <div class="activity-group-title">External</div>
                <div class="activity-item" draggable="true" data-type="WebActivity">
                    <div class="activity-icon">🌐</div>
                    <span>Web Activity</span>
                </div>
                <div class="activity-item" draggable="true" data-type="StoredProcedure">
                    <div class="activity-icon">ðŸ’¾</div>
                    <span>Stored Procedure</span>
                </div>
            </div>
        </div>

        <!-- Canvas Area -->
        <div class="canvas-container">
            <div class="toolbar">
                <button class="toolbar-button" id="saveBtn">ðŸ’¾ Save</button>
                <button class="toolbar-button" id="clearBtn">ðŸ—‘ï¸ Clear</button>
                <button class="toolbar-button" id="zoomInBtn">ðŸ”+ Zoom In</button>
                <button class="toolbar-button" id="zoomOutBtn">ðŸ”- Zoom Out</button>
                <button class="toolbar-button" id="fitBtn">â¬œ Fit to Screen</button>
            </div>
            <div class="canvas-wrapper" id="canvasWrapper">
                <canvas id="canvas"></canvas>
            </div>
        </div>

        <!-- Properties Panel (Right Sidebar) -->
        <div class="properties-panel">
            <div class="properties-header">Properties</div>
            <div id="propertiesContent" class="properties-content">
                <div class="empty-state">Select an activity to view its properties</div>
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
        console.log('=== Pipeline Editor Script Starting ===');
        const vscode = acquireVsCodeApi();
        console.log('vscode API acquired');
        
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
            constructor(type, x, y) {
                this.id = Date.now() + Math.random();
                this.type = type;
                this.x = x;
                this.y = y;
                this.width = 120;
                this.height = 80;
                this.name = type;
                this.description = '';
                this.color = this.getColorForType(type);
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
                return colors[type] || '#666';
            }

            contains(x, y) {
                return x >= this.x && x <= this.x + this.width &&
                       y >= this.y && y <= this.y + this.height;
            }

            draw(ctx, isSelected) {
                const radius = 8;
                const headerHeight = 32;
                
                // Shadow
                if (isSelected) {
                    ctx.shadowColor = 'rgba(66, 184, 131, 0.4)';
                    ctx.shadowBlur = 12;
                    ctx.shadowOffsetX = 0;
                    ctx.shadowOffsetY = 4;
                } else {
                    ctx.shadowColor = 'rgba(0, 0, 0, 0.15)';
                    ctx.shadowBlur = 8;
                    ctx.shadowOffsetX = 0;
                    ctx.shadowOffsetY = 2;
                }

                // Main box background
                this.roundRect(ctx, this.x, this.y, this.width, this.height, radius);
                ctx.fillStyle = '#ffffff';
                ctx.fill();

                // Border
                ctx.strokeStyle = isSelected ? '#42b883' : '#2c3e50';
                ctx.lineWidth = isSelected ? 3 : 2;
                ctx.stroke();
                
                ctx.shadowColor = 'transparent';
                ctx.shadowBlur = 0;

                // Header bar (colored)
                ctx.save();
                ctx.beginPath();
                ctx.moveTo(this.x + radius, this.y);
                ctx.lineTo(this.x + this.width - radius, this.y);
                ctx.arcTo(this.x + this.width, this.y, this.x + this.width, this.y + radius, radius);
                ctx.lineTo(this.x + this.width, this.y + headerHeight);
                ctx.lineTo(this.x, this.y + headerHeight);
                ctx.lineTo(this.x, this.y + radius);
                ctx.arcTo(this.x, this.y, this.x + radius, this.y, radius);
                ctx.closePath();
                ctx.fillStyle = this.color;
                ctx.fill();
                ctx.restore();

                // Activity name in header (white text)
                ctx.fillStyle = '#ffffff';
                ctx.font = 'bold 13px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
                ctx.textAlign = 'left';
                ctx.textBaseline = 'middle';
                
                let displayName = this.name || this.type || 'Activity';
                const maxWidth = this.width - 50;
                if (displayName && ctx.measureText(displayName).width > maxWidth) {
                    displayName = displayName.substring(0, 12) + '...';
                }
                ctx.fillText(displayName, this.x + 12, this.y + headerHeight / 2);

                // Icon in header
                ctx.font = '16px sans-serif';
                ctx.textAlign = 'right';
                ctx.fillText(this.getIcon(), this.x + this.width - 12, this.y + headerHeight / 2);

                // Type label in body
                ctx.fillStyle = '#666666';
                ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'top';
                ctx.fillText(this.type, this.x + this.width / 2, this.y + headerHeight + 10);

                // Connection points
                this.drawConnectionPoints(ctx, headerHeight);
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

            adjustColor(color, amount) {
                // Simple color adjustment - ensure color has # prefix
                if (!color || !color.startsWith('#')) {
                    return '#666666';
                }
                const hex = color.replace('#', '');
                if (hex.length !== 6) {
                    return color; // Return original if invalid
                }
                const r = Math.max(0, Math.min(255, parseInt(hex.substring(0, 2), 16) + amount));
                const g = Math.max(0, Math.min(255, parseInt(hex.substring(2, 4), 16) + amount));
                const b = Math.max(0, Math.min(255, parseInt(hex.substring(4, 6), 16) + amount));
                const rHex = r.toString(16).padStart(2, '0');
                const gHex = g.toString(16).padStart(2, '0');
                const bHex = b.toString(16).padStart(2, '0');
                return \`#\${rHex}\${gHex}\${bHex}\`;
            }

            roundRect(ctx, x, y, width, height, radius) {
                ctx.beginPath();
                ctx.moveTo(x + radius, y);
                ctx.lineTo(x + width - radius, y);
                ctx.quadraticCurveTo(x + width, y, x + width, y + radius);
                ctx.lineTo(x + width, y + height - radius);
                ctx.quadraticCurveTo(x + width, y + height, x + width - radius, y + height);
                ctx.lineTo(x + radius, y + height);
                ctx.quadraticCurveTo(x, y + height, x, y + height - radius);
                ctx.lineTo(x, y + radius);
                ctx.quadraticCurveTo(x, y, x + radius, y);
                ctx.closePath();
            }

            drawConnectionPoints(ctx, headerHeight = 32) {
                const points = [
                    { x: this.x + this.width / 2, y: this.y + headerHeight }, // top (below header)
                    { x: this.x + this.width, y: this.y + this.height / 2 }, // right
                    { x: this.x + this.width / 2, y: this.y + this.height }, // bottom
                    { x: this.x, y: this.y + this.height / 2 } // left
                ];

                points.forEach(point => {
                    // Baklava.js style ports - clean circles
                    ctx.fillStyle = '#ffffff';
                    ctx.strokeStyle = '#2c3e50';
                    ctx.lineWidth = 2;
                    ctx.beginPath();
                    ctx.arc(point.x, point.y, 6, 0, Math.PI * 2);
                    ctx.fill();
                    ctx.stroke();
                });
            }

            getConnectionPoint(position) {
                const headerHeight = 32;
                switch (position) {
                    case 'top': return { x: this.x + this.width / 2, y: this.y + headerHeight };
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

                // Color based on condition
                const colors = {
                    'Succeeded': '#00a86b',
                    'Failed': '#d13438',
                    'Skipped': '#ffa500',
                    'Completed': '#0078d4'
                };
                
                const color = colors[this.condition] || '#0078d4';
                ctx.strokeStyle = color;
                ctx.lineWidth = 3;
                ctx.beginPath();
                ctx.moveTo(start.x, start.y);

                // Smart bezier curve
                if (Math.abs(dx) > Math.abs(dy)) {
                    // Horizontal - use horizontal control points
                    const cp1x = start.x + Math.abs(dx) / 3 * (dx > 0 ? 1 : -1);
                    const cp2x = end.x - Math.abs(dx) / 3 * (dx > 0 ? 1 : -1);
                    ctx.bezierCurveTo(cp1x, start.y, cp2x, end.y, end.x, end.y);
                } else {
                    // Vertical - use vertical control points
                    const cp1y = start.y + Math.abs(dy) / 3 * (dy > 0 ? 1 : -1);
                    const cp2y = end.y - Math.abs(dy) / 3 * (dy > 0 ? 1 : -1);
                    ctx.bezierCurveTo(start.x, cp1y, end.x, cp2y, end.x, end.y);
                }
                
                ctx.stroke();

                // Arrow head
                const angle = Math.atan2(end.y - start.y, end.x - start.x);
                const arrowLength = 12;
                ctx.fillStyle = color;
                ctx.beginPath();
                ctx.moveTo(end.x, end.y);
                ctx.lineTo(
                    end.x - arrowLength * Math.cos(angle - Math.PI / 6),
                    end.y - arrowLength * Math.sin(angle - Math.PI / 6)
                );
                ctx.lineTo(
                    end.x - arrowLength * Math.cos(angle + Math.PI / 6),
                    end.y - arrowLength * Math.sin(angle + Math.PI / 6)
                );
                ctx.closePath();
                ctx.fill();

                // Draw condition label with background
                const midX = (start.x + end.x) / 2;
                const midY = (start.y + end.y) / 2;
                
                ctx.font = 'bold 11px sans-serif';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                
                const text = this.condition;
                const textWidth = ctx.measureText(text).width;
                
                // Background for label
                ctx.fillStyle = 'rgba(0, 0, 0, 0.7)';
                ctx.fillRect(midX - textWidth / 2 - 4, midY - 8, textWidth + 8, 16);
                
                // Label text
                ctx.fillStyle = '#ffffff';
                ctx.fillText(text, midX, midY);
            }
        }

        // Draw everything
        function draw() {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            
            // Draw grid
            drawGrid();

            // Draw connections
            connections.forEach(conn => conn.draw(ctx));

            // Draw activities
            activities.forEach(activity => {
                activity.draw(ctx, activity === selectedActivity);
            });

            // Draw temporary connection line (removed buggy event reference)
            // This will be drawn in mousemove instead
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
                const centerX = canvas.width / 2;
                const centerY = canvas.height / 2;
                const activity = new Activity(activityType, centerX - 60, centerY - 40 + activities.length * 20);
                activities.push(activity);
                console.log('Activities count:', activities.length);
                draw();
            });
        });

        canvas.addEventListener('dragover', (e) => {
            e.preventDefault();
            console.log('Drag over canvas');
        });

        canvas.addEventListener('drop', (e) => {
            e.preventDefault();
            const activityType = e.dataTransfer.getData('activityType');
            const rect = canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            
            console.log('Dropping activity:', activityType, 'at', x, y);
            const activity = new Activity(activityType, x - 60, y - 40);
            activities.push(activity);
            console.log('Activities count:', activities.length);
            draw();
        });

        canvas.addEventListener('mousedown', (e) => {
            const mousePos = getMousePos(e);
            
            // Check if clicking on connection points
            for (let activity of activities) {
                const points = [
                    { pos: activity.getConnectionPoint('top'), activity },
                    { pos: activity.getConnectionPoint('right'), activity },
                    { pos: activity.getConnectionPoint('bottom'), activity },
                    { pos: activity.getConnectionPoint('left'), activity }
                ];
                
                for (let point of points) {
                    const dx = mousePos.x - point.pos.x;
                    const dy = mousePos.y - point.pos.y;
                    if (Math.sqrt(dx * dx + dy * dy) < 8) {
                        // Start connection from this point
                        connectionStart = point.pos;
                        connectionStart.activity = activity;
                        canvas.style.cursor = 'crosshair';
                        draw();
                        return;
                    }
                }
            }
            
            // Check if clicking on an activity
            for (let i = activities.length - 1; i >= 0; i--) {
                if (activities[i].contains(mousePos.x, mousePos.y)) {
                    // Start dragging
                    selectedActivity = activities[i];
                    draggedActivity = activities[i];
                    isDragging = true;
                    dragOffset.x = mousePos.x - activities[i].x;
                    dragOffset.y = mousePos.y - activities[i].y;
                    canvas.style.cursor = 'move';
                    showProperties(selectedActivity);
                    draw();
                    return;
                }
            }

            // Deselect if clicking empty space
            selectedActivity = null;
            showProperties(null);
            draw();
        });

        canvas.addEventListener('mousemove', (e) => {
            const mousePos = getMousePos(e);
            
            if (isDragging && draggedActivity) {
                draggedActivity.x = mousePos.x - dragOffset.x;
                draggedActivity.y = mousePos.y - dragOffset.y;
                draw();
            } else if (connectionStart) {
                draw();
                // Draw temporary connection line
                ctx.strokeStyle = '#0078d4';
                ctx.lineWidth = 2;
                ctx.setLineDash([5, 5]);
                ctx.beginPath();
                ctx.moveTo(connectionStart.x, connectionStart.y);
                ctx.lineTo(mousePos.x, mousePos.y);
                ctx.stroke();
                ctx.setLineDash([]);
            }
        });

        canvas.addEventListener('mouseup', (e) => {
            if (connectionStart) {
                const mousePos = getMousePos(e);
                
                // Check if ending on another activity
                for (let activity of activities) {
                    if (activity !== connectionStart.activity && 
                        activity.contains(mousePos.x, mousePos.y)) {
                        // Show condition selector
                        showConnectionConditionDialog(connectionStart.activity, activity, e.clientX, e.clientY);
                        break;
                    }
                }
                
                connectionStart = null;
                canvas.style.cursor = 'default';
            }
            
            if (isDragging) {
                canvas.style.cursor = 'default';
            }
            
            isDragging = false;
            draggedActivity = null;
            draw();
        });

        canvas.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            const mousePos = getMousePos(e);
            
            for (let activity of activities) {
                if (activity.contains(mousePos.x, mousePos.y)) {
                    selectedActivity = activity;
                    showContextMenu(e.clientX, e.clientY);
                    return;
                }
            }
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
                activity.name = e.target.value;
                draw();
            });

            document.getElementById('propDescription').addEventListener('input', (e) => {
                activity.description = e.target.value;
            });
            
            document.getElementById('propX').addEventListener('input', (e) => {
                activity.x = parseInt(e.target.value) || 0;
                draw();
            });
            
            document.getElementById('propY').addEventListener('input', (e) => {
                activity.y = parseInt(e.target.value) || 0;
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
                const activity = new Activity(message.activityType, 100, 100);
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
