// pipelineEditorV2.js — Webview script for the V2 Pipeline Editor
// Step 1: Canvas view, drag-and-drop, connections. Save is disabled.
//         Activity properties panel shows read-only data.
//         Schema-driven editing is added per-activity in subsequent steps.
// Step 4: Schema-driven editable form for Wait, Fail, SetVariable, AppendVariable.

'use strict';

// Activity types with full editable form support in V2.
// Expanded as later steps complete each group.
const EDITABLE_TYPES = new Set(['Wait', 'Fail', 'SetVariable', 'AppendVariable', 'ExecutePipeline', 'Filter',
    'ForEach', 'Until', 'IfCondition', 'Switch',
    'Lookup', 'Delete', 'Validation', 'GetMetadata']);

const vscode = acquireVsCodeApi();

// ─── Global state ──────────────────────────────────────────────────────────────
let activities = [];
let connections = [];
let selectedActivity = null;
let isDirty = false;
let currentFilePath = null;

// Schemas populated via initSchemas message
let activitiesConfig = { categories: [] };
let activitySchemas = {};
let copyActivityConfig = {};
let datasetContents = {};
let datasetList = [];
let pipelineList = [];
let datasetTypeCategories = {};
let locationTypeToStoreSettings = {};
let datasetTypeToFormatSettings = {};

// Pipeline-level data
let pipelineData = { name: '', description: '', annotations: [], parameters: {}, variables: {}, concurrency: 1 };

// Canvas / interaction state
let canvas, ctx;
let isDragging = false;
let draggedActivity = null;
let dragOffset = { x: 0, y: 0 };
let connectionStart = null;
let isPanning = false;
let panStart = { x: 0, y: 0, scrollLeft: 0, scrollTop: 0 };
let scale = 1;
let tx = 0, ty = 0;  // pan offset in screen pixels
let animationFrameId = null;
let needsRedraw = false;

// ─── Utility ───────────────────────────────────────────────────────────────────
function log(msg) { vscode.postMessage({ type: 'log', text: msg }); }
function escHtml(str) { return String(str ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function markAsDirty() {
    if (!isDirty) {
        isDirty = true;
        vscode.postMessage({ type: 'contentChanged', isDirty: true });
    }
}

function markAsClean() {
    isDirty = false;
    vscode.postMessage({ type: 'contentChanged', isDirty: false });
}

// Strip canvas-only / non-serializable properties before sending to extension host for save
// Note: _datasetCategory and _datasetType are intentionally NOT stripped — the engine needs them
// to evaluate conditional field rules during serialization.
const CANVAS_ONLY_KEYS = new Set(['element', 'container', 'color', 'isContainer', 'x', 'y', 'width', 'height', 'id']);
function toSaveData(a) {
    const obj = {};
    for (const key of Object.keys(a)) {
        if (CANVAS_ONLY_KEYS.has(key)) continue;
        const val = a[key];
        if (typeof val === 'function') continue;
        if (val instanceof EventTarget) continue;
        obj[key] = val;
    }
    return obj;
}

function applyTransform() {
    document.getElementById('worldContainer').style.transform = `translate(${tx}px, ${ty}px) scale(${scale})`;
}

function screenToWorld(clientX, clientY) {
    const rect = document.getElementById('canvasWrapper').getBoundingClientRect();
    return { x: (clientX - rect.left - tx) / scale, y: (clientY - rect.top - ty) / scale };
}

function zoomBy(factor, cx, cy) {
    const oldScale = scale;
    scale = Math.min(Math.max(scale * factor, 0.1), 4);
    tx = cx - (cx - tx) * (scale / oldScale);
    ty = cy - (cy - ty) * (scale / oldScale);
    applyTransform();
    draw();
}

function fitToScreen() {
    if (activities.length === 0) { scale = 1; tx = 20; ty = 20; applyTransform(); draw(); return; }

    // Measure available canvas area using window dimensions for reliability
    const sidebarEl  = document.querySelector('.sidebar');
    const propsEl    = document.getElementById('propertiesPanel');
    const toolbarEl  = document.querySelector('.toolbar');
    const bannerEl   = document.querySelector('.v2-banner');
    const configEl   = document.querySelector('.config-panel');

    const sidebarW = sidebarEl ? sidebarEl.offsetWidth : 250;
    const propsW   = (propsEl && !propsEl.classList.contains('collapsed')) ? (propsEl.offsetWidth || 300) : 0;
    const toolbarH = toolbarEl ? toolbarEl.offsetHeight : 48;
    const bannerH  = bannerEl  ? bannerEl.offsetHeight  : 27;
    const configH  = configEl  ? configEl.offsetHeight  : 40;

    const vW = window.innerWidth  - sidebarW - propsW;
    const vH = window.innerHeight - bannerH - toolbarH - configH;

    if (vW < 80 || vH < 40) {
        log(`fitToScreen: retrying (vW=${vW} vH=${vH})`);
        setTimeout(fitToScreen, 80);
        return;
    }

    const padding = 40;
    const ACT_W = 180, ACT_H = 90;
    const minX = Math.min(...activities.map(a => a.x));
    const minY = Math.min(...activities.map(a => a.y));
    const maxX = Math.max(...activities.map(a => a.x + ACT_W));
    const maxY = Math.max(...activities.map(a => a.y + ACT_H));
    const worldW = Math.max(maxX - minX, 1);
    const worldH = Math.max(maxY - minY, 1);

    const viewW = vW - padding * 2;
    const viewH = vH - padding * 2;
    log(`fitToScreen: world=${worldW}x${worldH} view=${viewW}x${viewH} window=${window.innerWidth}x${window.innerHeight} sidebar=${sidebarW} props=${propsW}`);

    scale = Math.min(viewW / worldW, viewH / worldH, 1.0);
    scale = Math.max(0.15, scale);
    tx = padding - minX * scale;
    ty = padding - minY * scale;
    applyTransform();
    draw();
}

// ─── Panel toggles ─────────────────────────────────────────────────────────────
function toggleProperties() {
    const panel = document.getElementById('propertiesPanel');
    panel.classList.toggle('collapsed');
    document.body.classList.toggle('properties-visible', !panel.classList.contains('collapsed'));
}

function toggleConfig() {
    const panel = document.querySelector('.config-panel');
    const btn = document.getElementById('configCollapseBtn');
    const container = document.querySelector('.container');
    panel.classList.toggle('minimized');
    const isMin = panel.classList.contains('minimized');
    btn.textContent = isMin ? '«' : '»';
    container.style.height = isMin ? 'calc(100vh - 27px - 40px)' : 'calc(100vh - 27px - 250px)';
    // Re-fit after panel animates
    setTimeout(fitToScreen, 220);
}

function toggleCategory(element) {
    element.closest('.activity-group').classList.toggle('collapsed');
}

// ─── Sidebar — build from activitiesConfig ─────────────────────────────────────
function buildSidebar() {
    const container = document.getElementById('sidebarCategories');
    container.innerHTML = activitiesConfig.categories.map(cat => `
        <div class="activity-group collapsed">
            <div class="activity-group-title">
                <span class="category-arrow">▼</span> ${escHtml(cat.name)}
            </div>
            <div class="activity-group-content">
                ${cat.activities.map(act => `
                    <div class="activity-item" draggable="true" data-type="${escHtml(act.type)}">
                        <div class="activity-icon">${act.icon || '📦'}</div>
                        <span>${escHtml(act.name)}</span>
                    </div>`).join('')}
            </div>
        </div>`).join('');

    // Attach category collapse toggles via event delegation (CSP: no inline onclick)
    container.querySelectorAll('.activity-group-title').forEach(title => {
        title.addEventListener('click', () => {
            title.closest('.activity-group').classList.toggle('collapsed');
        });
    });

    // Attach drag events
    container.querySelectorAll('.activity-item').forEach(item => {
        item.addEventListener('dragstart', e => {
            e.dataTransfer.setData('activityType', item.getAttribute('data-type'));
        });
        item.addEventListener('dblclick', () => {
            const type = item.getAttribute('data-type');
            const wrapper = document.getElementById('worldContainer');
            const a = new Activity(type, 100 + activities.length * 20, 100 + activities.length * 20, wrapper);
            activities.push(a);
            markAsDirty();
            draw();
        });
    });
}

// ─── Config panel tabs ─────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    // Signal to extension host that the webview JS is ready to receive messages
    vscode.postMessage({ type: 'ready' });
    document.querySelectorAll('.config-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const target = tab.getAttribute('data-tab');
            document.querySelectorAll('.config-tab').forEach(t => {
                t.classList.remove('active');
                t.style.borderBottom = 'none';
                t.style.color = 'var(--vscode-tab-inactiveForeground)';
            });
            document.querySelectorAll('.config-tab-pane').forEach(p => {
                p.classList.remove('active');
                p.style.display = 'none';
            });
            tab.classList.add('active');
            tab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
            tab.style.color = 'var(--vscode-tab-activeForeground)';
            const pane = document.getElementById(`tab-${target}`);
            if (pane) { pane.classList.add('active'); pane.style.display = 'block'; }
        });
    });

    // Panel toggles
    document.getElementById('expandPropertiesBtn').addEventListener('click', toggleProperties);
    document.getElementById('propertiesCollapseBtn').addEventListener('click', toggleProperties);
    document.getElementById('configCollapseBtn').addEventListener('click', toggleConfig);

    // Start with properties panel collapsed and config panel minimized
    // so the canvas gets maximum initial space
    document.getElementById('propertiesPanel').classList.add('collapsed');
    document.querySelector('.config-panel')?.classList.add('minimized');
    document.getElementById('configCollapseBtn').textContent = '\u00ab';
    // body.properties-visible controls whether the expand button shows
    document.body.classList.remove('properties-visible');

    // Canvas setup
    canvas = document.getElementById('canvas');
    ctx = canvas.getContext('2d');
    resizeCanvas();
    window.addEventListener('resize', resizeCanvas);
    setupCanvasEvents();
    setupToolbarButtons();
    setupContextMenu();
});

// ─── Canvas ────────────────────────────────────────────────────────────────────
function resizeCanvas() {
    if (!canvas) return;
    canvas.width = 4000;
    canvas.height = 4000;
    draw();
}

function draw() {
    if (!ctx) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    drawGrid();
    connections.forEach(conn => conn.draw(ctx));

    // Highlight selected activity connection points (canvas-drawn overlay)
    // Activity boxes are DOM elements; canvas only draws connections + grid.
}

function requestDraw() {
    if (!needsRedraw) {
        needsRedraw = true;
        if (animationFrameId) cancelAnimationFrame(animationFrameId);
        animationFrameId = requestAnimationFrame(() => {
            draw();
            needsRedraw = false;
            animationFrameId = null;
        });
    }
}

function drawGrid() {
    const gridSize = 20;
    ctx.strokeStyle = 'rgba(128,128,128,0.1)';
    ctx.lineWidth = 1;
    for (let x = 0; x < canvas.width; x += gridSize) {
        ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, canvas.height); ctx.stroke();
    }
    for (let y = 0; y < canvas.height; y += gridSize) {
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(canvas.width, y); ctx.stroke();
    }
}

// ─── Activity class ─────────────────────────────────────────────────────────────
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
        this.color = Activity.colorForType(type);
        this.container = container;
        this.element = null;
        this.isContainer = ['ForEach', 'Until', 'IfCondition', 'Switch'].includes(type);

        // Container-type child activity arrays
        if (type === 'IfCondition') { this.expression = ''; this.ifTrueActivities = []; this.ifFalseActivities = []; }
        if (type === 'ForEach')     { this.items = ''; this.isSequential = false; this.activities = []; }
        if (type === 'Until')       { this.expression = ''; this.activities = []; }
        if (type === 'Switch')      { this.on = ''; this.cases = []; this.defaultActivities = []; }

        this._createDOMElement();
    }

    static colorForType(type) {
        const m = {
            Copy: '#0078d4', Delete: '#d13438', ForEach: '#7fba00',
            IfCondition: '#ff8c00', Until: '#e81123', Switch: '#6264a7',
            Wait: '#00bcf2', WebActivity: '#8661c5', WebHook: '#9b59b6',
            SqlServerStoredProcedure: '#847545', Script: '#005a9e',
            SynapseNotebook: '#f2c811', SparkJob: '#e07000'
        };
        return m[type] || '#0078d4';
    }

    static labelForType(type) {
        const m = {
            Copy: 'Copy data', Delete: 'Delete', ForEach: 'ForEach',
            IfCondition: 'If Condition', Until: 'Until', Switch: 'Switch',
            Wait: 'Wait', WebActivity: 'Web Activity', WebHook: 'WebHook',
            SqlServerStoredProcedure: 'Stored procedure', ExecutePipeline: 'Execute Pipeline',
            Lookup: 'Lookup', GetMetadata: 'Get Metadata', Validation: 'Validation',
            SetVariable: 'Set Variable', AppendVariable: 'Append Variable',
            Filter: 'Filter', Fail: 'Fail', Script: 'Script',
            SynapseNotebook: 'Notebook', SparkJob: 'Spark Job'
        };
        return m[type] || type;
    }

    static iconForType(type) {
        const m = {
            Copy: '📋', Delete: '🗑️', ForEach: '🔁', IfCondition: '❓',
            Until: '🔄', Switch: '🔀', Wait: '⏱️', WebActivity: '🌐',
            WebHook: '🪝', SqlServerStoredProcedure: '⚡', ExecutePipeline: '▶',
            Lookup: '🔍', GetMetadata: 'ℹ', Validation: '✅',
            SetVariable: '📝', AppendVariable: '➕', Filter: '⊲',
            Fail: '✖', Script: '📜', SynapseNotebook: '📓', SparkJob: '⚡'
        };
        return m[type] || '📦';
    }

    _createDOMElement() {
        if (this.element?.parentNode) this.element.parentNode.removeChild(this.element);

        const el = document.createElement('div');
        el.className = 'activity-box' + (this.isContainer ? ' container-activity' : '');
        el.style.left = this.x + 'px';
        el.style.top = this.y + 'px';
        el.style.setProperty('--activity-color', this.color);
        el.dataset.activityId = this.id;

        // Header
        const header = document.createElement('div');
        header.className = 'activity-header';
        const typeLabel = document.createElement('span');
        typeLabel.className = 'activity-type-label';
        typeLabel.textContent = Activity.labelForType(this.type);
        header.appendChild(typeLabel);

        // Body
        const body = document.createElement('div');
        body.className = 'activity-body';
        const icon = document.createElement('div');
        icon.className = 'activity-icon-large';
        icon.textContent = Activity.iconForType(this.type);
        const label = document.createElement('div');
        label.className = 'activity-label';
        label.textContent = this.name;
        body.appendChild(icon);
        body.appendChild(label);

        // Container info
        if (this.isContainer) {
            const info = document.createElement('div');
            info.className = 'container-info';
            info.dataset.infoEl = 'true';
            this._refreshContainerInfo(info);
            el.appendChild(header);
            el.appendChild(body);
            el.appendChild(info);
        } else {
            el.appendChild(header);
            el.appendChild(body);
        }

        // Action row (visible when selected)
        const actions = document.createElement('div');
        actions.className = 'activity-actions';
        const delBtn = document.createElement('button');
        delBtn.className = 'action-icon-btn';
        delBtn.title = 'Delete';
        delBtn.textContent = '×';
        delBtn.onclick = e => { e.stopPropagation(); this._handleDelete(); };
        actions.appendChild(delBtn);
        el.appendChild(actions);

        // Connection points
        ['top', 'right', 'bottom', 'left'].forEach(pos => {
            const pt = document.createElement('div');
            pt.className = `connection-point ${pos}`;
            pt.dataset.position = pos;
            pt.dataset.activityId = this.id;
            el.appendChild(pt);
        });

        this.element = el;
        this.container.appendChild(el);
        this._setupEvents();
    }

    _refreshContainerInfo(infoEl) {
        if (!infoEl) return;
        let html = '';
        if (this.type === 'IfCondition') {
            html = `<div class="container-stat">True: ${(this.ifTrueActivities||[]).length} activities</div>
                    <div class="container-stat">False: ${(this.ifFalseActivities||[]).length} activities</div>`;
        } else if (this.type === 'Switch') {
            const total = (this.cases||[]).reduce((n,c)=>n+(c.activities||[]).length,0) + (this.defaultActivities||[]).length;
            html = `<div class="container-stat">Cases: ${(this.cases||[]).length}</div>
                    <div class="container-stat">Total: ${total} activities</div>`;
        } else {
            html = `<div class="container-stat">${(this.activities||[]).length} activities</div>`;
        }
        infoEl.innerHTML = html;
    }

    _setupEvents() {
        this.element.addEventListener('mousedown', e => {
            if (e.target.classList.contains('connection-point')) return;
            e.stopPropagation();
            this._onMouseDown(e);
        });
        this.element.addEventListener('contextmenu', e => {
            e.preventDefault(); e.stopPropagation();
            selectedActivity = this;
            showContextMenu(e.clientX, e.clientY);
        });
        this.element.querySelectorAll('.connection-point').forEach(pt => {
            pt.addEventListener('mousedown', e => {
                e.stopPropagation();
                const pos = pt.dataset.position;
                connectionStart = { ...this.getConnectionPoint(pos), activity: this };
                canvas.style.cursor = 'crosshair';
                draw();
            });
        });
    }

    _onMouseDown(e) {
        selectedActivity = this;
        draggedActivity = this;
        isDragging = true;
        const wPos = screenToWorld(e.clientX, e.clientY);
        dragOffset.x = wPos.x - this.x;
        dragOffset.y = wPos.y - this.y;
        this.element.classList.add('dragging');
        this._setSelected(true);
        showProperties(this);
        draw();
    }

    _handleDelete() {
        activities = activities.filter(a => a !== this);
        connections = connections.filter(c => c.from !== this && c.to !== this);
        this.remove();
        selectedActivity = null;
        showProperties(null);
        markAsDirty();
        draw();
    }

    _setSelected(sel) {
        if (!this.element) return;
        if (sel) {
            this.element.classList.add('selected');
            activities.forEach(a => { if (a !== this) a.element?.classList.remove('selected'); });
        } else {
            this.element.classList.remove('selected');
        }
    }

    refreshNameLabel() {
        const el = this.element?.querySelector('.activity-label');
        if (el) el.textContent = this.name;
    }

    updatePosition(x, y) {
        this.x = x; this.y = y;
        if (this.element) { this.element.style.left = x + 'px'; this.element.style.top = y + 'px'; }
    }

    remove() {
        this.element?.parentNode?.removeChild(this.element);
        this.element = null;
    }

    getConnectionPoint(pos) {
        switch (pos) {
            case 'top':    return { x: this.x + this.width / 2, y: this.y };
            case 'right':  return { x: this.x + this.width,     y: this.y + this.height / 2 };
            case 'bottom': return { x: this.x + this.width / 2, y: this.y + this.height };
            case 'left':   return { x: this.x,                  y: this.y + this.height / 2 };
            default:       return { x: this.x + this.width / 2, y: this.y + this.height };
        }
    }
}

// ─── Connection class ──────────────────────────────────────────────────────────
class Connection {
    constructor(fromActivity, toActivity, condition = 'Succeeded') {
        this.id = Date.now() + Math.random();
        this.from = fromActivity;
        this.to = toActivity;
        this.condition = condition;
    }

    draw(ctx) {
        const fc = { x: this.from.x + this.from.width / 2, y: this.from.y + this.from.height / 2 };
        const tc = { x: this.to.x   + this.to.width   / 2, y: this.to.y   + this.to.height   / 2 };
        const dx = tc.x - fc.x, dy = tc.y - fc.y;

        let start, end;
        if (Math.abs(dx) > Math.abs(dy)) {
            start = this.from.getConnectionPoint(dx > 0 ? 'right' : 'left');
            end   = this.to.getConnectionPoint(dx > 0 ? 'left' : 'right');
        } else {
            start = this.from.getConnectionPoint(dy > 0 ? 'bottom' : 'top');
            end   = this.to.getConnectionPoint(dy > 0 ? 'top' : 'bottom');
        }

        const colors = { Succeeded: '#107c10', Failed: '#d13438', Skipped: '#ffa500', Completed: '#0078d4' };
        const color = colors[this.condition] || '#107c10';
        const snap = v => Math.floor(v) + 0.5;
        const sx = snap(start.x), sy = snap(start.y), ex = snap(end.x), ey = snap(end.y);

        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        if (Math.abs(dx) > Math.abs(dy)) {
            const mx = sx + (ex - sx) / 2;
            ctx.moveTo(sx, sy); ctx.lineTo(mx, sy); ctx.lineTo(mx, ey); ctx.lineTo(ex, ey);
        } else {
            const my = sy + (ey - sy) / 2;
            ctx.moveTo(sx, sy); ctx.lineTo(sx, my); ctx.lineTo(ex, my); ctx.lineTo(ex, ey);
        }
        ctx.stroke();

        // Arrowhead
        const arrowAngle = Math.abs(dx) > Math.abs(dy)
            ? (dx > 0 ? 0 : Math.PI)
            : (dy > 0 ? Math.PI / 2 : -Math.PI / 2);
        const s = 7;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.moveTo(ex, ey);
        ctx.lineTo(ex - s * Math.cos(arrowAngle - Math.PI / 6), ey - s * Math.sin(arrowAngle - Math.PI / 6));
        ctx.lineTo(ex - s * Math.cos(arrowAngle + Math.PI / 6), ey - s * Math.sin(arrowAngle + Math.PI / 6));
        ctx.closePath();
        ctx.fill();
    }
}

// ─── Mouse event handlers ──────────────────────────────────────────────────────
function setupCanvasEvents() {
    const wrapper = document.getElementById('canvasWrapper');
    const worldEl = document.getElementById('worldContainer');

    wrapper.addEventListener('dragover', e => e.preventDefault());
    wrapper.addEventListener('drop', e => {
        e.preventDefault();
        const type = e.dataTransfer.getData('activityType');
        if (!type) return;
        const pos = screenToWorld(e.clientX, e.clientY);
        const a = new Activity(type, pos.x - 90, pos.y - 28, worldEl);
        activities.push(a);
        selectedActivity = a;
        a._setSelected(true);
        showProperties(a);
        markAsDirty();
        draw();
    });

    wrapper.addEventListener('mousedown', e => {
        if (!e.target.closest('.activity-box') && !e.target.closest('.context-menu')) {
            selectedActivity = null;
            activities.forEach(a => a.element?.classList.remove('selected'));
            showProperties(null);
            draw();
            isPanning = true;
            panStart = { x: e.clientX, y: e.clientY, tx, ty };
            wrapper.style.cursor = 'grabbing';
            e.preventDefault();
        }
    });

    wrapper.addEventListener('wheel', e => {
        e.preventDefault();
        const rect = wrapper.getBoundingClientRect();
        zoomBy(e.deltaY < 0 ? 1.1 : 1 / 1.1, e.clientX - rect.left, e.clientY - rect.top);
    }, { passive: false });

    document.addEventListener('mousemove', e => {
        if (isPanning) {
            tx = panStart.tx + (e.clientX - panStart.x);
            ty = panStart.ty + (e.clientY - panStart.y);
            applyTransform();
            return;
        }
        if (isDragging && draggedActivity) {
            const pos = screenToWorld(e.clientX, e.clientY);
            draggedActivity.updatePosition(pos.x - dragOffset.x, pos.y - dragOffset.y);
            requestDraw();
            return;
        }
        if (connectionStart) {
            const pos = screenToWorld(e.clientX, e.clientY);
            draw();
            ctx.strokeStyle = '#0078d4';
            ctx.lineWidth = 2;
            ctx.setLineDash([5, 5]);
            ctx.beginPath();
            ctx.moveTo(connectionStart.x, connectionStart.y);
            ctx.lineTo(pos.x, pos.y);
            ctx.stroke();
            ctx.setLineDash([]);
        }
    });

    document.addEventListener('mouseup', e => {
        if (isPanning) {
            isPanning = false;
            wrapper.style.cursor = '';
        }
        if (connectionStart) {
            const target = document.elementFromPoint(e.clientX, e.clientY)?.closest('.activity-box');
            if (target?.dataset.activityId) {
                const targetId = parseFloat(target.dataset.activityId);
                const toActivity = activities.find(a => a.id === targetId);
                if (toActivity && toActivity !== connectionStart.activity) {
                    showConnectionConditionDialog(connectionStart.activity, toActivity, e.clientX, e.clientY);
                }
            }
            connectionStart = null;
            canvas.style.cursor = 'default';
        }
        if (isDragging) {
            draggedActivity?.element?.classList.remove('dragging');
            if (draggedActivity) markAsDirty();
        }
        isDragging = false;
        draggedActivity = null;
        draw();
    });
}

// ─── Toolbar ────────────────────────────────────────────────────────────────────
function setupToolbarButtons() {
    const saveBtn = document.getElementById('saveBtn');
    saveBtn.addEventListener('click', () => {
        saveBtn.disabled = true;
        saveBtn.textContent = 'Saving…';
        const saveData = activities.map(toSaveData);        vscode.postMessage({
            type: 'savePipeline',
            pipelineData,
            activities: saveData,
            connections: connections.map(c => ({
                fromName: c.from.name,
                toName:   c.to.name,
                condition: c.condition,
            })),
        });
    });

    document.getElementById('clearBtn').addEventListener('click', () => {
        if (activities.length === 0) return;
        if (confirm('Clear all activities?')) {
            activities.forEach(a => a.remove());
            activities = [];
            connections = [];
            selectedActivity = null;
            showProperties(null);
            markAsDirty();
            draw();
        }
    });

    document.getElementById('zoomInBtn').addEventListener('click', () => {
        const wEl = document.getElementById('canvasWrapper');
        zoomBy(1.25, wEl.clientWidth / 2, wEl.clientHeight / 2);
    });

    document.getElementById('zoomOutBtn').addEventListener('click', () => {
        const wEl = document.getElementById('canvasWrapper');
        zoomBy(1 / 1.25, wEl.clientWidth / 2, wEl.clientHeight / 2);
    });

    document.getElementById('fitBtn').addEventListener('click', fitToScreen);
}

// ─── Context menu ──────────────────────────────────────────────────────────────
function setupContextMenu() {
    document.addEventListener('click', () => {
        document.getElementById('contextMenu').style.display = 'none';
    });

    document.querySelectorAll('.context-menu-item').forEach(item => {
        item.addEventListener('click', () => {
            const action = item.getAttribute('data-action');
            if (action === 'delete' && selectedActivity) {
                selectedActivity.remove();
                activities = activities.filter(a => a !== selectedActivity);
                connections = connections.filter(c => c.from !== selectedActivity && c.to !== selectedActivity);
                selectedActivity = null;
                showProperties(null);
                markAsDirty();
                draw();
            }
        });
    });
}

function showContextMenu(x, y) {
    const menu = document.getElementById('contextMenu');
    menu.style.left = x + 'px';
    menu.style.top  = y + 'px';
    menu.style.display = 'block';
}

// ─── Connection condition dialog ───────────────────────────────────────────────
function showConnectionConditionDialog(fromActivity, toActivity, x, y) {
    const dialog = document.createElement('div');
    dialog.style.cssText = `position:fixed;left:${x}px;top:${y}px;background:var(--vscode-menu-background);border:1px solid var(--vscode-menu-border);border-radius:4px;padding:8px;z-index:10000;box-shadow:0 2px 8px rgba(0,0,0,0.3);min-width:150px;`;
    dialog.innerHTML = `
        <div style="font-size:12px;font-weight:600;margin-bottom:8px;">Dependency Condition</div>
        <button data-c="Succeeded" style="width:100%;margin:2px 0;padding:5px;background:#107c10;color:white;border:none;border-radius:2px;cursor:pointer;">✓ Succeeded</button>
        <button data-c="Failed"    style="width:100%;margin:2px 0;padding:5px;background:#d13438;color:white;border:none;border-radius:2px;cursor:pointer;">✗ Failed</button>
        <button data-c="Completed" style="width:100%;margin:2px 0;padding:5px;background:#0078d4;color:white;border:none;border-radius:2px;cursor:pointer;">⊙ Completed</button>
        <button data-c="Skipped"   style="width:100%;margin:2px 0;padding:5px;background:#ffa500;color:white;border:none;border-radius:2px;cursor:pointer;">⊘ Skipped</button>`;
    document.body.appendChild(dialog);

    dialog.querySelectorAll('button').forEach(btn => {
        btn.addEventListener('click', () => {
            const exists = connections.find(c => c.from.id === fromActivity.id && c.to.id === toActivity.id);
            if (exists) { alert('A dependency already exists between these activities.'); }
            else {
                connections.push(new Connection(fromActivity, toActivity, btn.getAttribute('data-c')));
                markAsDirty();
                draw();
            }
            document.body.removeChild(dialog);
        });
    });

    setTimeout(() => {
        const close = e => { if (!dialog.contains(e.target) && dialog.parentNode) { document.body.removeChild(dialog); document.removeEventListener('click', close); } };
        document.addEventListener('click', close);
    }, 100);
}

// ─── Properties panel ──────────────────────────────────────────────────────────
function showProperties(activity) {
    const rightPanel = document.getElementById('propertiesContent');
    const titleEl = document.getElementById('propertiesPanelTitle');
    const tabsContainer = document.getElementById('activityTabsContainer');
    const panesContainer = document.getElementById('activityPanesContainer');

    // Panels stay in whatever state the user left them — no auto-expand on activity click.

    // Show pipeline tabs when nothing selected
    document.querySelectorAll('.pipeline-tab').forEach(t => t.style.display = activity ? 'none' : '');
    document.querySelectorAll('.pipeline-pane').forEach(p => p.style.display = activity ? 'none' : '');
    tabsContainer.innerHTML = '';
    panesContainer.innerHTML = '';

    if (!activity) {
        titleEl.textContent = 'Pipeline Properties';
        rightPanel.innerHTML = `
            <div style="font-size:12px;margin-bottom:8px;">
                <strong>Name:</strong> ${escHtml(pipelineData.name || '(untitled)')}
            </div>
            <div style="font-size:12px;margin-bottom:8px;">
                <strong>Description:</strong> ${escHtml(pipelineData.description || '—')}
            </div>`;

        // Restore first pipeline tab as active
        const firstTab = document.querySelector('.pipeline-tab');
        if (firstTab) firstTab.click();
        return;
    }

    titleEl.textContent = activity.name;

    const editable = EDITABLE_TYPES.has(activity.type);
    const schema = activitySchemas[activity.type];

    // Right panel: always show name + type header
    rightPanel.innerHTML = `
        <div style="font-size:11px;color:var(--vscode-descriptionForeground);margin-bottom:2px;">${escHtml(Activity.labelForType(activity.type))}</div>
        ${!editable ? `<div class="v2-migration-note" style="margin-top:8px;">✦ Editing for <strong>${escHtml(Activity.labelForType(activity.type))}</strong> is coming soon.</div>` : ''}`;

    if (editable && schema) {
        // Apply schema defaults to the activity model before rendering the form.
        // This ensures freshly-dropped activities have correct defaults even if
        // the user never touches a field.
        _applySchemaDefaults(activity, schema);
        _detectDatasetCategory(activity, schema);

        // Build tabs from schema.tabs
        const tabs = schema.tabs || ['General', 'Settings'];
        tabsContainer.innerHTML = tabs.map((t, i) =>
            `<button class="config-tab${i === 0 ? ' active' : ''}" data-tab="v2-tab-${i}"${
                i === 0 ? ' style="border-bottom:2px solid var(--vscode-focusBorder);color:var(--vscode-tab-activeForeground);"' : ''
            }>${escHtml(t)}</button>`
        ).join('');
        panesContainer.innerHTML = tabs.map((t, i) => {
            const html = t === 'General'
                ? _buildFormPane(activity, schema.commonProperties || {}, 'general')
                : t === 'Settings'
                    ? _buildFormPane(activity, schema.typeProperties || {}, 'settings')
                    : t === 'Source'
                        ? _buildFormPane(activity, schema.sourceProperties || {}, 'source')
                        : t === 'Sink'
                            ? _buildFormPane(activity, schema.sinkProperties || {}, 'sink')
                            : t === 'Activities'
                                ? _buildActivitiesTab(activity, schema)
                                : `<div class="empty-state">Not yet implemented.</div>`;
            return `<div class="config-tab-pane${i === 0 ? ' active' : ''}" id="tab-v2-tab-${i}" style="display:${i === 0 ? 'block' : 'none'}">${html}</div>`;
        }).join('');
        // Wire all inputs to write back to the activity and mark dirty
        _wireFormInputs(panesContainer, activity);
        _wireKvFields(panesContainer, activity);
        _wireFieldLists(panesContainer, activity);
    } else {
        // Fallback read-only for not-yet-editable types
        tabsContainer.innerHTML = `
            <button class="config-tab active" data-tab="v2-general" style="border-bottom:2px solid var(--vscode-focusBorder);color:var(--vscode-tab-activeForeground);">General</button>
            <button class="config-tab" data-tab="v2-settings">Settings</button>`;
        panesContainer.innerHTML = `
            <div class="config-tab-pane active" id="tab-v2-general" style="display:block;">
                ${_buildReadOnlyGeneral(activity, schema)}
            </div>
            <div class="config-tab-pane" id="tab-v2-settings" style="display:none;">
                ${_buildReadOnlySettings(activity, schema)}
            </div>`;
    }

    tabsContainer.querySelectorAll('.config-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            tabsContainer.querySelectorAll('.config-tab').forEach(t => {
                t.classList.remove('active');
                t.style.borderBottom = 'none';
                t.style.color = 'var(--vscode-tab-inactiveForeground)';
            });
            panesContainer.querySelectorAll('.config-tab-pane').forEach(p => { p.classList.remove('active'); p.style.display = 'none'; });
            tab.classList.add('active');
            tab.style.borderBottom = '2px solid var(--vscode-focusBorder)';
            tab.style.color = 'var(--vscode-tab-activeForeground)';
            const pane = document.getElementById(`tab-${tab.getAttribute('data-tab')}`);
            if (pane) { pane.classList.add('active'); pane.style.display = 'block'; }
        });
    });
}

function _propRow(key, val) {
    return `<div class="v2-prop-row"><span class="v2-prop-key">${escHtml(key)}</span><span class="v2-prop-val">${escHtml(String(val ?? '—'))}</span></div>`;
}

// ─── Schema-driven form renderer ───────────────────────────────────────────────
// Renders editable HTML form fields for one group of schema fields.
// Fields hidden by `conditional` are rendered but show/hide via JS.
function _buildFormPane(activity, fields, paneId) {
    console.log(`[V2] _buildFormPane pane="${paneId}" activity="${activity.type}" fields:`, Object.fromEntries(Object.entries(fields).map(([k,d]) => [k, d.type])));
    let html = '';
    for (const [key, def] of Object.entries(fields)) {
        if (def.uiOnly) continue;
        if (def.type === 'containerActivities' || def.type === 'switchCases') continue;
        const val = activity[key] ?? def.default ?? '';
        console.log(`[V2]   field "${key}" type="${def.type}" val=`, val);
        const cond = def.conditional ? `data-cond-field="${escHtml(def.conditional.field)}" data-cond-value="${escHtml(Array.isArray(def.conditional.value) ? def.conditional.value.join(',') : def.conditional.value)}"` : '';
        const isBool = def.type === 'boolean';
        const isBlock = isBool || def.multiline || def.type === 'keyvalue' || def.type === 'getmetadata-fieldlist';
        html += `<div class="form-field${isBlock ? ' form-field--block' : ''}" data-field-key="${escHtml(key)}" ${cond}>`;
        if (!isBool) {
            html += `<label class="form-label">${escHtml(def.label || key)}${def.required ? ' <span style="color:var(--vscode-errorForeground)">*</span>' : ''}</label>`;
        }
        switch (def.type) {
            case 'boolean':
                html += `<label class="form-checkbox-label">${escHtml(def.label || key)}<input type="checkbox" class="form-checkbox" data-key="${escHtml(key)}" ${val ? 'checked' : ''} /></label>`;
                break;
            case 'radio-with-info':
            case 'radio':
                html += `<div class="form-radio-group">${(def.options || []).map((label, i) => {
                    const v = def.optionValues ? def.optionValues[i] : label;
                    const optCond = def.optionConditions?.[String(v)];
                    const condAttr = optCond ? ` data-cond-field="${escHtml(optCond.field)}" data-cond-value="${escHtml(Array.isArray(optCond.value) ? optCond.value.join(',') : optCond.value)}"` : '';
                    return `<label class="form-radio-label"${condAttr}><input type="radio" name="${escHtml(paneId + '-' + key)}" value="${escHtml(String(v))}" ${String(val) === String(v) ? 'checked' : ''} data-key="${escHtml(key)}" />${escHtml(String(label))}</label>`;
                }).join('')}</div>`;
                break;
            case 'select':
                html += `<select class="form-select" data-key="${escHtml(key)}">` +
                    (def.options || []).map(opt =>
                        `<option value="${escHtml(opt)}" ${String(val) === String(opt) ? 'selected' : ''}>${escHtml(def.optionLabels?.[opt] || opt)}</option>`
                    ).join('') + `</select>`;
                break;
            case 'number':
                html += `<input type="number" class="form-input" data-key="${escHtml(key)}" value="${escHtml(String(val))}"${def.min != null ? ` min="${def.min}"` : ''}${def.max != null ? ` max="${def.max}"` : ''} placeholder="${escHtml(def.placeholder || '')}" />`;
                break;
            case 'keyvalue':
                html += `<div class="form-kv-container" data-kv-field="${escHtml(key)}" data-kv-types="${escHtml((def.valueTypes || []).join(','))}"></div>`;
                break;
            case 'pipeline': {
                const currentName = val?.referenceName ?? '';
                html += `<select class="form-select" data-key="${escHtml(key)}" data-field-type="pipeline">`
                    + `<option value="">-- Select pipeline --</option>`
                    + pipelineList.map(p => `<option value="${escHtml(p)}"${p === currentName ? ' selected' : ''}>${escHtml(p)}</option>`).join('')
                    + `</select>`;
                break;
            }
            case 'dataset-lookup':
            case 'validation-dataset':
            case 'getmetadata-dataset':
            case 'dataset': {
                const currentDs = val?.referenceName ?? '';
                const filteredDs = def.datasetFilter === 'storageOnly'
                    ? datasetList.filter(d => datasetTypeCategories[datasetContents[d]?.properties?.type ?? ''] === 'storage')
                    : datasetList;
                html += `<select class="form-select" data-key="${escHtml(key)}" data-field-type="dataset">`
                    + `<option value="">-- Select dataset --</option>`
                    + filteredDs.map(d => `<option value="${escHtml(d)}"${d === currentDs ? ' selected' : ''}>${escHtml(d)}</option>`).join('')
                    + `</select>`;
                break;
            }
            case 'getmetadata-fieldlist': {
                const selectedDs = activity.dataset?.referenceName ?? '';
                const dsType = selectedDs ? (datasetContents[selectedDs]?.properties?.type ?? '') : '';
                const dsCategory = dsType ? (datasetTypeCategories[dsType] ?? '') : '';
                const allOptions = def.fieldListOptions ?? {};
                const options = dsCategory ? (allOptions[dsCategory] ?? []) : [];
                const placeholder = !selectedDs ? 'Select a dataset first to see available field options.' : (!dsCategory ? 'Unknown dataset type — enter field names manually.' : '');
                html += `<div class="form-fieldlist-dynamic" data-fieldlist-key="${escHtml(key)}" data-fieldlist-options="${escHtml(JSON.stringify(options))}" data-fieldlist-all-options="${escHtml(JSON.stringify(allOptions))}" data-fieldlist-placeholder="${escHtml(placeholder)}"></div>`;
                break;
            }
            case 'expression': {
                // Expression values are stored as {value, type} objects on disk; extract the value string for editing
                const exprStr = (val && typeof val === 'object' && 'value' in val) ? String(val.value) : String(val);
                if (def.multiline) {
                    html += `<textarea class="form-textarea" data-key="${escHtml(key)}" data-field-type="expression" rows="3" placeholder="${escHtml(def.placeholder || '')}">${escHtml(exprStr)}</textarea>`;
                } else {
                    html += `<input type="text" class="form-input" data-key="${escHtml(key)}" data-field-type="expression" value="${escHtml(exprStr)}" placeholder="${escHtml(def.placeholder || '')}" />`;
                }
                break;
            }
            case 'datetime': {
                let dtVal = '';
                if (val) { dtVal = String(val).replace('Z', '').substring(0, 16); }
                html += `<input type="datetime-local" class="form-input" data-key="${escHtml(key)}" data-field-type="datetime" value="${escHtml(dtVal)}" />`;
                break;
            }
            case 'text':
            case 'string':
            default:
                if (def.multiline) {
                    html += `<textarea class="form-textarea" data-key="${escHtml(key)}" rows="${def.rows ?? 2}" placeholder="${escHtml(def.placeholder || '')}">${escHtml(String(val))}</textarea>`;
                } else {
                    html += `<input type="text" class="form-input" data-key="${escHtml(key)}" value="${escHtml(String(val))}" placeholder="${escHtml(def.placeholder || '')}" />`;
                }
                break;
        }
        if (def.helpText) html += `<div class="form-help">${escHtml(def.helpText)}</div>`;
        html += `</div>`;
    }
    return html || '<div class="empty-state">No fields.</div>';
}

// Render the Activities tab for container types — shows a read-only list of nested activity names.
function _buildActivitiesTab(activity, schema) {
    const containerFields = Object.entries(schema.typeProperties || {})
        .filter(([, def]) => def.type === 'containerActivities' || def.type === 'switchCases');

    if (containerFields.length === 0) return '<div class="empty-state">No nested activities.</div>';

    let html = '';
    for (const [key, def] of containerFields) {
        const items = activity[key] || [];
        html += `<div style="font-weight:600;margin:8px 0 4px;font-size:12px;">${escHtml(def.label || key)}</div>`;
        if (def.type === 'switchCases') {
            // Switch cases: each item has {value, activities[]}
            if (items.length === 0 && !activity.defaultActivities?.length) {
                html += '<div class="empty-state" style="margin-bottom:8px;">No cases defined.</div>';
            } else {
                html += '<div class="container-activity-list">';
                for (const c of items) {
                    html += `<div class="container-activity-item"><span class="container-activity-badge">Case: ${escHtml(String(c.value ?? ''))}</span> — ${c.activities?.length ?? 0} activit${(c.activities?.length ?? 0) === 1 ? 'y' : 'ies'}</div>`;
                }
                if (activity.defaultActivities?.length) {
                    html += `<div class="container-activity-item"><span class="container-activity-badge">Default</span> — ${activity.defaultActivities.length} activit${activity.defaultActivities.length === 1 ? 'y' : 'ies'}</div>`;
                }
                html += '</div>';
            }
        } else {
            if (items.length === 0) {
                html += '<div class="empty-state" style="margin-bottom:8px;">No activities.</div>';
            } else {
                html += '<div class="container-activity-list">';
                for (const a of items) {
                    html += `<div class="container-activity-item"><span class="container-activity-badge">${escHtml(a.type || '?')}</span> ${escHtml(a.name || '(unnamed)')}</div>`;
                }
                html += '</div>';
            }
        }
    }
    html += `<div class="form-help" style="margin-top:8px;">Inner canvas editing coming in a future step.</div>`;
    return html;
}

// Apply schema defaults to the activity model for any undefined fields.
function _applySchemaDefaults(activity, schema) {
    const FIELD_GROUPS = ['commonProperties', 'typeProperties', 'advancedProperties', 'sourceProperties', 'sinkProperties'];
    for (const group of FIELD_GROUPS) {
        const fields = schema[group];
        if (!fields) continue;
        for (const [key, def] of Object.entries(fields)) {
            if (def.type === 'containerActivities' || def.type === 'switchCases') continue;
            if (activity[key] === undefined && def.default !== undefined) {
                activity[key] = def.default;
            }
        }
    }
}

// Detect the dataset's category (storage/sql/etc.) and set _datasetCategory on the activity.
// This drives conditional visibility for fields like modifiedDatetimeStart in GetMetadata.
function _detectDatasetCategory(activity, schema) {
    const DATASET_TYPES = new Set(['dataset', 'dataset-lookup', 'validation-dataset', 'getmetadata-dataset']);
    const groups = ['typeProperties', 'sourceProperties', 'sinkProperties', 'commonProperties'];
    for (const group of groups) {
        const fields = schema?.[group];
        if (!fields) continue;
        for (const [key, def] of Object.entries(fields)) {
            if (DATASET_TYPES.has(def.type)) {
                const dsName = activity[key]?.referenceName ?? '';
                if (dsName) {
                    const dsType = datasetContents[dsName]?.properties?.type ?? '';
                    activity._datasetCategory = datasetTypeCategories[dsType] ?? '';
                    activity._datasetType = dsType;
                    const locType = datasetContents[dsName]?.properties?.typeProperties?.location?.type ?? '';
                    activity._storeSettingsType = locationTypeToStoreSettings[locType] ?? '';
                    activity._formatSettingsType = datasetTypeToFormatSettings[dsType] ?? '';
                    return;
                }
            }
        }
    }
}

// Wire form inputs in a container to write back to the activity object.
// Also handles conditional visibility.
function _wireFormInputs(container, activity) {
    // Set initial conditional visibility
    _applyConditionals(container, activity);

    container.querySelectorAll('input[data-key], select[data-key], textarea[data-key]').forEach(el => {
        const key = el.dataset.key;
        if (!key) return;
        const tag = el.tagName.toLowerCase();
        const eventName = (tag === 'input' && el.type === 'checkbox') ? 'change'
            : (tag === 'input' && el.type === 'radio') ? 'change'
            : (tag === 'select') ? 'change'
            : 'input';
        el.addEventListener(eventName, () => {
            let value;
            if (tag === 'input' && el.type === 'checkbox') {
                value = el.checked;
            } else if (tag === 'input' && el.type === 'number') {
                value = el.value === '' ? '' : Number(el.value);
            } else if (el.dataset.fieldType === 'expression') {
                // Re-wrap into ADF Expression object
                value = { value: el.value, type: 'Expression' };
            } else if (el.dataset.fieldType === 'pipeline') {
                // Re-wrap into ADF PipelineReference object
                value = el.value ? { referenceName: el.value, type: 'PipelineReference' } : null;
            } else if (el.dataset.fieldType === 'dataset') {
                // Re-wrap into ADF DatasetReference object
                value = el.value ? { referenceName: el.value, type: 'DatasetReference' } : null;
                // Update _datasetCategory/_datasetType so conditional fields show/hide correctly
                const dsType = el.value ? (datasetContents[el.value]?.properties?.type ?? '') : '';
                activity._datasetCategory = datasetTypeCategories[dsType] ?? '';
                activity._datasetType = dsType;
                const locType = el.value ? (datasetContents[el.value]?.properties?.typeProperties?.location?.type ?? '') : '';
                activity._storeSettingsType = locationTypeToStoreSettings[locType] ?? '';
                activity._formatSettingsType = datasetTypeToFormatSettings[dsType] ?? '';
                // Clear fieldlist values so old entries from previous dataset don't carry over
                container.querySelectorAll('.form-fieldlist-dynamic').forEach(fl => {
                    const fieldKey = fl.dataset.fieldlistKey;
                    if (fieldKey) activity[fieldKey] = [];
                });
                // Rebuild any fieldlist options that depend on the selected dataset type
                container.querySelectorAll('.form-fieldlist-dynamic').forEach(fl => {
                    const allOpts = JSON.parse(fl.dataset.fieldlistAllOptions || '{}');
                    const catOpts = activity._datasetCategory ? (allOpts[activity._datasetCategory] ?? []) : [];
                    const ph = !el.value ? 'Select a dataset first to see available field options.'
                             : (!activity._datasetCategory ? 'Unknown dataset type — enter field names manually.' : '');
                    fl.dataset.fieldlistOptions = JSON.stringify(catOpts);
                    fl.dataset.fieldlistPlaceholder = ph;
                    _buildFieldlistUI(fl, activity);
                });
            } else if (el.dataset.fieldType === 'datetime') {
                value = el.value ? el.value + ':00Z' : '';
            } else {
                value = el.value;
            }
            activity[key] = value;
            // Update canvas name label if name field changed
            if (key === 'name') {
                activity.refreshNameLabel();
                document.getElementById('propertiesPanelTitle').textContent = value;
            }
            markAsDirty();
            _applyConditionals(container, activity);
        });
    });
}

// Show/hide form fields based on their conditional dependencies.
function _applyConditionals(container, activity) {
    container.querySelectorAll('.form-field[data-cond-field]').forEach(el => {
        const field = el.dataset.condField;
        const allowed = el.dataset.condValue.split(',');
        const current = String(activity[field] ?? '');
        el.style.display = allowed.includes(current) ? '' : 'none';
    });
    // Hide/show individual radio options that have per-option conditions (e.g. Prefix for Blob only)
    container.querySelectorAll('.form-radio-label[data-cond-field]').forEach(el => {
        const field = el.dataset.condField;
        const allowed = el.dataset.condValue.split(',');
        const current = String(activity[field] ?? '');
        el.style.display = allowed.includes(current) ? '' : 'none';
    });
    // Also hide/show kv containers whose parent form-field is conditional
    container.querySelectorAll('.form-kv-container').forEach(kvEl => {
        const parent = kvEl.closest('.form-field[data-cond-field]');
        if (parent) kvEl.style.display = parent.style.display;
    });
}

// ─── Field list renderer (GetMetadata fieldList) ──────────────────────────────
function _buildFieldlistUI(el, activity) {
    el.innerHTML = '';
    const key = el.dataset.fieldlistKey;
    if (!key) return;
    let options = [];
    try { options = JSON.parse(el.dataset.fieldlistOptions || '[]'); } catch { options = []; }
    const currentItems = Array.isArray(activity[key])
        ? activity[key].map(i => {
            if (typeof i === 'string') return i;
            if (i.type === 'Expression') return i.value ?? '';   // custom expression
            return i.type ?? '';                                  // legacy {type: "columnCount"} format
        })
        : [];

    const syncToModel = () => {
        const result = [];
        el.querySelectorAll('.form-fieldlist-row').forEach(row => {
            const sel = row.querySelector('.form-fieldlist-select');
            const custom = row.querySelector('.form-fieldlist-custom');
            const isCustom = sel?.value === '__custom';
            const value = isCustom ? (custom?.value?.trim() ?? '') : (sel?.value ?? '');
            if (!value) return;
            if (isCustom) {
                result.push({ value: value, type: 'Expression' });
            } else {
                result.push(value);
            }
        });
        activity[key] = result;
        markAsDirty();
    };

    const addRow = (currentValue) => {
        const row = document.createElement('div');
        row.className = 'form-fieldlist-row';
        const sel = document.createElement('select');
        sel.className = 'form-select form-fieldlist-select';
        const blank = document.createElement('option');
        blank.value = ''; blank.textContent = '-- Select field --';
        sel.appendChild(blank);
        for (const opt of options) {
            const o = document.createElement('option');
            o.value = opt.value ?? opt; o.textContent = opt.label ?? opt;
            if ((opt.value ?? opt) === currentValue) o.selected = true;
            sel.appendChild(o);
        }
        const customOpt = document.createElement('option');
        customOpt.value = '__custom'; customOpt.textContent = 'Custom...';
        const isCustom = !!currentValue && !options.find(o => (o.value ?? o) === currentValue);
        if (isCustom) customOpt.selected = true;
        sel.appendChild(customOpt);
        const customInput = document.createElement('input');
        customInput.type = 'text';
        customInput.className = 'form-input form-fieldlist-custom';
        customInput.placeholder = 'Custom field name';
        customInput.value = isCustom ? currentValue : '';
        customInput.style.display = isCustom ? '' : 'none';
        const removeBtn = document.createElement('button');
        removeBtn.className = 'action-icon-btn form-fieldlist-remove-btn';
        removeBtn.type = 'button';
        removeBtn.textContent = '×';
        sel.addEventListener('change', () => {
            customInput.style.display = sel.value === '__custom' ? '' : 'none';
            syncToModel();
        });
        customInput.addEventListener('input', syncToModel);
        removeBtn.addEventListener('click', () => { row.remove(); syncToModel(); });
        row.appendChild(sel);
        row.appendChild(customInput);
        row.appendChild(removeBtn);
        const addBtn = el.querySelector('.form-fieldlist-add-btn');
        el.insertBefore(row, addBtn);
    };

    const placeholder = el.dataset.fieldlistPlaceholder || '';
    if (placeholder && options.length === 0 && currentItems.length === 0) {
        const msg = document.createElement('div');
        msg.className = 'form-help';
        msg.style.fontStyle = 'italic';
        msg.textContent = placeholder;
        el.appendChild(msg);
        return;
    }
    const addBtn = document.createElement('button');
    addBtn.className = 'form-kv-add-btn form-fieldlist-add-btn';
    addBtn.type = 'button';
    addBtn.textContent = '+ New';
    el.appendChild(addBtn);
    for (const item of currentItems) { addRow(item); }
    addBtn.addEventListener('click', () => addRow(''));
}

function _wireFieldLists(container, activity) {
    container.querySelectorAll('.form-fieldlist-dynamic').forEach(el => _buildFieldlistUI(el, activity));
}

// ─── Key-value field renderer ────────────────────────────────────────────────────
// Used for SetVariable's "Pipeline return value" returnValues field.
function _wireKvFields(container, activity) {
    container.querySelectorAll('.form-kv-container').forEach(kvEl => {
        const fieldKey = kvEl.dataset.kvField;
        const valueTypes = (kvEl.dataset.kvTypes || '').split(',').filter(Boolean);
        if (!activity[fieldKey] || typeof activity[fieldKey] !== 'object' || Array.isArray(activity[fieldKey])) {
            activity[fieldKey] = {};
        }
        _renderKvField(kvEl, activity, fieldKey, valueTypes);
    });
}

function _renderKvField(kvEl, activity, fieldKey, valueTypes) {
    kvEl.innerHTML = '';
    const data = activity[fieldKey] || {};

    const table = document.createElement('table');
    table.className = 'form-kv-table';
    const thead = document.createElement('thead');
    thead.innerHTML = '<tr><th>Key</th><th>Type</th><th>Value</th><th></th></tr>';
    table.appendChild(thead);
    const tbody = document.createElement('tbody');
    table.appendChild(tbody);
    kvEl.appendChild(table);

    for (const [k, item] of Object.entries(data)) {
        _addKvRow(tbody, activity, fieldKey, valueTypes, k, item.type || 'String', item.value);
    }

    const addBtn = document.createElement('button');
    addBtn.className = 'form-kv-add-btn';
    addBtn.textContent = '+ Add';
    kvEl.appendChild(addBtn);
    addBtn.addEventListener('click', () => {
        const newKey = 'key' + (Object.keys(activity[fieldKey] || {}).length + 1);
        if (!activity[fieldKey]) activity[fieldKey] = {};
        activity[fieldKey][newKey] = { type: valueTypes[0] || 'String', value: '' };
        markAsDirty();
        _renderKvField(kvEl, activity, fieldKey, valueTypes);
    });
}

// Build the right value control for a kv row based on its type.
// Calls onChange(newValue) whenever the value changes.
function _makeKvValueWidget(type, value, onChange) {
    if (type === 'Null') {
        const el = document.createElement('input');
        el.type = 'text'; el.className = 'form-input form-kv-cell'; el.disabled = true; el.value = '';
        return el;
    }
    if (type === 'Boolean') {
        const sel = document.createElement('select');
        sel.className = 'form-select form-kv-cell';
        for (const v of ['true', 'false']) {
            const opt = document.createElement('option');
            opt.value = v; opt.textContent = v;
            if (String(value) === v) opt.selected = true;
            sel.appendChild(opt);
        }
        sel.addEventListener('change', () => onChange(sel.value === 'true'));
        return sel;
    }
    if (type === 'Array') {
        const items = Array.isArray(value) ? value.map(i => ({ ...i })) : [];
        const wrap = document.createElement('div');
        wrap.className = 'form-kv-array-wrap';
        const renderItems = () => {
            wrap.innerHTML = '';
            items.forEach((item, i) => {
                const row = document.createElement('div');
                row.className = 'form-kv-array-row';
                const inp = document.createElement('input');
                inp.type = 'text'; inp.className = 'form-input form-kv-cell';
                inp.value = item.content ?? '';
                inp.addEventListener('input', () => {
                    items[i] = { type: 'String', content: inp.value };
                    onChange([...items]);
                });
                const del = document.createElement('button');
                del.className = 'action-icon-btn'; del.textContent = '×';
                del.addEventListener('click', () => {
                    items.splice(i, 1);
                    onChange([...items]);
                    renderItems();
                });
                row.appendChild(inp); row.appendChild(del);
                wrap.appendChild(row);
            });
            const addBtn = document.createElement('button');
            addBtn.className = 'form-kv-add-btn form-kv-array-add'; addBtn.textContent = '+ Add item';
            addBtn.addEventListener('click', () => {
                items.push({ type: 'String', content: '' });
                onChange([...items]);
                renderItems();
            });
            wrap.appendChild(addBtn);
        };
        renderItems();
        return wrap;
    }
    // Default: plain text input
    const el = document.createElement('input');
    el.type = 'text'; el.className = 'form-input form-kv-cell';
    el.value = value ?? '';
    el.addEventListener('input', () => onChange(el.value));
    return el;
}

function _addKvRow(tbody, activity, fieldKey, valueTypes, key, type, value) {
    const tr = document.createElement('tr');
    tr.dataset.currentKey = key;

    // Key
    const keyInput = document.createElement('input');
    keyInput.type = 'text'; keyInput.className = 'form-input form-kv-cell'; keyInput.value = key;
    const keyTd = document.createElement('td');
    keyTd.appendChild(keyInput); tr.appendChild(keyTd);

    // Type
    const typeSelect = document.createElement('select');
    typeSelect.className = 'form-select form-kv-cell';
    for (const t of valueTypes) {
        const opt = document.createElement('option');
        opt.value = t; opt.textContent = t;
        if (t === type) opt.selected = true;
        typeSelect.appendChild(opt);
    }
    const typeTd = document.createElement('td');
    typeTd.appendChild(typeSelect); tr.appendChild(typeTd);

    // Value — rebuilt when type changes
    const valTd = document.createElement('td');
    tr.appendChild(valTd);
    let currentValue = value;
    const syncData = () => {
        const oldKey = tr.dataset.currentKey;
        const newKey = keyInput.value.trim() || oldKey;
        const newType = typeSelect.value;
        const data = activity[fieldKey] || {};
        if (oldKey !== newKey) delete data[oldKey];
        data[newKey] = { type: newType, value: newType === 'Null' ? undefined : currentValue };
        activity[fieldKey] = data;
        tr.dataset.currentKey = newKey;
        markAsDirty();
    };
    const rebuildValueCell = (newType, newValue) => {
        valTd.innerHTML = '';
        valTd.appendChild(_makeKvValueWidget(newType, newValue, (v) => { currentValue = v; syncData(); }));
    };
    rebuildValueCell(type, value);

    // Delete
    const delBtn = document.createElement('button');
    delBtn.className = 'action-icon-btn'; delBtn.textContent = '×';
    const delTd = document.createElement('td');
    delTd.appendChild(delBtn); tr.appendChild(delTd);

    tbody.appendChild(tr);

    keyInput.addEventListener('input', syncData);
    typeSelect.addEventListener('change', () => {
        const newType = typeSelect.value;
        currentValue = newType === 'Boolean' ? true : newType === 'Array' ? [] : '';
        rebuildValueCell(newType, currentValue);
        syncData();
    });
    delBtn.addEventListener('click', () => {
        const data = activity[fieldKey] || {};
        delete data[tr.dataset.currentKey];
        activity[fieldKey] = data;
        markAsDirty(); tr.remove();
    });
}

// Read-only fallbacks for not-yet-editable types
function _buildReadOnlyGeneral(a, schema) {
    const common = schema?.commonProperties || {};
    let rows = _propRow('Name', a.name) + _propRow('Description', a.description || '—');
    if (common.state)   rows += _propRow('State', a.state || 'Activated');
    if (common.timeout) rows += _propRow('Timeout', a.timeout || '—');
    if (common.retry !== undefined) rows += _propRow('Retry', a.retry ?? 0);
    return rows || '<div class="empty-state">No general properties.</div>';
}

function _buildReadOnlySettings(a, schema) {
    const tp = schema?.typeProperties || {};
    let rows = '';
    for (const [key, def] of Object.entries(tp)) {
        if (def.type === 'containerActivities' || def.type === 'switchCases') continue;
        const val = a[key];
        if (val === undefined || val === null) continue;
        const display = typeof val === 'object' ? JSON.stringify(val) : String(val);
        rows += _propRow(def.label || key, display);
    }
    return rows || '<div class="empty-state">No settings to display.</div>';
}

// ─── Load pipeline from JSON ───────────────────────────────────────────────────
function loadPipelineFromJson(pipelineJson, flatActivities) {
    try {
        activities.forEach(a => a.remove());
        activities = [];
        connections = [];

        const src = pipelineJson.properties?.activities || pipelineJson.activities || [];
        pipelineData.name        = pipelineJson.name || '';
        pipelineData.description = pipelineJson.properties?.description || '';
        pipelineData.annotations = pipelineJson.properties?.annotations || [];
        pipelineData.parameters  = pipelineJson.properties?.parameters  || {};
        pipelineData.variables   = pipelineJson.properties?.variables   || {};
        pipelineData.concurrency = pipelineJson.properties?.concurrency || 1;

        const wrapper = document.getElementById('worldContainer');
        const activityMap = new Map();

        // flatActivities: pre-deserialized flat objects from the extension host (engine.deserializeActivity).
        // Falls back to raw ADF JSON for unsupported types (same index order as src).
        const flats = flatActivities || src;

        flats.forEach((flat, index) => {
            const cols = 3;
            const x = 100 + (index % cols) * 210;
            const y = 100 + Math.floor(index / cols) * 160;

            const a = new Activity(flat.type, x, y, wrapper);

            // Copy all flat fields onto the activity object.
            // For pre-deserialized objects these are clean UI fields (e.g. dynamicAllocation, minExecutors).
            // For raw pass-through objects (unsupported types) this mirrors the old Object.assign behaviour.
            for (const key of Object.keys(flat)) {
                if (key === 'id') continue; // keep Activity's own id
                a[key] = flat[key];
            }

            a.refreshNameLabel();

            if (a.isContainer) {
                const infoEl = a.element?.querySelector('[data-info-el]');
                if (infoEl) a._refreshContainerInfo(infoEl);
            }

            activities.push(a);
            activityMap.set(a.name, a);
        });

        // Build connections from dependsOn (use raw src which always has dependsOn)
        src.forEach(ad => {
            if (!ad.dependsOn?.length) return;
            const toActivity = activityMap.get(ad.name);
            if (!toActivity) return;
            ad.dependsOn.forEach(dep => {
                const fromActivity = activityMap.get(dep.activity);
                if (fromActivity) {
                    const cond = dep.dependencyConditions?.[0] || 'Succeeded';
                    connections.push(new Connection(fromActivity, toActivity, cond));
                }
            });
        });

        showProperties(null);
        log(`Loaded ${activities.length} activities from "${pipelineData.name}"`);
        setTimeout(fitToScreen, 0);

        isDirty = false;
        vscode.postMessage({ type: 'contentChanged', isDirty: false });
    } catch (err) {
        log('Error loading pipeline: ' + err.message);
    }
}

// ─── Message handler ───────────────────────────────────────────────────────────
// eslint-disable-next-line no-undef
window.addEventListener('message', event => {
    const msg = event.data;

    if (msg.type === 'initSchemas') {
        activitiesConfig  = msg.activitiesConfig  || { categories: [] };
        activitySchemas   = msg.activitySchemas   || {};
        copyActivityConfig = msg.copyActivityConfig || {};
        datasetList       = msg.datasetList       || [];
        datasetContents   = msg.datasetContents   || {};
        pipelineList      = msg.pipelineList      || [];
        datasetTypeCategories       = msg.activitySchemas?.__meta?.datasetTypeCategories       || {};
        locationTypeToStoreSettings  = msg.activitySchemas?.__meta?.locationTypeToStoreSettings  || {};
        datasetTypeToFormatSettings  = msg.activitySchemas?.__meta?.datasetTypeToFormatSettings  || {};
        window.linkedServicesList = msg.linkedServicesList || [];
        buildSidebar();
        log('Schemas loaded. Activities config categories: ' + activitiesConfig.categories.length);
    }

    if (msg.type === 'loadPipeline') {
        currentFilePath = msg.filePath || null;
        loadPipelineFromJson(msg.data, msg.flatActivities);
    }

    if (msg.type === 'saveResult') {
        const saveBtn = document.getElementById('saveBtn');
        saveBtn.disabled = false;
        saveBtn.textContent = 'Save';
        if (msg.success) markAsClean();
    }

    if (msg.type === 'addActivity') {
        const wrapper = document.getElementById('worldContainer');
        const a = new Activity(msg.activityType, 100, 100 + activities.length * 30, wrapper);
        activities.push(a);
        markAsDirty();
        draw();
    }
});
