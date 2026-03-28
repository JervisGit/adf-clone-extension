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
    'Lookup', 'Delete', 'Validation', 'GetMetadata',
    'SynapseNotebook', 'SparkJob', 'Script', 'SqlServerStoredProcedure',
    'WebActivity', 'WebHook', 'Copy']);

const vscode = acquireVsCodeApi();

// ─── Global state ──────────────────────────────────────────────────────────────
let activities = [];
let connections = [];
let selectedActivity = null;
let isDirty = false;
let currentFilePath = null;
// Sub-canvas navigation stack. Each frame: { activities, connections, parentActivity, branchKey, caseIndex, breadcrumbLabel }
let canvasStack = [];

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
let kvLinkedServiceList = [];
let credentialList = [];
let allLinkedServicesList = [];

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
    // Always cache current state so extension can save-on-close
    vscode.postMessage({
        type: 'cacheState',
        data: {
            pipelineData,
            activities: activities.map(toSaveData),
            connections: connections.map(c => ({ fromName: c.from.name, toName: c.to.name, condition: c.condition })),
        },
    });
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
            if (item.dataset.restricted === 'true') return;
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

// Returns a name based on `base` that doesn't already exist in the activities array.
function uniqueActivityName(base) {
    const existing = new Set(activities.map(a => a.name));
    if (!existing.has(base)) return base;
    let n = 1;
    while (existing.has(base + n)) n++;
    return base + n;
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

    refreshState() {
        const isInactive = this.state === 'Inactive' || this.state === 'Deactivated';
        this.element?.classList.toggle('inactive', isInactive);
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
        // Block drop of restricted types when inside a container
        if (getRestrictedTypesForCurrentCanvas().has(type)) {
            const parentType = canvasStack[canvasStack.length - 1]?.parentActivity?.type;
            vscode.postMessage({ type: 'alert', text: `"${type}" cannot be placed inside a "${parentType}" container.` });
            return;
        }
        const pos = screenToWorld(e.clientX, e.clientY);
        const a = new Activity(type, pos.x - 90, pos.y - 28, worldEl);
        a.name = uniqueActivityName(a.name);
        a.refreshNameLabel();
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
        const saveData = getSavePayload();
        vscode.postMessage({
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
        titleEl.textContent = pipelineData.name || 'Pipeline Properties';
        rightPanel.innerHTML = `
            <div class="form-row" style="margin-bottom:8px;">
                <label class="form-label" style="font-size:12px;">Name</label>
                <input id="pipelineNameInput" class="form-input" type="text" value="${escHtml(pipelineData.name || '')}" placeholder="Pipeline name" style="font-size:12px;">
            </div>
            <div class="form-row" style="margin-bottom:8px;">
                <label class="form-label" style="font-size:12px;">Description</label>
                <textarea id="pipelineDescInput" class="form-input" rows="3" placeholder="Pipeline description" style="font-size:12px;resize:vertical;">${escHtml(pipelineData.description || '')}</textarea>
            </div>`;
        document.getElementById('pipelineNameInput').addEventListener('input', e => {
            pipelineData.name = e.target.value;
            titleEl.textContent = pipelineData.name || 'Pipeline Properties';
            markAsDirty();
        });
        document.getElementById('pipelineDescInput').addEventListener('input', e => {
            pipelineData.description = e.target.value;
            markAsDirty();
        });

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
        if (activity.type === 'Copy') _detectCopyDatasetTypes(activity);

        // Build tabs from schema.tabs
        const tabs = schema.tabs || ['General', 'Settings'];
        tabsContainer.innerHTML = tabs.map((t, i) =>
            `<button class="config-tab${i === 0 ? ' active' : ''}" data-tab="v2-tab-${i}"${
                i === 0 ? ' style="border-bottom:2px solid var(--vscode-focusBorder);color:var(--vscode-tab-activeForeground);"' : ''
            }>${escHtml(t)}</button>`
        ).join('');
        panesContainer.innerHTML = tabs.map((t, i) => {
            const sharedCP = activitySchemas.__meta?.sharedCommonProperties;
            const html = t === 'General'
                ? _buildFormPane(activity, schema.commonProperties || {}, 'general', sharedCP)
                : t === 'Settings'
                    ? _buildFormPane(activity, schema.typeProperties || {}, 'settings')
                    : t === 'Source'
                        ? (activity.type === 'Copy' ? _buildCopyDatasetPane(activity, 'source') : _buildFormPane(activity, schema.sourceProperties || {}, 'source'))
                        : t === 'Sink'
                            ? (activity.type === 'Copy' ? _buildCopyDatasetPane(activity, 'sink') : _buildFormPane(activity, schema.sinkProperties || {}, 'sink'))
                            : t === 'Advanced'
                                ? _buildFormPane(activity, schema.advancedProperties || {}, 'advanced')
                                : t === 'Activities'
                                    ? _buildActivitiesTab(activity, schema)
                                    : `<div class="empty-state">Not yet implemented.</div>`;
            return `<div class="config-tab-pane${i === 0 ? ' active' : ''}" id="tab-v2-tab-${i}" style="display:${i === 0 ? 'block' : 'none'}">${html}</div>`;
        }).join('');
        // Wire all inputs to write back to the activity and mark dirty
        _wireFormInputs(panesContainer, activity);
        _wireWebHeaders(panesContainer, activity);
    _wireWebRefLists(panesContainer, activity);
    _wireKvFields(panesContainer, activity);
        _wireScriptArrayFields(panesContainer, activity);
        _wireFieldLists(panesContainer, activity);
        _wireNotebookSelects(panesContainer, activity);
        _wireAkvSecretFields(panesContainer, activity);
        if (activity.type === 'Copy') _wireCopyConfigFields(panesContainer, activity);
        _wireActivitiesTab(panesContainer, activity);
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
function _buildFormPane(activity, fields, paneId, sharedFields) {
    console.log(`[V2] _buildFormPane pane="${paneId}" activity="${activity.type}" fields:`, Object.fromEntries(Object.entries(fields).map(([k,d]) => [k, d.type])));
    // Merge shared fields after 'description' (or at start if no description exists)
    let mergedFields;
    if (sharedFields) {
        const newShared = Object.entries(sharedFields).filter(([k]) => !(k in fields));
        const entries = Object.entries(fields);
        const descIdx = entries.findIndex(([k]) => k === 'description');
        const insertAt = descIdx >= 0 ? descIdx + 1 : 0;
        entries.splice(insertAt, 0, ...newShared);
        mergedFields = Object.fromEntries(entries);
    } else {
        mergedFields = fields;
    }
    let html = '';
    for (const [key, def] of Object.entries(mergedFields)) {
        if (def.uiOnly && !def.type) continue; // skip sentinel-only marker fields (no renderable type)
        if (def.type === 'containerActivities' || def.type === 'switchCases') continue;
        const val = activity[key] ?? def.default ?? '';
        console.log(`[V2]   field "${key}" type="${def.type}" val=`, val);
        const cond = def.conditional ? `data-cond-field="${escHtml(def.conditional.field)}" data-cond-value="${escHtml(Array.isArray(def.conditional.value) ? def.conditional.value.join(',') : def.conditional.value)}"` : '';
        const nestedCond = def.nestedConditional ? `data-nested-cond-field="${escHtml(def.nestedConditional.field)}" data-nested-cond-value="${escHtml(Array.isArray(def.nestedConditional.value) ? def.nestedConditional.value.join(',') : def.nestedConditional.value)}"` : '';
        const excl = def.excludeConditional ? `data-cond-exclude-field="${escHtml(def.excludeConditional.field)}" data-cond-exclude-value="${escHtml(Array.isArray(def.excludeConditional.value) ? def.excludeConditional.value.join(',') : def.excludeConditional.value)}"` : '';
        const isBool = def.type === 'boolean';
        const isBlock = isBool || def.multiline || def.type === 'keyvalue' || def.type === 'getmetadata-fieldlist' || def.type === 'script-array' || def.type === 'storedprocedure-parameters' || def.type === 'web-headers' || def.type === 'web-dataset-list' || def.type === 'web-linkedservice-list' || def.type === 'akv-secret';
        html += `<div class="form-field${isBlock ? ' form-field--block' : ''}" data-field-key="${escHtml(key)}" ${cond} ${nestedCond} ${excl}>`;
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
                    (def.options || []).map(opt => {
                        const optCond = def.optionConditions?.[opt];
                        const condAttr = optCond
                            ? ` data-cond-field="${escHtml(optCond.field)}" data-cond-value="${escHtml(Array.isArray(optCond.value) ? optCond.value.join(',') : optCond.value)}"`
                            : '';
                        return `<option value="${escHtml(opt)}"${condAttr} ${String(val) === String(opt) ? 'selected' : ''}>${escHtml(def.optionLabels?.[opt] || opt)}</option>`;
                    }).join('') + `</select>`;
                break;
            case 'number':
                html += `<input type="number" class="form-input" data-key="${escHtml(key)}" value="${escHtml(String(val))}"${def.min != null ? ` min="${def.min}"` : ''}${def.max != null ? ` max="${def.max}"` : ''} placeholder="${escHtml(def.placeholder || '')}" />`;
                break;
            case 'keyvalue':
                html += `<div class="form-kv-container" data-kv-field="${escHtml(key)}" data-kv-types="${escHtml((def.valueTypes || []).join(','))}"${def.nullableValues ? ' data-kv-nullable="true"' : ''}${def.simplePairs ? ` data-kv-simple="true" data-kv-key-label="${escHtml(def.keyLabel || 'Key')}" data-kv-value-label="${escHtml(def.valueLabel || 'Value')}"` : ''}></div>`;
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
                const currentDs = (typeof val === 'string') ? val : (val?.referenceName ?? '');
                const _lookupExcluded = new Set((activitySchemas?.__meta?.lookupExcludedTypes) || ['Binary', 'Iceberg', 'Excel']);
                const filteredDs = def.datasetFilter === 'storageOnly'
                    ? datasetList.filter(d => datasetTypeCategories[datasetContents[d]?.properties?.type ?? ''] === 'storage')
                    : def.datasetFilter === 'lookupCompatible'
                    ? datasetList.filter(d => !_lookupExcluded.has(datasetContents[d]?.properties?.type ?? ''))
                    : (def.datasetFilter && typeof def.datasetFilter === 'object' && Array.isArray(def.datasetFilter.excludeTypes))
                    ? datasetList.filter(d => !def.datasetFilter.excludeTypes.includes(datasetContents[d]?.properties?.type ?? ''))
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
            case 'notebook-select': {
                const nbVal = typeof val === 'string' ? val : (val?.referenceName ?? '');
                const nbList = window.notebookList || [];
                const isCustom = !!nbVal && !nbList.includes(nbVal);
                html += `<div style="display:flex;gap:6px;align-items:center;">`
                    + `<select class="form-select form-nb-select" data-key="${escHtml(key)}" data-field-type="notebook-select" style="${isCustom ? 'width:auto;max-width:180px;flex-shrink:0;' : 'flex:1;'}">`
                    + `<option value="">-- Select notebook --</option>`
                    + nbList.map(n => `<option value="${escHtml(n)}"${n === nbVal ? ' selected' : ''}>${escHtml(n)}</option>`).join('')
                    + `<option value="__custom"${isCustom ? ' selected' : ''}>Custom...</option>`
                    + `</select>`
                    + `<input type="text" class="form-input form-nb-custom" data-nb-custom-for="${escHtml(key)}" value="${escHtml(isCustom ? nbVal : '')}" placeholder="Enter notebook name" style="flex:1;display:${isCustom ? 'block' : 'none'};" />`
                    + `</div>`;
                break;
            }
            case 'script-linkedservice':
            case 'storedprocedure-linkedservice': {
                const currentLs = val?.referenceName ?? (typeof val === 'string' ? val : '');
                html += `<select class="form-select" data-key="${escHtml(key)}" data-field-type="linkedservice">`
                    + `<option value="">-- Select linked service --</option>`
                    + (window.linkedServicesList || []).map(ls =>
                        `<option value="${escHtml(ls.name)}"${ls.name === currentLs ? ' selected' : ''}>${escHtml(ls.name)}</option>`
                    ).join('')
                    + `</select>`;
                break;
            }
            case 'storedprocedure-parameters': {
                const spTypes = 'String,Boolean,Datetime,Datetimeoffset,Decimal,Double,Guid,Int16,Int32,Int64,Single,Timespan,Byte[]';
                html += `<div class="form-kv-container" data-kv-field="${escHtml(key)}" data-kv-types="${escHtml(spTypes)}" data-kv-nullable="true"></div>`;
                break;
            }
            case 'web-headers': {
                const headers = Array.isArray(val) ? val : [];
                html += `<div class="form-web-headers" data-headers-key="${escHtml(key)}"><table class="form-kv-table" style="width:100%;font-size:11px;"><thead><tr><th>Name</th><th>Value</th><th></th></tr></thead><tbody>`;
                for (const h of headers) {
                    html += `<tr><td><input type="text" class="form-input form-kv-cell wh-name" placeholder="Header name" value="${escHtml(h.name || '')}" /></td><td><input type="text" class="form-input form-kv-cell wh-value" placeholder="Header value" value="${escHtml(h.value || '')}" /></td><td><button class="action-icon-btn wh-remove" type="button" title="Remove">×</button></td></tr>`;
                }
                html += `</tbody></table><button class="form-kv-add-btn wh-add" type="button">+ Add header</button></div>`;
                break;
            }
            case 'akv-secret': {
                // AKV secret reference: KV linked service dropdown + secret name + version
                const akv = (val && typeof val === 'object' && val.type === 'AzureKeyVaultSecret') ? val : null;
                const akvStore = akv?.store?.referenceName ?? '';
                const akvName = akv?.secretName ?? '';
                const akvVer = akv?.secretVersion ?? '';
                html += `<div class="form-akv-secret" data-akv-key="${escHtml(key)}">`;
                html += `<div style="display:flex;gap:6px;margin-bottom:4px;"><label style="font-size:11px;flex:1;">Key Vault linked service</label></div>`;
                html += `<select class="form-select akv-store" style="margin-bottom:4px;"><option value="">-- Select Key Vault --</option>`;
                for (const kv of kvLinkedServiceList) {
                    html += `<option value="${escHtml(kv)}"${kv === akvStore ? ' selected' : ''}>${escHtml(kv)}</option>`;
                }
                html += `</select>`;
                html += `<div style="display:flex;gap:6px;">`;
                html += `<input type="text" class="form-input akv-name" value="${escHtml(akvName)}" placeholder="Secret name" style="flex:1;" />`;
                html += `<input type="text" class="form-input akv-version" value="${escHtml(akvVer)}" placeholder="Version (optional)" style="flex:0 0 130px;" />`;
                html += `</div></div>`;
                break;
            }
            case 'web-credential': {
                // Credential reference dropdown filtered by credentialFilter
                const filter = def.credentialFilter || '';
                const currentCred = typeof val === 'string' ? val : '';
                const filtered = credentialList.filter(c => !filter || c.type === filter);
                html += `<select class="form-select" data-key="${escHtml(key)}" data-field-type="web-credential">`;
                html += `<option value="">-- Select credential --</option>`;
                for (const c of filtered) {
                    html += `<option value="${escHtml(c.name)}"${c.name === currentCred ? ' selected' : ''}>${escHtml(c.name)}</option>`;
                }
                html += `</select>`;
                break;
            }
            case 'web-dataset-list': {
                const datasets = Array.isArray(val) ? val.map(d => d?.referenceName ?? d) : [];
                html += `<div class="form-web-reflist" data-reflist-key="${escHtml(key)}" data-reflist-type="dataset">`;
                for (const d of datasets) {
                    html += `<div class="form-web-reflist-row"><select class="form-select form-web-reflist-select"><option value="">-- Select dataset --</option>${datasetList.map(ds => `<option value="${escHtml(ds)}"${ds === d ? ' selected' : ''}>${escHtml(ds)}</option>`).join('')}</select><button class="action-icon-btn form-web-reflist-remove" type="button" title="Remove">×</button></div>`;
                }
                html += `<button class="form-kv-add-btn form-web-reflist-add" type="button">+ Add dataset</button></div>`;
                break;
            }
            case 'web-linkedservice-list': {
                const linkedServices = Array.isArray(val) ? val.map(ls => ls?.referenceName ?? ls) : [];
                html += `<div class="form-web-reflist" data-reflist-key="${escHtml(key)}" data-reflist-type="linkedservice">`;
                for (const ls of linkedServices) {
                    html += `<div class="form-web-reflist-row"><select class="form-select form-web-reflist-select"><option value="">-- Select linked service --</option>${allLinkedServicesList.map(s => `<option value="${escHtml(s.name)}"${s.name === ls ? ' selected' : ''}>${escHtml(s.name)}</option>`).join('')}</select><button class="action-icon-btn form-web-reflist-remove" type="button" title="Remove">×</button></div>`;
                }
                html += `<button class="form-kv-add-btn form-web-reflist-add" type="button">+ Add linked service</button></div>`;
                break;
            }
            case 'script-array': {
                const scripts = Array.isArray(val) ? val : [];
                html += `<div class="form-script-array" data-script-key="${escHtml(key)}"><div class="form-script-rows">`;
                scripts.forEach((s, si) => {
                    html += `<div class="form-script-row" style="border:1px solid var(--vscode-panel-border);border-radius:3px;padding:6px;margin-bottom:6px;">`
                        + `<div style="display:flex;gap:12px;margin-bottom:4px;align-items:center;">`
                        + `<label style="display:flex;align-items:center;gap:4px;cursor:pointer;font-size:12px;"><input type="radio" class="form-script-type" name="stype-${escHtml(key)}-${si}" value="Query"${s.type !== 'NonQuery' ? ' checked' : ''}> Query</label>`
                        + `<label style="display:flex;align-items:center;gap:4px;cursor:pointer;font-size:12px;"><input type="radio" class="form-script-type" name="stype-${escHtml(key)}-${si}" value="NonQuery"${s.type === 'NonQuery' ? ' checked' : ''}> NonQuery</label>`
                        + `<button class="form-script-remove action-icon-btn" style="margin-left:auto;" title="Remove">×</button>`
                        + `</div><textarea class="form-textarea form-script-text" rows="3" placeholder="Enter SQL script...">${escHtml(s.text || '')}</textarea></div>`;
                });
                html += `</div><button class="form-script-add form-kv-add-btn" style="margin-top:4px;">+ Add script</button></div>`;
                break;
            }
            case 'text':
            case 'string':
            default:
                if (def.multiline) {
                    html += `<textarea class="form-textarea" data-key="${escHtml(key)}" rows="${def.rows ?? 2}" placeholder="${escHtml(def.placeholder || '')}">${escHtml(String(val))}</textarea>`;
                } else {
                    html += `<input type="text" class="form-input" data-key="${escHtml(key)}" value="${escHtml(String(val))}" placeholder="${escHtml(def.placeholder || '')}"${def.readonly ? ' readonly style="opacity:0.6;cursor:default;"' : ''} />`;
                }
                break;
        }
        if (def.helpText) html += `<div class="form-help">${escHtml(def.helpText)}</div>`;
        html += `</div>`;
    }
    return html || '<div class="empty-state">No fields.</div>';
}

// Render the Activities tab for container types — shows nested activity lists with Edit buttons.
function _buildActivitiesTab(activity, schema) {
    const containerFields = Object.entries(schema.typeProperties || {})
        .filter(([, def]) => def.type === 'containerActivities' || def.type === 'switchCases');

    if (containerFields.length === 0) return '<div class="empty-state">No nested activities.</div>';

    const BRANCH_LABELS = { activities: 'Activities', ifTrueActivities: 'True Branch', ifFalseActivities: 'False Branch', defaultActivities: 'Default' };

    let html = '';
    for (const [key, def] of containerFields) {
        const items = activity[key] || [];
        html += `<div style="font-weight:600;margin:8px 0 4px;font-size:12px;">${escHtml(def.label || key)}</div>`;
        if (def.type === 'switchCases') {
            if (items.length === 0 && !activity.defaultActivities?.length) {
                html += '<div class="empty-state" style="margin-bottom:8px;">No cases defined.</div>';
            } else {
                for (let i = 0; i < items.length; i++) {
                    const c = items[i];
                    const count = c.activities?.length ?? 0;
                    html += `<div class="container-branch-row">
                        <span class="container-activity-badge">Case: ${escHtml(String(c.value ?? ''))}</span>
                        <span class="container-branch-count">${count} activit${count === 1 ? 'y' : 'ies'}</span>
                        <button class="form-kv-add-btn enter-subcanvas-btn" data-branch-type="switchCase" data-case-index="${i}">Edit ▶</button>
                    </div>`;
                }
                const defCount = activity.defaultActivities?.length ?? 0;
                html += `<div class="container-branch-row">
                    <span class="container-activity-badge">Default</span>
                    <span class="container-branch-count">${defCount} activit${defCount === 1 ? 'y' : 'ies'}</span>
                    <button class="form-kv-add-btn enter-subcanvas-btn" data-branch-type="defaultActivities">Edit ▶</button>
                </div>`;
            }
        } else {
            const count = items.length;
            html += `<div class="container-branch-row">
                <span class="container-branch-count">${count} activit${count === 1 ? 'y' : 'ies'}</span>
                <button class="form-kv-add-btn enter-subcanvas-btn" data-branch-type="${escHtml(key)}"
                    title="Edit ${escHtml(BRANCH_LABELS[key] || key)}">Edit ▶</button>
            </div>`;
            if (items.length > 0) {
                html += '<div class="container-activity-list" style="margin-top:4px;">';
                for (const a of items) {
                    html += `<div class="container-activity-item"><span class="container-activity-badge">${escHtml(a.type || '?')}</span> ${escHtml(a.name || '(unnamed)')}</div>`;
                }
                html += '</div>';
            } else {
                html += '<div class="empty-state" style="margin-top:4px;">No activities yet.</div>';
            }
        }
    }
    return html;
}

// Wire the Edit ▶ buttons in the Activities tab to enter sub-canvas navigation.
function _wireActivitiesTab(container, activity) {
    const BRANCH_LABELS = { activities: 'Activities', ifTrueActivities: 'True Branch', ifFalseActivities: 'False Branch', defaultActivities: 'Default' };
    container.querySelectorAll('.enter-subcanvas-btn').forEach(btn => {
        const branchType = btn.dataset.branchType;
        const caseIdx = btn.dataset.caseIndex !== undefined ? parseInt(btn.dataset.caseIndex) : null;
        btn.addEventListener('click', () => {
            let flatNested, branchKey, branchLabel;
            if (branchType === 'switchCase') {
                flatNested  = activity.cases[caseIdx]?.activities || [];
                branchKey   = null;
                branchLabel = `Case: ${escHtml(String(activity.cases[caseIdx]?.value ?? ''))}`;
            } else {
                flatNested  = activity[branchType] || [];
                branchKey   = branchType;
                branchLabel = BRANCH_LABELS[branchType] || branchType;
            }
            enterSubCanvas(flatNested, activity, branchKey, caseIdx, `${activity.name} › ${branchLabel}`);
        });
    });
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
    // Apply defaults from shared common fields (state, onInactiveMarkAs)
    const shared = activitySchemas.__meta?.sharedCommonProperties;
    if (shared) {
        for (const [key, def] of Object.entries(shared)) {
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

// ─── Copy Activity support ─────────────────────────────────────────────────────

// Detect and cache _sourceDatasetType, _sinkDatasetType, _sourceLocationType,
// _sinkLocationType for Copy activities and, on first open, parse the stashed
// raw source/sink objects into flat src_*/snk_* form fields.
function _detectCopyDatasetTypes(activity) {
    const srcName = typeof activity.sourceDataset === 'object'
        ? activity.sourceDataset?.referenceName : activity.sourceDataset;
    const snkName = typeof activity.sinkDataset === 'object'
        ? activity.sinkDataset?.referenceName : activity.sinkDataset;

    if (srcName && datasetContents[srcName]) {
        const props = datasetContents[srcName].properties || {};
        activity._sourceDatasetType    = props.type || '';
        activity._sourceLocationType   = props.typeProperties?.location?.type || '';
    }
    if (snkName && datasetContents[snkName]) {
        const props = datasetContents[snkName].properties || {};
        activity._sinkDatasetType  = props.type || '';
        activity._sinkLocationType = props.typeProperties?.location?.type || '';
    }

    // On first open: parse raw source/sink objects → flat src_*/snk_* fields
    if (activity._sourceObject && activity._sourceDatasetType && !activity._srcParsed) {
        const typeConf = copyActivityConfig.datasetTypes?.[activity._sourceDatasetType];
        if (typeConf) Object.assign(activity, _parseCopyObjToForm(activity._sourceObject, typeConf, 'source'));
        activity._srcParsed = true;
    }
    if (activity._sinkObject && activity._sinkDatasetType && !activity._snkParsed) {
        const typeConf = copyActivityConfig.datasetTypes?.[activity._sinkDatasetType];
        if (typeConf) Object.assign(activity, _parseCopyObjToForm(activity._sinkObject, typeConf, 'sink'));
        activity._snkParsed = true;
    }
}

// Webview-side equivalent of parseCopySourceToForm / parseCopySinkToForm.
// Reads jsonPath values from obj and returns a flat {fieldKey: value} map.
function _parseCopyObjToForm(obj, typeConf, side) {
    const result = {};
    const fields = typeConf.fields?.[side] || {};
    function _getByDotPath(o, path) {
        let cur = o;
        for (const seg of path.split('.')) {
            if (cur == null) return undefined;
            cur = cur[seg];
        }
        return cur;
    }
    for (const [fieldKey, fieldConf] of Object.entries(fields)) {
        if (!fieldConf.jsonPath) continue;
        const value = _getByDotPath(obj, fieldConf.jsonPath);
        if (value !== undefined) result[fieldKey] = value;
        else if (fieldConf.default !== undefined) result[fieldKey] = fieldConf.default;
    }
    return result;
}

// Apply field defaults from copyActivityConfig to the activity object for a given side.
// Must be called before rendering config fields so that conditional visibility works from first paint.
// Skips any field already set on the activity (only fills in undefined).
function _applyCopyConfigDefaults(activity, typeConf, side) {
    const fields = typeConf?.fields?.[side] || {};
    for (const [key, def] of Object.entries(fields)) {
        if (activity[key] === undefined && def.default !== undefined) {
            activity[key] = def.default;
        }
    }
}

// Build the Source or Sink pane for a Copy activity:
//   1. Schema-driven dataset picker (from sourceProperties / sinkProperties)
//   2. Config-driven fields from copyActivityConfig.datasetTypes[type].fields.source|sink
function _buildCopyDatasetPane(activity, side) {
    const schema = activitySchemas[activity.type] || {};
    const schemaProps = side === 'source' ? (schema.sourceProperties || {}) : (schema.sinkProperties || {});
    const paneId = side;
    // Part 1: dataset picker (reuse _buildFormPane for the schema-driven part)
    let html = _buildFormPane(activity, schemaProps, paneId);

    // Part 2: config-driven fields (only when dataset type is known)
    const dsType = side === 'source' ? activity._sourceDatasetType : activity._sinkDatasetType;
    const typeConf = dsType ? copyActivityConfig.datasetTypes?.[dsType] : null;
    if (typeConf) {
        // Apply config field defaults so conditionals (e.g. src_filePathType, src_namespaces) work on first render
        _applyCopyConfigDefaults(activity, typeConf, side);
        const configFields = typeConf.fields?.[side] || {};
        html += `<div class="copy-config-section" data-copy-side="${escHtml(side)}">`;
        html += `<div class="copy-config-section-title">${escHtml(typeConf.name || dsType)} — ${side === 'source' ? 'Source' : 'Sink'} Settings</div>`;
        html += _buildCopyConfigFields(activity, configFields, paneId);
        html += `</div>`;
    } else if (!dsType) {
        html += `<div class="form-help" style="margin-top:8px;">Select a dataset above to see ${side} settings.</div>`;
    } else {
        html += `<div class="form-help" style="margin-top:8px;">Dataset type "${escHtml(dsType)}" is not yet supported for Copy activity editing.</div>`;
    }
    return html;
}

// Build config-driven field HTML from a copyActivityConfig fields block.
// Supports all copy-specific field types in addition to standard ones.
function _buildCopyConfigFields(activity, fields, paneId) {
    let html = '';
    for (const [key, def] of Object.entries(fields)) {
        if (!def.type) continue;
        const val = activity[key] !== undefined ? activity[key] : (def.default !== undefined ? def.default : '');
        // Build conditional attributes
        const condAttr   = def.conditional
            ? `data-cond-field="${escHtml(def.conditional.field)}" data-cond-value="${escHtml(Array.isArray(def.conditional.value) ? def.conditional.value.join(',') : String(def.conditional.value))}"${def.conditional.notEmpty ? ' data-cond-not-empty="true"' : ''}`
            : '';
        const nestedAttr = def.nestedConditional
            ? `data-nested-cond-field="${escHtml(def.nestedConditional.field)}" data-nested-cond-value="${escHtml(Array.isArray(def.nestedConditional.value) ? def.nestedConditional.value.join(',') : String(def.nestedConditional.value))}"`
            : '';
        const condAllAttr = def.conditionalAll
            ? `data-cond-all="${escHtml(JSON.stringify(def.conditionalAll))}"`
            : '';
        const isBool  = def.type === 'boolean';
        const isBlock = isBool || def.type === 'additional-columns' || def.type === 'string-list'
            || def.type === 'copy-sp-parameters' || def.type === 'copy-cmd-default-values'
            || def.type === 'copy-cmd-additional-options' || def.type === 'namespace-prefixes'
            || def.multiline;

        html += `<div class="form-field${isBlock ? ' form-field--block' : ''}" data-field-key="${escHtml(key)}" ${condAttr} ${nestedAttr} ${condAllAttr}>`;
        if (!isBool) {
            html += `<label class="form-label">${escHtml(def.label || key)}${def.required ? ' <span style="color:var(--vscode-errorForeground)">*</span>' : ''}</label>`;
        }

        switch (def.type) {
            case 'boolean':
                html += `<label class="form-checkbox-label">${escHtml(def.label || key)}<input type="checkbox" class="form-checkbox" data-key="${escHtml(key)}" ${val ? 'checked' : ''} /></label>`;
                break;
            case 'radio': {
                const radioOptions = def.options || [];
                const radioValues  = def.optionValues || radioOptions;
                html += `<div class="form-radio-group">`;
                for (let ri = 0; ri < radioOptions.length; ri++) {
                    const rv = radioValues[ri];
                    const oc = def.optionConditionals?.[String(rv)];
                    const ocAttr = oc ? ` data-cond-field="${escHtml(oc.field)}" data-cond-value="${escHtml(Array.isArray(oc.value) ? oc.value.join(',') : String(oc.value))}"` : '';
                    const isDisabled = def.disabledOptionValues?.includes(rv) ? ' disabled' : '';
                    html += `<label class="form-radio-label"${ocAttr}><input type="radio" name="${escHtml(paneId + '-' + key)}" value="${escHtml(String(rv))}" ${String(val) === String(rv) ? 'checked' : ''}${isDisabled} data-key="${escHtml(key)}" />${escHtml(String(radioOptions[ri]))}</label>`;
                }
                html += `</div>`;
                break;
            }
            case 'select':
                html += `<select class="form-select" data-key="${escHtml(key)}">`;
                (def.options || []).forEach(opt => {
                    const ov = def.optionValues ? def.optionValues[def.options.indexOf(opt)] : opt;
                    html += `<option value="${escHtml(String(ov))}"${String(val) === String(ov) ? ' selected' : ''}>${escHtml(opt)}</option>`;
                });
                html += `</select>`;
                break;
            case 'number':
                html += `<input type="number" class="form-input" data-key="${escHtml(key)}" value="${escHtml(String(val))}"${def.min != null ? ` min="${def.min}"` : ''}${def.max != null ? ` max="${def.max}"` : ''} placeholder="${escHtml(def.placeholder || '')}"${def.readonly ? ' readonly style="opacity:0.6;cursor:default;"' : ''} />`;
                break;
            case 'textarea':
            case 'text':
            case 'string':
                if (def.multiline || def.type === 'textarea') {
                    html += `<textarea class="form-textarea" data-key="${escHtml(key)}" rows="3" placeholder="${escHtml(def.placeholder || '')}">${escHtml(String(val))}</textarea>`;
                } else {
                    html += `<input type="text" class="form-input" data-key="${escHtml(key)}" value="${escHtml(String(val))}" placeholder="${escHtml(def.placeholder || '')}"${def.readonly ? ' readonly style="opacity:0.6;cursor:default;"' : ''} />`;
                }
                break;
            case 'datetime': {
                let dtVal = '';
                if (val) dtVal = String(val).replace('Z', '').substring(0, 16);
                html += `<input type="datetime-local" class="form-input" data-key="${escHtml(key)}" data-field-type="datetime" value="${escHtml(dtVal)}" />`;
                break;
            }
            case 'additional-columns': {
                const rows = Array.isArray(val) ? val : [];
                const dfltRowVal = def.defaultRowValue || '';
                html += `<div class="form-copy-addcol" data-addcol-key="${escHtml(key)}" data-addcol-default="${escHtml(dfltRowVal)}">`;
                html += `<table class="form-kv-table"><thead><tr><th>Name</th><th>Value</th><th></th></tr></thead><tbody>`;
                for (const row of rows) {
                    html += `<tr>`
                        + `<td><input type="text" class="form-input form-kv-cell addcol-name" value="${escHtml(row.name || '')}" placeholder="Column name" /></td>`
                        + `<td><input type="text" class="form-input form-kv-cell addcol-value" value="${escHtml(row.value || dfltRowVal)}" placeholder="${escHtml(dfltRowVal || 'Value or expression')}" /></td>`
                        + `<td><button class="action-icon-btn addcol-remove" type="button" title="Remove">×</button></td></tr>`;
                }
                html += `</tbody></table><button class="form-kv-add-btn addcol-add" type="button">+ Add column</button></div>`;
                break;
            }
            case 'string-list': {
                const items = Array.isArray(val) ? val : [];
                html += `<div class="form-copy-strlist" data-strlist-key="${escHtml(key)}">`;
                for (const item of items) {
                    html += `<div class="copy-strlist-row" style="display:flex;gap:4px;margin-bottom:3px;">`
                        + `<input type="text" class="form-input strlist-item" value="${escHtml(item || '')}" placeholder="Enter value" style="flex:1;" />`
                        + `<button class="action-icon-btn strlist-remove" type="button" title="Remove">×</button></div>`;
                }
                html += `<button class="form-kv-add-btn strlist-add" type="button">+ Add</button></div>`;
                break;
            }
            case 'copy-sp-parameters': {
                const spParams = (val && typeof val === 'object' && !Array.isArray(val)) ? val : {};
                const spTypes = 'String,Boolean,Datetime,Datetimeoffset,Decimal,Double,Guid,Int16,Int32,Int64,Single,Timespan,Byte[]';
                html += `<div class="form-copy-spparams" data-spparams-key="${escHtml(key)}">`;
                html += `<table class="form-kv-table"><thead><tr><th>Name</th><th>Value</th><th>Type</th><th></th></tr></thead><tbody>`;
                for (const [pName, pDef] of Object.entries(spParams)) {
                    const pVal  = (typeof pDef === 'object' && pDef !== null) ? (pDef.value ?? '') : pDef;
                    const pType = (typeof pDef === 'object' && pDef !== null) ? (pDef.type ?? 'String') : 'String';
                    html += `<tr>`
                        + `<td><input type="text" class="form-input form-kv-cell spp-name" value="${escHtml(pName)}" placeholder="Param name" /></td>`
                        + `<td><input type="text" class="form-input form-kv-cell spp-value" value="${escHtml(String(pVal))}" placeholder="Value" /></td>`
                        + `<td><select class="form-select spp-type">${spTypes.split(',').map(t => `<option value="${escHtml(t)}"${t === pType ? ' selected' : ''}>${escHtml(t)}</option>`).join('')}</select></td>`
                        + `<td><button class="action-icon-btn spp-remove" type="button" title="Remove">×</button></td></tr>`;
                }
                html += `</tbody></table><button class="form-kv-add-btn spp-add" type="button">+ Add parameter</button></div>`;
                break;
            }
            case 'copy-cmd-default-values': {
                const cmdDefs = Array.isArray(val) ? val : [];
                html += `<div class="form-copy-cmddef" data-cmddef-key="${escHtml(key)}">`;
                html += `<table class="form-kv-table"><thead><tr><th>Column name</th><th>Default value</th><th></th></tr></thead><tbody>`;
                for (const row of cmdDefs) {
                    html += `<tr>`
                        + `<td><input type="text" class="form-input form-kv-cell cmddef-col" value="${escHtml(row.columnName || '')}" placeholder="Column name" /></td>`
                        + `<td><input type="text" class="form-input form-kv-cell cmddef-val" value="${escHtml(row.defaultValue || '')}" placeholder="Default value" /></td>`
                        + `<td><button class="action-icon-btn cmddef-remove" type="button" title="Remove">×</button></td></tr>`;
                }
                html += `</tbody></table><button class="form-kv-add-btn cmddef-add" type="button">+ Add</button></div>`;
                break;
            }
            case 'copy-cmd-additional-options': {
                const opts = (val && typeof val === 'object' && !Array.isArray(val)) ? val : {};
                html += `<div class="form-copy-cmdopts" data-cmdopts-key="${escHtml(key)}">`;
                html += `<table class="form-kv-table"><thead><tr><th>Key</th><th>Value</th><th></th></tr></thead><tbody>`;
                for (const [k, v] of Object.entries(opts)) {
                    html += `<tr>`
                        + `<td><input type="text" class="form-input form-kv-cell cmdopt-key" value="${escHtml(k)}" placeholder="Key" /></td>`
                        + `<td><input type="text" class="form-input form-kv-cell cmdopt-val" value="${escHtml(String(v || ''))}" placeholder="Value" /></td>`
                        + `<td><button class="action-icon-btn cmdopt-remove" type="button" title="Remove">×</button></td></tr>`;
                }
                html += `</tbody></table><button class="form-kv-add-btn cmdopt-add" type="button">+ Add</button></div>`;
                break;
            }
            case 'namespace-prefixes': {
                const nsp = (val && typeof val === 'object' && !Array.isArray(val)) ? val : {};
                html += `<div class="form-copy-nspfx" data-nspfx-key="${escHtml(key)}">`;
                html += `<table class="form-kv-table"><thead><tr><th>Prefix</th><th>Namespace URI</th><th></th></tr></thead><tbody>`;
                for (const [prefix, ns] of Object.entries(nsp)) {
                    html += `<tr>`
                        + `<td><input type="text" class="form-input form-kv-cell nspfx-prefix" value="${escHtml(prefix)}" placeholder="Prefix" /></td>`
                        + `<td><input type="text" class="form-input form-kv-cell nspfx-ns" value="${escHtml(String(ns || ''))}" placeholder="Namespace URI" /></td>`
                        + `<td><button class="action-icon-btn nspfx-remove" type="button" title="Remove">×</button></td></tr>`;
                }
                html += `</tbody></table><button class="form-kv-add-btn nspfx-add" type="button">+ Add</button></div>`;
                break;
            }
            default:
                html += `<input type="text" class="form-input" data-key="${escHtml(key)}" value="${escHtml(String(val))}" placeholder="${escHtml(def.placeholder || '')}" />`;
                break;
        }
        if (def.helpText) html += `<div class="form-help">${escHtml(def.helpText)}</div>`;
        html += `</div>`;
    }
    return html || '<div class="empty-state">No settings for this dataset type.</div>';
}

// Wire all Copy-config dynamic field types (additional-columns, string-list, etc.)
// and standard inputs inside the Copy config sections.
function _wireCopyConfigFields(container, activity) {
    // 1. Standard inputs in copy-config sections (text, number, select, radio, boolean, datetime)
    container.querySelectorAll('.copy-config-section input[data-key], .copy-config-section select[data-key], .copy-config-section textarea[data-key]').forEach(el => {
        const key = el.dataset.key;
        if (!key) return;
        const tag  = el.tagName.toLowerCase();
        const evt  = (tag === 'input' && (el.type === 'checkbox' || el.type === 'radio')) ? 'change'
                   : tag === 'select' ? 'change' : 'input';
        el.addEventListener(evt, () => {
            let value;
            if (tag === 'input' && el.type === 'checkbox') value = el.checked;
            else if (tag === 'input' && el.type === 'number') value = el.value === '' ? '' : Number(el.value);
            else if (el.dataset.fieldType === 'datetime') value = el.value ? el.value + ':00Z' : '';
            else value = el.value;
            activity[key] = value;
            markAsDirty();
            _applyCopyConditionals(container, activity);
        });
    });

    // 2. additional-columns widgets
    container.querySelectorAll('.form-copy-addcol').forEach(el => {
        const key = el.dataset.addcolKey;
        if (!key) return;
        const dflt = el.dataset.addcolDefault || '';
        const syncModel = () => {
            const rows = [];
            el.querySelectorAll('tbody tr').forEach(tr => {
                const n = tr.querySelector('.addcol-name')?.value?.trim() || '';
                const v = tr.querySelector('.addcol-value')?.value || '';
                rows.push({ name: n, value: v });
            });
            activity[key] = rows; markAsDirty();
        };
        el.querySelector('.addcol-add')?.addEventListener('click', () => {
            const tbody = el.querySelector('tbody');
            const tr = document.createElement('tr');
            tr.innerHTML = `<td><input type="text" class="form-input form-kv-cell addcol-name" placeholder="Column name" /></td>`
                + `<td><input type="text" class="form-input form-kv-cell addcol-value" value="${escHtml(dflt)}" placeholder="${escHtml(dflt || 'Value or expression')}" /></td>`
                + `<td><button class="action-icon-btn addcol-remove" type="button" title="Remove">×</button></td>`;
            tbody.appendChild(tr);
            tr.querySelector('.addcol-remove').addEventListener('click', () => { tr.remove(); syncModel(); });
            tr.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
            syncModel();
        });
        el.querySelectorAll('.addcol-remove').forEach(btn => btn.addEventListener('click', () => { btn.closest('tr').remove(); syncModel(); }));
        el.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
    });

    // 3. string-list widgets
    container.querySelectorAll('.form-copy-strlist').forEach(el => {
        const key = el.dataset.strlistKey;
        if (!key) return;
        const syncModel = () => {
            activity[key] = Array.from(el.querySelectorAll('.strlist-item')).map(i => i.value);
            markAsDirty();
        };
        el.querySelector('.strlist-add')?.addEventListener('click', () => {
            const div = document.createElement('div');
            div.className = 'copy-strlist-row';
            div.style.cssText = 'display:flex;gap:4px;margin-bottom:3px;';
            div.innerHTML = `<input type="text" class="form-input strlist-item" placeholder="Enter value" style="flex:1;" /><button class="action-icon-btn strlist-remove" type="button" title="Remove">×</button>`;
            el.insertBefore(div, el.querySelector('.strlist-add'));
            div.querySelector('.strlist-remove').addEventListener('click', () => { div.remove(); syncModel(); });
            div.querySelector('.strlist-item').addEventListener('input', syncModel);
            syncModel();
        });
        el.querySelectorAll('.strlist-remove').forEach(btn => btn.addEventListener('click', () => { btn.closest('.copy-strlist-row').remove(); syncModel(); }));
        el.querySelectorAll('.strlist-item').forEach(i => i.addEventListener('input', syncModel));
    });

    // 4. copy-sp-parameters widgets
    container.querySelectorAll('.form-copy-spparams').forEach(el => {
        const key = el.dataset.spparamsKey;
        if (!key) return;
        const spTypes = 'String,Boolean,Datetime,Datetimeoffset,Decimal,Double,Guid,Int16,Int32,Int64,Single,Timespan,Byte[]';
        const syncModel = () => {
            const result = {};
            el.querySelectorAll('tbody tr').forEach(tr => {
                const n  = tr.querySelector('.spp-name')?.value || '';
                const v  = tr.querySelector('.spp-value')?.value || '';
                const t  = tr.querySelector('.spp-type')?.value || 'String';
                result[n] = { value: v, type: t };
            });
            activity[key] = result; markAsDirty();
        };
        el.querySelector('.spp-add')?.addEventListener('click', () => {
            const tbody = el.querySelector('tbody');
            const tr = document.createElement('tr');
            tr.innerHTML = `<td><input type="text" class="form-input form-kv-cell spp-name" placeholder="Param name" /></td>`
                + `<td><input type="text" class="form-input form-kv-cell spp-value" placeholder="Value" /></td>`
                + `<td><select class="form-select spp-type">${spTypes.split(',').map(t => `<option value="${escHtml(t)}">${escHtml(t)}</option>`).join('')}</select></td>`
                + `<td><button class="action-icon-btn spp-remove" type="button" title="Remove">×</button></td>`;
            tbody.appendChild(tr);
            tr.querySelector('.spp-remove').addEventListener('click', () => { tr.remove(); syncModel(); });
            tr.querySelectorAll('input,select').forEach(i => i.addEventListener('change', syncModel));
            tr.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
            syncModel();
        });
        el.querySelectorAll('.spp-remove').forEach(btn => btn.addEventListener('click', () => { btn.closest('tr').remove(); syncModel(); }));
        el.querySelectorAll('input,select').forEach(i => { i.addEventListener('change', syncModel); i.addEventListener('input', syncModel); });
    });

    // 5. copy-cmd-default-values widgets
    container.querySelectorAll('.form-copy-cmddef').forEach(el => {
        const key = el.dataset.cmddefKey;
        if (!key) return;
        const syncModel = () => {
            const rows = [];
            el.querySelectorAll('tbody tr').forEach(tr => {
                const col = tr.querySelector('.cmddef-col')?.value || '';
                const val = tr.querySelector('.cmddef-val')?.value || '';
                rows.push({ columnName: col, defaultValue: val });
            });
            activity[key] = rows; markAsDirty();
        };
        el.querySelector('.cmddef-add')?.addEventListener('click', () => {
            const tbody = el.querySelector('tbody');
            const tr = document.createElement('tr');
            tr.innerHTML = `<td><input type="text" class="form-input form-kv-cell cmddef-col" placeholder="Column name" /></td>`
                + `<td><input type="text" class="form-input form-kv-cell cmddef-val" placeholder="Default value" /></td>`
                + `<td><button class="action-icon-btn cmddef-remove" type="button" title="Remove">×</button></td>`;
            tbody.appendChild(tr);
            tr.querySelector('.cmddef-remove').addEventListener('click', () => { tr.remove(); syncModel(); });
            tr.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
            syncModel();
        });
        el.querySelectorAll('.cmddef-remove').forEach(btn => btn.addEventListener('click', () => { btn.closest('tr').remove(); syncModel(); }));
        el.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
    });

    // 6. copy-cmd-additional-options widgets
    container.querySelectorAll('.form-copy-cmdopts').forEach(el => {
        const key = el.dataset.cmdoptsKey;
        if (!key) return;
        const syncModel = () => {
            const result = {};
            el.querySelectorAll('tbody tr').forEach(tr => {
                const k = tr.querySelector('.cmdopt-key')?.value || '';
                const v = tr.querySelector('.cmdopt-val')?.value || '';
                if (k.trim()) result[k] = v;
            });
            activity[key] = result; markAsDirty();
        };
        el.querySelector('.cmdopt-add')?.addEventListener('click', () => {
            const tbody = el.querySelector('tbody');
            const tr = document.createElement('tr');
            tr.innerHTML = `<td><input type="text" class="form-input form-kv-cell cmdopt-key" placeholder="Key" /></td>`
                + `<td><input type="text" class="form-input form-kv-cell cmdopt-val" placeholder="Value" /></td>`
                + `<td><button class="action-icon-btn cmdopt-remove" type="button" title="Remove">×</button></td>`;
            tbody.appendChild(tr);
            tr.querySelector('.cmdopt-remove').addEventListener('click', () => { tr.remove(); syncModel(); });
            tr.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
        });
        el.querySelectorAll('.cmdopt-remove').forEach(btn => btn.addEventListener('click', () => { btn.closest('tr').remove(); syncModel(); }));
        el.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
    });

    // 7. namespace-prefixes widgets
    container.querySelectorAll('.form-copy-nspfx').forEach(el => {
        const key = el.dataset.nspfxKey;
        if (!key) return;
        const syncModel = () => {
            const result = {};
            el.querySelectorAll('tbody tr').forEach(tr => {
                const p  = tr.querySelector('.nspfx-prefix')?.value || '';
                const ns = tr.querySelector('.nspfx-ns')?.value || '';
                if (p.trim()) result[p] = ns;
            });
            activity[key] = result; markAsDirty();
        };
        el.querySelector('.nspfx-add')?.addEventListener('click', () => {
            const tbody = el.querySelector('tbody');
            const tr = document.createElement('tr');
            tr.innerHTML = `<td><input type="text" class="form-input form-kv-cell nspfx-prefix" placeholder="Prefix" /></td>`
                + `<td><input type="text" class="form-input form-kv-cell nspfx-ns" placeholder="Namespace URI" /></td>`
                + `<td><button class="action-icon-btn nspfx-remove" type="button" title="Remove">×</button></td>`;
            tbody.appendChild(tr);
            tr.querySelector('.nspfx-remove').addEventListener('click', () => { tr.remove(); syncModel(); });
            tr.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
        });
        el.querySelectorAll('.nspfx-remove').forEach(btn => btn.addEventListener('click', () => { btn.closest('tr').remove(); syncModel(); }));
        el.querySelectorAll('input').forEach(i => i.addEventListener('input', syncModel));
    });

    // Apply initial conditional visibility for copy config fields
    _applyCopyConditionals(container, activity);
}

// Apply conditional visibility rules for copy config section fields.
// Extends _applyConditionals with conditionalAll and notEmpty support.
function _applyCopyConditionals(container, activity) {
    // Standard conditionals (including notEmpty)
    container.querySelectorAll('.copy-config-section .form-field[data-cond-field]').forEach(el => {
        const field    = el.dataset.condField;
        const allowed  = el.dataset.condValue.split(',');
        const notEmpty = el.dataset.condNotEmpty === 'true';
        const cur      = activity[field];
        let visible;
        if (notEmpty) {
            visible = cur !== undefined && cur !== null && cur !== '';
        } else {
            visible = allowed.includes(String(cur ?? ''));
        }
        if (visible && el.dataset.nestedCondField) {
            const nField   = el.dataset.nestedCondField;
            const nAllowed = el.dataset.nestedCondValue.split(',');
            if (!nAllowed.includes(String(activity[nField] ?? ''))) visible = false;
        }
        el.style.display = visible ? '' : 'none';
    });
    // conditionalAll (AND logic)
    container.querySelectorAll('.copy-config-section .form-field[data-cond-all]').forEach(el => {
        let conditions;
        try { conditions = JSON.parse(el.dataset.condAll); } catch { conditions = []; }
        const visible = conditions.every(c => {
            const v = activity[c.field];
            return Array.isArray(c.value) ? c.value.includes(v) : v === c.value;
        });
        el.style.display = visible ? '' : 'none';
    });
    // Radio option visibility (optionConditionals)
    container.querySelectorAll('.copy-config-section .form-radio-label[data-cond-field]').forEach(el => {
        const field   = el.dataset.condField;
        const allowed = el.dataset.condValue.split(',');
        el.style.display = allowed.includes(String(activity[field] ?? '')) ? '' : 'none';
    });
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
            } else if (el.dataset.fieldType === 'web-credential') {
                // Credential reference: store the plain name string
                value = el.value || '';
            } else if (el.dataset.fieldType === 'expression') {
                // Re-wrap into ADF Expression object
                value = { value: el.value, type: 'Expression' };
            } else if (el.dataset.fieldType === 'pipeline') {
                // Re-wrap into ADF PipelineReference object
                value = el.value ? { referenceName: el.value, type: 'PipelineReference' } : null;
            } else if (el.dataset.fieldType === 'dataset') {
                // Copy activities store sourceDataset/sinkDataset as plain strings (engine uses string directly).
                // All other dataset fields use the full DatasetReference object.
                const isCopyDataset = activity.type === 'Copy' && (key === 'sourceDataset' || key === 'sinkDataset');
                value = isCopyDataset
                    ? (el.value || null)
                    : (el.value ? { referenceName: el.value, type: 'DatasetReference' } : null);
                // Update _datasetCategory/_datasetType so conditional fields show/hide correctly
                const dsType = el.value ? (datasetContents[el.value]?.properties?.type ?? '') : '';
                activity._datasetCategory = datasetTypeCategories[dsType] ?? '';
                activity._datasetType = dsType;
                const locType = el.value ? (datasetContents[el.value]?.properties?.typeProperties?.location?.type ?? '') : '';
                activity._storeSettingsType = locationTypeToStoreSettings[locType] ?? '';
                activity._formatSettingsType = datasetTypeToFormatSettings[dsType] ?? '';
                // Copy activity: also update _sourceDatasetType / _sinkDatasetType and re-render the config section
                if (activity.type === 'Copy') {
                    if (key === 'sourceDataset') {
                        // Reset stale src_* fields from previous dataset type
                        const prevSrcType = activity._sourceDatasetType;
                        if (prevSrcType && prevSrcType !== dsType) {
                            const prevConf = copyActivityConfig.datasetTypes?.[prevSrcType];
                            if (prevConf) for (const fk of Object.keys(prevConf.fields?.source || {})) delete activity[fk];
                        }
                        activity._sourceDatasetType  = dsType;
                        activity._sourceLocationType = locType;
                        activity._srcParsed = false;
                        activity._sourceObject = null;
                    } else if (key === 'sinkDataset') {
                        const prevSnkType = activity._sinkDatasetType;
                        if (prevSnkType && prevSnkType !== dsType) {
                            const prevConf = copyActivityConfig.datasetTypes?.[prevSnkType];
                            if (prevConf) for (const fk of Object.keys(prevConf.fields?.sink || {})) delete activity[fk];
                        }
                        activity._sinkDatasetType  = dsType;
                        activity._sinkLocationType = locType;
                        activity._snkParsed = false;
                        activity._sinkObject = null;
                    }
                    // Re-render the Source or Sink pane with the new config section
                    const side = key === 'sourceDataset' ? 'source' : 'sink';
                    const paneEl = el.closest('.config-tab-pane');
                    if (paneEl) {
                        activity[key] = value; // must set before re-render so picker shows selection
                        markAsDirty();
                        paneEl.innerHTML = _buildCopyDatasetPane(activity, side);
                        _wireFormInputs(paneEl, activity);
                        _wireCopyConfigFields(paneEl, activity);
                        return;
                    }
                }
                // When switching away from sql, reset sql-specific fields that would leave stale conditionals
                if (activity._datasetCategory !== 'sql') {
                    activity.partitionOption = 'None';
                    delete activity.partitionColumnName;
                    delete activity.partitionUpperBound;
                    delete activity.partitionLowerBound;
                    delete activity.isolationLevel;
                    delete activity.useQuery;
                    delete activity.sqlReaderQuery;
                    delete activity.sqlReaderStoredProcedureName;
                    delete activity.storedProcedureParameters;
                }
                // When switching away from storage, reset storage-specific fields
                if (activity._datasetCategory !== 'storage') {
                    activity.filePathType = 'filePathInDataset';
                    delete activity.prefix;
                    delete activity.wildcardFolderPath;
                    delete activity.wildcardFileName;
                    delete activity.fileListPath;
                    delete activity.enablePartitionDiscovery;
                    delete activity.partitionRootPath;
                }
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
            } else if (el.dataset.fieldType === 'linkedservice') {
                value = el.value ? { referenceName: el.value, type: 'LinkedServiceReference' } : null;
            } else {
                value = el.value;
            }
            activity[key] = value;
            // Mirror driverSize from executorSize for SynapseNotebook
            if (key === 'executorSize') {
                activity.driverSize = value;
                const driverInput = container.querySelector('[data-key="driverSize"]');
                if (driverInput) driverInput.value = value;
            }
            // Update canvas name label if name field changed
            if (key === 'name') {
                activity.refreshNameLabel();
                document.getElementById('propertiesPanelTitle').textContent = value;
            }
            // Update inactive shading when state changes
            if (key === 'state') {
                activity.refreshState();
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
        let visible = allowed.includes(current);
        // Check nestedConditional if present (both must be met to show the field)
        if (visible && el.dataset.nestedCondField) {
            const nField = el.dataset.nestedCondField;
            const nAllowed = el.dataset.nestedCondValue.split(',');
            if (!nAllowed.includes(String(activity[nField] ?? ''))) visible = false;
        }
        // Also check excludeConditional — hide if the exclude field matches
        if (visible && el.dataset.condExcludeField) {
            const exField = el.dataset.condExcludeField;
            const exValues = el.dataset.condExcludeValue.split(',');
            if (exValues.includes(String(activity[exField] ?? ''))) visible = false;
        }
        el.style.display = visible ? '' : 'none';
    });
    // Also hide form fields that have only excludeConditional (no main cond-field)
    container.querySelectorAll('.form-field[data-cond-exclude-field]:not([data-cond-field])').forEach(el => {
        const exField = el.dataset.condExcludeField;
        const exValues = el.dataset.condExcludeValue.split(',');
        el.style.display = exValues.includes(String(activity[exField] ?? '')) ? 'none' : '';
    });
    // Hide/show individual radio options that have per-option conditions (e.g. Prefix for Blob only)
    container.querySelectorAll('.form-radio-label[data-cond-field]').forEach(el => {
        const field = el.dataset.condField;
        const allowed = el.dataset.condValue.split(',');
        const current = String(activity[field] ?? '');
        el.style.display = allowed.includes(current) ? '' : 'none';
    });
    // Hide/show individual <option> elements in selects that have per-option conditions
    container.querySelectorAll('select option[data-cond-field]').forEach(opt => {
        const field = opt.dataset.condField;
        const allowed = opt.dataset.condValue.split(',');
        const current = String(activity[field] ?? '');
        const visible = allowed.includes(current);
        opt.style.display = visible ? '' : 'none';
        opt.disabled = !visible;
    });
    // If any select's currently selected option is now hidden, reset to first visible option
    container.querySelectorAll('select').forEach(sel => {
        const selectedOpt = sel.options[sel.selectedIndex];
        if (selectedOpt && (selectedOpt.style.display === 'none' || selectedOpt.disabled)) {
            const firstVisible = Array.from(sel.options).find(o => o.style.display !== 'none' && !o.disabled);
            if (firstVisible) {
                sel.value = firstVisible.value;
                const key = sel.dataset.key;
                if (key) {
                    activity[key] = firstVisible.value;
                    markAsDirty();
                }
            }
        }
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

function _wireNotebookSelects(container, activity) {
    container.querySelectorAll('.form-nb-select').forEach(sel => {
        const key = sel.dataset.key;
        if (!key) return;
        const customInput = container.querySelector(`.form-nb-custom[data-nb-custom-for="${key}"]`);
        const applyLayout = () => {
            const isCustom = sel.value === '__custom';
            if (isCustom) {
                sel.style.flex = ''; sel.style.width = 'auto'; sel.style.maxWidth = '180px'; sel.style.flexShrink = '0';
            } else {
                sel.style.flex = '1'; sel.style.width = ''; sel.style.maxWidth = ''; sel.style.flexShrink = '';
            }
            if (customInput) customInput.style.display = isCustom ? 'block' : 'none';
        };
        const syncValue = () => {
            const v = sel.value === '__custom' ? (customInput?.value?.trim() ?? '') : sel.value;
            activity[key] = v;
            markAsDirty();
        };
        sel.addEventListener('change', () => { applyLayout(); syncValue(); });
        if (customInput) customInput.addEventListener('input', syncValue);
    });
}

const _SCRIPT_PARAM_TYPES = ['Boolean','Byte[]','Datetime','Datetimeoffset','Decimal','Double','Guid','Int16','Int32','Int64','Single','String','Timespan'];

function _wireScriptArrayFields(container, activity) {
    container.querySelectorAll('.form-script-array').forEach(scriptArray => {
        const key = scriptArray.dataset.scriptKey;
        if (!key) return;

        function readBack() {
            activity[key] = Array.from(scriptArray.querySelectorAll('.form-script-row')).map(row => {
                const entry = {
                    type: row.querySelector('.form-script-type:checked')?.value || 'Query',
                    text: row.querySelector('.form-script-text').value,
                };
                if (row._scriptParams && row._scriptParams.length > 0) entry.parameters = row._scriptParams;
                return entry;
            });
            markAsDirty();
        }

        function buildParamsUI(row) {
            const old = row.querySelector('.form-script-params-section');
            if (old) old.remove();

            const section = document.createElement('div');
            section.className = 'form-script-params-section';
            section.style.cssText = 'margin-top:8px;border-top:1px solid var(--vscode-panel-border);padding-top:6px;';
            const hdr = document.createElement('div');
            hdr.style.cssText = 'display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;';
            hdr.innerHTML = '<span style="font-size:11px;font-weight:600;">Parameters</span>';
            const addParamBtn = document.createElement('button');
            addParamBtn.className = 'form-kv-add-btn'; addParamBtn.type = 'button';
            addParamBtn.textContent = '+ Add parameter';
            hdr.appendChild(addParamBtn); section.appendChild(hdr);

            const table = document.createElement('table');
            table.className = 'form-kv-table'; table.style.cssText = 'width:100%;font-size:11px;';
            table.innerHTML = '<thead><tr><th>Name</th><th>Type</th><th>Value</th><th>Null</th><th>Direction</th><th>Size</th><th></th></tr></thead>';
            const tbody = document.createElement('tbody');
            table.appendChild(tbody); section.appendChild(table);

            const syncParams = () => {
                row._scriptParams = Array.from(tbody.querySelectorAll('tr')).map(tr => {
                    const type = tr.querySelector('.sp-type').value, dir = tr.querySelector('.sp-direction').value;
                    const isNull = tr.querySelector('.sp-null').checked, rawSize = tr.querySelector('.sp-size').value;
                    const obj = { name: tr.querySelector('.sp-name').value, type, value: isNull ? null : tr.querySelector('.sp-value').value, direction: dir };
                    if ((dir === 'Output' || dir === 'InputOutput') && (type === 'String' || type === 'Byte[]') && rawSize) obj.size = parseInt(rawSize, 10);
                    return obj;
                });
                readBack();
            };

            const addParamRow = (p) => {
                p = p || { name: '', type: 'String', value: '', direction: 'Input' };
                const isByte = p.type === 'Byte[]', isNull = p.value === null;
                const showSize = (p.direction === 'Output' || p.direction === 'InputOutput') && (p.type === 'String' || isByte);
                const tr = document.createElement('tr');

                const nameIn     = _appendTd(tr, _mkInput('text',   'sp-name',      p.name || '',        'Name'));
                const typeSelect = _appendTd(tr, _mkSelect(          'sp-type',      _SCRIPT_PARAM_TYPES, p.type || 'String'));
                const valIn      = _appendTd(tr, _mkInput('text',    'sp-value',     isNull ? '' : (p.value ?? ''), 'Value'));
                if (isNull) valIn.disabled = true;
                const nullCk = document.createElement('input'); nullCk.type = 'checkbox'; nullCk.className = 'sp-null'; if (isNull) nullCk.checked = true;
                _appendTd(tr, nullCk, 'text-align:center');
                const dirSel     = _appendTd(tr, _mkSelect(          'sp-direction', ['Input', 'Output', 'InputOutput'], p.direction || 'Input'));
                if (isByte) { dirSel.options[0].disabled = true; dirSel.options[2].disabled = true; }
                const sizeIn     = _appendTd(tr, _mkInput('number',  'sp-size',      p.size ?? '',        'Size'));
                sizeIn.style.width = '60px';
                if (!showSize) { sizeIn.disabled = true; sizeIn.style.opacity = '0.4'; }
                const rmBtn = document.createElement('button');
                rmBtn.className = 'action-icon-btn'; rmBtn.type = 'button'; rmBtn.title = 'Remove'; rmBtn.textContent = '×';
                _appendTd(tr, rmBtn);

                const updateSize = () => {
                    const en = (dirSel.value === 'Output' || dirSel.value === 'InputOutput') && (typeSelect.value === 'String' || typeSelect.value === 'Byte[]');
                    sizeIn.disabled = !en; sizeIn.style.opacity = en ? '' : '0.4'; if (!en) sizeIn.value = '';
                };
                const updateDir = () => {
                    const b = typeSelect.value === 'Byte[]';
                    dirSel.options[0].disabled = b; dirSel.options[2].disabled = b;
                    if (b && (dirSel.value === 'Input' || dirSel.value === 'InputOutput')) dirSel.value = 'Output';
                };
                nullCk.addEventListener('change', () => { valIn.disabled = nullCk.checked; if (nullCk.checked) valIn.value = ''; syncParams(); });
                typeSelect.addEventListener('change', () => { updateDir(); updateSize(); syncParams(); });
                dirSel.addEventListener('change', () => { updateSize(); syncParams(); });
                [nameIn, valIn, sizeIn].forEach(el => el.addEventListener('input', syncParams));
                rmBtn.addEventListener('click', () => { tr.remove(); syncParams(); });
                tbody.appendChild(tr);
            };

            for (const p of (row._scriptParams || [])) addParamRow(p);
            addParamBtn.addEventListener('click', () => { addParamRow(null); syncParams(); });
            row.appendChild(section);
        }

        function wireRow(row, existingParams) {
            row._scriptParams = existingParams || [];
            row.querySelectorAll('.form-script-type').forEach(r => r.addEventListener('change', readBack));
            row.querySelector('.form-script-text').addEventListener('input', readBack);
            row.querySelector('.form-script-remove').addEventListener('click', () => { row.remove(); readBack(); });
            buildParamsUI(row);
        }

        scriptArray.querySelectorAll('.form-script-row').forEach((row, ri) => {
            const existing = (activity[key] || [])[ri]?.parameters || [];
            wireRow(row, existing);
        });

        scriptArray.querySelector('.form-script-add').addEventListener('click', () => {
            const rowsEl = scriptArray.querySelector('.form-script-rows');
            const uid = key + '-' + Date.now();
            const row = document.createElement('div');
            row.className = 'form-script-row';
            row.style.cssText = 'border:1px solid var(--vscode-panel-border);border-radius:3px;padding:6px;margin-bottom:6px;';
            row.innerHTML = `<div style="display:flex;gap:12px;margin-bottom:4px;align-items:center;">` +
                `<label style="display:flex;align-items:center;gap:4px;cursor:pointer;font-size:12px;"><input type="radio" class="form-script-type" name="stype-${uid}" value="Query" checked> Query</label>` +
                `<label style="display:flex;align-items:center;gap:4px;cursor:pointer;font-size:12px;"><input type="radio" class="form-script-type" name="stype-${uid}" value="NonQuery"> NonQuery</label>` +
                `<button class="form-script-remove action-icon-btn" style="margin-left:auto;" title="Remove">\u00d7</button>` +
                `</div><textarea class="form-textarea form-script-text" rows="3" placeholder="Enter SQL script..."></textarea>`;
            rowsEl.appendChild(row);
            wireRow(row, []);
            readBack();
        });
    });
}

// ─── Web headers field wiring ──────────────────────────────────────────────────
function _wireWebHeaders(container, activity) {
    container.querySelectorAll('.form-web-headers').forEach(el => {
        const key = el.dataset.headersKey;
        if (!key) return;
        if (!Array.isArray(activity[key])) activity[key] = [];

        const syncHeaders = () => {
            activity[key] = Array.from(el.querySelectorAll('tbody tr')).map(tr => ({
                name:  tr.querySelector('.wh-name').value,
                value: tr.querySelector('.wh-value').value,
            }));
            markAsDirty();
        };

        el.querySelectorAll('tbody tr').forEach(tr => {
            tr.querySelector('.wh-name').addEventListener('input', syncHeaders);
            tr.querySelector('.wh-value').addEventListener('input', syncHeaders);
            tr.querySelector('.wh-remove').addEventListener('click', () => { tr.remove(); syncHeaders(); });
        });

        el.querySelector('.wh-add').addEventListener('click', () => {
            const tbody = el.querySelector('tbody');
            const tr = document.createElement('tr');
            tr.innerHTML = `<td><input type="text" class="form-input form-kv-cell wh-name" placeholder="Header name" /></td><td><input type="text" class="form-input form-kv-cell wh-value" placeholder="Header value" /></td><td><button class="action-icon-btn wh-remove" type="button" title="Remove">×</button></td>`;
            tbody.appendChild(tr);
            tr.querySelector('.wh-name').addEventListener('input', syncHeaders);
            tr.querySelector('.wh-value').addEventListener('input', syncHeaders);
            tr.querySelector('.wh-remove').addEventListener('click', () => { tr.remove(); syncHeaders(); });
            syncHeaders();
        });
    });
}

// ─── Web dataset/linked-service reference list wiring ─────────────────────────
function _wireWebRefLists(container, activity) {
    container.querySelectorAll('.form-web-reflist').forEach(el => {
        const key = el.dataset.reflistKey;
        const refType = el.dataset.reflistType; // 'dataset' or 'linkedservice'
        if (!key) return;
        if (!Array.isArray(activity[key])) activity[key] = [];

        const syncList = () => {
            activity[key] = Array.from(el.querySelectorAll('.form-web-reflist-select'))
                .map(sel => sel.value)
                .filter(v => v)
                .map(v => ({ referenceName: v, type: refType === 'dataset' ? 'DatasetReference' : 'LinkedServiceReference' }));
            markAsDirty();
        };

        el.querySelectorAll('.form-web-reflist-row').forEach(row => {
            row.querySelector('.form-web-reflist-select').addEventListener('change', syncList);
            row.querySelector('.form-web-reflist-remove').addEventListener('click', () => { row.remove(); syncList(); });
        });

        el.querySelector('.form-web-reflist-add').addEventListener('click', () => {
            const opts = refType === 'dataset'
                ? datasetList.map(d => `<option value="${escHtml(d)}">${escHtml(d)}</option>`).join('')
                : allLinkedServicesList.map(s => `<option value="${escHtml(s.name)}">${escHtml(s.name)}</option>`).join('');
            const row = document.createElement('div');
            row.className = 'form-web-reflist-row';
            row.innerHTML = `<select class="form-select form-web-reflist-select"><option value="">-- Select --</option>${opts}</select><button class="action-icon-btn form-web-reflist-remove" type="button" title="Remove">×</button>`;
            el.insertBefore(row, el.querySelector('.form-web-reflist-add'));
            row.querySelector('.form-web-reflist-select').addEventListener('change', syncList);
            row.querySelector('.form-web-reflist-remove').addEventListener('click', () => { row.remove(); syncList(); });
            syncList();
        });
    });
}

// ─── AKV secret field wiring ──────────────────────────────────────────────────
function _wireAkvSecretFields(container, activity) {
    container.querySelectorAll('.form-akv-secret').forEach(el => {
        const key = el.dataset.akvKey;
        if (!key) return;

        const storeSelect = el.querySelector('.akv-store');
        const nameInput = el.querySelector('.akv-name');
        const versionInput = el.querySelector('.akv-version');

        const sync = () => {
            const store = storeSelect.value;
            const secretName = nameInput.value.trim();
            const secretVersion = versionInput.value.trim();
            if (!store && !secretName) {
                activity[key] = '';
            } else {
                const obj = {
                    type: 'AzureKeyVaultSecret',
                    store: { referenceName: store, type: 'LinkedServiceReference' },
                    secretName: secretName,
                };
                if (secretVersion) obj.secretVersion = secretVersion;
                activity[key] = obj;
            }
            markAsDirty();
        };

        storeSelect.addEventListener('change', sync);
        nameInput.addEventListener('input', sync);
        versionInput.addEventListener('input', sync);
    });
}

// ─── Key-value field renderer ────────────────────────────────────────────────────
// Used for SetVariable's "Pipeline return value" returnValues field.
function _wireKvFields(container, activity) {
    container.querySelectorAll('.form-kv-container').forEach(kvEl => {
        const fieldKey = kvEl.dataset.kvField;
        const simple = kvEl.dataset.kvSimple === 'true';
        if (!activity[fieldKey] || typeof activity[fieldKey] !== 'object' || Array.isArray(activity[fieldKey])) {
            activity[fieldKey] = {};
        }
        if (simple) {
            const keyLabel = kvEl.dataset.kvKeyLabel || 'Key';
            const valueLabel = kvEl.dataset.kvValueLabel || 'Value';
            _renderSimplePairsField(kvEl, activity, fieldKey, keyLabel, valueLabel);
        } else {
            const valueTypes = (kvEl.dataset.kvTypes || '').split(',').filter(Boolean);
            const nullable = kvEl.dataset.kvNullable === 'true';
            _renderKvField(kvEl, activity, fieldKey, valueTypes, nullable);
        }
    });
}

function _renderSimplePairsField(kvEl, activity, fieldKey, keyLabel, valueLabel) {
    kvEl.innerHTML = '';
    const data = activity[fieldKey] || {};
    const table = document.createElement('table');
    table.className = 'form-kv-table';
    const thead = document.createElement('thead');
    thead.innerHTML = `<tr><th>${escHtml(keyLabel)}</th><th>${escHtml(valueLabel)}</th><th></th></tr>`;
    table.appendChild(thead);
    const tbody = document.createElement('tbody');
    table.appendChild(tbody);
    kvEl.appendChild(table);
    for (const [k, v] of Object.entries(data)) {
        _addSimplePairsRow(tbody, activity, fieldKey, keyLabel, valueLabel, k, v);
    }
    const addBtn = document.createElement('button');
    addBtn.className = 'form-kv-add-btn';
    addBtn.textContent = '+ Add';
    kvEl.appendChild(addBtn);
    addBtn.addEventListener('click', () => {
        const base = keyLabel.toLowerCase();
        let newKey = base;
        let i = 1;
        while (newKey in (activity[fieldKey] || {})) newKey = base + i++;
        if (!activity[fieldKey]) activity[fieldKey] = {};
        activity[fieldKey][newKey] = '';
        markAsDirty();
        _renderSimplePairsField(kvEl, activity, fieldKey, keyLabel, valueLabel);
    });
}

function _addSimplePairsRow(tbody, activity, fieldKey, keyLabel, valueLabel, key, value) {
    const tr = document.createElement('tr');
    tr.dataset.currentKey = key;
    const keyInput = document.createElement('input');
    keyInput.type = 'text'; keyInput.className = 'form-input form-kv-cell';
    keyInput.value = key; keyInput.placeholder = keyLabel;
    const keyTd = document.createElement('td'); keyTd.appendChild(keyInput); tr.appendChild(keyTd);
    const valInput = document.createElement('input');
    valInput.type = 'text'; valInput.className = 'form-input form-kv-cell';
    valInput.value = value ?? ''; valInput.placeholder = valueLabel;
    const valTd = document.createElement('td'); valTd.appendChild(valInput); tr.appendChild(valTd);
    const syncData = () => {
        const oldKey = tr.dataset.currentKey;
        const newKey = keyInput.value;
        const data = activity[fieldKey] || {};
        if (oldKey !== newKey && oldKey !== '') delete data[oldKey];
        if (newKey !== '') data[newKey] = valInput.value;
        activity[fieldKey] = data;
        tr.dataset.currentKey = newKey;
        markAsDirty();
    };
    const delBtn = document.createElement('button');
    delBtn.className = 'action-icon-btn'; delBtn.textContent = '\u00d7';
    const delTd = document.createElement('td'); delTd.appendChild(delBtn); tr.appendChild(delTd);
    tbody.appendChild(tr);
    keyInput.addEventListener('input', syncData);
    valInput.addEventListener('input', syncData);
    delBtn.addEventListener('click', () => {
        const data = activity[fieldKey] || {};
        delete data[tr.dataset.currentKey];
        activity[fieldKey] = data;
        markAsDirty(); tr.remove();
    });
}

function _renderKvField(kvEl, activity, fieldKey, valueTypes, nullable) {
    kvEl.innerHTML = '';
    const data = activity[fieldKey] || {};

    const table = document.createElement('table');
    table.className = 'form-kv-table';
    const thead = document.createElement('thead');
    thead.innerHTML = nullable
        ? '<tr><th>Key</th><th>Type</th><th>Value</th><th>Null</th><th></th></tr>'
        : '<tr><th>Key</th><th>Type</th><th>Value</th><th></th></tr>';
    table.appendChild(thead);
    const tbody = document.createElement('tbody');
    table.appendChild(tbody);
    kvEl.appendChild(table);

    for (const [k, item] of Object.entries(data)) {
        _addKvRow(tbody, activity, fieldKey, valueTypes, k, item.type || 'String', item.value, nullable);
    }

    const addBtn = document.createElement('button');
    addBtn.className = 'form-kv-add-btn';
    addBtn.textContent = '+ Add';
    kvEl.appendChild(addBtn);
    addBtn.addEventListener('click', () => {
        if (!activity[fieldKey]) activity[fieldKey] = {};
        activity[fieldKey][''] = { type: valueTypes[0] || 'String', value: '' };
        markAsDirty();
        _renderKvField(kvEl, activity, fieldKey, valueTypes, nullable);
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

// ─── Shared micro DOM helpers ─────────────────────────────────────────────────
function _mkInput(type, cls, val, placeholder) {
    const el = document.createElement('input');
    el.type = type; el.className = 'form-input form-kv-cell' + (cls ? ' ' + cls : '');
    el.value = val ?? ''; if (placeholder) el.placeholder = placeholder;
    return el;
}
function _mkSelect(cls, options, current) {
    const sel = document.createElement('select');
    sel.className = 'form-select form-kv-cell' + (cls ? ' ' + cls : '');
    for (const opt of options) {
        const o = document.createElement('option'); o.value = opt; o.textContent = opt;
        if (opt === current) o.selected = true; sel.appendChild(o);
    }
    return sel;
}
function _appendTd(tr, el, tdStyle) {
    const td = document.createElement('td'); if (tdStyle) td.style.cssText = tdStyle;
    td.appendChild(el); tr.appendChild(td); return el;
}

function _addKvRow(tbody, activity, fieldKey, valueTypes, key, type, value, nullable) {
    const tr = document.createElement('tr');
    tr.dataset.currentKey = key;

    const keyInput   = _appendTd(tr, _mkInput('text', '', key));
    const typeSelect = _appendTd(tr, _mkSelect('', valueTypes, type));

    // Value cell — rebuilt when type or null changes
    const valTd = document.createElement('td'); tr.appendChild(valTd);
    let isNull = nullable && value === null;
    let currentValue = isNull ? null : value;
    const syncData = () => {
        const oldKey = tr.dataset.currentKey;
        const newKey = keyInput.value.trim();
        const newType = typeSelect.value;
        const data = activity[fieldKey] || {};
        if (oldKey !== newKey) delete data[oldKey];
        if (newKey === '') {
            // Remove the entry and the row if key is empty
            delete data[oldKey];
            activity[fieldKey] = data;
            markAsDirty();
            tr.remove();
            return;
        }
        // Keep '' key in memory so validation can block save; serializer strips it before writing
        data[newKey] = { type: newType, value: newType === 'Null' ? undefined : currentValue };
        activity[fieldKey] = data;
        tr.dataset.currentKey = newKey;
        markAsDirty();
    };
    const rebuildValueCell = (newType, newValue) => {
        valTd.innerHTML = '';
        if (isNull) {
            const dis = _mkInput('text', '', ''); dis.disabled = true; valTd.appendChild(dis);
        } else {
            valTd.appendChild(_makeKvValueWidget(newType, newValue, (v) => { currentValue = v; syncData(); }));
        }
    };
    rebuildValueCell(type, value);

    if (nullable) {
        const nullCheck = document.createElement('input');
        nullCheck.type = 'checkbox'; nullCheck.title = 'Treat as null'; nullCheck.checked = isNull;
        nullCheck.style.cssText = 'margin:0 auto;display:block;';
        _appendTd(tr, nullCheck, 'text-align:center');
        nullCheck.addEventListener('change', () => {
            isNull = nullCheck.checked;
            currentValue = isNull ? null : (typeSelect.value === 'Boolean' ? true : typeSelect.value === 'Array' ? [] : '');
            rebuildValueCell(typeSelect.value, currentValue);
            syncData();
        });
    }

    const delBtn = document.createElement('button');
    delBtn.className = 'action-icon-btn'; delBtn.textContent = '×';
    _appendTd(tr, delBtn);
    tbody.appendChild(tr);

    keyInput.addEventListener('input', syncData);
    typeSelect.addEventListener('change', () => {
        const newType = typeSelect.value;
        if (!isNull) currentValue = newType === 'Boolean' ? true : newType === 'Array' ? [] : '';
        rebuildValueCell(newType, currentValue);
        syncData();
    });
    delBtn.addEventListener('click', () => {
        const data = activity[fieldKey] || {};
        delete data[tr.dataset.currentKey];
        activity[fieldKey] = data; markAsDirty(); tr.remove();
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
// Compute left-to-right dependency layout.
// Returns Map<name, {x,y}> where column = dependency depth (0 = no deps), row = order within column.
function computeLayout(flats, src) {
    const COL_W = 240, ROW_H = 160, START_X = 80, START_Y = 80;
    const upstreamOf = new Map(src.map(ad => [ad.name, (ad.dependsOn || []).map(d => d.activity)]));
    // Iterative longest-path: propagate columns until stable
    const col = new Map(flats.map(f => [f.name, 0]));
    for (let pass = 0; pass < flats.length; pass++) {
        let changed = false;
        for (const ad of src) {
            const ups = upstreamOf.get(ad.name) || [];
            if (!ups.length) continue;
            const needed = Math.max(...ups.map(u => (col.get(u) ?? 0))) + 1;
            if ((col.get(ad.name) ?? 0) < needed) { col.set(ad.name, needed); changed = true; }
        }
        if (!changed) break;
    }
    // Assign rows within each column in source order
    const rowCount = new Map();
    const positions = new Map();
    for (const flat of flats) {
        const c = col.get(flat.name) ?? 0;
        const r = rowCount.get(c) ?? 0;
        rowCount.set(c, r + 1);
        positions.set(flat.name, { x: START_X + c * COL_W, y: START_Y + r * ROW_H });
    }
    return positions;
}

// ─── Nesting restrictions (mirrors ADF rules and V1 behaviour) ───────────────────────────
const NESTING_RESTRICTIONS = {
    'ForEach':     new Set(['ForEach', 'Until']),
    'Until':       new Set(['Validation', 'ForEach', 'Until']),
    'IfCondition': new Set(['IfCondition', 'ForEach', 'Until', 'Switch']),
    'Switch':      new Set(['IfCondition', 'ForEach', 'Until', 'Switch']),
};

// Returns the set of activity types forbidden on the current canvas (union of all ancestor restrictions).
function getRestrictedTypesForCurrentCanvas() {
    const forbidden = new Set();
    for (const frame of canvasStack) {
        const parentType = frame.parentActivity?.type;
        for (const t of (NESTING_RESTRICTIONS[parentType] || [])) forbidden.add(t);
    }
    return forbidden;
}

// Grey-out forbidden activity palette items and block dblclick-add for them.
function updateSidebarRestrictions() {
    const forbidden = getRestrictedTypesForCurrentCanvas();
    document.querySelectorAll('.activity-item').forEach(item => {
        const actType = item.getAttribute('data-type');
        if (forbidden.has(actType)) {
            item.style.opacity = '0.4';
            item.style.cursor = 'not-allowed';
            item.setAttribute('draggable', 'false');
            item.dataset.restricted = 'true';
        } else {
            item.style.opacity = '';
            item.style.cursor = '';
            item.setAttribute('draggable', 'true');
            delete item.dataset.restricted;
        }
    });
}

// ─── Sub-canvas navigation ────────────────────────────────────────────────────

function renderBreadcrumb() {
    const bc = document.getElementById('breadcrumb');
    if (!bc) return;
    if (canvasStack.length === 0) {
        bc.innerHTML = `<span style="font-weight:600;">${escHtml(pipelineData.name || 'Pipeline')}</span>`;
        return;
    }
    let html = `<span class="bc-link" data-depth="-1">${escHtml(pipelineData.name || 'Pipeline')}</span>`;
    for (let i = 0; i < canvasStack.length; i++) {
        html += ' &rsaquo;&nbsp;';
        if (i < canvasStack.length - 1) {
            html += `<span class="bc-link" data-depth="${i}">${escHtml(canvasStack[i].breadcrumbLabel)}</span>`;
        } else {
            html += `<strong>${escHtml(canvasStack[i].breadcrumbLabel)}</strong>`;
        }
    }
    bc.innerHTML = html;
    bc.querySelectorAll('.bc-link').forEach(el => {
        el.addEventListener('click', () => {
            const depth = parseInt(el.dataset.depth);
            if (depth === -1) { while (canvasStack.length > 0) exitSubCanvas(); }
            else              { while (canvasStack.length > depth + 1) exitSubCanvas(); }
        });
    });
}

// Load a flat activity array onto the canvas, replacing whatever is there.
// Does not touch pipelineData. Used for sub-canvas navigation.
function _loadActivitiesToCanvas(flatActivities) {
    activities.forEach(a => a.remove());
    activities = [];
    connections = [];

    const wrapper = document.getElementById('worldContainer');
    const activityMap = new Map();
    const positions = computeLayout(flatActivities, flatActivities);

    flatActivities.forEach((flat, index) => {
        const pos = positions.get(flat.name) || { x: 80 + (index % 4) * 240, y: 80 + Math.floor(index / 4) * 160 };
        const a = new Activity(flat.type, pos.x, pos.y, wrapper);
        for (const key of Object.keys(flat)) {
            if (key === 'id') continue;
            a[key] = flat[key];
        }
        a.refreshNameLabel();
        a.refreshState();
        if (a.isContainer) {
            const infoEl = a.element?.querySelector('[data-info-el]');
            if (infoEl) a._refreshContainerInfo(infoEl);
        }
        activities.push(a);
        activityMap.set(a.name, a);
    });

    flatActivities.forEach(flat => {
        if (!flat.dependsOn?.length) return;
        const toActivity = activityMap.get(flat.name);
        if (!toActivity) return;
        flat.dependsOn.forEach(dep => {
            const fromActivity = activityMap.get(dep.activity);
            if (fromActivity)
                connections.push(new Connection(fromActivity, toActivity, dep.dependencyConditions?.[0] || 'Succeeded'));
        });
    });

    selectedActivity = null;
    showProperties(null);
    draw();
    setTimeout(fitToScreen, 0);
}

// Enter a sub-canvas to edit nested activities of a container.
function enterSubCanvas(flatNested, parentActivity, branchKey, caseIndex, label) {
    const wrapper = document.getElementById('worldContainer');
    // Detach current Activity elements from DOM (preserves the Activity instances in memory)
    for (const a of activities) {
        if (a.element.parentNode === wrapper) wrapper.removeChild(a.element);
    }
    canvasStack.push({ activities: [...activities], connections: [...connections], parentActivity, branchKey, caseIndex, breadcrumbLabel: label });
    activities = [];
    connections = [];
    _loadActivitiesToCanvas(flatNested);
    renderBreadcrumb();
    updateSidebarRestrictions();
    markAsDirty();
}

// Exit the current sub-canvas, syncing changes back to the parent and restoring the outer canvas.
function exitSubCanvas() {
    if (!canvasStack.length) return;

    // Serialize current inner canvas and write back to parent
    const innerFlats = activities.map(toSaveData);
    const frame = canvasStack[canvasStack.length - 1];
    if (frame.caseIndex != null) {
        frame.parentActivity.cases[frame.caseIndex].activities = innerFlats;
    } else {
        frame.parentActivity[frame.branchKey] = innerFlats;
    }

    // Destroy sub-canvas Activity DOM elements
    activities.forEach(a => a.remove());
    activities = [];
    connections = [];

    canvasStack.pop();

    // Restore outer canvas Activity elements
    const wrapper = document.getElementById('worldContainer');
    for (const a of frame.activities) {
        activities.push(a);
        wrapper.appendChild(a.element);
    }
    for (const c of frame.connections) connections.push(c);

    // Refresh container info badge on the parent activity
    if (frame.parentActivity?.isContainer) {
        const infoEl = frame.parentActivity.element?.querySelector('[data-info-el]');
        if (infoEl) frame.parentActivity._refreshContainerInfo(infoEl);
    }

    selectedActivity = frame.parentActivity;
    showProperties(frame.parentActivity);
    renderBreadcrumb();
    updateSidebarRestrictions();
    draw();
}

// Build the save payload, syncing all sub-canvas levels up first without changing navigation state.
function getSavePayload() {
    if (canvasStack.length === 0) return activities.map(toSaveData);

    // Walk from innermost to outermost, writing each level back to its parent
    let innerFlats = activities.map(toSaveData);
    for (let i = canvasStack.length - 1; i >= 0; i--) {
        const frame = canvasStack[i];
        if (frame.caseIndex != null) {
            frame.parentActivity.cases[frame.caseIndex].activities = innerFlats;
        } else {
            frame.parentActivity[frame.branchKey] = innerFlats;
        }
        innerFlats = frame.activities.map(toSaveData);
    }
    return innerFlats;
}

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

        // Warn if a file has duplicate activity names (invalid in ADF/Synapse).
        const seenNames = new Set();
        const dupNames = new Set();
        flats.forEach(f => { if (seenNames.has(f.name)) dupNames.add(f.name); else seenNames.add(f.name); });
        if (dupNames.size > 0) {
            vscode.postMessage({ type: 'alert', text: `Warning: this pipeline has duplicate activity names (${[...dupNames].join(', ')}), which is invalid in ADF/Synapse. Please rename them before saving.` });
        }

        const positions = computeLayout(flats, src);

        flats.forEach((flat, index) => {
            const pos = positions.get(flat.name) || { x: 80 + (index % 4) * 240, y: 80 + Math.floor(index / 4) * 160 };
            const { x, y } = pos;

            const a = new Activity(flat.type, x, y, wrapper);

            // Copy all flat fields onto the activity object.
            // For pre-deserialized objects these are clean UI fields (e.g. dynamicAllocation, minExecutors).
            // For raw pass-through objects (unsupported types) this mirrors the old Object.assign behaviour.
            for (const key of Object.keys(flat)) {
                if (key === 'id') continue; // keep Activity's own id
                a[key] = flat[key];
            }

            a.refreshNameLabel();
            a.refreshState();

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
        renderBreadcrumb();
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
        window.notebookList = msg.notebookList || [];
        kvLinkedServiceList = msg.kvLinkedServiceList || [];
        credentialList = msg.credentialList || [];
        allLinkedServicesList = msg.allLinkedServicesList || [];
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
        if (msg.success) {
            markAsClean();
            // If file was renamed on save, update our tracked path
            if (msg.newFilePath) {
                currentFilePath = msg.newFilePath;
            }
        }
    }

    if (msg.type === 'addActivity') {
        const wrapper = document.getElementById('worldContainer');
        const a = new Activity(msg.activityType, 100, 100 + activities.length * 30, wrapper);
        a.name = uniqueActivityName(a.name);
        a.refreshNameLabel();
        activities.push(a);
        markAsDirty();
        draw();
    }
});
