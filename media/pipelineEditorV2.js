// pipelineEditorV2.js — Webview script for the V2 Pipeline Editor
// Step 1: Canvas view, drag-and-drop, connections. Save is disabled.
//         Activity properties panel shows read-only data.
//         Schema-driven editing is added per-activity in subsequent steps.

'use strict';

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
        vscode.postMessage({
            type: 'savePipeline',
            pipelineData,
            activities: activities.map(toSaveData),
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

    // Auto-expand properties panel when something is selected
    if (activity) {
        const propsPanel = document.getElementById('propertiesPanel');
        if (propsPanel.classList.contains('collapsed')) {
            propsPanel.classList.remove('collapsed');
            document.body.classList.add('properties-visible');
        }
    }

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

    // Right panel: name, type, description
    rightPanel.innerHTML = `
        <div class="v2-migration-note">
            ✦ V2 editor preview — full field editing for <strong>${escHtml(Activity.labelForType(activity.type))}</strong> is being migrated. Data shown is read-only.
        </div>
        ${_propRow('Name', activity.name)}
        ${_propRow('Type', Activity.labelForType(activity.type))}
        ${activity.description ? _propRow('Description', activity.description) : ''}`;

    // Bottom panel: General + Settings stubs
    tabsContainer.innerHTML = `
        <button class="config-tab active" data-tab="v2-general" style="border-bottom:2px solid var(--vscode-focusBorder);color:var(--vscode-tab-activeForeground);">General</button>
        <button class="config-tab" data-tab="v2-settings">Settings (read-only)</button>`;
    panesContainer.innerHTML = `
        <div class="config-tab-pane active" id="tab-v2-general" style="display:block;">
            ${_buildGeneralPane(activity)}
        </div>
        <div class="config-tab-pane" id="tab-v2-settings" style="display:none;">
            ${_buildSettingsPane(activity)}
        </div>`;

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

function _buildGeneralPane(a) {
    const schema = activitySchemas[a.type];
    const common = schema?.commonProperties || {};
    let rows = _propRow('Name', a.name) + _propRow('Description', a.description || '—');
    if (common.state)   rows += _propRow('State', a.state || 'Activated');
    if (common.timeout) rows += _propRow('Timeout', a.timeout || '—');
    if (common.retry !== undefined) rows += _propRow('Retry', a.retry ?? 0);
    return rows || '<div class="empty-state">No general properties.</div>';
}

function _buildSettingsPane(a) {
    const schema = activitySchemas[a.type];
    const tp = schema?.typeProperties || {};
    let rows = '';
    // Show each typeProperty field whose key exists on the activity
    for (const [key, def] of Object.entries(tp)) {
        const val = a[key];
        if (val === undefined || val === null) continue;
        const display = typeof val === 'object' ? JSON.stringify(val) : String(val);
        rows += _propRow(def.label || key, display);
    }
    if (!rows) {
        // Fallback: dump raw typeProperties-like keys
        const skipKeys = new Set(['id','type','x','y','width','height','color','container','element','isContainer']);
        for (const [k, v] of Object.entries(a)) {
            if (skipKeys.has(k) || typeof v === 'function') continue;
            if (k.startsWith('if') && Array.isArray(v)) continue; // skip branch arrays
            if (['activities','cases','defaultActivities'].includes(k)) continue;
            const display = typeof v === 'object' ? JSON.stringify(v) : String(v);
            rows += _propRow(k, display);
        }
    }
    return rows || '<div class="empty-state">No settings to display.</div>';
}

// ─── Load pipeline from JSON ───────────────────────────────────────────────────
function loadPipelineFromJson(pipelineJson) {
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

        src.forEach((ad, index) => {
            const cols = 3;
            const x = 100 + (index % cols) * 210;
            const y = 100 + Math.floor(index / cols) * 160;
            const a = new Activity(ad.type, x, y, wrapper);
            a.name = ad.name;
            a.refreshNameLabel();
            a.description = ad.description || '';
            a.userProperties = ad.userProperties || [];
            if (ad.state) a.state = ad.state;
            if (ad.onInactiveMarkAs) a.onInactiveMarkAs = ad.onInactiveMarkAs;
            if (ad.policy) {
                a.timeout = ad.policy.timeout;
                a.retry = ad.policy.retry;
                a.retryIntervalInSeconds = ad.policy.retryIntervalInSeconds;
                a.secureOutput = ad.policy.secureOutput;
                a.secureInput = ad.policy.secureInput;
            }

            // Flatten typeProperties onto the activity object for the properties panel
            if (ad.typeProperties) {
                Object.assign(a, ad.typeProperties);
            }

            // Store child activity arrays for container types (read-only in Step 1)
            if (ad.type === 'IfCondition' && ad.typeProperties) {
                a.expression = ad.typeProperties.expression?.value ?? ad.typeProperties.expression ?? '';
                a.ifTrueActivities = ad.typeProperties.ifTrueActivities || [];
                a.ifFalseActivities = ad.typeProperties.ifFalseActivities || [];
            }
            if (ad.type === 'ForEach' && ad.typeProperties) {
                a.items = ad.typeProperties.items?.value ?? ad.typeProperties.items ?? '';
                a.activities = ad.typeProperties.activities || [];
            }
            if (ad.type === 'Until' && ad.typeProperties) {
                a.expression = ad.typeProperties.expression?.value ?? ad.typeProperties.expression ?? '';
                a.activities = ad.typeProperties.activities || [];
            }
            if (ad.type === 'Switch' && ad.typeProperties) {
                a.on = ad.typeProperties.on?.value ?? ad.typeProperties.on ?? '';
                a.cases = ad.typeProperties.cases || [];
                a.defaultActivities = ad.typeProperties.defaultActivities || [];
            }

            // Refresh container info display if needed
            if (a.isContainer) {
                const infoEl = a.element?.querySelector('[data-info-el]');
                if (infoEl) a._refreshContainerInfo(infoEl);
            }

            activities.push(a);
            activityMap.set(ad.name, a);
        });

        // Build connections from dependsOn
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
        // Defer fit until after paint so offsetWidth/Height are accurate
        setTimeout(fitToScreen, 0);

        // Mark clean after load
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
        window.linkedServicesList = msg.linkedServicesList || [];
        buildSidebar();
        log('Schemas loaded. Activities config categories: ' + activitiesConfig.categories.length);
    }

    if (msg.type === 'loadPipeline') {
        currentFilePath = msg.filePath || null;
        loadPipelineFromJson(msg.data);
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
