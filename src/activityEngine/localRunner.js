'use strict';
// localRunner.js â€” locally executes a Synapse pipeline definition.
//
// Design principle: all activity type dispatch is driven by local-run-config.json.
// To add/disable a handler, edit the config â€” do not add if/else chains here.
//
// Usage:
//   const runner = new LocalPipelineRunner(pipelineJson, parameters, workspaceRoot);
//   runner.on('activityUpdate', ({name, status, output, error}) => ...);
//   runner.on('pipelineEnd',    ({status, error}) => ...);
//   await runner.run();
//   runner.cancel();  // graceful cancellation

const EventEmitter = require('events');
const path = require('path');
const fs   = require('fs');
const os   = require('os');
const { evaluate } = require('./expressionEvaluator');
const runConfig = require('../local-run-config.json');
const { SynapseClient, NOTEBOOK_LANG_TO_KIND } = require('./synapseClient');
const { ADLSRestClient } = require('../adlsRestClient');
const { resolveDatasetToAdls, buildAdlsPath } = require('./datasetResolver');
const { resolveSqlLinkedService, resolveSqlDataset } = require('./sqlResolver');
const { SqlClient, readParquetFile } = require('./sqlClient');
const copyConfig = require('../copyActivityConfig.json');

const RUNNERS     = runConfig.activityRunners;
const RUN_LIMITS  = runConfig.runLimits;

/**
 * Parse an ADF TimeSpan string to total seconds.
 * Supported formats:
 *   D.HH:MM:SS  (e.g. "0.00:30:00" = 1800 seconds)
 *   HH:MM:SS    (e.g. "00:30:00"   = 1800 seconds)
 *   PT...       ISO 8601 (e.g. "PT30S", "PT5M", "PT1H") — kept for compat
 *   plain int   (seconds, legacy)
 */
function parseAdfTimespan(val, defaultSec = 600) {
    if (!val) return defaultSec;
    const s = String(val).trim();
    // D.HH:MM:SS or HH:MM:SS
    const ts = s.match(/^(?:(\d+)\.)?(\d+):(\d+):(\d+)$/);
    if (ts) {
        return (parseInt(ts[1] || '0', 10) * 86400 +
                parseInt(ts[2],          10) * 3600  +
                parseInt(ts[3],          10) * 60    +
                parseInt(ts[4],          10));
    }
    // ISO 8601 duration PT[nH][nM][nS]
    const iso = s.match(/^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$/i);
    if (iso) {
        return (parseInt(iso[1] || '0', 10) * 3600 +
                parseInt(iso[2] || '0', 10) * 60    +
                parseFloat(iso[3] || '0'));
    }
    const n = parseInt(s, 10);
    return isNaN(n) ? defaultSec : n;
}

class LocalPipelineRunner extends EventEmitter {
    /**
     * @param {object}  pipelineJson   â€” raw pipeline JSON { name, properties: { activities, parameters, variables } }
     * @param {object}  parameters     â€” user-supplied parameter values { name: value }
     * @param {string}  workspaceRoot  â€” absolute path to the workspace folder (for ExecutePipeline cross-refs)
     */
    constructor(pipelineJson, parameters, workspaceRoot, extensionPath) {
        super();
        this.pipelineJson  = pipelineJson;
        this.parameters    = parameters || {};
        this.workspaceRoot = workspaceRoot;
        this.extensionPath = extensionPath || null;

        this.runId        = _randomId();
        this.pipelineName = pipelineJson?.name ?? 'pipeline';
        this.variables    = _initVariables(pipelineJson?.properties?.variables ?? {});
        this.activityOutputs  = {};  // { activityName: outputObject }
        this.activityStatuses = {};  // { activityName: 'Succeeded' | 'Failed' | 'Skipped' }
        this.activityRuns     = [];  // array of activity run result records (for viewer)
        this._cancelled   = false;
        this._startTime   = null;
    }

    /**
     * Start executing the pipeline.  Resolves when the run finishes (success, failure, or cancel).
     */
    async run() {
        this._startTime = new Date();
        this._cancelled = false;

        // Enforce max run duration
        const timeoutHandle = setTimeout(() => {
            this._cancelled = true;
        }, RUN_LIMITS.maxRunDurationMs);

        try {
            const activities = this.pipelineJson?.properties?.activities ?? [];
            await this._executeActivityList(activities);
            clearTimeout(timeoutHandle);
            const hasFailed  = Object.values(this.activityStatuses).some(s => s === 'Failed');
            const finalStatus = this._cancelled ? 'Cancelled' : (hasFailed ? 'Failed' : 'Succeeded');
            this.emit('pipelineEnd', { runId: this.runId, status: finalStatus, activityRuns: this.activityRuns });
        } catch (err) {
            clearTimeout(timeoutHandle);
            this.emit('pipelineEnd', { runId: this.runId, status: 'Failed', error: err.message, activityRuns: this.activityRuns });
        }
    }

    /**
     * Request cancellation of the current run.
     */
    cancel() {
        this._cancelled = true;
    }

    // â”€â”€â”€ Core execution engine â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    /**
     * Executes a flat list of activities honouring dependsOn relationships.
     * Resolves when all activities at this level have completed (or been skipped/failed).
     */
    async _executeActivityList(activities) {
        // Build a ready-queue: topological sort respecting dependsOn
        const remaining = [...activities];
        const completed = new Set();

        while (remaining.length > 0) {
            if (this._cancelled) break;
            this._enforceRunDuration();

            // Find all activities whose dependsOn conditions are satisfied
            const ready = remaining.filter(a => this._isDependsOnSatisfied(a, completed));
            if (ready.length === 0) {
                // No activity is ready: all remaining activities are blocked (failed deps or cycle)
                for (const blocked of remaining) {
                    this._emitSkipped(blocked, 'Blocked by upstream activity failure');
                    completed.add(blocked.name);
                }
                remaining.length = 0;
                break;
            }

            // Run all ready activities in parallel (ADF default is parallel within a level)
            try {
                await Promise.all(ready.map(a => {
                    const idx = remaining.indexOf(a);
                    remaining.splice(idx, 1);
                    return this._executeOne(a).then(() => completed.add(a.name));
                }));
            } catch (err) {
                if (!err.isPipelineFail) throw err; // unexpected — propagate
                // Fail activity: ensure all dispatched activities are in completed
                // (the .then() didn't run for the one that threw), then continue
                // the loop so remaining activities are Skipped/run per their deps.
                for (const a of ready) {
                    if (!completed.has(a.name)) completed.add(a.name);
                }
            }
        }
    }

    _isDependsOnSatisfied(activity, completed) {
        for (const dep of (activity.dependsOn || [])) {
            if (!completed.has(dep.activity)) return false;
            const depStatus = this.activityStatuses[dep.activity];
            const conditions = dep.dependencyConditions || ['Succeeded'];
            // 'Completed' means any terminal state
            if (conditions.includes('Completed')) continue;
            if (!conditions.includes(depStatus)) return false;
        }
        return true;
    }

    _shouldSkip(activity) {
        for (const dep of (activity.dependsOn || [])) {
            const depStatus = this.activityStatuses[dep.activity];
            const conditions = dep.dependencyConditions || ['Succeeded'];
            if (conditions.includes('Completed')) continue;
            if (!conditions.includes(depStatus)) return true;
        }
        return false;
    }

    async _executeOne(activity) {
        if (this._cancelled) { this._emitSkipped(activity, 'Run cancelled'); return; }
        if (this._shouldSkip(activity)) { this._emitSkipped(activity, 'Upstream dependency not satisfied'); return; }

        // Skip deactivated activities
        if (activity.state === 'Inactive' || activity.state === 'Deactivated') {
            const markAs = activity.onInactiveMarkAs || 'Succeeded';
            this.activityStatuses[activity.name] = markAs;
            this._recordRun(activity, markAs, null, null, { skippedReason: 'Activity is deactivated' });
            this.emit('activityUpdate', { name: activity.name, type: activity.type, status: markAs, output: null, error: null, input: null });
            return;
        }

        const runnerConf = RUNNERS[activity.type];
        const handler = runnerConf?.handler ?? 'notSupportedHandler';
        const startTime = new Date();
        this.emit('activityUpdate', { name: activity.name, type: activity.type, status: 'Running', output: null, error: null, input: activity.typeProperties || {} });

        try {
            const output = await HANDLER_REGISTRY[handler].call(this, activity);
            const endTime = new Date();
            this.activityOutputs[activity.name]  = output ?? {};
            this.activityStatuses[activity.name] = 'Succeeded';
            this._recordRun(activity, 'Succeeded', startTime, endTime, output);
            this.emit('activityUpdate', { name: activity.name, type: activity.type, status: 'Succeeded', output, error: null, input: activity.typeProperties || {} });
        } catch (err) {
            const endTime = new Date();
            // If cancelled mid-activity, mark as Cancelled rather than Failed
            let status = (err.isCancelled || this._cancelled) ? 'Cancelled' : 'Failed';
            if (err.isPipelineFail) status = 'Failed';
            this.activityStatuses[activity.name] = status;
            this._recordRun(activity, status, startTime, endTime, null, err.message);
            this.emit('activityUpdate', { name: activity.name, type: activity.type, status, output: null, error: err.message, input: activity.typeProperties || {} });
            if (err.isPipelineFail) throw err;
        }
    }

    _emitSkipped(activity, reason) {
        this.activityStatuses[activity.name] = 'Skipped';
        this._recordRun(activity, 'Skipped', null, null, null, reason);
        this.emit('activityUpdate', { name: activity.name, type: activity.type, status: 'Skipped', output: null, error: reason, input: null });
    }

    _recordRun(activity, status, startTime, endTime, output, errorMsg) {
        this.activityRuns.push({
            activityName:    activity.name,
            activityType:    activity.type,
            activityRunId:   _randomId(),
            status,
            activityRunStart: startTime ? startTime.toISOString() : null,
            activityRunEnd:   endTime   ? endTime.toISOString()   : null,
            durationInMs:     (startTime && endTime) ? (endTime - startTime) : null,
            output: output ?? null,
            error:  errorMsg ? { message: errorMsg } : null,
            input: null,
        });
    }

    // â”€â”€â”€ Context helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    _context(extra) {
        return {
            parameters:       this.parameters,
            variables:        this.variables,
            activityOutputs:  this.activityOutputs,
            activityStatuses: this.activityStatuses,
            globalParameters: {},
            ...extra,
        };
    }

    _eval(value, extra) {
        // Unwrap ADF expression objects: { value: "@...", type: "Expression" }
        if (value !== null && typeof value === 'object' && typeof value.value === 'string' && value.type === 'Expression') {
            value = value.value;
        }
        return evaluate(value, this._context(extra));
    }

    _enforceRunDuration() {
        if (this._startTime && ((Date.now() - this._startTime) > RUN_LIMITS.maxRunDurationMs)) {
            this._cancelled = true;
        }
    }
}

// â”€â”€â”€ Handler registry â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// All handlers are functions(activity) called with `this` bound to the LocalPipelineRunner.
// Add new activity type support by adding an entry here AND in local-run-config.json.

const HANDLER_REGISTRY = {

    // â”€â”€ Wait â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async waitHandler(activity) {
        const tp = activity.typeProperties || {};
        let seconds = parseFloat(this._eval(tp.waitTimeInSeconds ?? tp.waitInSeconds ?? 1, {}));
        if (isNaN(seconds) || seconds < 0) seconds = 0;
        seconds = Math.min(seconds, RUN_LIMITS.waitMaxSeconds);

        await new Promise((resolve, reject) => {
            const t = setTimeout(resolve, seconds * 1000);
            // Poll for cancellation every 100 ms
            const poll = setInterval(() => {
                if (this._cancelled) { clearTimeout(t); clearInterval(poll); reject(Object.assign(new Error('Run cancelled'), { isCancelled: true })); }
            }, 100);
            setTimeout(() => clearInterval(poll), seconds * 1000 + 200);
        });
        return { waitTimeInSeconds: seconds };
    },

    // â”€â”€ Fail â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async failHandler(activity) {
        const tp = activity.typeProperties || {};
        const msg       = String(this._eval(tp.message ?? tp.errorMessage ?? 'Pipeline failed', {}));
        const errorCode = String(this._eval(tp.errorCode ?? '500', {}));
        const err = new Error(`${msg} (errorCode: ${errorCode})`);
        err.isPipelineFail = true;
        throw err;
    },

    // â”€â”€ SetVariable â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async setVariableHandler(activity) {
        const tp = activity.typeProperties || {};
        const varName = String(this._eval(tp.variableName, {}));
        let value     = this._eval(tp.value, {});
        if (tp.setSystemVariable) {
            return { variableName: varName, value };
        }
        // If the variable is declared as Array/Object and the value is a JSON string, parse it
        const varDef = this.pipelineJson?.properties?.variables?.[varName];
        if (typeof value === 'string' && varDef?.type && ['Array', 'Object'].includes(varDef.type)) {
            try { value = JSON.parse(value); } catch { /* keep as string */ }
        }
        this.variables[varName] = value;
        return { variableName: varName, value };
    },

    // â”€â”€ AppendVariable â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async appendVariableHandler(activity) {
        const tp = activity.typeProperties || {};
        const varName = String(this._eval(tp.variableName, {}));
        const value   = this._eval(tp.value, {});
        if (!Array.isArray(this.variables[varName])) this.variables[varName] = [];
        this.variables[varName].push(value);
        return { variableName: varName, value: this.variables[varName] };
    },

    // â”€â”€ Filter â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async filterHandler(activity) {
        const tp = activity.typeProperties || {};
        const items     = this._eval(tp.items, {});
        const condition = tp.condition;
        if (!Array.isArray(items)) throw new Error(`Filter: "items" must evaluate to an array (got ${typeof items})`);

        const filtered = [];
        for (const item of items) {
            if (this._eval(condition, { currentItem: item })) filtered.push(item);
        }
        return { value: filtered, filterCount: filtered.length };
    },

    // â”€â”€ ForEach â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async forEachHandler(activity) {
        const tp = activity.typeProperties || {};
        const items     = this._eval(tp.items, {});
        const isSequential = tp.isSequential !== false; // default true
        const batchCount   = tp.batchCount ? parseInt(this._eval(tp.batchCount, {})) : 20;

        if (!Array.isArray(items)) throw new Error(`ForEach: "items" did not evaluate to an array (got ${typeof items})`);

        const limited = items.slice(0, RUN_LIMITS.maxForEachItems);
        const children = tp.activities || [];

        if (isSequential) {
            for (let i = 0; i < limited.length; i++) {
                if (this._cancelled) break;
                const child = new LocalPipelineRunner(
                    { name: `${activity.name}[${i}]`, properties: { activities: children } },
                    this.parameters, this.workspaceRoot
                );
                // Share variables & outputs with parent (ForEach writes to parent scope)
                child.variables        = this.variables;
                child.activityOutputs  = {};
                child.activityStatuses = {};
                child._startTime       = this._startTime;
                // Override eval context with currentItem
                child._forEachItem      = limited[i];
                child._forEachItemIndex = i;
                _patchForEachContext(child, limited[i]);

                // Forward child activity events to parent with iteration context
                child.on('activityUpdate', (update) => {
                    this.emit('activityUpdate', { ...update, parentActivity: activity.name, iteration: i });
                });

                await new Promise((resolve, reject) => {
                    child.on('pipelineEnd', (e) => e.status !== 'Failed' ? resolve() : reject(new Error(`ForEach iteration ${i} failed`)));
                    child.run().catch(reject);
                });
                // Propagate run records to parent
                for (const rec of child.activityRuns) this.activityRuns.push({ ...rec, _forEachIteration: i, _parentActivity: activity.name });
            }
        } else {
            // Parallel â€” run up to batchCount at a time
            for (let start = 0; start < limited.length; start += batchCount) {
                if (this._cancelled) break;
                const batch = limited.slice(start, start + batchCount);
                await Promise.all(batch.map((item, bIdx) => {
                    const i = start + bIdx;
                    const child = new LocalPipelineRunner(
                        { name: `${activity.name}[${i}]`, properties: { activities: children } },
                        this.parameters, this.workspaceRoot
                    );
                    child.variables        = this.variables;
                    child.activityOutputs  = {};
                    child.activityStatuses = {};
                    child._startTime       = this._startTime;
                    _patchForEachContext(child, item);

                    // Forward child activity events to parent with iteration context
                    child.on('activityUpdate', (update) => {
                        this.emit('activityUpdate', { ...update, parentActivity: activity.name, iteration: i });
                    });

                    return new Promise((resolve) => {
                        child.on('pipelineEnd', () => resolve());
                        child.run().catch(() => resolve());
                    }).then(() => {
                        for (const rec of child.activityRuns) this.activityRuns.push({ ...rec, _forEachIteration: i, _parentActivity: activity.name });
                    });
                }));
            }
        }
        return { count: limited.length };
    },

    // â”€â”€ Until â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async untilHandler(activity) {
        const tp = activity.typeProperties || {};
        const expression = tp.expression;
        const children   = tp.activities || [];

        let iterations = 0;
        while (iterations < RUN_LIMITS.maxUntilIterations) {
            if (this._cancelled) break;
            this._enforceRunDuration();

            const child = new LocalPipelineRunner(
                { name: `${activity.name}[iter${iterations}]`, properties: { activities: children } },
                this.parameters, this.workspaceRoot
            );
            child.variables        = this.variables;
            child.activityOutputs  = this.activityOutputs;
            child.activityStatuses = {};
            child._startTime       = this._startTime;

            // Forward child activity events to parent with iteration context
            child.on('activityUpdate', (update) => {
                this.emit('activityUpdate', { ...update, parentActivity: activity.name, iteration: iterations, branchLabel: String(iterations) });
            });

            await new Promise((resolve) => {
                child.on('pipelineEnd', resolve);
                child.run().catch(resolve);
            });
            for (const rec of child.activityRuns) this.activityRuns.push({ ...rec, _untilIteration: iterations, _parentActivity: activity.name });

            iterations++;
            const cond = this._eval(expression?.value ?? expression, {});
            if (cond === true || cond === 'true') break;
        }
        return { iterations };
    },

    // â”€â”€ IfCondition â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async ifConditionHandler(activity) {
        const tp = activity.typeProperties || {};
        const condValue = tp.expression?.value ?? tp.expression;
        const result    = this._eval(condValue, {});
        const branch    = result ? (tp.ifTrueActivities || []) : (tp.ifFalseActivities || []);
        const branchName = result ? 'True' : 'False';

        const child = new LocalPipelineRunner(
            { name: `${activity.name}[${branchName}]`, properties: { activities: branch } },
            this.parameters, this.workspaceRoot
        );
        child.variables        = this.variables;
        child.activityOutputs  = this.activityOutputs;
        child.activityStatuses = {};
        child._startTime       = this._startTime;

        // Forward child activity events to parent
        child.on('activityUpdate', (update) => {
            this.emit('activityUpdate', { ...update, parentActivity: activity.name, iteration: 0, branchLabel: branchName });
        });

        await new Promise((resolve, reject) => {
            child.on('pipelineEnd', (e) => e.status === 'Failed' ? reject(new Error(e.error)) : resolve());
            child.run().catch(reject);
        });
        for (const rec of child.activityRuns) this.activityRuns.push({ ...rec, _ifBranch: branchName, _parentActivity: activity.name });
        return { expression: result, branch: branchName };
    },

    // â”€â”€ Switch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async switchHandler(activity) {
        const tp = activity.typeProperties || {};
        const onValue   = String(this._eval(tp.on?.value ?? tp.on, {}));
        const cases     = tp.cases || [];
        const matched   = cases.find(c => String(c.value) === onValue);
        const branch    = matched?.activities ?? tp.defaultActivities ?? [];
        const branchLabel = matched ? `case:${onValue}` : 'default';

        const child = new LocalPipelineRunner(
            { name: `${activity.name}[${branchLabel}]`, properties: { activities: branch } },
            this.parameters, this.workspaceRoot
        );
        child.variables        = this.variables;
        child.activityOutputs  = this.activityOutputs;
        child.activityStatuses = {};
        child._startTime       = this._startTime;

        // Forward child activity events to parent
        child.on('activityUpdate', (update) => {
            this.emit('activityUpdate', { ...update, parentActivity: activity.name, iteration: 0, branchLabel });
        });

        await new Promise((resolve, reject) => {
            child.on('pipelineEnd', (e) => e.status === 'Failed' ? reject(new Error(e.error)) : resolve());
            child.run().catch(reject);
        });
        for (const rec of child.activityRuns) this.activityRuns.push({ ...rec, _switchCase: branchLabel, _parentActivity: activity.name });
        return { on: onValue, branch: branchLabel };
    },

    // â”€â”€ ExecutePipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async executePipelineHandler(activity) {
        const tp = activity.typeProperties || {};
        const refName  = tp.pipeline?.referenceName;
        const waitOnCompletion = tp.waitOnCompletion !== false;

        if (!waitOnCompletion) {
            return { referencedPipeline: refName, waitOnCompletion: false, note: 'Fired and forgotten â€” sub-pipeline not tracked in local run mode.' };
        }

        // Resolve the referenced pipeline JSON from the workspace
        if (!this.workspaceRoot || !refName) {
            throw new Error(`ExecutePipeline: cannot resolve pipeline "${refName}" â€” workspaceRoot is not set`);
        }
        const filePath = path.join(this.workspaceRoot, 'pipeline', `${refName}.json`);
        if (!fs.existsSync(filePath)) {
            throw new Error(`ExecutePipeline: pipeline file "${refName}.json" not found in workspace pipeline/ folder`);
        }
        const subJson = JSON.parse(fs.readFileSync(filePath, 'utf8'));

        // Evaluate parameter overrides
        const subParams = {};
        for (const [k, v] of Object.entries(tp.parameters || {})) {
            subParams[k] = this._eval(v?.value ?? v, {});
        }

        const child = new LocalPipelineRunner(subJson, subParams, this.workspaceRoot);
        child._startTime = this._startTime;

        // Forward child activity events to parent
        child.on('activityUpdate', (update) => {
            this.emit('activityUpdate', { ...update, parentActivity: activity.name, iteration: 0, branchLabel: refName });
        });

        let childStatus = 'Succeeded', childError = null;
        await new Promise((resolve) => {
            child.on('pipelineEnd', (e) => { childStatus = e.status; childError = e.error; resolve(); });
            child.run().catch(() => resolve());
        });
        for (const rec of child.activityRuns) this.activityRuns.push({ ...rec, _subPipeline: refName, _parentActivity: activity.name });

        if (childStatus === 'Failed') {
            throw new Error(`Sub-pipeline "${refName}" failed: ${childError}`);
        }
        return { referencedPipeline: refName, status: childStatus };
    },

    // â”€â”€ WebActivity â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async webActivityHandler(activity) {
        const tp = activity.typeProperties || {};
        const url    = String(this._eval(tp.url, {}));
        const method = (tp.method || 'GET').toUpperCase();
        const body   = tp.body ? this._eval(tp.body, {}) : undefined;

        // Build headers
        const headers = {};
        if (tp.headers && typeof tp.headers === 'object') {
            for (const [k, v] of Object.entries(tp.headers)) {
                headers[k] = String(this._eval(v?.value ?? v, {}));
            }
        }
        if (!headers['Content-Type'] && body) headers['Content-Type'] = 'application/json';

        const https = require('https');
        const http  = require('http');
        const { URL: NodeURL } = require('url');

        const parsed  = new NodeURL(url);
        const lib     = parsed.protocol === 'https:' ? https : http;
        const bodyStr = body !== undefined ? (typeof body === 'string' ? body : JSON.stringify(body)) : null;

        const responseText = await new Promise((resolve, reject) => {
            const opts = {
                hostname: parsed.hostname,
                port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
                path:     parsed.pathname + parsed.search,
                method,
                headers: { ...headers, ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}) },
            };
            const req = lib.request(opts, (res) => {
                let data = '';
                res.on('data', chunk => { data += chunk; });
                res.on('end', () => {
                    if (res.statusCode >= 400) {
                        reject(new Error(`WebActivity HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
                    } else {
                        resolve(data);
                    }
                });
            });
            req.on('error', reject);
            if (bodyStr) req.write(bodyStr);
            req.end();
        });

        try { return JSON.parse(responseText); } catch { return { response: responseText }; }
    },

    // â”€â”€ SynapseNotebook (Livy session, batch-style: single statement) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    // All code cells are concatenated into one statement so the notebook runs
    // end-to-end without per-cell round-trips ("run and done" semantics).
    async synapseNotebookHandler(activity) {
        const tp           = activity.typeProperties || {};
        const notebookName = tp.notebook?.referenceName ?? tp.notebookPath;
        const sparkPool    = tp.sparkPool?.referenceName
            ?? tp.sparkPool
            ?? _loadSynapseWorkspaceConfig(this.workspaceRoot, this.extensionPath).defaultSparkPool;

        if (!notebookName) throw new Error('SynapseNotebook: missing notebook.referenceName in typeProperties');
        if (!sparkPool)    throw new Error(
            'SynapseNotebook: no Spark pool specified. Set sparkPool on the activity or ' +
            '"defaultSparkPool" in synapse-local-run.json.'
        );

        const wsConfig = _loadSynapseWorkspaceConfig(this.workspaceRoot, this.extensionPath);
        if (!wsConfig.synapseEndpoint) {
            throw new Error(
                'SynapseNotebook: set "synapseEndpoint" in synapse-local-run.json in your workspace root or extension folder.\n' +
                'Example: { "synapseEndpoint": "https://YOUR-WORKSPACE.dev.azuresynapse.net", "defaultSparkPool": "pool1" }'
            );
        }

        // Read notebook JSON from workspace
        const nbFile = path.join(this.workspaceRoot, 'notebook', `${notebookName}.json`);
        if (!fs.existsSync(nbFile)) {
            throw new Error(`SynapseNotebook: "${notebookName}.json" not found in workspace notebook/ folder`);
        }
        const nb    = JSON.parse(fs.readFileSync(nbFile, 'utf8'));
        const cells = nb.properties?.cells ?? nb.cells ?? [];
        const lang  = nb.properties?.metadata?.language_info?.name ?? 'python';
        const kind  = NOTEBOOK_LANG_TO_KIND[lang] ?? 'pyspark';

        // Map ADF size names â†’ driver/executor cores+memory for the MemoryOptimized family.
        // If the activity specifies executorSize/driverSize, those take precedence over
        // synapse-local-run.json sessionConfig.
        const SIZE_MAP = {
            Small:   { cores: 4,  memory: '28g'  },
            Medium:  { cores: 8,  memory: '56g'  },
            Large:   { cores: 16, memory: '112g' },
            XLarge:  { cores: 32, memory: '224g' },
            XXLarge: { cores: 64, memory: '432g' },
        };
        const driverSpec   = SIZE_MAP[tp.driverSize]   ?? SIZE_MAP[tp.executorSize] ?? null;
        const executorSpec = SIZE_MAP[tp.executorSize] ?? driverSpec ?? null;
        const baseConfig   = wsConfig.sessionConfig ?? {};
        const sessionConfig = {
            driverCores:    driverSpec?.cores    ?? baseConfig.driverCores    ?? 4,
            driverMemory:   driverSpec?.memory   ?? baseConfig.driverMemory   ?? '28g',
            executorCores:  executorSpec?.cores  ?? baseConfig.executorCores  ?? 4,
            executorMemory: executorSpec?.memory ?? baseConfig.executorMemory ?? '28g',
            numExecutors:   baseConfig.numExecutors ?? 2,
        };

        // Merge activity-level Spark conf with sessionConfig
        const activityConf = (tp.conf && typeof tp.conf === 'object') ? tp.conf : {};

        // Build parameter injection preamble
        const params    = tp.parameters ?? {};
        const paramCode = Object.keys(params).length > 0
            ? Object.entries(params)
                .map(([k, v]) => `${k} = ${JSON.stringify(this._eval(v?.value ?? v, {}))}`)
                .join('\n') + '\n'
            : '';

        // Collect code cells
        const codeCells = cells.filter(c => (c.cell_type ?? c.cellType) === 'code');
        if (codeCells.length === 0) {
            return { notebookName, kind, note: 'No code cells to execute.' };
        }

        const livyCfg = runConfig.synapseWorkspace;
        const client  = new SynapseClient(wsConfig.synapseEndpoint);
        const apiVer  = livyCfg.livyApiVersion;

        const session   = await client.createSession(
            sparkPool, apiVer, `localRun-${this.runId.slice(0, 8)}`, kind, activityConf, sessionConfig
        );
        const sessionId = session.id;

        try {
            // Wait for session to become idle (may take 2â€“5 min on cold pool)
            await client.waitForSessionIdle(
                sparkPool, apiVer, sessionId,
                livyCfg.sessionPollIntervalMs,
                livyCfg.sessionTimeoutMinutes * 60_000,
                (state) => this.emit('activityUpdate', {
                    name: activity.name, status: 'Running', output: null, error: null,
                    _detail: `Starting Spark session (state: ${state})...`,
                })
            );

            // Execute parameter preamble first (if any), then each cell individually
            // so we can collect per-cell outputs for the notebook snapshot.
            const cellResults = [];

            if (paramCode.trim()) {
                await client.submitStatement(sparkPool, apiVer, sessionId, paramCode, kind)
                    .then(s => client.waitForStatement(sparkPool, apiVer, sessionId, s.id, livyCfg.statementPollIntervalMs))
                    .catch(() => {}); // parameter injection errors are non-fatal
            }

            for (let i = 0; i < codeCells.length; i++) {
                const cell = codeCells[i];
                const src  = Array.isArray(cell.source) ? cell.source.join('') : (cell.source ?? '');
                if (!src.trim()) { cellResults.push({ source: src, output: null }); continue; }

                this.emit('activityUpdate', {
                    name: activity.name, status: 'Running', output: null, error: null,
                    _detail: `Executing cell ${i + 1} / ${codeCells.length}...`,
                });

                const stmt   = await client.submitStatement(sparkPool, apiVer, sessionId, src, kind);
                const result = await client.waitForStatement(
                    sparkPool, apiVer, sessionId, stmt.id, livyCfg.statementPollIntervalMs
                );

                if (result.output?.status === 'error') {
                    const trace = (result.output.traceback ?? []).join('\n');
                    throw new Error(`Cell ${i + 1}: ${result.output.evalue ?? 'execution error'}${trace ? '\n' + trace : ''}`);
                }

                cellResults.push({ source: src, output: result.output ?? null });
            }

            // Build evaluated parameter map for display (show resolved values, not raw ADF structures)
            const evaluatedParams = Object.fromEntries(
                Object.entries(params).map(([k, v]) => {
                    try { return [k, this._eval(v?.value ?? v, {})]; } catch { return [k, String(v)]; }
                })
            );

            // Emit snapshot event so the UI can open a notebook viewer panel
            this.emit('notebookSnapshot', {
                activityName: activity.name,
                notebookName,
                generatedAt: new Date().toISOString(),
                evaluatedParams,
                cells: _mergeSnapshotCells(cells, cellResults),
            });

            return {
                notebookName, sessionId, kind,
                cellsExecuted: codeCells.length,
                output: cellResults.map(r => {
                    const t = r.output?.text ?? r.output?.data?.['text/plain'];
                    return Array.isArray(t) ? t.join('') : (t ?? '');
                }).filter(Boolean).join('\n') || null,
            };
        } finally {
            await client.deleteSession(sparkPool, apiVer, sessionId).catch(() => {});
        }
    },

    // â”€â”€ SparkJob (Livy batch) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async sparkJobHandler(activity) {
        const tp        = activity.typeProperties || {};
        const jobRef    = tp.sparkJob?.referenceName;
        const sparkPool = tp.sparkPool?.referenceName ?? tp.sparkPool;

        if (!jobRef)    throw new Error('SparkJob: missing sparkJob.referenceName in typeProperties');
        if (!sparkPool) throw new Error('SparkJob: missing sparkPool.referenceName in typeProperties');

        const wsConfig = _loadSynapseWorkspaceConfig(this.workspaceRoot, this.extensionPath);
        if (!wsConfig.synapseEndpoint) {
            throw new Error(
                'SparkJob: set "synapseEndpoint" in synapse-local-run.json in your workspace root or extension folder.'
            );
        }

        // Read SparkJobDefinition from workspace if available
        const jobFile = path.join(this.workspaceRoot, 'sparkJobDefinition', `${jobRef}.json`);
        let jobDef = null;
        if (fs.existsSync(jobFile)) {
            jobDef = JSON.parse(fs.readFileSync(jobFile, 'utf8'));
        }

        const livyCfg  = runConfig.synapseWorkspace;
        const client   = new SynapseClient(wsConfig.synapseEndpoint);
        const apiVer   = livyCfg.livyApiVersion;
        const jobProps = jobDef?.properties?.jobProperties ?? {};

        const batchReq = {
            name:      `localRun-${jobRef}-${this.runId.slice(0, 8)}`,
            file:      jobProps.file ?? tp.file,
            className: jobProps.className ?? tp.className,
            args:      jobProps.args  ?? tp.args  ?? [],
            conf:      jobProps.conf  ?? tp.conf  ?? {},
        };

        if (!batchReq.file) {
            throw new Error(
                `SparkJob: cannot determine Spark job file path for "${jobRef}". ` +
                `Ensure sparkJobDefinition/${jobRef}.json exists in the workspace.`
            );
        }

        const batch   = await client.createBatch(sparkPool, apiVer, batchReq);
        const batchId = batch.id;

        try {
            await client.waitForBatch(
                sparkPool, apiVer, batchId,
                livyCfg.sessionPollIntervalMs,
                livyCfg.batchTimeoutMinutes * 60_000,
                (state) => this.emit('activityUpdate', {
                    name: activity.name, status: 'Running', output: null, error: null,
                    _detail: `Spark batch state: ${state}`,
                })
            );
            return { jobRef, batchId, status: 'Succeeded' };
        } catch (err) {
            await client.deleteBatch(sparkPool, apiVer, batchId).catch(() => {});
            throw err;
        }
    },

    // â”€â”€ WebHook â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async webHookHandler(activity) {
        const tp      = activity.typeProperties || {};
        const url     = String(this._eval(tp.url, {}));
        const method  = (tp.method || 'POST').toUpperCase();
        const body    = tp.body ? this._eval(tp.body, {}) : undefined;
        // Timeout may be a plain number (seconds) or ISO 8601 duration (e.g. "PT1M", "PT30S").
        const _parseTimeout = (val) => {
            if (!val) return 600000;
            const s = String(val);
            const iso = s.match(/^PT?(?:(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/);
            if (iso && s.startsWith('P')) {
                const h = parseFloat(iso[1] || 0), m = parseFloat(iso[2] || 0), sec = parseFloat(iso[3] || 0);
                return Math.max(1000, (h * 3600 + m * 60 + sec) * 1000);
            }
            const sec = parseFloat(s);
            return isNaN(sec) ? 600000 : Math.max(1000, sec * 1000);
        };
        const timeout = _parseTimeout(tp.callBackTimeoutInSecs ?? tp.timeout);

        const headers = {};
        if (tp.headers && typeof tp.headers === 'object') {
            for (const [k, v] of Object.entries(tp.headers)) {
                headers[k] = String(this._eval(v?.value ?? v, {}));
            }
        }
        if (!headers['Content-Type'] && body) headers['Content-Type'] = 'application/json';

        const https2 = require('https');
        const http2  = require('http');
        const { URL: NodeURL2 } = require('url');
        const parsed  = new NodeURL2(url);
        const lib     = parsed.protocol === 'https:' ? https2 : http2;
        const bodyStr = body !== undefined
            ? (typeof body === 'string' ? body : JSON.stringify(body))
            : null;

        const responseText = await new Promise((resolve, reject) => {
            const timer = setTimeout(
                () => reject(new Error(`WebHook timed out after ${timeout / 1000}s`)),
                timeout
            );
            const opts = {
                hostname: parsed.hostname,
                port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
                path:     parsed.pathname + parsed.search,
                method,
                headers: { ...headers, ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}) },
            };
            const req = lib.request(opts, (res) => {
                let data = '';
                res.on('data', c => { data += c; });
                res.on('end', () => {
                    clearTimeout(timer);
                    if (res.statusCode >= 400) {
                        reject(new Error(`WebHook HTTP ${res.statusCode}: ${data.slice(0, 200)}`));
                    } else {
                        resolve(data);
                    }
                });
            });
            req.on('error', (err) => { clearTimeout(timer); reject(err); });
            if (bodyStr) req.write(bodyStr);
            req.end();
        });

        try { return JSON.parse(responseText); } catch { return { response: responseText }; }
    },

    // â”€â”€ Lookup (ADLS/Blob only) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async lookupHandler(activity) {
        const tp          = activity.typeProperties || {};
        const dsName      = tp.source?.dataset?.referenceName ?? tp.dataset?.referenceName;
        const firstRowOnly = tp.firstRowOnly !== false;

        if (!dsName) throw new Error('Lookup: missing dataset reference in typeProperties');

        const loc = resolveDatasetToAdls(dsName, this.workspaceRoot, this.extensionPath);
        if (!loc || !loc.storageAccount) {
            throw new Error(
                `Lookup: dataset "${dsName}" does not resolve to an ADLS Gen2 / Blob location. ` +
                'Only AzureBlobFS and AzureBlobStorage linked services are supported in local run Lookup.'
            );
        }

        const adls     = new ADLSRestClient(loc.storageAccount);
        const filePath = buildAdlsPath(loc);
        const rawText  = await adls.readFile(loc.container, filePath);

        // Detect format from dataset type name
        const dsFile = path.join(this.workspaceRoot, 'dataset', `${dsName}.json`);
        const ds     = JSON.parse(fs.readFileSync(dsFile, 'utf8'));
        const dsType = ds.properties?.type ?? '';

        let rows = [];
        if (dsType.includes('Json') || filePath.endsWith('.json')) {
            const parsed = JSON.parse(rawText);
            rows = Array.isArray(parsed) ? parsed : (parsed.value ?? [parsed]);
        } else if (dsType.includes('DelimitedText') || dsType.includes('Csv') || filePath.endsWith('.csv')) {
            rows = _parseCsv(rawText).rows;
        } else {
            rows = [{ value: rawText }];
        }

        if (firstRowOnly) {
            return { firstRow: rows[0] ?? null, count: rows.length };
        }
        return { value: rows, count: rows.length };
    },

    // â”€â”€ GetMetadata (ADLS/Blob only) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async getMetadataHandler(activity) {
        const tp        = activity.typeProperties || {};
        const dsName    = tp.dataset?.referenceName;
        const fieldList = tp.fieldList ?? [];

        if (!dsName) throw new Error('GetMetadata: missing dataset reference in typeProperties');

        const loc = resolveDatasetToAdls(dsName, this.workspaceRoot, this.extensionPath);
        if (!loc || !loc.storageAccount) {
            throw new Error(
                `GetMetadata: dataset "${dsName}" does not resolve to an ADLS Gen2 / Blob location. ` +
                'Only AzureBlobFS and AzureBlobStorage linked services are supported in local run GetMetadata.'
            );
        }

        const adls     = new ADLSRestClient(loc.storageAccount);
        const filePath = buildAdlsPath(loc);
        const output   = {};

        for (const fieldEntry of fieldList) {
            const field = typeof fieldEntry === 'object' ? (fieldEntry.value ?? fieldEntry.field) : fieldEntry;
            switch (field) {
                case 'itemName':
                    output.itemName = filePath.split('/').pop() || filePath;
                    break;
                case 'itemType':
                    output.itemType = loc.fileName ? 'File' : 'Folder';
                    break;
                case 'size': {
                    const props = await adls.getFileProperties(loc.container, filePath);
                    output.size = props.contentLength ? parseInt(props.contentLength, 10) : 0;
                    break;
                }
                case 'lastModified': {
                    const props = await adls.getFileProperties(loc.container, filePath);
                    output.lastModified = props.lastModified ?? null;
                    break;
                }
                case 'childItems': {
                    try {
                        const items = await adls.listPaths(loc.container, filePath, false);
                        output.childItems = items.map(item => ({
                            name: (item.name ?? '').split('/').pop(),
                            type: item.isDirectory ? 'Folder' : 'File',
                        }));
                    } catch {
                        output.childItems = [];
                    }
                    break;
                }
                case 'exists': {
                    try {
                        await adls.getFileProperties(loc.container, filePath);
                        output.exists = true;
                    } catch {
                        output.exists = false;
                    }
                    break;
                }
                case 'count': {
                    try {
                        const items = await adls.listPaths(loc.container, filePath, false);
                        output.count = items.length;
                    } catch {
                        output.count = 0;
                    }
                    break;
                }
                case 'contentMD5': {
                    const props = await adls.getFileProperties(loc.container, filePath);
                    output.contentMD5 = props.contentMD5 ?? null;
                    break;
                }
                default:
                    output[field] = null;
            }
        }
        return output;
    },

    // â”€â”€ Delete (ADLS/Blob only) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async deleteHandler(activity) {
        const tp     = activity.typeProperties || {};
        const dsName = tp.dataset?.referenceName
            ?? tp.storeSettings?.linkedServiceName?.referenceName;

        if (!dsName) throw new Error('Delete: missing dataset reference in typeProperties');

        const loc = resolveDatasetToAdls(dsName, this.workspaceRoot, this.extensionPath);
        if (!loc || !loc.storageAccount) {
            throw new Error(
                `Delete: dataset "${dsName}" does not resolve to an ADLS Gen2 / Blob location. ` +
                'Only AzureBlobFS and AzureBlobStorage linked services are supported in local run Delete.'
            );
        }

        const filePath = buildAdlsPath(loc);
        const adls     = new ADLSRestClient(loc.storageAccount);
        await adls.deleteFile(loc.container, filePath);
        return { datasetName: dsName, deletedPath: `${loc.container}/${filePath}`, status: 'Succeeded' };
    },

    // â”€â”€ Validation (ADLS/Blob only) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    async validationHandler(activity) {
        const tp         = activity.typeProperties || {};
        const dsName     = tp.dataset?.referenceName;
        const sleepSecs  = parseInt(tp.sleep    ?? '10',  10);
        const timeoutSec = parseAdfTimespan(tp.timeout, 600);
        const minSize    = tp.minimumSize !== undefined ? parseInt(tp.minimumSize, 10) : null;

        if (!dsName) throw new Error('Validation: missing dataset reference in typeProperties');

        const loc = resolveDatasetToAdls(dsName, this.workspaceRoot, this.extensionPath);
        if (!loc || !loc.storageAccount) {
            throw new Error(
                `Validation: dataset "${dsName}" does not resolve to an ADLS Gen2 / Blob location. ` +
                'Only AzureBlobFS and AzureBlobStorage linked services are supported in local run Validation.'
            );
        }

        const filePath = buildAdlsPath(loc);
        const adls     = new ADLSRestClient(loc.storageAccount);
        const start    = Date.now();
        const timeoutMs = timeoutSec * 1000;

        while (Date.now() - start < timeoutMs) {
            if (this._cancelled) break;
            try {
                const props = await adls.getFileProperties(loc.container, filePath);
                if (minSize !== null) {
                    const size = parseInt(props.contentLength ?? '0', 10);
                    if (size < minSize) {
                        await _sleepMs(sleepSecs * 1000);
                        continue;
                    }
                }
                return { datasetName: dsName, path: `${loc.container}/${filePath}`, existed: true };
            } catch {
                // File not yet present â€” sleep and retry
                await _sleepMs(sleepSecs * 1000);
            }
        }

        throw new Error(
            `Validation timed out after ${timeoutSec}s: "${dsName}" (${loc.container}/${filePath}) ` +
            `did not appear${minSize !== null ? ` with size â‰¥ ${minSize}` : ''} within the timeout.`
        );
    },

    // -- Copy (ADLS/Blob -> ADLS/Blob, or ADLS CSV/JSON -> Azure SQL) --
    async copyHandler(activity) {
        const tp = activity.typeProperties || {};
        const srcName  = tp.source?.dataset?.referenceName ?? (activity.inputs?.[0]?.referenceName);
        const sinkName = tp.sink?.dataset?.referenceName   ?? (activity.outputs?.[0]?.referenceName);
        if (!srcName)  throw new Error('Copy: cannot determine source dataset reference');
        if (!sinkName) throw new Error('Copy: cannot determine sink dataset reference');
        const srcLoc = resolveDatasetToAdls(srcName, this.workspaceRoot, this.extensionPath);
        if (!srcLoc || !srcLoc.storageAccount)
            throw new Error(`Copy: source dataset "${srcName}" does not resolve to ADLS Gen2 / Blob.`);
        // -- SQL sink path --
        const sinkSql = resolveSqlDataset(sinkName, this.workspaceRoot, this.extensionPath);
        if (sinkSql) {
            const sqlConn = resolveSqlLinkedService(sinkSql.linkedServiceName, this.workspaceRoot, this.extensionPath);
            if (!sqlConn) throw new Error(`Copy: cannot resolve SQL linked service for sink "${sinkName}"`);
            const srcAdls   = new ADLSRestClient(srcLoc.storageAccount);
            const srcPath   = buildAdlsPath(srcLoc);
            const rawText   = await srcAdls.readFile(srcLoc.container, srcPath);
            const srcDsFile = path.join(this.workspaceRoot, 'dataset', `${srcName}.json`);
            const srcDsType = fs.existsSync(srcDsFile) ? (JSON.parse(fs.readFileSync(srcDsFile, 'utf8')).properties?.type || '') : '';
            let rows = [], columns = [];
            if (!srcDsType || srcDsType === 'DelimitedText') {
                ({ rows, columns } = _parseCsv(rawText));
            } else if (srcDsType === 'Json') {
                const parsed = JSON.parse(rawText);
                const arr = Array.isArray(parsed) ? parsed : [parsed];
                columns = arr.length ? Object.keys(arr[0]) : [];
                rows = arr;
            } else if (srcDsType === 'Parquet') {
                const rawBuf  = await srcAdls.readFileBuffer(srcLoc.container, srcPath);
                const tmpPath = path.join(os.tmpdir(), `adf-parquet-${Date.now()}.parquet`);
                fs.writeFileSync(tmpPath, rawBuf);
                try {
                    ({ rows, columns } = await readParquetFile(tmpPath));
                } finally {
                    try { fs.unlinkSync(tmpPath); } catch (_) {}
                }
            } else {
                throw new Error(`Copy: source format "${srcDsType}" not supported for SQL sink in local run.`);
            }
            const client = new SqlClient(sqlConn.server, sqlConn.database);
            const result = await client.bulkInsert(sinkSql.schema, sinkSql.table, rows, columns);
            return {
                source: `${srcLoc.storageAccount}/${srcLoc.container}/${srcPath}`,
                sink: `${sqlConn.server}/${sqlConn.database}/${sinkSql.schema}.${sinkSql.table}`,
                rowsCopied: result.rowsAffected, format: srcDsType || 'DelimitedText',
            };
        }
        // -- ADLS/Blob -> ADLS/Blob path --
        const sinkLoc = resolveDatasetToAdls(sinkName, this.workspaceRoot, this.extensionPath);
        if (!sinkLoc || !sinkLoc.storageAccount)
            throw new Error(`Copy: sink dataset "${sinkName}" does not resolve to ADLS Gen2 / Blob or Azure SQL.`);
        const srcType  = srcLoc.isAdls  ? 'adls' : 'blob';
        const sinkType = sinkLoc.isAdls ? 'adls' : 'blob';
        const pairKey  = `${srcType}->${sinkType}`;
        if (!copyConfig.supportedPairs[pairKey]) {
            const reason = copyConfig.unsupportedPairs?.[pairKey] || `Copy from "${srcType}" to "${sinkType}" is not supported in local run mode.`;
            throw new Error(`Copy: ${reason}`);
        }
        const srcDsFile  = path.join(this.workspaceRoot, 'dataset', `${srcName}.json`);
        const sinkDsFile = path.join(this.workspaceRoot, 'dataset', `${sinkName}.json`);
        const srcDsType  = fs.existsSync(srcDsFile)  ? (JSON.parse(fs.readFileSync(srcDsFile,  'utf8')).properties?.type || '') : '';
        const sinkDsType = fs.existsSync(sinkDsFile) ? (JSON.parse(fs.readFileSync(sinkDsFile, 'utf8')).properties?.type || '') : '';
        if (srcDsType && sinkDsType && srcDsType !== sinkDsType)
            throw new Error(`Copy: format conversion from "${srcDsType}" to "${sinkDsType}" is not supported in local run mode.`);
        const srcAdls  = new ADLSRestClient(srcLoc.storageAccount);
        const sinkAdls = new ADLSRestClient(sinkLoc.storageAccount);
        const srcPath  = buildAdlsPath(srcLoc);
        const sinkPath = buildAdlsPath(sinkLoc);
        const content  = await srcAdls.readFile(srcLoc.container, srcPath);
        await sinkAdls.writeFile(sinkLoc.container, sinkPath, content);
        return { source: `${srcLoc.storageAccount}/${srcLoc.container}/${srcPath}`, sink: `${sinkLoc.storageAccount}/${sinkLoc.container}/${sinkPath}`, bytes: Buffer.byteLength(content, 'utf8'), format: srcDsType || 'unknown', note: 'Local run: raw text stream copy (same format only)' };
    },

    // -- Script (Azure SQL via Python subprocess) --
    async scriptHandler(activity) {
        const lsName = activity.linkedServiceName?.referenceName;
        if (!lsName) throw new Error('Script: missing linkedServiceName');
        const tp = activity.typeProperties || {};
        const scripts = (tp.scripts || []).map(s => ({ type: s.type || 'Query', text: String(this._eval(s.text || '', {}) || '') }));
        if (!scripts.length) throw new Error('Script: no scripts in typeProperties.scripts');
        const sqlConn = resolveSqlLinkedService(lsName, this.workspaceRoot, this.extensionPath);
        if (!sqlConn) throw new Error(`Script: cannot resolve linked service "${lsName}" to a SQL connection.`);
        const client = new SqlClient(sqlConn.server, sqlConn.database);
        const results = await client.executeScripts(scripts);
        const last = results[results.length - 1] || {};
        return { resultSetCount: results.length, recordsetCount: results.reduce((n, r) => n + (r.recordset?.length || 0), 0), recordset: last.recordset || [], rowsAffected: last.rowsAffected || 0 };
    },

    // -- SqlServerStoredProcedure (Azure SQL via Python subprocess) --
    async storedProcedureHandler(activity) {
        const lsName = activity.linkedServiceName?.referenceName;
        if (!lsName) throw new Error('SqlServerStoredProcedure: missing linkedServiceName');
        const tp = activity.typeProperties || {};
        const procName = String(this._eval(tp.storedProcedureName || tp.storedProcedure || '', {}) || '');
        if (!procName) throw new Error('SqlServerStoredProcedure: missing storedProcedureName');
        const rawParams = tp.storedProcedureParameters || {};
        const params = {};
        for (const [name, def] of Object.entries(rawParams))
            params[name] = { type: def.type, value: this._eval(def.value, {}) };
        const sqlConn = resolveSqlLinkedService(lsName, this.workspaceRoot, this.extensionPath);
        if (!sqlConn) throw new Error(`SqlServerStoredProcedure: cannot resolve linked service "${lsName}".`);
        const client = new SqlClient(sqlConn.server, sqlConn.database);
        const result = await client.executeStoredProcedure(procName, params);
        return { storedProcedureName: procName, rowsAffected: result.rowsAffected, recordset: result.recordset || [] };
    },
    async notSupportedHandler(activity) {
        const conf   = RUNNERS[activity.type];
        const reason = conf?.notSupportedReason ?? `"${activity.type}" is not supported in local run mode.`;
        const err    = new Error(reason);
        err.notSupported = true;
        throw err;
    },
};

// â”€â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _randomId() {
    return Math.random().toString(36).slice(2, 10) + Math.random().toString(36).slice(2, 10);
}

function _initVariables(variableDefs) {
    const vars = {};
    for (const [name, def] of Object.entries(variableDefs)) {
        if (def.defaultValue !== undefined) vars[name] = def.defaultValue;
        else if (def.type === 'Array')   vars[name] = [];
        else if (def.type === 'Boolean' || def.type === 'Bool') vars[name] = false;
        else vars[name] = '';
    }
    return vars;
}

function _patchForEachContext(childRunner, currentItem) {
    // Monkey-patch child's _eval to inject @item() context
    const origEval = childRunner._eval.bind(childRunner);
    childRunner._eval = (value, extra) => origEval(value, { ...extra, currentItem });
}

// â”€â”€ Synapse workspace config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Reads synapse-local-run.json from the workspace root. Returns {} if absent.
function _loadSynapseWorkspaceConfig(workspaceRoot, extensionPath) {
    // Prefer the extension folder (developer convenience), fall back to workspace root.
    for (const dir of [extensionPath, workspaceRoot].filter(Boolean)) {
        const cfgFile = path.join(dir, 'synapse-local-run.json');
        if (fs.existsSync(cfgFile)) {
            try { return JSON.parse(fs.readFileSync(cfgFile, 'utf8')); }
            catch { /* corrupt file â€” try next location */ }
        }
    }
    return {};
}

function _sleepMs(ms) { return new Promise(r => setTimeout(r, ms)); }

// Merges all notebook cells (markdown + code) with per-code-cell execution results.
// Returns a flat array for the snapshot viewer: each item has { cellType, source, output? }.
function _mergeSnapshotCells(allCells, cellResults) {
    const merged = [];
    let codeIdx = 0;
    for (const cell of allCells) {
        const cellType = cell.cell_type ?? cell.cellType ?? 'code';
        const src = Array.isArray(cell.source) ? cell.source.join('') : (cell.source ?? '');
        if (cellType === 'markdown') {
            merged.push({ cellType: 'markdown', source: src });
        } else {
            const result = cellResults[codeIdx++] ?? null;
            merged.push({ cellType: 'code', source: src, output: result?.output ?? null });
        }
    }
    return merged;
}

// Minimal CSV parser: handles quoted fields, returns array of row objects.
function _parseCsv(text) {
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
    if (lines.length === 0) return { rows: [], columns: [] };
    const columns = _splitCsvLine(lines[0]);
    const rows = lines.slice(1).map(line => {
        const vals = _splitCsvLine(line);
        const row  = {};
        columns.forEach((h, i) => { row[h] = vals[i] ?? ''; });
        return row;
    });
    return { rows, columns };
}

function _splitCsvLine(line) {
    const result = [];
    let current  = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        if (line[i] === '"') {
            if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
            else { inQuotes = !inQuotes; }
        } else if (line[i] === ',' && !inQuotes) {
            result.push(current);
            current = '';
        } else {
            current += line[i];
        }
    }
    result.push(current);
    return result;
}

module.exports = { LocalPipelineRunner, _parseCsv, parseAdfTimespan };

