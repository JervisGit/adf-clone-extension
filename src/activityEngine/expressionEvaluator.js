'use strict';
// expressionEvaluator.js — evaluates ADF expression strings in a local run context.
//
// Accepts:
//   value      : any (could be a literal, a string starting with @, or an interpolated @{} string)
//   context    : { parameters, variables, activityOutputs, currentItem, currentItemIndex, runId, pipeline }
//
// Returns the evaluated JavaScript value, or the original string if evaluation is not possible.
// Never throws — failures are surfaced as the string "[EVAL ERROR: <message>]" so the runner can continue.
//
// Design principle: evaluation logic is table-driven where possible.
// Adding a new ADF function = add one entry to FUNCTIONS map.

const runConfig = require('../local-run-config.json');
const ENABLED_FUNCTIONS = new Set(runConfig.expressionEvaluator.enabledFunctions);

// ─── Public API ───────────────────────────────────────────────────────────────
module.exports = { evaluate, isExpression };

/**
 * Returns true when a value is an ADF expression (starts with @ or contains @{}).
 */
function isExpression(value) {
    if (typeof value !== 'string') return false;
    return value.startsWith('@') || value.includes('@{');
}

/**
 * Evaluate an ADF expression or interpolated string.
 * Returns the computed value.  Never throws.
 */
function evaluate(value, context) {
    if (!isExpression(value)) return value;
    try {
        if (value.startsWith('@{') || (!value.startsWith('@@') && value.includes('@{'))) {
            return evaluateInterpolated(value, context);
        }
        if (value.startsWith('@@')) {
            // Escaped @ — return literal @ + rest
            return '@' + value.slice(2);
        }
        // Pure expression: @<expr>
        return evalExpr(value.slice(1).trim(), context);
    } catch (e) {
        return `[EVAL ERROR: ${e.message}]`;
    }
}

// ─── Interpolated strings ─────────────────────────────────────────────────────
// Handles strings like "prefix @{expr1} middle @{expr2} suffix"
function evaluateInterpolated(str, context) {
    return str.replace(/@{([^}]*)}/g, (_, inner) => {
        try {
            const result = evalExpr(inner.trim(), context);
            return result === null || result === undefined ? '' : String(result);
        } catch (e) {
            return `[EVAL ERROR: ${e.message}]`;
        }
    });
}

// ─── Core expression evaluator ────────────────────────────────────────────────
// Parses and evaluates a single ADF expression (without leading @).
function evalExpr(expr, context) {
    expr = expr.trim();

    // ── String literals ────────────────────────────────────────────────────────
    if ((expr.startsWith("'") && expr.endsWith("'")) ||
        (expr.startsWith('"') && expr.endsWith('"'))) {
        return expr.slice(1, -1);
    }

    // ── Numeric literals ──────────────────────────────────────────────────────
    if (!isNaN(Number(expr)) && expr !== '') {
        return Number(expr);
    }

    // ── Boolean / null literals ────────────────────────────────────────────────
    if (expr === 'true')  return true;
    if (expr === 'false') return false;
    if (expr === 'null')  return null;

    // ── pipeline().parameters.X ───────────────────────────────────────────────
    {
        const m = /^pipeline\(\)\.parameters\.(\w+)$/.exec(expr);
        if (m) {
            const params = context.parameters || {};
            return params.hasOwnProperty(m[1]) ? params[m[1]] : null;
        }
    }
    // pipeline().parameters['X'] (bracket notation)
    {
        const m = /^pipeline\(\)\.parameters\['([^']+)'\]$/.exec(expr);
        if (m) {
            const params = context.parameters || {};
            return params.hasOwnProperty(m[1]) ? params[m[1]] : null;
        }
    }

    // ── pipeline().globalParameters.X ─────────────────────────────────────────
    {
        const m = /^pipeline\(\)\.globalParameters\.(\w+)$/.exec(expr);
        if (m) {
            const gp = context.globalParameters || {};
            return gp.hasOwnProperty(m[1]) ? gp[m[1]] : null;
        }
    }

    // ── variables('X') ────────────────────────────────────────────────────────
    {
        const m = /^variables\('([^']+)'\)$/.exec(expr);
        if (m) {
            const vars = context.variables || {};
            return vars.hasOwnProperty(m[1]) ? vars[m[1]] : null;
        }
    }

    // ── activity('X').output[.path...] ────────────────────────────────────────
    {
        const m = /^activity\('([^']+)'\)\.output(.*)$/.exec(expr);
        if (m) {
            const outputs = context.activityOutputs || {};
            const out = outputs[m[1]];
            if (out === undefined) return null;
            if (!m[2]) return out;
            return getNestedField(out, m[2].replace(/^\./, ''));
        }
    }

    // ── activity('X').status ──────────────────────────────────────────────────
    {
        const m = /^activity\('([^']+)'\)\.status$/.exec(expr);
        if (m) {
            const statuses = context.activityStatuses || {};
            return statuses[m[1]] ?? null;
        }
    }

    // ── @item() / @iterationItem() ────────────────────────────────────────────
    if (expr === 'item()' || expr === 'iterationItem()') {
        return context.currentItem !== undefined ? context.currentItem : null;
    }
    {
        const m = /^(?:item|iterationItem)\(\)\.(.+)$/.exec(expr);
        if (m) {
            if (context.currentItem !== null && typeof context.currentItem === 'object') {
                return getNestedField(context.currentItem, m[1]);
            }
            return null;
        }
    }

    // ── @range(start, count) ─────────────────────────────────────────────────
    {
        const m = /^range\((.+),\s*(.+)\)$/.exec(expr);
        if (m) {
            const start = evalExpr(m[1].trim(), context);
            const count = evalExpr(m[2].trim(), context);
            if (typeof start === 'number' && typeof count === 'number') {
                return Array.from({ length: count }, (_, i) => start + i);
            }
        }
    }

    // ── Function calls ────────────────────────────────────────────────────────
    {
        const funcMatch = /^(\w+)\((.*)?\)$/.exec(expr);
        if (funcMatch) {
            const fnName = funcMatch[1];
            if (ENABLED_FUNCTIONS.has(fnName) && FUNCTIONS[fnName]) {
                const argsRaw = funcMatch[2] ? parseArgs(funcMatch[2]) : [];
                const args = argsRaw.map(a => evalExpr(a.trim(), context));
                return FUNCTIONS[fnName](args, context);
            }
        }
    }

    // ── Bare identifier: @varName shorthand for @variables('varName') ─────────
    // ADF allows `@myVar` as a shorthand when it matches a pipeline variable.
    // We also check parameters as a fallback (useful for `@param` shorthand).
    {
        const identMatch = /^[A-Za-z_]\w*$/.exec(expr);
        if (identMatch) {
            const vars   = context.variables   || {};
            const params = context.parameters  || {};
            if (vars.hasOwnProperty(expr))   return vars[expr];
            if (params.hasOwnProperty(expr)) return params[expr];
        }
    }

    throw new Error(`Unsupported expression: "${expr}"`);
}

// ─── Nested field path resolver ───────────────────────────────────────────────
// Handles paths like "firstRow.columnA" or "['key with spaces']"
function getNestedField(obj, path) {
    if (!path || obj == null) return obj;
    const parts = [];
    let remaining = path;
    while (remaining) {
        const bracketMatch = /^\['([^']+)'\]\.?(.*)$/.exec(remaining);
        const dotMatch = /^([^.[]+)\.?(.*)$/.exec(remaining);
        if (bracketMatch) {
            parts.push(bracketMatch[1]);
            remaining = bracketMatch[2];
        } else if (dotMatch) {
            parts.push(dotMatch[1]);
            remaining = dotMatch[2];
        } else {
            break;
        }
    }
    let cur = obj;
    for (const p of parts) {
        if (cur == null) return null;
        cur = cur[p];
    }
    return cur ?? null;
}

// ─── Function argument parser ─────────────────────────────────────────────────
// Splits top-level comma-separated args, respecting quotes, parentheses and brackets.
function parseArgs(raw) {
    const args = [];
    let depth = 0, inSingle = false, inDouble = false, cur = '';
    for (let i = 0; i < raw.length; i++) {
        const c = raw[i];
        if (c === "'" && !inDouble) { inSingle = !inSingle; cur += c; continue; }
        if (c === '"' && !inSingle)  { inDouble = !inDouble; cur += c; continue; }
        if (inSingle || inDouble)    { cur += c; continue; }
        if (c === '(' || c === '[')  { depth++; cur += c; continue; }
        if (c === ')' || c === ']')  { depth--; cur += c; continue; }
        if (c === ',' && depth === 0) { args.push(cur.trim()); cur = ''; continue; }
        cur += c;
    }
    if (cur.trim()) args.push(cur.trim());
    return args;
}

// ─── ADF function implementations ─────────────────────────────────────────────
// Each function receives (evaluatedArgs[], context) and returns a value.
const FUNCTIONS = {
    // String
    concat:   (a) => a.map(x => x === null || x === undefined ? '' : String(x)).join(''),
    string:   (a) => a[0] === null || a[0] === undefined ? '' : String(a[0]),
    trim:     (a) => (typeof a[0] === 'string') ? a[0].trim() : a[0],
    ltrim:    (a) => (typeof a[0] === 'string') ? a[0].replace(/^\s+/, '') : a[0],
    rtrim:    (a) => (typeof a[0] === 'string') ? a[0].replace(/\s+$/, '') : a[0],
    toLower:  (a) => (typeof a[0] === 'string') ? a[0].toLowerCase() : a[0],
    toUpper:  (a) => (typeof a[0] === 'string') ? a[0].toUpperCase() : a[0],
    replace:  (a) => (typeof a[0] === 'string') ? a[0].split(a[1]).join(a[2]) : a[0],
    split:    (a) => (typeof a[0] === 'string') ? a[0].split(a[1]) : [],
    join:     (a) => Array.isArray(a[0]) ? a[0].join(a[1] ?? '') : String(a[0]),
    substring:(a) => (typeof a[0] === 'string') ? a[0].substring(a[1], a[2] !== undefined ? a[1] + a[2] : undefined) : a[0],
    startsWith:(a)=> (typeof a[0] === 'string') ? a[0].startsWith(a[1]) : false,
    endsWith: (a) => (typeof a[0] === 'string') ? a[0].endsWith(a[1]) : false,
    indexOf:  (a) => (typeof a[0] === 'string') ? a[0].indexOf(a[1]) : -1,
    encodeUriComponent: (a) => (typeof a[0] === 'string') ? encodeURIComponent(a[0]) : a[0],
    uriComponent: (a) => (typeof a[0] === 'string') ? encodeURIComponent(a[0]) : a[0],
    uriComponentToString: (a) => (typeof a[0] === 'string') ? decodeURIComponent(a[0]) : a[0],
    decodeUriComponent: (a) => (typeof a[0] === 'string') ? decodeURIComponent(a[0]) : a[0],
    base64:         (a) => (typeof a[0] === 'string') ? Buffer.from(a[0]).toString('base64') : a[0],
    base64ToString: (a) => (typeof a[0] === 'string') ? Buffer.from(a[0], 'base64').toString('utf8') : a[0],

    // Numeric
    int:   (a) => parseInt(String(a[0]), 10),
    float: (a) => parseFloat(String(a[0])),
    add:   (a) => (Number(a[0]) + Number(a[1])),
    sub:   (a) => (Number(a[0]) - Number(a[1])),
    mul:   (a) => (Number(a[0]) * Number(a[1])),
    div:   (a) => (Number(a[0]) / Number(a[1])),
    mod:   (a) => (Number(a[0]) % Number(a[1])),
    min:   (a) => Math.min(...a.map(Number)),
    max:   (a) => Math.max(...a.map(Number)),

    // Boolean / comparison
    bool:          (a) => Boolean(a[0]),
    not:           (a) => !a[0],
    and:           (a) => a.every(Boolean),
    or:            (a) => a.some(Boolean),
    equals:        (a) => a[0] === a[1],
    greater:       (a) => a[0] > a[1],
    greaterOrEquals:(a)=> a[0] >= a[1],
    less:          (a) => a[0] < a[1],
    lessOrEquals:  (a) => a[0] <= a[1],
    if:            (a) => a[0] ? a[1] : a[2],
    coalesce:      (a) => a.find(x => x !== null && x !== undefined) ?? null,
    empty:         (a) => {
        if (a[0] === null || a[0] === undefined) return true;
        if (typeof a[0] === 'string') return a[0].length === 0;
        if (Array.isArray(a[0])) return a[0].length === 0;
        if (typeof a[0] === 'object') return Object.keys(a[0]).length === 0;
        return false;
    },
    null: () => null,

    // Array / object
    json:         (a) => { try { return JSON.parse(a[0]); } catch { return null; } },
    array:        (a) => Array.isArray(a[0]) ? a[0] : [a[0]],
    createArray:  (a) => a,
    contains:     (a) => {
        if (Array.isArray(a[0])) return a[0].includes(a[1]);
        if (typeof a[0] === 'string') return a[0].includes(a[1]);
        if (a[0] && typeof a[0] === 'object') return Object.prototype.hasOwnProperty.call(a[0], a[1]);
        return false;
    },
    length:       (a) => Array.isArray(a[0]) ? a[0].length : (typeof a[0] === 'string' ? a[0].length : 0),
    first:        (a) => Array.isArray(a[0]) ? a[0][0] : (typeof a[0] === 'string' ? a[0][0] : null),
    last:         (a) => Array.isArray(a[0]) ? a[0][a[0].length - 1] : (typeof a[0] === 'string' ? a[0][a[0].length - 1] : null),
    take:         (a) => Array.isArray(a[0]) ? a[0].slice(0, a[1]) : (typeof a[0] === 'string' ? a[0].slice(0, a[1]) : a[0]),
    skip:         (a) => Array.isArray(a[0]) ? a[0].slice(a[1]) : (typeof a[0] === 'string' ? a[0].slice(a[1]) : a[0]),
    union:        (a) => {
        if (Array.isArray(a[0])) {
            const seen = new Set(a[0].map(JSON.stringify));
            const result = [...a[0]];
            for (let i = 1; i < a.length; i++) {
                for (const item of (Array.isArray(a[i]) ? a[i] : [])) {
                    const key = JSON.stringify(item);
                    if (!seen.has(key)) { seen.add(key); result.push(item); }
                }
            }
            return result;
        }
        return Object.assign({}, ...a);
    },
    intersection: (a) => {
        if (!Array.isArray(a[0])) return {};
        return a[0].filter(item => a.slice(1).every(arr => Array.isArray(arr) && arr.some(x => JSON.stringify(x) === JSON.stringify(item))));
    },

    // Date/time (basic)
    utcNow: (a) => {
        const fmt = a[0];
        const now = new Date().toISOString();
        if (!fmt) return now.replace('Z', '0000000Z').replace('T', ' ');
        // Return ISO string; custom format not fully implemented
        return now;
    },
    formatDateTime: (a) => {
        // a[0] = datetime string, a[1] = format (limited support)
        try {
            const d = new Date(String(a[0]));
            if (isNaN(d)) return String(a[0]);
            return d.toISOString();
        } catch { return String(a[0]); }
    },
    addSeconds: (a) => {
        const d = new Date(String(a[0]));
        d.setSeconds(d.getSeconds() + Number(a[1]));
        return d.toISOString();
    },
    addMinutes: (a) => {
        const d = new Date(String(a[0]));
        d.setMinutes(d.getMinutes() + Number(a[1]));
        return d.toISOString();
    },
    addHours: (a) => {
        const d = new Date(String(a[0]));
        d.setHours(d.getHours() + Number(a[1]));
        return d.toISOString();
    },
    addDays: (a) => {
        const d = new Date(String(a[0]));
        d.setDate(d.getDate() + Number(a[1]));
        return d.toISOString();
    },
    ticks: (a) => {
        // Windows FILETIME ticks: 100-nanosecond intervals since 1601-01-01
        const EPOCH_DIFF = 116444736000000000n;
        const d = new Date(String(a[0]));
        return String(BigInt(d.getTime()) * 10000n + EPOCH_DIFF);
    },
    guid:    () => generateSimpleGuid(),
    newGuid: () => generateSimpleGuid(),
};

function generateSimpleGuid() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
}
