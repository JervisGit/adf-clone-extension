'use strict';
// synapseClient.js — Synapse data-plane Livy API client.
// Used by SynapseNotebook and SparkJob local-run handlers.
//
// Auth: DefaultAzureCredential (az login / env / managed identity)
// Scope: https://dev.azuresynapse.net/.default

const { DefaultAzureCredential } = require('@azure/identity');
const https = require('https');
const http  = require('http');
const { URL: NodeURL } = require('url');

const SYNAPSE_SCOPE = 'https://dev.azuresynapse.net/.default';

// Livy session/batch states (Synapse Spark)
const SESSION_IDLE_STATES  = new Set(['idle']);
const SESSION_ERROR_STATES = new Set(['dead', 'error', 'killed', 'shutting_down', 'recovering']);
const BATCH_DONE_STATES    = new Set(['success']);
const BATCH_ERROR_STATES   = new Set(['dead', 'error', 'killed']);

// Maps notebook metadata language to Livy session kind
const NOTEBOOK_LANG_TO_KIND = {
    python: 'pyspark',
    scala:  'spark',
    r:      'sparkr',
    sql:    'sql',
    pyspark:'pyspark',
    spark:  'spark',
    sparkr: 'sparkr',
};

class SynapseClient {
    constructor(endpoint) {
        this.endpoint   = endpoint.replace(/\/$/, '');
        this.credential = new DefaultAzureCredential();
        this._token     = null;
        this._tokenExp  = 0;
    }

    // ── Auth ──────────────────────────────────────────────────────────────────

    async _getToken() {
        if (!this._token || Date.now() >= this._tokenExp - 30_000) {
            const t = await this.credential.getToken(SYNAPSE_SCOPE);
            this._token    = t.token;
            this._tokenExp = t.expiresOnTimestamp;
        }
        return this._token;
    }

    // ── HTTP ──────────────────────────────────────────────────────────────────

    async _request(method, urlPath, body) {
        const token   = await this._getToken();
        const fullUrl = new NodeURL(`${this.endpoint}${urlPath}`);
        const lib     = fullUrl.protocol === 'https:' ? https : http;
        const bodyStr = body ? JSON.stringify(body) : null;

        return new Promise((resolve, reject) => {
            const opts = {
                hostname: fullUrl.hostname,
                port:     fullUrl.port || (fullUrl.protocol === 'https:' ? 443 : 80),
                path:     fullUrl.pathname + fullUrl.search,
                method,
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type':  'application/json',
                    ...(bodyStr ? { 'Content-Length': Buffer.byteLength(bodyStr) } : {}),
                },
            };

            const req = lib.request(opts, (res) => {
                let data = '';
                res.on('data', c => { data += c; });
                res.on('end', () => {
                    if (res.statusCode === 404) { reject(new Error(`Not found: ${method} ${urlPath}`)); return; }
                    if (res.statusCode >= 400)  { reject(new Error(`Synapse API ${res.statusCode}: ${data.slice(0, 400)}`)); return; }
                    if (res.statusCode === 204 || !data.trim()) { resolve({}); return; }
                    try { resolve(JSON.parse(data)); }
                    catch { resolve({ _raw: data }); }
                });
            });
            req.on('error', reject);
            if (bodyStr) req.write(bodyStr);
            req.end();
        });
    }

    // ── Livy Sessions (interactive notebook execution) ─────────────────────────

    async createSession(sparkPool, apiVersion, name, kind, conf) {
        return this._request('POST',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/sessions`,
            { name, kind, conf: conf || {}, heartbeatTimeoutInSecond: 3600 });
    }

    async getSession(sparkPool, apiVersion, sessionId) {
        return this._request('GET',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/sessions/${sessionId}`);
    }

    async listSessions(sparkPool, apiVersion) {
        const res = await this._request('GET',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/sessions`);
        return res.sessions || [];
    }

    async deleteSession(sparkPool, apiVersion, sessionId) {
        return this._request('DELETE',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/sessions/${sessionId}`);
    }

    async submitStatement(sparkPool, apiVersion, sessionId, code, kind) {
        return this._request('POST',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/sessions/${sessionId}/statements`,
            { code, kind });
    }

    async getStatement(sparkPool, apiVersion, sessionId, stmtId) {
        return this._request('GET',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/sessions/${sessionId}/statements/${stmtId}`);
    }

    // ── Livy Batches (Spark job execution) ────────────────────────────────────

    async createBatch(sparkPool, apiVersion, batchReq) {
        return this._request('POST',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/batches`,
            batchReq);
    }

    async getBatch(sparkPool, apiVersion, batchId) {
        return this._request('GET',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/batches/${batchId}`);
    }

    async deleteBatch(sparkPool, apiVersion, batchId) {
        return this._request('DELETE',
            `/livyApi/versions/${apiVersion}/sparkPools/${sparkPool}/batches/${batchId}`);
    }

    // ── Poll helpers ──────────────────────────────────────────────────────────

    /**
     * Waits for a Livy session to become idle (ready to accept statements).
     * onStatus(state) is called each poll cycle so callers can surface progress.
     */
    async waitForSessionIdle(sparkPool, apiVersion, sessionId, pollMs, timeoutMs, onStatus) {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            const s = await this.getSession(sparkPool, apiVersion, sessionId);
            onStatus?.(s.state);
            if (SESSION_IDLE_STATES.has(s.state))  return s;
            if (SESSION_ERROR_STATES.has(s.state)) {
                throw new Error(`Spark session entered error state "${s.state}"`);
            }
            await _sleep(pollMs);
        }
        throw new Error(
            `Timed out (>${Math.round(timeoutMs / 60000)} min) waiting for Spark session to become idle. ` +
            `The session may still be starting — check Synapse Studio to verify pool capacity.`
        );
    }

    /**
     * Polls a submitted statement until it is 'available' (done) or errors.
     */
    async waitForStatement(sparkPool, apiVersion, sessionId, stmtId, pollMs) {
        while (true) {
            const s = await this.getStatement(sparkPool, apiVersion, sessionId, stmtId);
            if (s.state === 'available') return s;
            if (s.state === 'error' || s.state === 'cancelled') {
                const msg = s.output?.evalue
                    || s.output?.text
                    || `Statement entered state "${s.state}"`;
                throw new Error(msg);
            }
            await _sleep(pollMs);
        }
    }

    /**
     * Polls a Livy batch job until success or failure.
     */
    async waitForBatch(sparkPool, apiVersion, batchId, pollMs, timeoutMs, onStatus) {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            const b = await this.getBatch(sparkPool, apiVersion, batchId);
            onStatus?.(b.state);
            if (BATCH_DONE_STATES.has(b.state))  return b;
            if (BATCH_ERROR_STATES.has(b.state)) {
                const logTail = (b.log || []).slice(-5).join('\n');
                throw new Error(
                    `Spark batch entered state "${b.state}".${logTail ? '\n' + logTail : ''}`
                );
            }
            await _sleep(pollMs);
        }
        throw new Error('Timed out waiting for Spark batch job to complete.');
    }
}

function _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

module.exports = { SynapseClient, NOTEBOOK_LANG_TO_KIND };
