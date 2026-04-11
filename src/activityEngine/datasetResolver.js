'use strict';
// datasetResolver.js — Resolves an ADF/Synapse dataset reference to ADLS storage
// coordinates usable by ADLSRestClient.
//
// Supported linked service types: AzureBlobFS (ADLS Gen2), AzureBlobStorage.
// Returns null for SQL-based or unsupported linked service types.

const fs   = require('fs');
const path = require('path');

/**
 * @typedef {{
 *   isAdls: boolean,
 *   storageAccount: string|null,
 *   container: string|null,
 *   folderPath: string|null,
 *   fileName: string|null,
 * }} DatasetLocation
 */

/**
 * Resolves a dataset name to storage coordinates by reading workspace JSON files.
 *
 * @param {string} datasetName
 * @param {string} workspaceRoot  absolute path to workspace Documents folder
 * @returns {DatasetLocation|null}  null if unsupported, missing, or unparseable
 */
function resolveDatasetToAdls(datasetName, workspaceRoot) {
    if (!datasetName || !workspaceRoot) return null;

    // ── Read dataset JSON ─────────────────────────────────────────────────────
    const dsFile = path.join(workspaceRoot, 'dataset', `${datasetName}.json`);
    if (!fs.existsSync(dsFile)) return null;

    let dataset;
    try { dataset = JSON.parse(fs.readFileSync(dsFile, 'utf8')); }
    catch { return null; }

    const dsProps  = dataset.properties   || {};
    const location = dsProps.typeProperties?.location || {};
    const lsName   = dsProps.linkedServiceName?.referenceName;
    if (!lsName) return null;

    // ── Read linked service JSON ──────────────────────────────────────────────
    const lsFile = path.join(workspaceRoot, 'linkedService', `${lsName}.json`);
    if (!fs.existsSync(lsFile)) return null;

    let ls;
    try { ls = JSON.parse(fs.readFileSync(lsFile, 'utf8')); }
    catch { return null; }

    const lsType  = ls.properties?.type;
    const lsProps = ls.properties?.typeProperties || {};

    // ── ADLS Gen2 (AzureBlobFS) ───────────────────────────────────────────────
    if (lsType === 'AzureBlobFS') {
        const url = lsProps.url || lsProps.endpoint || '';
        const m   = url.match(/https?:\/\/([^.]+)\.dfs\.core\.windows\.net/);
        return {
            isAdls:         true,
            storageAccount: m?.[1] ?? null,
            container:      location.fileSystem  ?? null,
            folderPath:     location.folderPath  ?? null,
            fileName:       location.fileName    ?? null,
        };
    }

    // ── Azure Blob Storage ────────────────────────────────────────────────────
    if (lsType === 'AzureBlobStorage') {
        let acct = null;
        const rawConn = lsProps.connectionString?.value ?? lsProps.connectionString ?? '';
        const m1 = String(rawConn).match(/AccountName=([^;]+)/i);
        if (m1) {
            acct = m1[1];
        } else {
            const svcUrl = lsProps.serviceEndpoint ?? '';
            const m2 = svcUrl.match(/https?:\/\/([^.]+)\./);
            if (m2) acct = m2[1];
        }
        return {
            isAdls:         false,
            storageAccount: acct,
            container:      location.container  ?? location.fileSystem ?? null,
            folderPath:     location.folderPath ?? null,
            fileName:       location.fileName   ?? null,
        };
    }

    return null; // Not an ADLS/Blob linked service (e.g., SQL, REST, etc.)
}

/**
 * Builds the full ADLS path for an activity: folderPath/fileName (or just folderPath).
 * @param {DatasetLocation} loc
 * @returns {string}
 */
function buildAdlsPath(loc) {
    const parts = [loc.folderPath, loc.fileName].filter(Boolean);
    return parts.join('/');
}

module.exports = { resolveDatasetToAdls, buildAdlsPath };
