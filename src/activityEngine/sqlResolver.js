'use strict';
/**
 * sqlResolver.js — resolves an ADF linked service name to a SQL connection config
 * { server, database } for use with sqlClient.js.
 *
 * Supported linked service types:
 *   AzureSqlDatabase, AzureSqlDatabaseTable, AzureSqlDW, SqlServer
 *
 * Authentication is always delegated to DefaultAzureCredential in local run;
 * the JSON's authenticationType / encryptedCredential are intentionally ignored.
 */

const path = require('path');
const fs   = require('fs');

/**
 * @param {string} lsName       — linkedServiceName.referenceName
 * @param {string} workspaceRoot
 * @param {string} [extensionPath]
 * @returns {{ server: string, database: string } | null}
 */
function resolveSqlLinkedService(lsName, workspaceRoot, extensionPath) {
    for (const dir of [workspaceRoot, extensionPath].filter(Boolean)) {
        const lsFile = path.join(dir, 'linkedService', `${lsName}.json`);
        if (!fs.existsSync(lsFile)) continue;
        try {
            const ls = JSON.parse(fs.readFileSync(lsFile, 'utf8'));
            const tp = ls.properties?.typeProperties ?? {};

            // Format 1: separate server + database fields
            if (tp.server && tp.database) {
                return { server: tp.server, database: tp.database };
            }

            // Format 2: connectionString  "Server=...;Database=...;"
            const cs = tp.connectionString;
            if (typeof cs === 'string') {
                const server   = cs.match(/(?:Server|Data Source)=([^;]+)/i)?.[1]?.trim();
                const database = cs.match(/(?:Database|Initial Catalog)=([^;]+)/i)?.[1]?.trim();
                if (server && database) return { server, database };
            }
        } catch { /* try next dir */ }
    }
    return null;
}

/**
 * Resolves an AzureSqlTable / AzureSqlDatabaseTable / AzureSQLDWTableDataset
 * dataset to its { server, database, schema, table, linkedServiceName } or null.
 */
function resolveSqlDataset(datasetName, workspaceRoot, extensionPath) {
    for (const dir of [workspaceRoot, extensionPath].filter(Boolean)) {
        const dsFile = path.join(dir, 'dataset', `${datasetName}.json`);
        if (!fs.existsSync(dsFile)) continue;
        try {
            const ds = JSON.parse(fs.readFileSync(dsFile, 'utf8'));
            const tp = ds.properties?.typeProperties ?? {};
            const lsName = ds.properties?.linkedServiceName?.referenceName;
            const sqlTypes = new Set(['AzureSqlTable', 'AzureSqlDatabaseTable', 'AzureSQLDWTable', 'SqlServerTable']);
            if (!sqlTypes.has(ds.properties?.type)) return null;
            return {
                linkedServiceName: lsName,
                schema: tp.schema ?? 'dbo',
                table:  tp.table  ?? tp.tableName,
            };
        } catch { /* try next dir */ }
    }
    return null;
}

module.exports = { resolveSqlLinkedService, resolveSqlDataset };
