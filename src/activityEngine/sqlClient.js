'use strict';
const { spawn } = require('child_process');
const path = require('path');
const RUNNER_PY = path.join(__dirname, 'sql_runner.py');
function _pythonExe() { return process.env.PYTHON_PATH || 'python'; }
function _runPython(cmd) {
    return new Promise((resolve, reject) => {
        const py = spawn(_pythonExe(), [RUNNER_PY], { stdio: ['pipe','pipe','pipe'] });
        let stdout = '', stderr = '';
        py.stdout.on('data', d => { stdout += d.toString(); });
        py.stderr.on('data', d => { stderr += d.toString(); });
        py.on('error', err => {
            if (err.code === 'ENOENT') {
                reject(new Error('Python not found. Set PYTHON_PATH env var. Required: pip install pyodbc azure-identity'));
            } else { reject(err); }
        });
        py.on('close', () => {
            let result;
            try { result = JSON.parse(stdout); } catch {
                reject(new Error('sql_runner.py returned non-JSON. stdout: ' + stdout + ' stderr: ' + stderr));
                return;
            }
            if (!result.ok) { reject(new Error(result.error || 'sql_runner.py error')); return; }
            resolve(result);
        });
        py.stdin.write(JSON.stringify(cmd));
        py.stdin.end();
    });
}
class SqlClient {
    constructor(server, database) { this.server = server; this.database = database; }
    async connect() { return this; }
    async close() {}
    async executeScripts(scripts) {
        const r = await _runPython({ server: this.server, database: this.database, operation: 'scripts', scripts });
        return [{ rowsAffected: r.rowsAffected, recordset: r.recordset || [] }];
    }
    async executeStoredProcedure(procName, params) {
        const r = await _runPython({ server: this.server, database: this.database, operation: 'storedProcedure', procName, params });
        return { rowsAffected: r.rowsAffected, recordset: r.recordset || [] };
    }
    async bulkInsert(schema, table, rows, columns) {
        if (!rows || !rows.length) return { rowsAffected: 0 };
        const cols = columns || Object.keys(rows[0]);
        const r = await _runPython({ server: this.server, database: this.database, operation: 'bulkInsert', schema, table, columns: cols, rows });
        return { rowsAffected: r.rowsAffected };
    }
    async readTable(schema, table) {
        const r = await _runPython({ server: this.server, database: this.database, operation: 'selectTable', schema, table });
        return { rows: r.rows, columns: r.columns };
    }
    async executeQuery(query) {
        const r = await _runPython({ server: this.server, database: this.database, operation: 'selectQuery', query });
        return { rows: r.rows, columns: r.columns };
    }
}
async function readParquetFile(filePath) {
    const r = await _runPython({ operation: 'readParquet', server: '', database: '', filePath });
    return { rows: r.rows, columns: r.columns };
}
async function readExcelFile(filePath, opts = {}) {
    const r = await _runPython({ operation: 'readExcel', server: '', database: '', filePath,
        sheetName:          opts.sheetName          ?? null,
        firstRowAsHeader:   opts.firstRowAsHeader   !== false,
        nullValue:          opts.nullValue           ?? '',
    });
    return { rows: r.rows, columns: r.columns };
}
async function readXmlFile(filePath, opts = {}) {
    const r = await _runPython({ operation: 'readXml', server: '', database: '', filePath,
        rowTag:    opts.rowTag    ?? null,
        nullValue: opts.nullValue ?? '',
    });
    return { rows: r.rows, columns: r.columns };
}
module.exports = { SqlClient, readParquetFile, readExcelFile, readXmlFile };