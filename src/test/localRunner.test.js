'use strict';
// localRunner.test.js — Unit tests for localRunner.js utility functions and
// LocalPipelineRunner control-flow execution.
//
// Run with:  npm test  (or npx jest src/test/localRunner.test.js)
//
// These tests cover:
//   - _parseCsv: returns { rows, columns }, handles quotes, empty input, custom opts
//   - _splitDelimitedLine: parameterized delimiter/quoteChar/escapeChar
//   - _serializeCsv: round-trip CSV serialization
//   - parseAdfTimespan: D.HH:MM:SS, HH:MM:SS, PT…, plain int, defaults
//   - LocalPipelineRunner: Wait, SetVariable, Fail, Filter activities (no external I/O)

const path = require('path');
const { LocalPipelineRunner, _parseCsv, _splitDelimitedLine, _serializeCsv, parseAdfTimespan } = require('../activityEngine/localRunner');

// ─── _parseCsv ────────────────────────────────────────────────────────────────

describe('_parseCsv', () => {
    test('returns { rows, columns } shape', () => {
        const result = _parseCsv('a,b,c\n1,2,3');
        expect(result).toHaveProperty('rows');
        expect(result).toHaveProperty('columns');
    });

    test('parses headers into columns array', () => {
        const { columns } = _parseCsv('id,name,price\n1,Mouse,25.99');
        expect(columns).toEqual(['id', 'name', 'price']);
    });

    test('parses data rows into array of objects', () => {
        const { rows } = _parseCsv('id,name\n101,Mouse\n102,Keyboard');
        expect(rows).toHaveLength(2);
        expect(rows[0]).toEqual({ id: '101', name: 'Mouse' });
        expect(rows[1]).toEqual({ id: '102', name: 'Keyboard' });
    });

    test('handles quoted fields with commas inside', () => {
        const { rows } = _parseCsv('id,name\n1,"Smith, John"');
        expect(rows[0].name).toBe('Smith, John');
    });

    test('handles escaped quotes inside quoted fields', () => {
        const { rows } = _parseCsv('id,desc\n1,"say ""hello"""');
        expect(rows[0].desc).toBe('say "hello"');
    });

    test('handles Windows-style CRLF line endings', () => {
        const { rows, columns } = _parseCsv('id,val\r\n1,foo\r\n2,bar');
        expect(columns).toEqual(['id', 'val']);
        expect(rows).toHaveLength(2);
    });

    test('returns empty rows and columns for empty input', () => {
        const result = _parseCsv('');
        expect(result.rows).toHaveLength(0);
        expect(result.columns).toHaveLength(0);
    });

    test('returns empty rows for header-only input', () => {
        const { rows, columns } = _parseCsv('a,b,c');
        expect(columns).toEqual(['a', 'b', 'c']);
        expect(rows).toHaveLength(0);
    });

    test('regression: does NOT return a plain array (pre-fix behaviour)', () => {
        const result = _parseCsv('a,b\n1,2');
        expect(Array.isArray(result)).toBe(false);
    });
});

// ─── parseAdfTimespan ─────────────────────────────────────────────────────────

describe('parseAdfTimespan', () => {
    test('parses D.HH:MM:SS  (0.00:30:00 → 1800)', () => {
        expect(parseAdfTimespan('0.00:30:00')).toBe(1800);
    });

    test('parses D.HH:MM:SS with days  (1.00:00:00 → 86400)', () => {
        expect(parseAdfTimespan('1.00:00:00')).toBe(86400);
    });

    test('parses HH:MM:SS  (00:05:00 → 300)', () => {
        expect(parseAdfTimespan('00:05:00')).toBe(300);
    });

    test('parses ISO 8601 PT30S → 30', () => {
        expect(parseAdfTimespan('PT30S')).toBe(30);
    });

    test('parses ISO 8601 PT5M → 300', () => {
        expect(parseAdfTimespan('PT5M')).toBe(300);
    });

    test('parses ISO 8601 PT1H30M → 5400', () => {
        expect(parseAdfTimespan('PT1H30M')).toBe(5400);
    });

    test('parses plain integer string → number', () => {
        expect(parseAdfTimespan('120')).toBe(120);
    });

    test('parses plain integer number → same number', () => {
        expect(parseAdfTimespan(60)).toBe(60);
    });

    test('returns default when value is null', () => {
        expect(parseAdfTimespan(null, 600)).toBe(600);
    });

    test('returns default when value is undefined', () => {
        expect(parseAdfTimespan(undefined, 300)).toBe(300);
    });

    test('returns default when value is empty string', () => {
        expect(parseAdfTimespan('', 600)).toBe(600);
    });

    test('returns default when value is 0', () => {
        // 0 is falsy — treat as "not set" → default
        expect(parseAdfTimespan(0, 600)).toBe(600);
    });
});

// ─── LocalPipelineRunner helpers ──────────────────────────────────────────────

/** Build the minimal pipeline JSON structure needed by LocalPipelineRunner. */
function makePipeline(activities, parameters = {}, variables = {}) {
    return {
        name: 'TestPipeline',
        properties: {
            activities,
            parameters: Object.fromEntries(
                Object.entries(parameters).map(([k, v]) => [k, { type: 'String', defaultValue: v }])
            ),
            variables: Object.fromEntries(
                Object.entries(variables).map(([k, v]) => [k, { type: typeof v === 'boolean' ? 'Boolean' : Array.isArray(v) ? 'Array' : 'String', defaultValue: v }])
            ),
        },
    };
}

/** Collect all activityUpdate events from a runner run. */
async function runAndCollect(pipeline, params = {}) {
    const fakeRoot = path.join(__dirname, '../../'); // points at extension root (no real files needed for control flow)
    const runner = new LocalPipelineRunner(pipeline, params, fakeRoot, fakeRoot);
    const updates = [];
    const ends = [];
    runner.on('activityUpdate', u => updates.push({ ...u }));
    runner.on('pipelineEnd', e => ends.push({ ...e }));
    await runner.run();
    return { updates, end: ends[0] };
}

// ─── Wait ─────────────────────────────────────────────────────────────────────

describe('LocalPipelineRunner — Wait', () => {
    const pipeline = makePipeline([{
        name: 'W1',
        type: 'Wait',
        dependsOn: [],
        userProperties: [],
        typeProperties: { waitTimeInSeconds: 0 },
    }]);

    test('pipeline ends with Succeeded', async () => {
        const { end } = await runAndCollect(pipeline);
        expect(end.status).toBe('Succeeded');
    });

    test('Wait activity ends with Succeeded status', async () => {
        const { updates } = await runAndCollect(pipeline);
        const final = updates.filter(u => u.name === 'W1').at(-1);
        expect(final.status).toBe('Succeeded');
    });

    test('Wait output contains waitTimeInSeconds', async () => {
        const { updates } = await runAndCollect(pipeline);
        const succeeded = updates.find(u => u.name === 'W1' && u.status === 'Succeeded');
        expect(succeeded.output).toMatchObject({ waitTimeInSeconds: 0 });
    });
}, 10000);

// ─── Fail ─────────────────────────────────────────────────────────────────────

describe('LocalPipelineRunner — Fail', () => {
    const pipeline = makePipeline([{
        name: 'F1',
        type: 'Fail',
        dependsOn: [],
        userProperties: [],
        typeProperties: { message: 'Test failure', errorCode: 'ERR42' },
    }]);

    test('pipeline ends with Failed status', async () => {
        const { end } = await runAndCollect(pipeline);
        expect(end.status).toBe('Failed');
    });

    test('Fail activity emits error message', async () => {
        const { updates } = await runAndCollect(pipeline);
        const failed = updates.find(u => u.name === 'F1' && u.status === 'Failed');
        expect(failed.error).toMatch(/Test failure/);
    });
});

// ─── SetVariable ──────────────────────────────────────────────────────────────

describe('LocalPipelineRunner — SetVariable', () => {
    const pipeline = makePipeline([{
        name: 'SV1',
        type: 'SetVariable',
        dependsOn: [],
        userProperties: [],
        typeProperties: { variableName: 'myVar', value: 'hello' },
    }], {}, { myVar: '' });

    test('pipeline ends Succeeded', async () => {
        const { end } = await runAndCollect(pipeline);
        expect(end.status).toBe('Succeeded');
    });

    test('SetVariable output contains variableName and value', async () => {
        const { updates } = await runAndCollect(pipeline);
        const s = updates.find(u => u.name === 'SV1' && u.status === 'Succeeded');
        expect(s.output).toMatchObject({ variableName: 'myVar', value: 'hello' });
    });
});

// ─── AppendVariable ───────────────────────────────────────────────────────────

describe('LocalPipelineRunner — AppendVariable', () => {
    function makeFreshPipeline() {
        return makePipeline([
            { name: 'AV1', type: 'AppendVariable', dependsOn: [], userProperties: [],
              typeProperties: { variableName: 'items', value: 'a' } },
            { name: 'AV2', type: 'AppendVariable',
              dependsOn: [{ activity: 'AV1', dependencyConditions: ['Succeeded'] }],
              userProperties: [], typeProperties: { variableName: 'items', value: 'b' } },
        ], {}, { items: [] });
    }

    test('pipeline ends Succeeded', async () => {
        const { end } = await runAndCollect(makeFreshPipeline());
        expect(end.status).toBe('Succeeded');
    });

    test('second append output contains both elements', async () => {
        const { updates } = await runAndCollect(makeFreshPipeline());
        const s = updates.find(u => u.name === 'AV2' && u.status === 'Succeeded');
        expect(s.output.value).toEqual(['a', 'b']);
    });
});

// ─── Filter ───────────────────────────────────────────────────────────────────

describe('LocalPipelineRunner — Filter', () => {
    const pipeline = makePipeline([{
        name: 'FLT1',
        type: 'Filter',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            items: { value: '@createArray(1,2,3,4,5)', type: 'Expression' },
            condition: { value: '@greater(item(), 2)', type: 'Expression' },
        },
    }]);

    test('pipeline ends Succeeded', async () => {
        const { end } = await runAndCollect(pipeline);
        expect(end.status).toBe('Succeeded');
    });

    test('Filter output value contains only items > 2', async () => {
        const { updates } = await runAndCollect(pipeline);
        const s = updates.find(u => u.name === 'FLT1' && u.status === 'Succeeded');
        expect(s.output.value).toEqual([3, 4, 5]);
        expect(s.output.filterCount).toBe(3);
    });
});

// ─── IfCondition ──────────────────────────────────────────────────────────────

describe('LocalPipelineRunner — IfCondition', () => {
    function makeIfPipeline(expr) {
        return makePipeline([{
            name: 'IF1',
            type: 'IfCondition',
            dependsOn: [],
            userProperties: [],
            typeProperties: {
                expression: { value: expr, type: 'Expression' },
                ifTrueActivities: [{
                    name: 'TrueWait',
                    type: 'Wait',
                    dependsOn: [],
                    userProperties: [],
                    typeProperties: { waitTimeInSeconds: 0 },
                }],
                ifFalseActivities: [{
                    name: 'FalseFail',
                    type: 'Fail',
                    dependsOn: [],
                    userProperties: [],
                    typeProperties: { message: 'false branch', errorCode: '0' },
                }],
            },
        }]);
    }

    test('true branch runs when condition is true', async () => {
        const { updates, end } = await runAndCollect(makeIfPipeline('@equals(1,1)'));
        const trueStatus = updates.filter(u => u.name === 'TrueWait').at(-1)?.status;
        expect(trueStatus).toBe('Succeeded');
        expect(end.status).toBe('Succeeded');
    });

    test('false branch fails when condition is false', async () => {
        const { end, updates } = await runAndCollect(makeIfPipeline('@equals(1,2)'));
        const falseStatus = updates.filter(u => u.name === 'FalseFail').at(-1)?.status;
        expect(falseStatus).toBe('Failed');
        expect(end.status).toBe('Failed');
    });
}, 10000);

// ─── Dependency / skip logic ──────────────────────────────────────────────────

describe('LocalPipelineRunner — dependency skip logic', () => {
    test('debug: print all updates for Fail→Wait(Succeeded dep)', async () => {
        const pipeline = makePipeline([
            { name: 'A', type: 'Fail', dependsOn: [], userProperties: [],
              typeProperties: { message: 'boom', errorCode: '1' } },
            { name: 'B', type: 'Wait',
              dependsOn: [{ activity: 'A', dependencyConditions: ['Succeeded'] }],
              userProperties: [], typeProperties: { waitTimeInSeconds: 0 } },
        ]);
        const { updates } = await runAndCollect(pipeline);
        // eslint-disable-next-line no-console
        console.log('UPDATES:', JSON.stringify(updates.map(u => ({ name: u.name, status: u.status }))));
        expect(true).toBe(true); // always pass — for diagnostics
    });
    test('downstream activity is Skipped when upstream Fails', async () => {
        const pipeline = makePipeline([
            { name: 'A', type: 'Fail', dependsOn: [], userProperties: [],
              typeProperties: { message: 'boom', errorCode: '1' } },
            { name: 'B', type: 'Wait',
              dependsOn: [{ activity: 'A', dependencyConditions: ['Succeeded'] }],
              userProperties: [], typeProperties: { waitTimeInSeconds: 0 } },
        ]);
        const { updates } = await runAndCollect(pipeline);
        const bStatus = updates.find(u => u.name === 'B')?.status;
        expect(bStatus).toBe('Skipped');
    });

    test('failure-branch activity runs when upstream Fails', async () => {
        const pipeline = makePipeline([
            { name: 'A', type: 'Fail', dependsOn: [], userProperties: [],
              typeProperties: { message: 'boom', errorCode: '1' } },
            { name: 'B', type: 'Wait',
              dependsOn: [{ activity: 'A', dependencyConditions: ['Failed'] }],
              userProperties: [], typeProperties: { waitTimeInSeconds: 0 } },
        ]);
        const { updates } = await runAndCollect(pipeline);
        const bStatus = updates.find(u => u.name === 'B' && u.status === 'Succeeded')?.status;
        expect(bStatus).toBe('Succeeded');
    });
});

// ─── _parseCsv — custom delimiter options ─────────────────────────────────────

describe('_parseCsv — custom delimiter opts', () => {
    test('tab delimiter', () => {
        const { rows, columns } = _parseCsv('id\tname\tprice\n1\tMouse\t25.99', { delimiter: '\t' });
        expect(columns).toEqual(['id', 'name', 'price']);
        expect(rows[0]).toEqual({ id: '1', name: 'Mouse', price: '25.99' });
    });

    test('pipe delimiter', () => {
        const { rows } = _parseCsv('a|b\n1|2', { delimiter: '|' });
        expect(rows[0]).toEqual({ a: '1', b: '2' });
    });

    test('nullValue maps matching string to null', () => {
        const { rows } = _parseCsv('a,b\n1,NULL', { nullValue: 'NULL' });
        expect(rows[0].b).toBeNull();
    });

    test('nullValue does not affect non-matching values', () => {
        const { rows } = _parseCsv('a,b\n1,foo', { nullValue: 'NULL' });
        expect(rows[0].b).toBe('foo');
    });

    test('empty string nullValue maps empty fields to null', () => {
        const { rows } = _parseCsv('a,b\n1,', { nullValue: '' });
        expect(rows[0].b).toBeNull();
    });

    test('custom quoteChar (single quote)', () => {
        const { rows } = _parseCsv("a,b\n1,'hello, world'", { quoteChar: "'" });
        expect(rows[0].b).toBe('hello, world');
    });
});

// ─── _splitDelimitedLine ──────────────────────────────────────────────────────

describe('_splitDelimitedLine', () => {
    test('splits by comma (default)', () => {
        expect(_splitDelimitedLine('a,b,c')).toEqual(['a', 'b', 'c']);
    });

    test('splits by tab', () => {
        expect(_splitDelimitedLine('a\tb\tc', '\t')).toEqual(['a', 'b', 'c']);
    });

    test('splits by pipe', () => {
        expect(_splitDelimitedLine('a|b|c', '|')).toEqual(['a', 'b', 'c']);
    });

    test('handles quoted field with delimiter inside', () => {
        expect(_splitDelimitedLine('"a,b",c', ',')).toEqual(['a,b', 'c']);
    });

    test('handles doubled-quote inside quoted field (RFC 4180)', () => {
        expect(_splitDelimitedLine('"say ""hi"""', ',')).toEqual(['say "hi"']);
    });

    test('handles multi-char delimiter', () => {
        expect(_splitDelimitedLine('a||b||c', '||')).toEqual(['a', 'b', 'c']);
    });

    test('empty field at end', () => {
        expect(_splitDelimitedLine('a,b,', ',')).toEqual(['a', 'b', '']);
    });
});

// ─── _serializeCsv ────────────────────────────────────────────────────────────

describe('_serializeCsv', () => {
    const rows = [{ id: '1', name: 'Mouse', price: '25.99' }, { id: '2', name: 'Keyboard', price: '49.99' }];
    const columns = ['id', 'name', 'price'];

    test('produces header + data lines', () => {
        const csv = _serializeCsv(rows, columns);
        const lines = csv.split('\n');
        expect(lines[0]).toBe('id,name,price');
        expect(lines[1]).toBe('1,Mouse,25.99');
        expect(lines[2]).toBe('2,Keyboard,49.99');
    });

    test('uses custom delimiter', () => {
        const tsv = _serializeCsv(rows, columns, { delimiter: '\t' });
        expect(tsv.split('\n')[0]).toBe('id\tname\tprice');
    });

    test('quotes fields containing the delimiter', () => {
        const r = [{ a: 'hello,world', b: '1' }];
        const csv = _serializeCsv(r, ['a', 'b']);
        expect(csv.split('\n')[1]).toBe('"hello,world",1');
    });

    test('writes null as nullValue string', () => {
        const r = [{ a: '1', b: null }];
        const csv = _serializeCsv(r, ['a', 'b'], { nullValue: 'NULL' });
        expect(csv.split('\n')[1]).toBe('1,NULL');
    });

    test('round-trip: _parseCsv(_serializeCsv(...)) yields original rows', () => {
        const csv = _serializeCsv(rows, columns);
        const { rows: parsed } = _parseCsv(csv);
        expect(parsed).toEqual(rows);
    });

    test('round-trip with tab delimiter', () => {
        const tsv = _serializeCsv(rows, columns, { delimiter: '\t' });
        const { rows: parsed } = _parseCsv(tsv, { delimiter: '\t' });
        expect(parsed).toEqual(rows);
    });
});
