'use strict';
// engine.test.js — Unit tests for src/activityEngine/engine.js
//
// Run with:  npm run test:engine
//
// Coverage:
//   - deserializeActivity: raw ADF JSON → flat canvas object (what the form shows)
//   - serializeActivity:   flat canvas object → ADF JSON (what gets written to disk)
//   - Round-trip:          deserialize(serialize(deserialize(raw))) === deserialize(raw)
//   - validateActivity:    required-field checking
//   - setVariableReturnValues transformer: all value types incl. Array, Boolean
//   - Nested container arrays are preserved as-is (inner editing deferred)

const engine = require('../activityEngine/engine');

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Serialize a flat object back to ADF JSON then deserialize again — should equal the original flat. */
function roundTrip(raw) {
    const flat = engine.deserializeActivity(raw);
    const serialized = engine.serializeActivity(flat);
    return engine.deserializeActivity(serialized);
}

/** Strip volatile id/canvas-only fields before comparing two flat objects. */
function stableFlat(flat) {
    const { id, element, container, color, isContainer, x, y, width, height, ...rest } = flat;
    return rest;
}

// ─── Wait ─────────────────────────────────────────────────────────────────────

describe('Wait', () => {
    const raw = {
        name: 'Wait1',
        type: 'Wait',
        dependsOn: [],
        userProperties: [],
        typeProperties: { waitTimeInSeconds: 5 },
    };

    test('deserialize reads waitTimeInSeconds', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.waitTimeInSeconds).toBe(5);
        expect(flat.name).toBe('Wait1');
    });

    test('serialize writes waitTimeInSeconds back', () => {
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.waitTimeInSeconds).toBe(5);
        expect(out.name).toBe('Wait1');
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validate passes when valid', () => {
        expect(engine.validateActivity(engine.deserializeActivity(raw))).toHaveLength(0);
    });

    test('validate fails when waitTimeInSeconds missing', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: {} });
        expect(engine.validateActivity(flat).length).toBeGreaterThan(0);
    });
});

// ─── Fail ─────────────────────────────────────────────────────────────────────

describe('Fail', () => {
    const raw = {
        name: 'Fail1',
        type: 'Fail',
        dependsOn: [],
        userProperties: [],
        typeProperties: { message: 'something went wrong', errorCode: 'ERR_001' },
    };

    test('deserialize reads message and errorCode', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.message).toBe('something went wrong');
        expect(flat.errorCode).toBe('ERR_001');
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validate fails when message missing', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: { errorCode: 'X' } });
        const errors = engine.validateActivity(flat);
        expect(errors.some(e => e.includes('Message') || e.includes('message'))).toBe(true);
    });
});

// ─── AppendVariable ───────────────────────────────────────────────────────────

describe('AppendVariable', () => {
    const raw = {
        name: 'AppendVar1',
        type: 'AppendVariable',
        dependsOn: [],
        userProperties: [],
        typeProperties: { variableName: 'myArray', value: 'hello' },
    };

    test('deserialize reads variableName and value', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.variableName).toBe('myArray');
        expect(flat.value).toBe('hello');
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validate fails when variableName missing', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: { value: 'x' } });
        expect(engine.validateActivity(flat).length).toBeGreaterThan(0);
    });
});

// ─── SetVariable — Pipeline variable ─────────────────────────────────────────

describe('SetVariable (pipeline variable)', () => {
    const raw = {
        name: 'SetVar1',
        type: 'SetVariable',
        dependsOn: [],
        userProperties: [],
        policy: { secureOutput: false, secureInput: false },
        typeProperties: { variableName: 'myVar', value: 'abc' },
    };

    test('deserialize reads variableName and value', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.variableName).toBe('myVar');
        expect(flat.value).toBe('abc');
        expect(flat.variableType).toBe('Pipeline variable');
    });

    test('serialize writes variableName and value back', () => {
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.variableName).toBe('myVar');
        expect(out.typeProperties.value).toBe('abc');
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });
});

// ─── SetVariable — Pipeline return value transformer ─────────────────────────

describe('SetVariable (pipeline return value transformer)', () => {
    const raw = {
        name: 'SetReturnVal',
        type: 'SetVariable',
        dependsOn: [],
        userProperties: [],
        policy: { secureOutput: false, secureInput: false },
        typeProperties: {
            variableName: 'pipelineReturnValue',
            setSystemVariable: true,
            value: [
                { key: 'key1', value: { type: 'String',  content: 'hello' } },
                { key: 'key2', value: { type: 'Boolean', content: true } },
                { key: 'key3', value: { type: 'Int',     content: 42 } },
                { key: 'key4', value: { type: 'Float',   content: 3.14 } },
                { key: 'key5', value: { type: 'Null' } },
                { key: 'key6', value: { type: 'Array',   content: [
                    { type: 'String', content: 'a' },
                    { type: 'String', content: 'b' },
                ] } },
            ],
        },
    };

    test('deserialize detects Pipeline return value mode', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.variableType).toBe('Pipeline return value');
        expect(flat.variableName).toBeUndefined();
    });

    test('deserialize reads String value', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.returnValues['key1']).toEqual({ type: 'String', value: 'hello' });
    });

    test('deserialize reads Boolean value', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.returnValues['key2']).toEqual({ type: 'Boolean', value: true });
    });

    test('deserialize reads Int value', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.returnValues['key3']).toEqual({ type: 'Int', value: 42 });
    });

    test('deserialize reads Float value', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.returnValues['key4']).toEqual({ type: 'Float', value: 3.14 });
    });

    test('deserialize reads Null value', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.returnValues['key5'].type).toBe('Null');
    });

    test('deserialize reads Array value (preserves sub-items)', () => {
        const flat = engine.deserializeActivity(raw);
        const arr = flat.returnValues['key6'].value;
        expect(Array.isArray(arr)).toBe(true);
        expect(arr).toHaveLength(2);
        expect(arr[0]).toEqual({ type: 'String', content: 'a' });
    });

    test('serialize round-trip preserves all return value types', () => {
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.variableName).toBe('pipelineReturnValue');
        expect(out.typeProperties.setSystemVariable).toBe(true);
        const outMap = Object.fromEntries(out.typeProperties.value.map(v => [v.key, v.value]));
        expect(outMap.key1).toEqual({ type: 'String',  content: 'hello' });
        expect(outMap.key2).toEqual({ type: 'Boolean', content: true });
        expect(outMap.key3).toEqual({ type: 'Int',     content: 42 });
        expect(outMap.key4).toEqual({ type: 'Float',   content: 3.14 });
        expect(outMap.key5.type).toBe('Null');
        expect(outMap.key6).toEqual({ type: 'Array', content: [
            { type: 'String', content: 'a' },
            { type: 'String', content: 'b' },
        ] });
    });
});

// ─── ExecutePipeline ──────────────────────────────────────────────────────────

describe('ExecutePipeline', () => {
    const raw = {
        name: 'ExecPipe1',
        type: 'ExecutePipeline',
        dependsOn: [],
        userProperties: [],
        policy: { secureInput: false },
        typeProperties: {
            pipeline: { referenceName: 'MyOtherPipeline', type: 'PipelineReference' },
            waitOnCompletion: true,
        },
    };

    test('deserialize reads pipeline reference object', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.pipeline).toEqual({ referenceName: 'MyOtherPipeline', type: 'PipelineReference' });
        expect(flat.waitOnCompletion).toBe(true);
    });

    test('serialize writes pipeline reference back', () => {
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.pipeline).toEqual({ referenceName: 'MyOtherPipeline', type: 'PipelineReference' });
    });

    test('validate fails when pipeline not set', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: { waitOnCompletion: true } });
        expect(engine.validateActivity(flat).length).toBeGreaterThan(0);
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });
});

// ─── Filter ───────────────────────────────────────────────────────────────────

describe('Filter', () => {
    const raw = {
        name: 'Filter1',
        type: 'Filter',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            items:     { value: '@pipeline().parameters.myArray', type: 'Expression' },
            condition: { value: "@greater(item(), 5)", type: 'Expression' },
        },
    };

    test('deserialize reads items and condition as expression objects', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.items).toEqual({ value: '@pipeline().parameters.myArray', type: 'Expression' });
        expect(flat.condition).toEqual({ value: '@greater(item(), 5)', type: 'Expression' });
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validate fails when items or condition missing', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: {} });
        expect(engine.validateActivity(flat).length).toBeGreaterThan(0);
    });
});

// ─── ForEach ──────────────────────────────────────────────────────────────────

describe('ForEach', () => {
    const raw = {
        name: 'ForEach1',
        type: 'ForEach',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            items: { value: '@pipeline().parameters.arr', type: 'Expression' },
            isSequential: false,
            batchCount: 10,
            activities: [
                {
                    name: 'InnerWait',
                    type: 'Wait',
                    dependsOn: [],
                    userProperties: [],
                    typeProperties: { waitTimeInSeconds: 1 },
                },
            ],
        },
    };

    test('deserialize reads items expression and isSequential', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.items).toEqual({ value: '@pipeline().parameters.arr', type: 'Expression' });
        expect(flat.isSequential).toBe(false);
        expect(flat.batchCount).toBe(10);
    });

    test('nested activities array is preserved as raw JSON', () => {
        const flat = engine.deserializeActivity(raw);
        expect(Array.isArray(flat.activities)).toBe(true);
        expect(flat.activities[0].name).toBe('InnerWait');
        expect(flat.activities[0].type).toBe('Wait');
    });

    test('serialize writes nested activities back unchanged', () => {
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.activities).toHaveLength(1);
        expect(out.typeProperties.activities[0].name).toBe('InnerWait');
        expect(out.typeProperties.activities[0].typeProperties.waitTimeInSeconds).toBe(1);
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validate fails when items missing', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: { ...raw.typeProperties, items: undefined } });
        expect(engine.validateActivity(flat).length).toBeGreaterThan(0);
    });
});

// ─── Until ────────────────────────────────────────────────────────────────────

describe('Until', () => {
    const raw = {
        name: 'Until1',
        type: 'Until',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            expression: { value: "@equals(variables('done'), true)", type: 'Expression' },
            timeout: '0.01:00:00',
            activities: [],
        },
    };

    test('deserialize reads expression and timeout', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.expression).toEqual({ value: "@equals(variables('done'), true)", type: 'Expression' });
        expect(flat.timeout).toBe('0.01:00:00');
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });
});

// ─── IfCondition ──────────────────────────────────────────────────────────────

describe('IfCondition', () => {
    const raw = {
        name: 'If1',
        type: 'IfCondition',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            expression: { value: "@equals(1, 1)", type: 'Expression' },
            ifTrueActivities: [
                { name: 'TrueWait', type: 'Wait', dependsOn: [], userProperties: [], typeProperties: { waitTimeInSeconds: 1 } },
            ],
            ifFalseActivities: [],
        },
    };

    test('deserialize reads expression', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.expression).toEqual({ value: '@equals(1, 1)', type: 'Expression' });
    });

    test('ifTrueActivities preserved as raw JSON', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.ifTrueActivities[0].name).toBe('TrueWait');
    });

    test('serialize writes both branches back', () => {
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.ifTrueActivities).toHaveLength(1);
        expect(out.typeProperties.ifFalseActivities).toHaveLength(0);
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });
});

// ─── Switch ───────────────────────────────────────────────────────────────────

describe('Switch', () => {
    const raw = {
        name: 'Switch1',
        type: 'Switch',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            on: { value: "@pipeline().parameters.channel", type: 'Expression' },
            cases: [
                { value: 'A', activities: [{ name: 'WaitA', type: 'Wait', dependsOn: [], userProperties: [], typeProperties: { waitTimeInSeconds: 1 } }] },
                { value: 'B', activities: [] },
            ],
            defaultActivities: [
                { name: 'WaitDefault', type: 'Wait', dependsOn: [], userProperties: [], typeProperties: { waitTimeInSeconds: 2 } },
            ],
        },
    };

    test('deserialize reads on expression', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.on).toEqual({ value: '@pipeline().parameters.channel', type: 'Expression' });
    });

    test('cases preserved as raw JSON', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.cases).toHaveLength(2);
        expect(flat.cases[0].value).toBe('A');
        expect(flat.cases[0].activities[0].name).toBe('WaitA');
    });

    test('defaultActivities preserved as raw JSON', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.defaultActivities[0].name).toBe('WaitDefault');
    });

    test('serialize writes cases and defaultActivities back', () => {
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.cases).toHaveLength(2);
        expect(out.typeProperties.defaultActivities[0].name).toBe('WaitDefault');
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });
});

// ─── Validation ───────────────────────────────────────────────────────────────

describe('Validation', () => {
    const raw = {
        name: 'Val1',
        type: 'Validation',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            dataset: { referenceName: 'DS1', type: 'DatasetReference' },
            timeout: '7.00:00:00',
            sleep: 10,
            childItems: true,
        },
    };

    test('deserialize reads dataset, timeout, sleep', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.dataset).toEqual({ referenceName: 'DS1', type: 'DatasetReference' });
        expect(flat.timeout).toBe('7.00:00:00');
        expect(flat.sleep).toBe(10);
    });

    test('childItems transformer: true → "true"', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.childItems).toBe('true');
    });

    test('childItems transformer: undefined → "ignore"', () => {
        const rawNoCI = { ...raw, typeProperties: { ...raw.typeProperties, childItems: undefined } };
        const flat = engine.deserializeActivity(rawNoCI);
        expect(flat.childItems).toBe('ignore');
    });

    test('serialize: childItems "true" → boolean true', () => {
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.childItems).toBe(true);
    });

    test('serialize: childItems "ignore" → omitted', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: { ...raw.typeProperties, childItems: undefined } });
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.childItems).toBeUndefined();
    });

    test('serialize: childItems "false" → boolean false', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: { ...raw.typeProperties, childItems: false } });
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.childItems).toBe(false);
    });

    test('round-trip is stable', () => {
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validation: missing dataset is an error', () => {
        const flat = engine.deserializeActivity({ ...raw, typeProperties: {} });
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => e.includes('Dataset'))).toBe(true);
    });
});

// ─── GetMetadata ───────────────────────────────────────────────────────────────

describe('GetMetadata', () => {
    const rawAdls = {
        name: 'GM1',
        type: 'GetMetadata',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            dataset: { referenceName: 'ADLS1', type: 'DatasetReference' },
            fieldList: ['childItems', 'itemName', { value: 'MyCustom', type: 'Expression' }],
            storeSettings: {
                type: 'AzureBlobFSReadSettings',
                modifiedDatetimeStart: '2026-01-01T00:00:00Z',
                modifiedDatetimeEnd: '2026-01-31T00:00:00Z',
            },
            formatSettings: {
                type: 'DelimitedTextReadSettings',
                skipLineCount: 3,
            },
        },
    };

    const rawSql = {
        name: 'GM2',
        type: 'GetMetadata',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            dataset: { referenceName: 'SQL1', type: 'DatasetReference' },
            fieldList: ['columnCount', 'structure'],
        },
    };

    test('deserialize reads dataset and fieldList', () => {
        const flat = engine.deserializeActivity(rawAdls);
        expect(flat.dataset).toEqual({ referenceName: 'ADLS1', type: 'DatasetReference' });
        expect(flat.fieldList).toEqual(['childItems', 'itemName', { value: 'MyCustom', type: 'Expression' }]);
    });

    test('deserialize reads _storeSettingsType and _formatSettingsType', () => {
        const flat = engine.deserializeActivity(rawAdls);
        expect(flat._storeSettingsType).toBe('AzureBlobFSReadSettings');
        expect(flat._formatSettingsType).toBe('DelimitedTextReadSettings');
    });

    test('deserialize reads modifiedDatetimeStart/End and skipLineCount', () => {
        const flat = engine.deserializeActivity(rawAdls);
        expect(flat.modifiedDatetimeStart).toBe('2026-01-01T00:00:00Z');
        expect(flat.modifiedDatetimeEnd).toBe('2026-01-31T00:00:00Z');
        expect(flat.skipLineCount).toBe(3);
    });

    test('serialize writes storeSettings with type first', () => {
        const flat = engine.deserializeActivity(rawAdls);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storeSettings.type).toBe('AzureBlobFSReadSettings');
        expect(out.typeProperties.storeSettings.enablePartitionDiscovery).toBe(false);
        expect(out.typeProperties.storeSettings.modifiedDatetimeStart).toBe('2026-01-01T00:00:00Z');
    });

    test('serialize writes formatSettings with type first and skipLineCount', () => {
        const flat = engine.deserializeActivity(rawAdls);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.formatSettings.type).toBe('DelimitedTextReadSettings');
        expect(out.typeProperties.formatSettings.skipLineCount).toBe(3);
    });

    test('serialize does NOT write storeSettings or formatSettings for SQL dataset', () => {
        const flat = engine.deserializeActivity(rawSql);
        // simulate webview not setting store/format types (SQL has no location)
        flat._storeSettingsType = '';
        flat._formatSettingsType = '';
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storeSettings).toBeUndefined();
        expect(out.typeProperties.formatSettings).toBeUndefined();
    });

    test('round-trip is stable (ADLS)', () => {
        expect(stableFlat(roundTrip(rawAdls))).toEqual(stableFlat(engine.deserializeActivity(rawAdls)));
    });

    test('validation: missing dataset is an error', () => {
        const flat = engine.deserializeActivity({ ...rawAdls, typeProperties: { fieldList: ['itemName'] } });
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => e.includes('Dataset'))).toBe(true);
    });

    test('validation: empty fieldList is an error', () => {
        const flat = engine.deserializeActivity({ ...rawAdls, typeProperties: { ...rawAdls.typeProperties, fieldList: [] } });
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => e.includes('Field list'))).toBe(true);
    });

    test('validation: valid ADLS activity passes', () => {
        const flat = engine.deserializeActivity(rawAdls);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });
});

// ─── Delete ───────────────────────────────────────────────────────────────────

describe('Delete', () => {
    const rawFilePathInDataset = {
        name: 'Del1',
        type: 'Delete',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'ADLS1', type: 'DatasetReference' },
            storeSettings: {
                type: 'AzureBlobFSReadSettings',
                recursive: true,
                enablePartitionDiscovery: false,
                maxConcurrentConnections: 5,
            },
            enableLogging: false,
        },
    };

    const rawPrefix = {
        name: 'Del2',
        type: 'Delete',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'Blob1', type: 'DatasetReference' },
            storeSettings: {
                type: 'AzureBlobStorageReadSettings',
                prefix: 'mycontainer/myprefix',
                recursive: false,
                enablePartitionDiscovery: false,
            },
            enableLogging: false,
        },
    };

    const rawFileList = {
        name: 'Del3',
        type: 'Delete',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'ADLS1', type: 'DatasetReference' },
            storeSettings: {
                type: 'AzureBlobFSReadSettings',
                fileListPath: 'folder/filelist.txt',
                enablePartitionDiscovery: false,
            },
            enableLogging: false,
        },
    };

    test('deserialize reads dataset and _storeSettingsType', () => {
        const flat = engine.deserializeActivity(rawFilePathInDataset);
        expect(flat.dataset).toEqual({ referenceName: 'ADLS1', type: 'DatasetReference' });
        expect(flat._storeSettingsType).toBe('AzureBlobFSReadSettings');
    });

    test('deserialize infers filePathType = filePathInDataset when no path fields', () => {
        const flat = engine.deserializeActivity(rawFilePathInDataset);
        expect(flat.filePathType).toBe('filePathInDataset');
    });

    test('deserialize infers filePathType = prefix from storeSettings.prefix', () => {
        const flat = engine.deserializeActivity(rawPrefix);
        expect(flat.filePathType).toBe('prefix');
        expect(flat.prefix).toBe('mycontainer/myprefix');
    });

    test('deserialize infers filePathType = listOfFiles from storeSettings.fileListPath', () => {
        const flat = engine.deserializeActivity(rawFileList);
        expect(flat.filePathType).toBe('listOfFiles');
        expect(flat.fileListPath).toBe('folder/filelist.txt');
    });

    test('serialize writes storeSettings.type and enablePartitionDiscovery', () => {
        const flat = engine.deserializeActivity(rawFilePathInDataset);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storeSettings.type).toBe('AzureBlobFSReadSettings');
        expect(out.typeProperties.storeSettings.enablePartitionDiscovery).toBe(false);
    });

    test('serialize writes maxConcurrentConnections inside storeSettings', () => {
        const flat = engine.deserializeActivity(rawFilePathInDataset);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storeSettings.maxConcurrentConnections).toBe(5);
        expect(out.typeProperties.maxConcurrentConnections).toBeUndefined();
    });

    test('serialize always writes enableLogging: false at typeProperties level', () => {
        const flat = engine.deserializeActivity(rawFilePathInDataset);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.enableLogging).toBe(false);
    });

    test('serialize writes prefix inside storeSettings', () => {
        const flat = engine.deserializeActivity(rawPrefix);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storeSettings.prefix).toBe('mycontainer/myprefix');
    });

    test('serialize writes fileListPath inside storeSettings', () => {
        const flat = engine.deserializeActivity(rawFileList);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storeSettings.fileListPath).toBe('folder/filelist.txt');
    });

    test('round-trip is stable (filePathInDataset)', () => {
        expect(stableFlat(roundTrip(rawFilePathInDataset))).toEqual(stableFlat(engine.deserializeActivity(rawFilePathInDataset)));
    });

    test('round-trip is stable (prefix)', () => {
        expect(stableFlat(roundTrip(rawPrefix))).toEqual(stableFlat(engine.deserializeActivity(rawPrefix)));
    });

    test('round-trip is stable (listOfFiles)', () => {
        expect(stableFlat(roundTrip(rawFileList))).toEqual(stableFlat(engine.deserializeActivity(rawFileList)));
    });

    test('validation: missing dataset is an error', () => {
        const flat = engine.deserializeActivity({ ...rawFilePathInDataset, typeProperties: {} });
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => e.includes('Dataset'))).toBe(true);
    });

    test('validation: valid activity passes', () => {
        const flat = engine.deserializeActivity(rawFilePathInDataset);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });
});

// ─── Lookup ───────────────────────────────────────────────────────────────────

describe('Lookup', () => {
    const rawStorageAdls = {
        name: 'LU1',
        type: 'Lookup',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'ADLS1', type: 'DatasetReference' },
            firstRowOnly: true,
            source: {
                type: 'DelimitedTextSource',
                storeSettings: { type: 'AzureBlobFSReadSettings', enablePartitionDiscovery: false, recursive: true },
                formatSettings: { type: 'DelimitedTextReadSettings', skipLineCount: 2 },
            },
        },
    };

    const rawStorageBlob = {
        name: 'LU2',
        type: 'Lookup',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'Blob1', type: 'DatasetReference' },
            firstRowOnly: false,
            source: {
                type: 'DelimitedTextSource',
                storeSettings: { type: 'AzureBlobStorageReadSettings', prefix: 'logs/', enablePartitionDiscovery: false },
                formatSettings: { type: 'DelimitedTextReadSettings' },
            },
        },
    };

    const rawSql = {
        name: 'LU3',
        type: 'Lookup',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'SQL1', type: 'DatasetReference' },
            firstRowOnly: true,
            source: {
                type: 'AzureSqlSource',
                sqlReaderQuery: 'SELECT 1',
                queryTimeout: '02:00:00',
            },
        },
    };

    const rawSqlStoredProc = {
        name: 'LU4',
        type: 'Lookup',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'SQL1', type: 'DatasetReference' },
            firstRowOnly: false,
            source: {
                type: 'AzureSqlSource',
                sqlReaderStoredProcedureName: 'dbo.MyProc',
                queryTimeout: '02:00:00',
            },
        },
    };

    // ── Storage (ADLS) ──────────────────────────────────────────────────────

    test('deserialize storage: reads dataset and firstRowOnly', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        expect(flat.dataset).toEqual({ referenceName: 'ADLS1', type: 'DatasetReference' });
        expect(flat.firstRowOnly).toBe(true);
    });

    test('deserialize storage: reads _storeSettingsType and _formatSettingsType', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        expect(flat._storeSettingsType).toBe('AzureBlobFSReadSettings');
        expect(flat._formatSettingsType).toBe('DelimitedTextReadSettings');
    });

    test('deserialize storage: infers filePathType = filePathInDataset', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        expect(flat.filePathType).toBe('filePathInDataset');
    });

    test('deserialize storage blob: infers filePathType = prefix', () => {
        const flat = engine.deserializeActivity(rawStorageBlob);
        expect(flat.filePathType).toBe('prefix');
        expect(flat.prefix).toBe('logs/');
    });

    test('deserialize storage: reads skipLineCount', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        expect(flat.skipLineCount).toBe(2);
    });

    test('serialize storage: writes source.type', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.type).toBe('DelimitedTextSource');
    });

    test('serialize storage: writes storeSettings.type and enablePartitionDiscovery', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings.type).toBe('AzureBlobFSReadSettings');
        expect(out.typeProperties.source.storeSettings.enablePartitionDiscovery).toBe(false);
    });

    test('serialize storage: writes formatSettings.type and skipLineCount', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.formatSettings.type).toBe('DelimitedTextReadSettings');
        expect(out.typeProperties.source.formatSettings.skipLineCount).toBe(2);
    });

    test('serialize storage blob prefix: writes prefix inside storeSettings', () => {
        const flat = engine.deserializeActivity(rawStorageBlob);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobStorageReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings.prefix).toBe('logs/');
    });

    test('round-trip stable (ADLS storage)', () => {
        expect(stableFlat(roundTrip(rawStorageAdls))).toEqual(stableFlat(engine.deserializeActivity(rawStorageAdls)));
    });

    // ── SQL ─────────────────────────────────────────────────────────────────

    test('deserialize SQL: infers useQuery = Query', () => {
        const flat = engine.deserializeActivity(rawSql);
        expect(flat.useQuery).toBe('Query');
    });

    test('deserialize SQL: reads sqlReaderQuery as plain string', () => {
        const flat = engine.deserializeActivity(rawSql);
        expect(flat.sqlReaderQuery).toBe('SELECT 1');
    });

    test('deserialize SQL: infers useQuery = StoredProcedure', () => {
        const flat = engine.deserializeActivity(rawSqlStoredProc);
        expect(flat.useQuery).toBe('StoredProcedure');
        expect(flat.sqlReaderStoredProcedureName).toBe('dbo.MyProc');
    });

    test('serialize SQL: writes source.type AzureSqlSource', () => {
        const flat = engine.deserializeActivity(rawSql);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.type).toBe('AzureSqlSource');
    });

    test('serialize SQL: writes sqlReaderQuery and queryTimeout', () => {
        const flat = engine.deserializeActivity(rawSql);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.sqlReaderQuery).toBe('SELECT 1');
        expect(out.typeProperties.source.queryTimeout).toBe('02:00:00');
    });

    test('serialize SQL: does NOT write storeSettings or formatSettings', () => {
        const flat = engine.deserializeActivity(rawSql);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings).toBeUndefined();
        expect(out.typeProperties.source.formatSettings).toBeUndefined();
    });

    test('serialize SQL stored proc: writes sqlReaderStoredProcedureName', () => {
        const flat = engine.deserializeActivity(rawSqlStoredProc);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.sqlReaderStoredProcedureName).toBe('dbo.MyProc');
    });

    // Expression object from old JSON should be unwrapped to plain string
    test('deserialize SQL: unwraps legacy Expression object for sqlReaderQuery', () => {
        const rawWithExpr = {
            ...rawSql,
            typeProperties: {
                ...rawSql.typeProperties,
                source: { type: 'AzureSqlSource', sqlReaderQuery: { value: 'SELECT 2', type: 'Expression' }, queryTimeout: '02:00:00' },
            },
        };
        const flat = engine.deserializeActivity(rawWithExpr);
        expect(flat.sqlReaderQuery).toBe('SELECT 2');
    });

    test('round-trip stable (SQL stored proc)', () => {
        expect(stableFlat(roundTrip(rawSqlStoredProc))).toEqual(stableFlat(engine.deserializeActivity(rawSqlStoredProc)));
    });

    // ── Validation ──────────────────────────────────────────────────────────

    test('validation: missing dataset is an error', () => {
        const flat = engine.deserializeActivity({ ...rawStorageAdls, typeProperties: { firstRowOnly: true } });
        expect(engine.validateActivity(flat).some(e => e.includes('dataset') || e.includes('Dataset'))).toBe(true);
    });

    test('validation: valid storage activity passes', () => {
        const flat = engine.deserializeActivity(rawStorageAdls);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: valid SQL activity passes', () => {
        const flat = engine.deserializeActivity(rawSql);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    // ── New fields: storage partition discovery ──────────────────────────────

    const rawStoragePartitioned = {
        name: 'LU_EPD',
        type: 'Lookup',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'ADLS1', type: 'DatasetReference' },
            firstRowOnly: false,
            source: {
                type: 'DelimitedTextSource',
                storeSettings: {
                    type: 'AzureBlobFSReadSettings',
                    enablePartitionDiscovery: true,
                    partitionRootPath: 'container/root',
                    recursive: false,
                    maxConcurrentConnections: 4,
                },
                formatSettings: { type: 'DelimitedTextReadSettings' },
            },
        },
    };

    test('deserialize storage: reads recursive and enablePartitionDiscovery', () => {
        const flat = engine.deserializeActivity(rawStoragePartitioned);
        expect(flat.recursive).toBe(false);
        expect(flat.enablePartitionDiscovery).toBe(true);
    });

    test('deserialize storage: reads partitionRootPath when enablePartitionDiscovery=true', () => {
        const flat = engine.deserializeActivity(rawStoragePartitioned);
        expect(flat.partitionRootPath).toBe('container/root');
    });

    test('deserialize storage: reads maxConcurrentConnections', () => {
        const flat = engine.deserializeActivity(rawStoragePartitioned);
        expect(flat.maxConcurrentConnections).toBe(4);
    });

    test('serialize storage: writes enablePartitionDiscovery=true (not hardcoded false)', () => {
        const flat = engine.deserializeActivity(rawStoragePartitioned);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings.enablePartitionDiscovery).toBe(true);
    });

    test('serialize storage: writes partitionRootPath when enabled', () => {
        const flat = engine.deserializeActivity(rawStoragePartitioned);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings.partitionRootPath).toBe('container/root');
    });

    test('serialize storage: omits partitionRootPath when enablePartitionDiscovery=false', () => {
        const flat = engine.deserializeActivity(rawStorageAdls); // EPD = false
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings.enablePartitionDiscovery).toBe(false);
        expect(out.typeProperties.source.storeSettings.partitionRootPath).toBeUndefined();
    });

    test('serialize storage: writes maxConcurrentConnections', () => {
        const flat = engine.deserializeActivity(rawStoragePartitioned);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'DelimitedText';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings.maxConcurrentConnections).toBe(4);
    });

    test('round-trip stable (storage with partition discovery)', () => {
        expect(stableFlat(roundTrip(rawStoragePartitioned))).toEqual(stableFlat(engine.deserializeActivity(rawStoragePartitioned)));
    });

    // ── New fields: SQL isolation level and partition settings ───────────────

    const rawSqlPartitioned = {
        name: 'LU_SQLPart',
        type: 'Lookup',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'SQL1', type: 'DatasetReference' },
            firstRowOnly: true,
            source: {
                type: 'AzureSqlSource',
                queryTimeout: '02:00:00',
                isolationLevel: 'ReadCommitted',
                partitionOption: 'DynamicRange',
                partitionSettings: {
                    partitionColumnName: 'id',
                    partitionUpperBound: '100',
                    partitionLowerBound: '0',
                },
            },
        },
    };

    const rawSqlStoredProcParams = {
        name: 'LU_SP',
        type: 'Lookup',
        dependsOn: [],
        userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'SQL1', type: 'DatasetReference' },
            firstRowOnly: false,
            source: {
                type: 'AzureSqlSource',
                sqlReaderStoredProcedureName: 'dbo.GetData',
                queryTimeout: '02:00:00',
                storedProcedureParameters: {
                    startDate: { value: '2024-01-01', type: 'string' },
                    endDate: { value: '2024-12-31', type: 'string' },
                },
            },
        },
    };

    test('deserialize SQL: reads isolationLevel', () => {
        const flat = engine.deserializeActivity(rawSqlPartitioned);
        expect(flat.isolationLevel).toBe('ReadCommitted');
    });

    test('deserialize SQL: reads partitionOption', () => {
        const flat = engine.deserializeActivity(rawSqlPartitioned);
        expect(flat.partitionOption).toBe('DynamicRange');
    });

    test('deserialize SQL: reads partition bounds', () => {
        const flat = engine.deserializeActivity(rawSqlPartitioned);
        expect(flat.partitionColumnName).toBe('id');
        expect(flat.partitionUpperBound).toBe('100');
        expect(flat.partitionLowerBound).toBe('0');
    });

    test('serialize SQL: writes isolationLevel', () => {
        const flat = engine.deserializeActivity(rawSqlPartitioned);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.isolationLevel).toBe('ReadCommitted');
    });

    test('serialize SQL: writes partitionOption and partitionSettings', () => {
        const flat = engine.deserializeActivity(rawSqlPartitioned);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.partitionOption).toBe('DynamicRange');
        expect(out.typeProperties.source.partitionSettings.partitionColumnName).toBe('id');
        expect(out.typeProperties.source.partitionSettings.partitionUpperBound).toBe('100');
        expect(out.typeProperties.source.partitionSettings.partitionLowerBound).toBe('0');
    });

    test('serialize SQL: does NOT write storeSettings alongside partitionSettings', () => {
        const flat = engine.deserializeActivity(rawSqlPartitioned);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storeSettings).toBeUndefined();
    });

    test('round-trip stable (SQL with partition settings)', () => {
        expect(stableFlat(roundTrip(rawSqlPartitioned))).toEqual(stableFlat(engine.deserializeActivity(rawSqlPartitioned)));
    });

    test('deserialize SQL: reads storedProcedureParameters', () => {
        const flat = engine.deserializeActivity(rawSqlStoredProcParams);
        expect(flat.storedProcedureParameters).toEqual({
            startDate: { value: '2024-01-01', type: 'string' },
            endDate: { value: '2024-12-31', type: 'string' },
        });
    });

    test('serialize SQL: writes storedProcedureParameters for StoredProcedure mode', () => {
        const flat = engine.deserializeActivity(rawSqlStoredProcParams);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storedProcedureParameters).toEqual({
            startDate: { value: '2024-01-01', type: 'string' },
            endDate: { value: '2024-12-31', type: 'string' },
        });
    });

    test('round-trip stable (SQL stored proc with parameters)', () => {
        expect(stableFlat(roundTrip(rawSqlStoredProcParams))).toEqual(stableFlat(engine.deserializeActivity(rawSqlStoredProcParams)));
    });

    // ── SQL: useQuery=Table inference ────────────────────────────────────────

    test('deserialize SQL: infers useQuery = Table when no query fields present', () => {
        const rawTable = {
            name: 'LU_Table', type: 'Lookup', dependsOn: [], userProperties: [],
            policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
            typeProperties: {
                dataset: { referenceName: 'SQL1', type: 'DatasetReference' },
                firstRowOnly: true,
                source: { type: 'AzureSqlSource', queryTimeout: '02:00:00' },
            },
        };
        const flat = engine.deserializeActivity(rawTable);
        expect(flat.useQuery).toBe('Table');
    });

    // ── SQL: AzureSqlDWTable → SqlDWSource ───────────────────────────────────

    const rawSqlDW = {
        name: 'LU_DW', type: 'Lookup', dependsOn: [], userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'DW1', type: 'DatasetReference' },
            firstRowOnly: true,
            source: { type: 'SqlDWSource', queryTimeout: '02:00:00' },
        },
    };

    test('deserialize SqlDW: infers _datasetCategory = sql', () => {
        const flat = engine.deserializeActivity(rawSqlDW);
        expect(flat._datasetCategory).toBe('sql');
    });

    test('serialize SqlDW: writes source.type = SqlDWSource for AzureSqlDWTable', () => {
        const flat = engine.deserializeActivity(rawSqlDW);
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlDWTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.type).toBe('SqlDWSource');
    });

    // ── SQL: storedProcedureParameters with null value ───────────────────────

    test('round-trip preserves storedProcedureParameters with null value', () => {
        const rawNullParam = {
            name: 'LU_NullParam', type: 'Lookup', dependsOn: [], userProperties: [],
            policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
            typeProperties: {
                dataset: { referenceName: 'SQL1', type: 'DatasetReference' },
                firstRowOnly: false,
                source: {
                    type: 'AzureSqlSource',
                    sqlReaderStoredProcedureName: 'dbo.Proc',
                    queryTimeout: '02:00:00',
                    storedProcedureParameters: {
                        active: { value: 'yes', type: 'String' },
                        removed: { value: null, type: 'String' },
                    },
                },
            },
        };
        const flat = engine.deserializeActivity(rawNullParam);
        expect(flat.storedProcedureParameters.removed).toEqual({ value: null, type: 'String' });
        flat._datasetCategory = 'sql';
        flat._datasetType = 'AzureSqlTable';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.storedProcedureParameters.removed).toEqual({ value: null, type: 'String' });
    });

    // ── Storage: filePathType wildcard and list-of-files inference ───────────

    test('deserialize: infers filePathType = wildcardFilePath from wildcardFileName', () => {
        const rawWildcard = {
            name: 'LU_Wildcard', type: 'Lookup', dependsOn: [], userProperties: [],
            policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
            typeProperties: {
                dataset: { referenceName: 'ADLS1', type: 'DatasetReference' },
                firstRowOnly: true,
                source: {
                    type: 'DelimitedTextSource',
                    storeSettings: {
                        type: 'AzureBlobFSReadSettings',
                        wildcardFolderPath: 'data/*',
                        wildcardFileName: '*.csv',
                        enablePartitionDiscovery: false,
                    },
                    formatSettings: { type: 'DelimitedTextReadSettings' },
                },
            },
        };
        const flat = engine.deserializeActivity(rawWildcard);
        expect(flat.filePathType).toBe('wildcardFilePath');
        expect(flat.wildcardFolderPath).toBe('data/*');
        expect(flat.wildcardFileName).toBe('*.csv');
    });

    test('deserialize: infers filePathType = listOfFiles from fileListPath', () => {
        const rawList = {
            name: 'LU_List', type: 'Lookup', dependsOn: [], userProperties: [],
            policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
            typeProperties: {
                dataset: { referenceName: 'ADLS1', type: 'DatasetReference' },
                firstRowOnly: true,
                source: {
                    type: 'DelimitedTextSource',
                    storeSettings: {
                        type: 'AzureBlobFSReadSettings',
                        fileListPath: 'container/filelist.txt',
                        enablePartitionDiscovery: false,
                    },
                    formatSettings: { type: 'DelimitedTextReadSettings' },
                },
            },
        };
        const flat = engine.deserializeActivity(rawList);
        expect(flat.filePathType).toBe('listOfFiles');
        expect(flat.fileListPath).toBe('container/filelist.txt');
    });

    // ── Storage: XML formatSettings ──────────────────────────────────────────

    const rawXml = {
        name: 'LU_XML', type: 'Lookup', dependsOn: [], userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: {
            dataset: { referenceName: 'XmlDs1', type: 'DatasetReference' },
            firstRowOnly: true,
            source: {
                type: 'XmlSource',
                storeSettings: { type: 'AzureBlobFSReadSettings', enablePartitionDiscovery: false },
                formatSettings: { type: 'XmlReadSettings', validationMode: 'xsd', detectDataType: true, namespaces: true },
            },
        },
    };

    test('deserialize XML: reads _formatSettingsType = XmlReadSettings', () => {
        const flat = engine.deserializeActivity(rawXml);
        expect(flat._formatSettingsType).toBe('XmlReadSettings');
    });

    test('deserialize XML: reads validationMode, detectDataType, namespaces', () => {
        const flat = engine.deserializeActivity(rawXml);
        expect(flat.validationMode).toBe('xsd');
        expect(flat.detectDataType).toBe(true);
        expect(flat.namespaces).toBe(true);
    });

    test('serialize XML: writes formatSettings.type = XmlReadSettings', () => {
        const flat = engine.deserializeActivity(rawXml);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'Xml';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.formatSettings.type).toBe('XmlReadSettings');
        expect(out.typeProperties.source.formatSettings.validationMode).toBe('xsd');
    });

    test('round-trip stable (XML source)', () => {
        expect(stableFlat(roundTrip(rawXml))).toEqual(stableFlat(engine.deserializeActivity(rawXml)));
    });

    // ── Storage: Parquet formatSettings ─────────────────────────────────────

    test('serialize Parquet: writes formatSettings.type = ParquetReadSettings', () => {
        const rawParquet = {
            name: 'LU_Parquet', type: 'Lookup', dependsOn: [], userProperties: [],
            policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
            typeProperties: {
                dataset: { referenceName: 'Parquet1', type: 'DatasetReference' },
                firstRowOnly: true,
                source: {
                    type: 'ParquetSource',
                    storeSettings: { type: 'AzureBlobFSReadSettings', enablePartitionDiscovery: false },
                    formatSettings: { type: 'ParquetReadSettings' },
                },
            },
        };
        const flat = engine.deserializeActivity(rawParquet);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'Parquet';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.formatSettings.type).toBe('ParquetReadSettings');
    });

    // ── Storage: JSON formatSettings ─────────────────────────────────────────

    test('serialize Json: writes formatSettings.type = JsonReadSettings', () => {
        const rawJson = {
            name: 'LU_Json', type: 'Lookup', dependsOn: [], userProperties: [],
            policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
            typeProperties: {
                dataset: { referenceName: 'Json1', type: 'DatasetReference' },
                firstRowOnly: true,
                source: {
                    type: 'JsonSource',
                    storeSettings: { type: 'AzureBlobFSReadSettings', enablePartitionDiscovery: false },
                    formatSettings: { type: 'JsonReadSettings' },
                },
            },
        };
        const flat = engine.deserializeActivity(rawJson);
        flat._datasetCategory = 'storage';
        flat._datasetType = 'Json';
        flat._storeSettingsType = 'AzureBlobFSReadSettings';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.formatSettings.type).toBe('JsonReadSettings');
    });
});

// ─── SynapseNotebook ─────────────────────────────────────────────────────────

describe('SynapseNotebook', () => {
    const rawBasic = {
        name: 'NB1',
        type: 'SynapseNotebook',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            notebook: { referenceName: 'My_Notebook', type: 'NotebookReference' },
            sparkPool: { referenceName: 'mypool', type: 'BigDataPoolReference' },
            executorSize: 'Small',
            driverSize: 'Small',
            snapshot: true,
            numExecutors: 2,
            conf: {
                'spark.dynamicAllocation.enabled': false,
                'spark.dynamicAllocation.minExecutors': 2,
                'spark.dynamicAllocation.maxExecutors': 2,
            },
        },
    };

    const rawDynamic = {
        name: 'NB2',
        type: 'SynapseNotebook',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            notebook: { referenceName: 'Other_Notebook', type: 'NotebookReference' },
            executorSize: 'Medium',
            driverSize: 'Medium',
            snapshot: true,
            conf: {
                'spark.dynamicAllocation.enabled': true,
                'spark.dynamicAllocation.minExecutors': 1,
                'spark.dynamicAllocation.maxExecutors': 4,
            },
        },
    };

    const rawNoConf = {
        name: 'NB3',
        type: 'SynapseNotebook',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            notebook: { referenceName: 'Plain_Notebook', type: 'NotebookReference' },
            snapshot: true,
        },
    };

    // ── Deserialize ─────────────────────────────────────────────────────────

    test('deserialize: unwraps notebook reference → string', () => {
        const flat = engine.deserializeActivity(rawBasic);
        expect(flat.notebook).toBe('My_Notebook');
    });

    test('deserialize: unwraps sparkPool reference → string', () => {
        const flat = engine.deserializeActivity(rawBasic);
        expect(flat.sparkPool).toBe('mypool');
    });

    test('deserialize: conf disabled → dynamicAllocation Disabled', () => {
        const flat = engine.deserializeActivity(rawBasic);
        expect(flat.dynamicAllocation).toBe('Disabled');
    });

    test('deserialize: reads minExecutors and maxExecutors from conf', () => {
        const flat = engine.deserializeActivity(rawBasic);
        expect(flat.minExecutors).toBe(2);
        expect(flat.maxExecutors).toBe(2);
    });

    test('deserialize: conf enabled → dynamicAllocation Enabled', () => {
        const flat = engine.deserializeActivity(rawDynamic);
        expect(flat.dynamicAllocation).toBe('Enabled');
        expect(flat.minExecutors).toBe(1);
        expect(flat.maxExecutors).toBe(4);
    });

    test('deserialize: no conf → dynamicAllocation undefined', () => {
        const flat = engine.deserializeActivity(rawNoConf);
        expect(flat.dynamicAllocation).toBeUndefined();
    });

    // ── Serialize ───────────────────────────────────────────────────────────

    test('serialize: wraps notebook string → reference object', () => {
        const flat = engine.deserializeActivity(rawBasic);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.notebook).toEqual({ referenceName: 'My_Notebook', type: 'NotebookReference' });
    });

    test('serialize: wraps sparkPool string → reference object', () => {
        const flat = engine.deserializeActivity(rawBasic);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.sparkPool).toEqual({ referenceName: 'mypool', type: 'BigDataPoolReference' });
    });

    test('serialize: always writes snapshot = true', () => {
        const flat = engine.deserializeActivity(rawNoConf);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.snapshot).toBe(true);
    });

    test('serialize: mirrors driverSize from executorSize', () => {
        const flat = engine.deserializeActivity(rawBasic);
        flat.executorSize = 'Large';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.driverSize).toBe('Large');
    });

    test('serialize: disabled conf writes min=max=numExecutors', () => {
        const flat = engine.deserializeActivity(rawBasic);
        flat.dynamicAllocation = 'Disabled';
        flat.numExecutors = 3;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.conf['spark.dynamicAllocation.enabled']).toBe(false);
        expect(out.typeProperties.conf['spark.dynamicAllocation.minExecutors']).toBe(3);
        expect(out.typeProperties.conf['spark.dynamicAllocation.maxExecutors']).toBe(3);
        expect(out.typeProperties.numExecutors).toBe(3);
    });

    test('serialize: enabled conf writes min and max executors separately', () => {
        const flat = engine.deserializeActivity(rawDynamic);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.conf['spark.dynamicAllocation.enabled']).toBe(true);
        expect(out.typeProperties.conf['spark.dynamicAllocation.minExecutors']).toBe(1);
        expect(out.typeProperties.conf['spark.dynamicAllocation.maxExecutors']).toBe(4);
    });

    test('round-trip stable (basic with conf)', () => {
        expect(stableFlat(roundTrip(rawBasic))).toEqual(stableFlat(engine.deserializeActivity(rawBasic)));
    });

    test('round-trip stable (dynamic allocation)', () => {
        expect(stableFlat(roundTrip(rawDynamic))).toEqual(stableFlat(engine.deserializeActivity(rawDynamic)));
    });

    // ── Validation ──────────────────────────────────────────────────────────

    test('validation: missing notebook is an error', () => {
        const flat = engine.deserializeActivity({ ...rawBasic, typeProperties: { snapshot: true } });
        expect(engine.validateActivity(flat).some(e => e.toLowerCase().includes('notebook'))).toBe(true);
    });

    test('validation: valid activity passes', () => {
        const flat = engine.deserializeActivity(rawBasic);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });
});

// ─── SparkJob ─────────────────────────────────────────────────────────────────

describe('SparkJob', () => {
    const rawSparkJob = {
        name: 'SJ1',
        type: 'SparkJob',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            sparkJob: { referenceName: 'MySparkJobDef', type: 'SparkJobDefinitionReference' },
            sparkPool: { referenceName: 'sparkpool1', type: 'BigDataPoolReference' },
            executorSize: 'Small',
            driverSize: 'Small',
            numExecutors: 2,
        },
    };

    const rawSparkJobNoPool = {
        name: 'SJ2',
        type: 'SparkJob',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            sparkJob: { referenceName: 'AnotherJob', type: 'SparkJobDefinitionReference' },
            executorSize: 'Medium',
            driverSize: 'Medium',
            numExecutors: 4,
        },
    };

    test('deserialize: unwraps sparkJob reference → string', () => {
        const flat = engine.deserializeActivity(rawSparkJob);
        expect(flat.sparkJob).toBe('MySparkJobDef');
    });

    test('deserialize: unwraps sparkPool reference → string', () => {
        const flat = engine.deserializeActivity(rawSparkJob);
        expect(flat.sparkPool).toBe('sparkpool1');
    });

    test('deserialize: reads executorSize and numExecutors', () => {
        const flat = engine.deserializeActivity(rawSparkJob);
        expect(flat.executorSize).toBe('Small');
        expect(flat.numExecutors).toBe(2);
    });

    test('serialize: wraps sparkJob string → SparkJobDefinitionReference', () => {
        const flat = engine.deserializeActivity(rawSparkJob);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.sparkJob).toEqual({ referenceName: 'MySparkJobDef', type: 'SparkJobDefinitionReference' });
    });

    test('serialize: wraps sparkPool string → BigDataPoolReference', () => {
        const flat = engine.deserializeActivity(rawSparkJob);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.sparkPool).toEqual({ referenceName: 'sparkpool1', type: 'BigDataPoolReference' });
    });

    test('serialize: no sparkPool → omits sparkPool', () => {
        const flat = engine.deserializeActivity(rawSparkJobNoPool);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.sparkPool).toBeUndefined();
    });

    test('serialize: writes executorSize, driverSize, numExecutors', () => {
        const flat = engine.deserializeActivity(rawSparkJob);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.executorSize).toBe('Small');
        expect(out.typeProperties.driverSize).toBe('Small');
        expect(out.typeProperties.numExecutors).toBe(2);
    });

    test('round-trip stable', () => {
        expect(stableFlat(roundTrip(rawSparkJob))).toEqual(stableFlat(engine.deserializeActivity(rawSparkJob)));
    });

    test('validation: missing sparkJob is an error', () => {
        const flat = engine.deserializeActivity({ ...rawSparkJobNoPool, typeProperties: {} });
        expect(engine.validateActivity(flat).some(e => e.toLowerCase().includes('spark job'))).toBe(true);
    });

    test('validation: valid activity passes', () => {
        const flat = engine.deserializeActivity(rawSparkJob);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });
});

// ─── Script ───────────────────────────────────────────────────────────────────

describe('Script', () => {
    const rawScript = {
        name: 'Script1', type: 'Script', dependsOn: [], userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        linkedServiceName: { referenceName: 'AzureSqlDatabase1', type: 'LinkedServiceReference' },
        typeProperties: {
            scripts: [{ type: 'Query', text: 'SELECT 1' }],
            scriptBlockExecutionTimeout: '02:00:00',
        },
    };

    test('deserialize: reads linkedServiceName reference', () => {
        const flat = engine.deserializeActivity(rawScript);
        expect(flat.linkedServiceName).toEqual({ referenceName: 'AzureSqlDatabase1', type: 'LinkedServiceReference' });
    });

    test('deserialize: reads scripts array', () => {
        const flat = engine.deserializeActivity(rawScript);
        expect(flat.scripts).toEqual([{ type: 'Query', text: 'SELECT 1' }]);
    });

    test('serialize: writes linkedServiceName at top level', () => {
        const flat = engine.deserializeActivity(rawScript);
        const out = engine.serializeActivity(flat);
        expect(out.linkedServiceName).toEqual({ referenceName: 'AzureSqlDatabase1', type: 'LinkedServiceReference' });
    });

    test('serialize: writes scripts array', () => {
        const flat = engine.deserializeActivity(rawScript);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.scripts).toEqual([{ type: 'Query', text: 'SELECT 1' }]);
    });

    test('serialize: writes scriptBlockExecutionTimeout', () => {
        const flat = engine.deserializeActivity(rawScript);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.scriptBlockExecutionTimeout).toBe('02:00:00');
    });

    test('round-trip stable', () => {
        expect(stableFlat(roundTrip(rawScript))).toEqual(stableFlat(engine.deserializeActivity(rawScript)));
    });

    test('validation: missing linkedService is an error', () => {
        const flat = engine.deserializeActivity({ ...rawScript, linkedServiceName: undefined });
        expect(engine.validateActivity(flat).some(e => /linked/i.test(e))).toBe(true);
    });

    test('validation: missing scripts is an error', () => {
        const flat = engine.deserializeActivity({ ...rawScript, typeProperties: { scriptBlockExecutionTimeout: '02:00:00' } });
        delete flat.scripts;
        expect(engine.validateActivity(flat).some(e => /script/i.test(e))).toBe(true);
    });
});

// ─── SqlServerStoredProcedure ─────────────────────────────────────────────────

describe('SqlServerStoredProcedure', () => {
    const rawSP = {
        name: 'SP1', type: 'SqlServerStoredProcedure', dependsOn: [], userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        linkedServiceName: { referenceName: 'AzureSqlDatabase1', type: 'LinkedServiceReference' },
        typeProperties: {
            storedProcedureName: 'dbo.MyProc',
            storedProcedureParameters: {
                param1: { value: 'hello', type: 'String' },
                param2: { value: 42, type: 'Int32' },
            },
        },
    };

    test('deserialize: reads linkedServiceName', () => {
        const flat = engine.deserializeActivity(rawSP);
        expect(flat.linkedServiceName).toEqual({ referenceName: 'AzureSqlDatabase1', type: 'LinkedServiceReference' });
    });

    test('deserialize: reads storedProcedureName', () => {
        const flat = engine.deserializeActivity(rawSP);
        expect(flat.storedProcedureName).toBe('dbo.MyProc');
    });

    test('deserialize: reads storedProcedureParameters', () => {
        const flat = engine.deserializeActivity(rawSP);
        expect(flat.storedProcedureParameters.param1).toEqual({ value: 'hello', type: 'String' });
    });

    test('serialize: writes linkedServiceName at top level', () => {
        const flat = engine.deserializeActivity(rawSP);
        const out = engine.serializeActivity(flat);
        expect(out.linkedServiceName).toEqual({ referenceName: 'AzureSqlDatabase1', type: 'LinkedServiceReference' });
    });

    test('serialize: writes storedProcedureName and parameters', () => {
        const flat = engine.deserializeActivity(rawSP);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storedProcedureName).toBe('dbo.MyProc');
        expect(out.typeProperties.storedProcedureParameters.param2).toEqual({ value: 42, type: 'Int32' });
    });

    test('round-trip stable', () => {
        expect(stableFlat(roundTrip(rawSP))).toEqual(stableFlat(engine.deserializeActivity(rawSP)));
    });

    test('validation: missing linkedService is an error', () => {
        const flat = engine.deserializeActivity({ ...rawSP, linkedServiceName: undefined });
        expect(engine.validateActivity(flat).some(e => /linked/i.test(e))).toBe(true);
    });

    test('validation: missing storedProcedureName is an error', () => {
        const flat = engine.deserializeActivity({ ...rawSP, typeProperties: {} });
        expect(engine.validateActivity(flat).some(e => /procedure/i.test(e))).toBe(true);
    });
});

// ─── validateActivityList ─────────────────────────────────────────────────────

describe('validateActivityList', () => {
    test('returns empty object when all activities are valid', () => {
        const activities = [
            engine.deserializeActivity({ name: 'W1', type: 'Wait', dependsOn: [], userProperties: [], typeProperties: { waitTimeInSeconds: 1 } }),
        ];
        expect(engine.validateActivityList(activities)).toEqual({});
    });

    test('returns errors keyed by activity name', () => {
        const activities = [
            engine.deserializeActivity({ name: 'BadWait', type: 'Wait', dependsOn: [], userProperties: [], typeProperties: {} }),
        ];
        const errs = engine.validateActivityList(activities);
        expect(errs['BadWait']).toBeDefined();
        expect(errs['BadWait'].length).toBeGreaterThan(0);
    });

    test('collects errors across multiple activities', () => {
        const activities = [
            engine.deserializeActivity({ name: 'W1', type: 'Wait', dependsOn: [], userProperties: [], typeProperties: {} }),
            engine.deserializeActivity({ name: 'W2', type: 'Wait', dependsOn: [], userProperties: [], typeProperties: {} }),
        ];
        const errs = engine.validateActivityList(activities);
        expect(Object.keys(errs)).toHaveLength(2);
    });

    test('reports error for duplicate activity names', () => {
        const activities = [
            engine.deserializeActivity({ name: 'AppendVariable', type: 'AppendVariable', dependsOn: [], userProperties: [], typeProperties: { variableName: 'v1', value: 'a' } }),
            engine.deserializeActivity({ name: 'AppendVariable', type: 'AppendVariable', dependsOn: [], userProperties: [], typeProperties: { variableName: 'v2', value: 'b' } }),
        ];
        const errs = engine.validateActivityList(activities);
        expect(errs['AppendVariable']).toBeDefined();
        expect(errs['AppendVariable'][0]).toMatch(/[Dd]uplicate/);
    });

    test('no duplicate error when all names are unique', () => {
        const activities = [
            engine.deserializeActivity({ name: 'Append1', type: 'AppendVariable', dependsOn: [], userProperties: [], typeProperties: { variableName: 'v1', value: 'a' } }),
            engine.deserializeActivity({ name: 'Append2', type: 'AppendVariable', dependsOn: [], userProperties: [], typeProperties: { variableName: 'v2', value: 'b' } }),
        ];
        const errs = engine.validateActivityList(activities);
        expect(errs['Append1']).toBeUndefined();
        expect(errs['Append2']).toBeUndefined();
    });
});
