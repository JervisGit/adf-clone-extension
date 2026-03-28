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
//   - Nested container activities are recursively deserialized/serialized

const engine = require('../activityEngine/engine');

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Serialize a flat object back to ADF JSON then deserialize again — should equal the original flat. */
function roundTrip(raw) {
    const flat = engine.deserializeActivity(raw);
    const serialized = engine.serializeActivity(flat);
    return engine.deserializeActivity(serialized);
}

/** Strip volatile id/canvas-only fields before comparing two flat objects (recursive for nested activities). */
function stableFlat(flat) {
    const { id, element, container, color, isContainer, x, y, width, height, ...rest } = flat;
    for (const key of ['activities', 'ifTrueActivities', 'ifFalseActivities', 'defaultActivities']) {
        if (Array.isArray(rest[key])) rest[key] = rest[key].map(stableFlat);
    }
    if (Array.isArray(rest.cases)) {
        rest.cases = rest.cases.map(c => ({ ...c, activities: (c.activities || []).map(stableFlat) }));
    }
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

    test('nested activities are deserialized into flat objects', () => {
        const flat = engine.deserializeActivity(raw);
        expect(Array.isArray(flat.activities)).toBe(true);
        expect(flat.activities[0].name).toBe('InnerWait');
        expect(flat.activities[0].type).toBe('Wait');
        expect(flat.activities[0].waitTimeInSeconds).toBe(1);
        expect(flat.activities[0].typeProperties).toBeUndefined();
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

    test('ifTrueActivities are deserialized into flat objects', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.ifTrueActivities[0].name).toBe('TrueWait');
        expect(flat.ifTrueActivities[0].waitTimeInSeconds).toBe(1);
        expect(flat.ifTrueActivities[0].typeProperties).toBeUndefined();
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

    test('cases activities are deserialized into flat objects', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.cases).toHaveLength(2);
        expect(flat.cases[0].value).toBe('A');
        expect(flat.cases[0].activities[0].name).toBe('WaitA');
        expect(flat.cases[0].activities[0].waitTimeInSeconds).toBe(1);
        expect(flat.cases[0].activities[0].typeProperties).toBeUndefined();
    });

    test('defaultActivities are deserialized into flat objects', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.defaultActivities[0].name).toBe('WaitDefault');
        expect(flat.defaultActivities[0].waitTimeInSeconds).toBe(2);
        expect(flat.defaultActivities[0].typeProperties).toBeUndefined();
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

// ─── Container: recursive round-trip (3 levels deep) ──────────────────────────

describe('Container \u2014 recursive nested round-trip', () => {
    const deep = {
        name: 'ForEach1',
        type: 'ForEach',
        dependsOn: [],
        userProperties: [],
        typeProperties: {
            items: { value: '@pipeline().parameters.items', type: 'Expression' },
            isSequential: true,
            activities: [
                {
                    name: 'Until1',
                    type: 'Until',
                    dependsOn: [],
                    userProperties: [],
                    typeProperties: {
                        expression: { value: "@equals(variables('done'), true)", type: 'Expression' },
                        timeout: '0.01:00:00',
                        activities: [
                            {
                                name: 'InnerWait',
                                type: 'Wait',
                                dependsOn: [],
                                userProperties: [],
                                typeProperties: { waitTimeInSeconds: 3 },
                            },
                        ],
                    },
                },
            ],
        },
    };

    test('3-level deserialization: ForEach > Until > Wait', () => {
        const flat = engine.deserializeActivity(deep);
        expect(flat.type).toBe('ForEach');
        const until = flat.activities[0];
        expect(until.type).toBe('Until');
        expect(until.typeProperties).toBeUndefined();
        const wait = until.activities[0];
        expect(wait.type).toBe('Wait');
        expect(wait.waitTimeInSeconds).toBe(3);
        expect(wait.typeProperties).toBeUndefined();
    });

    test('3-level serialize writes correct ADF JSON structure', () => {
        const out = engine.serializeActivity(engine.deserializeActivity(deep));
        expect(out.typeProperties.activities[0].name).toBe('Until1');
        expect(out.typeProperties.activities[0].typeProperties.activities[0].name).toBe('InnerWait');
        expect(out.typeProperties.activities[0].typeProperties.activities[0].typeProperties.waitTimeInSeconds).toBe(3);
    });

    test('3-level round-trip is stable', () => {
        expect(stableFlat(roundTrip(deep))).toEqual(stableFlat(engine.deserializeActivity(deep)));
    });

    test('validateActivityList recurses 3 levels and catches inner error', () => {
        const flat = engine.deserializeActivity(deep);
        // Inject a validation error 3 levels deep
        flat.activities[0].activities[0].waitTimeInSeconds = undefined;
        const errors = engine.validateActivityList([flat]);
        expect(Object.keys(errors)).toContain('InnerWait');
    });
});

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

    test('validation: script with empty text is an error', () => {
        const flat = engine.deserializeActivity({ ...rawScript, typeProperties: { scripts: [{ type: 'Query', text: '' }] } });
        expect(engine.validateActivity(flat).some(e => /text cannot be empty/i.test(e))).toBe(true);
    });

    test('validation: script with whitespace-only text is an error', () => {
        const flat = engine.deserializeActivity({ ...rawScript, typeProperties: { scripts: [{ type: 'Query', text: '   ' }] } });
        expect(engine.validateActivity(flat).some(e => /text cannot be empty/i.test(e))).toBe(true);
    });

    test('validation: script with valid text passes', () => {
        const flat = engine.deserializeActivity(rawScript);
        expect(engine.validateActivity(flat).filter(e => /text cannot be empty/i.test(e))).toHaveLength(0);
    });

    test('deserialize: reads script parameters', () => {
        const raw = { ...rawScript, typeProperties: { scripts: [{ type: 'Query', text: 'SELECT 1', parameters: [{ name: 'p1', type: 'String', value: 'hello', direction: 'Input' }] }] } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.scripts[0].parameters[0]).toEqual({ name: 'p1', type: 'String', value: 'hello', direction: 'Input' });
    });

    test('serialize: scripts with parameters writes parameters array', () => {
        const raw = { ...rawScript, typeProperties: { scripts: [{ type: 'Query', text: 'SELECT 1', parameters: [{ name: 'p1', type: 'String', value: 'hello', direction: 'Input' }] }] } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.scripts[0].parameters).toEqual([{ name: 'p1', type: 'String', value: 'hello', direction: 'Input' }]);
    });

    test('serialize: script parameter with null value preserves null', () => {
        const raw = { ...rawScript, typeProperties: { scripts: [{ type: 'Query', text: 'SELECT 1', parameters: [{ name: 'p1', type: 'String', value: null, direction: 'Input' }] }] } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.scripts[0].parameters[0].value).toBeNull();
    });

    test('serialize: script parameter with Output+String writes size', () => {
        const raw = { ...rawScript, typeProperties: { scripts: [{ type: 'NonQuery', text: 'EXEC sp', parameters: [{ name: 'out1', type: 'String', value: null, direction: 'Output', size: 50 }] }] } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.scripts[0].parameters[0].size).toBe(50);
    });

    test('round-trip with parameters stable', () => {
        const raw = { ...rawScript, typeProperties: { scripts: [{ type: 'Query', text: 'SELECT 1', parameters: [{ name: 'p1', type: 'Int32', value: '42', direction: 'Input' }] }] } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
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

    test('validation: empty-key storedProcedureParameters is an error', () => {
        const flat = engine.deserializeActivity({ ...rawSP, typeProperties: { storedProcedureName: 'dbo.MyProc', storedProcedureParameters: { '': { value: 'x', type: 'String' } } } });
        expect(engine.validateActivity(flat).some(e => /empty/i.test(e))).toBe(true);
    });

    test('validation: named storedProcedureParameters passes', () => {
        const flat = engine.deserializeActivity(rawSP);
        expect(engine.validateActivity(flat).filter(e => /empty/i.test(e))).toHaveLength(0);
    });

    test('serialize: enforceOneTimeExecution true is written to JSON', () => {
        const flat = engine.deserializeActivity({ ...rawSP, typeProperties: { storedProcedureName: 'dbo.MyProc', enforceOneTimeExecution: true } });
        flat.enforceOneTimeExecution = true;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.enforceOneTimeExecution).toBe(true);
    });

    test('serialize: enforceOneTimeExecution false is omitted from JSON', () => {
        const flat = engine.deserializeActivity(rawSP);
        flat.enforceOneTimeExecution = false;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties).not.toHaveProperty('enforceOneTimeExecution');
    });

    test('serialize: strips empty-key parameter and does not write to JSON', () => {
        const flat = engine.deserializeActivity({ ...rawSP, typeProperties: { storedProcedureName: 'dbo.MyProc', storedProcedureParameters: { '': { value: 'x', type: 'String' } } } });
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.storedProcedureParameters).toBeUndefined();
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

// ─── WebActivity ──────────────────────────────────────────────────────────────

describe('WebActivity', () => {
    const base = {
        name: 'Web1', type: 'WebActivity', dependsOn: [], userProperties: [],
        policy: { timeout: '0.12:00:00', retry: 0, retryIntervalInSeconds: 30, secureOutput: false, secureInput: false },
        typeProperties: { url: 'https://example.com', method: 'GET' },
    };

    // ── None auth ─────────────────────────────────────────────────────────────
    test('deserialize: reads url and method', () => {
        const flat = engine.deserializeActivity(base);
        expect(flat.url).toBe('https://example.com');
        expect(flat.method).toBe('GET');
    });

    test('deserialize: no auth → authenticationType None', () => {
        const flat = engine.deserializeActivity(base);
        expect(flat.authenticationType).toBe('None');
    });

    test('serialize: writes url and method', () => {
        const flat = engine.deserializeActivity(base);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.url).toBe('https://example.com');
        expect(out.typeProperties.method).toBe('GET');
    });

    test('serialize: no authentication key when auth is None', () => {
        const flat = engine.deserializeActivity(base);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication).toBeUndefined();
    });

    test('round-trip stable (GET, no auth)', () => {
        expect(stableFlat(roundTrip(base))).toEqual(stableFlat(engine.deserializeActivity(base)));
    });

    // ── Body conditional ──────────────────────────────────────────────────────
    test('serialize: body written for POST', () => {
        const raw = { ...base, typeProperties: { url: 'https://x.com', method: 'POST', body: '{"key":"val"}' } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.body).toBe('{"key":"val"}');
    });

    // ── Headers ───────────────────────────────────────────────────────────────
    test('deserialize: headers object → array', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, headers: { 'Content-Type': 'application/json' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.headers).toEqual([{ name: 'Content-Type', value: 'application/json' }]);
    });

    test('serialize: headers array → object', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, headers: { 'X-Custom': 'abc' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.headers).toEqual({ 'X-Custom': 'abc' });
    });

    test('serialize: empty headers not written', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [];
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.headers).toBeUndefined();
    });

    // ── Advanced fields ───────────────────────────────────────────────────────
    test('deserialize: reads disableAsyncPattern from turnOffAsync', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, turnOffAsync: true } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.disableAsyncPattern).toBe(true);
    });

    test('serialize: disableAsyncPattern → turnOffAsync', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, turnOffAsync: true } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.turnOffAsync).toBe(true);
        expect(out.typeProperties.disableAsyncPattern).toBeUndefined();
    });

    test('serialize: omits turnOffAsync when false', () => {
        const flat = engine.deserializeActivity(base);
        flat.disableAsyncPattern = false;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.turnOffAsync).toBeUndefined();
    });

    test('serialize: omits disableCertValidation when false', () => {
        const flat = engine.deserializeActivity(base);
        flat.disableCertValidation = false;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.disableCertValidation).toBeUndefined();
    });

    test('serialize: writes disableCertValidation when true', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, disableCertValidation: true } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.disableCertValidation).toBe(true);
    });

    // ── Basic auth ────────────────────────────────────────────────────────────
    test('deserialize: Basic auth reads username and password', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'usr', password: 'pwd' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.authenticationType).toBe('Basic');
        expect(flat.username).toBe('usr');
        expect(flat.password).toBe('pwd');
    });

    test('serialize: Basic auth writes correct structure', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'usr', password: 'pwd' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication).toEqual({ type: 'Basic', username: 'usr', password: 'pwd' });
    });

    test('round-trip stable (Basic auth)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'usr', password: 'pwd' } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    // ── MSI auth ──────────────────────────────────────────────────────────────
    test('deserialize: MSI auth reads resource', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'MSI', resource: 'https://storage.azure.com' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.authenticationType).toBe('MSI');
        expect(flat.resource).toBe('https://storage.azure.com');
    });

    test('serialize: MSI auth writes correct structure', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'MSI', resource: 'https://storage.azure.com' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication).toEqual({ type: 'MSI', resource: 'https://storage.azure.com' });
    });

    // ── ClientCertificate auth ────────────────────────────────────────────────
    test('deserialize: ClientCertificate reads pfx and pfxPassword (from password)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ClientCertificate', pfx: 'base64cert', password: 'certpass' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.authenticationType).toBe('ClientCertificate');
        expect(flat.pfx).toBe('base64cert');
        expect(flat.pfxPassword).toBe('certpass');
    });

    test('serialize: ClientCertificate writes pfx and password', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ClientCertificate', pfx: 'base64cert', password: 'certpass' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.pfx).toBe('base64cert');
        expect(out.typeProperties.authentication.password).toBe('certpass');
        expect(out.typeProperties.authentication.type).toBe('ClientCertificate');
    });

    test('round-trip stable (ClientCertificate)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ClientCertificate', pfx: 'b64', password: 'pw' } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    // ── ServicePrincipal Inline key ────────────────────────────────────────────
    test('deserialize: SP Inline reads userTenant→tenant and username→servicePrincipalId', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 'myTenant', username: 'spId', password: 'spKey', resource: 'https://res' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.authenticationType).toBe('ServicePrincipal');
        expect(flat.servicePrincipalAuthMethod).toBe('Inline');
        expect(flat.tenant).toBe('myTenant');
        expect(flat.servicePrincipalId).toBe('spId');
        expect(flat.servicePrincipalKey).toBe('spKey');
        expect(flat.servicePrincipalCredentialType).toBe('Service Principal Key');
        expect(flat.servicePrincipalResource).toBe('https://res');
    });

    test('serialize: SP Inline key writes userTenant and username', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 'myTenant', username: 'spId', password: 'spKey', resource: 'https://res' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.type).toBe('ServicePrincipal');
        expect(out.typeProperties.authentication.userTenant).toBe('myTenant');
        expect(out.typeProperties.authentication.username).toBe('spId');
        expect(out.typeProperties.authentication.password).toBe('spKey');
        expect(out.typeProperties.authentication.resource).toBe('https://res');
        expect(out.typeProperties.authentication.tenant).toBeUndefined();
        expect(out.typeProperties.authentication.servicePrincipalId).toBeUndefined();
    });

    test('round-trip stable (SP Inline key)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 't', username: 'u', password: 'k', resource: 'r' } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    // ── ServicePrincipal Inline certificate ───────────────────────────────────
    test('deserialize: SP Inline cert sets servicePrincipalCredentialType = Certificate', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 't', username: 'u', pfx: 'certdata', resource: 'r' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.servicePrincipalCredentialType).toBe('Service Principal Certificate');
        expect(flat.servicePrincipalCert).toBe('certdata');
    });

    test('serialize: SP Inline cert writes pfx', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 't', username: 'u', pfx: 'certdata', resource: 'r' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.pfx).toBe('certdata');
        expect(out.typeProperties.authentication.password).toBeUndefined();
    });

    // ── ServicePrincipal Credential method ────────────────────────────────────
    test('deserialize: SP Credential (no type) sets servicePrincipalAuthMethod = Credential', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { credential: { referenceName: 'MyCred', type: 'CredentialReference' }, resource: 'https://res' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.authenticationType).toBe('ServicePrincipal');
        expect(flat.servicePrincipalAuthMethod).toBe('Credential');
        expect(flat.credential).toBe('MyCred');
        expect(flat.credentialResource).toBe('https://res');
    });

    test('serialize: SP Credential writes no type, wraps credential as reference', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { credential: { referenceName: 'MyCred', type: 'CredentialReference' }, resource: 'https://res' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.type).toBeUndefined();
        expect(out.typeProperties.authentication.credential).toEqual({ referenceName: 'MyCred', type: 'CredentialReference' });
        expect(out.typeProperties.authentication.resource).toBe('https://res');
    });

    test('round-trip stable (SP Credential)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { credential: { referenceName: 'MyCred', type: 'CredentialReference' }, resource: 'r' } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    // ── UserAssignedManagedIdentity ────────────────────────────────────────────
    test('deserialize: UAMI reads resource and unwraps credential', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'UserAssignedManagedIdentity', credential: { referenceName: 'UamiCred', type: 'CredentialReference' }, resource: 'https://storage' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.authenticationType).toBe('UserAssignedManagedIdentity');
        expect(flat.credentialUserAssigned).toBe('UamiCred');
        expect(flat.resource).toBe('https://storage');
    });

    test('serialize: UAMI wraps credential as reference', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'UserAssignedManagedIdentity', credential: { referenceName: 'UamiCred', type: 'CredentialReference' }, resource: 'https://storage' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.type).toBe('UserAssignedManagedIdentity');
        expect(out.typeProperties.authentication.credential).toEqual({ referenceName: 'UamiCred', type: 'CredentialReference' });
        expect(out.typeProperties.authentication.resource).toBe('https://storage');
    });

    test('round-trip stable (UAMI)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'UserAssignedManagedIdentity', credential: { referenceName: 'c', type: 'CredentialReference' }, resource: 'r' } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    // ── Validation ────────────────────────────────────────────────────────────
    test('validation: missing url is an error', () => {
        const flat = engine.deserializeActivity({ ...base, typeProperties: { method: 'GET' } });
        expect(engine.validateActivity(flat).some(e => /url/i.test(e))).toBe(true);
    });

    test('validation: valid GET activity passes', () => {
        const flat = engine.deserializeActivity(base);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: POST without body is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.method = 'POST';
        flat.body = '';
        expect(engine.validateActivity(flat).some(e => /body/i.test(e))).toBe(true);
    });

    test('validation: PUT without body is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.method = 'PUT';
        flat.body = '';
        expect(engine.validateActivity(flat).some(e => /body/i.test(e))).toBe(true);
    });

    test('validation: DELETE without body does not error', () => {
        const flat = engine.deserializeActivity(base);
        flat.method = 'DELETE';
        flat.body = '';
        expect(engine.validateActivity(flat).every(e => !/body/i.test(e))).toBe(true);
    });

    test('validation: POST with body passes', () => {
        const flat = engine.deserializeActivity(base);
        flat.method = 'POST';
        flat.body = '{"key":"value"}';
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: Basic auth without username fails', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', password: 'pw' } } };
        const flat = engine.deserializeActivity(raw);
        // username is required for Basic; need to clear it
        flat.username = '';
        expect(engine.validateActivity(flat).some(e => /username/i.test(e))).toBe(true);
    });

    test('validation: SP Inline requires tenant + servicePrincipalId + resource', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'ServicePrincipal';
        flat.servicePrincipalAuthMethod = 'Inline';
        flat.tenant = '';
        flat.servicePrincipalId = '';
        flat.servicePrincipalResource = '';
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => /tenant/i.test(e))).toBe(true);
        expect(errs.some(e => /service principal id/i.test(e))).toBe(true);
        expect(errs.some(e => /resource/i.test(e))).toBe(true);
    });

    test('validation: SP Credential should NOT require tenant (nestedConditional)', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'ServicePrincipal';
        flat.servicePrincipalAuthMethod = 'Credential';
        flat.credential = 'MyCred';
        flat.credentialResource = 'https://res';
        flat.tenant = '';  // tenant is for Inline only; should not be required here
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => /tenant/i.test(e))).toBe(false);
    });

    test('validation: header with missing name is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: '', value: 'v1' }];
        expect(engine.validateActivity(flat).some(e => /name.*required/i.test(e))).toBe(true);
    });

    test('validation: header with missing value is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: 'X-My-Header', value: '' }];
        expect(engine.validateActivity(flat).some(e => /value.*required/i.test(e))).toBe(true);
    });

    test('validation: duplicate header names are an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: 'X-Hdr', value: 'a' }, { name: 'X-Hdr', value: 'b' }];
        expect(engine.validateActivity(flat).some(e => /duplicate/i.test(e))).toBe(true);
    });

    test('validation: duplicate header names are case-insensitive', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: 'X-HDR', value: 'a' }, { name: 'x-hdr', value: 'b' }];
        expect(engine.validateActivity(flat).some(e => /duplicate/i.test(e))).toBe(true);
    });

    test('validation: valid headers pass', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: 'Content-Type', value: 'application/json' }, { name: 'X-Custom', value: 'val' }];
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: header row with empty name AND empty value is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: '', value: '' }];
        expect(engine.validateActivity(flat).some(e => /name is required/i.test(e))).toBe(true);
    });

    test('validation: header row with empty value is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: 'X-Header', value: '' }];
        expect(engine.validateActivity(flat).some(e => /value is required/i.test(e))).toBe(true);
    });

    // ── httpRequestTimeout validation ──────────────────────────────────────────
    test('validation: httpRequestTimeout with invalid format is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.httpRequestTimeout = '5 minutes';
        expect(engine.validateActivity(flat).some(e => /HH:MM:SS/i.test(e))).toBe(true);
    });

    test('validation: httpRequestTimeout below 1 minute is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.httpRequestTimeout = '00:00:30';
        expect(engine.validateActivity(flat).some(e => /between 1 and 10 minutes/i.test(e))).toBe(true);
    });

    test('validation: httpRequestTimeout above 10 minutes is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.httpRequestTimeout = '00:11:00';
        expect(engine.validateActivity(flat).some(e => /between 1 and 10 minutes/i.test(e))).toBe(true);
    });

    test('validation: httpRequestTimeout exactly 1 minute passes', () => {
        const flat = engine.deserializeActivity(base);
        flat.httpRequestTimeout = '00:01:00';
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: httpRequestTimeout exactly 10 minutes passes', () => {
        const flat = engine.deserializeActivity(base);
        flat.httpRequestTimeout = '00:10:00';
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: httpRequestTimeout 5 minutes passes', () => {
        const flat = engine.deserializeActivity(base);
        flat.httpRequestTimeout = '00:05:00';
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: empty httpRequestTimeout is not an error (optional field)', () => {
        const flat = engine.deserializeActivity(base);
        flat.httpRequestTimeout = '';
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    // ── AKV secret fields ──────────────────────────────────────────────────────
    const akvObj = { type: 'AzureKeyVaultSecret', store: { referenceName: 'AzureKeyVault1', type: 'LinkedServiceReference' }, secretName: 'my-secret' };
    const akvObjWithVersion = { ...akvObj, secretVersion: 'abc123' };

    test('deserialize: Basic auth preserves AKV object for password', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'user', password: akvObj } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.password).toEqual(akvObj);
    });

    test('serialize: Basic auth writes AKV object for password', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'user', password: akvObj } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.password).toEqual(akvObj);
    });

    test('serialize: Basic auth strips secretVersion "latest"', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = { ...akvObj, secretVersion: 'latest' };
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.password.secretVersion).toBeUndefined();
    });

    test('serialize: Basic auth preserves real secretVersion', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'user', password: akvObjWithVersion } } };
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.authentication.password.secretVersion).toBe('abc123');
    });

    test('round-trip stable (Basic auth with AKV password)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'user', password: akvObj } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('deserialize: ClientCertificate preserves AKV pfx and password', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ClientCertificate', pfx: akvObj, password: akvObj } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.pfx).toEqual(akvObj);
        expect(flat.pfxPassword).toEqual(akvObj);
    });

    test('serialize: ClientCertificate writes AKV objects', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ClientCertificate', pfx: akvObj, password: akvObj } } };
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.authentication.pfx).toEqual(akvObj);
        expect(out.typeProperties.authentication.password).toEqual(akvObj);
    });

    test('round-trip stable (ClientCertificate with AKV)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ClientCertificate', pfx: akvObj, password: akvObj } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('deserialize: SP Inline preserves AKV key', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 'tenant1', username: 'spid', password: akvObj, resource: 'https://res' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.servicePrincipalKey).toEqual(akvObj);
    });

    test('serialize: SP Inline writes AKV key', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 'tenant1', username: 'spid', password: akvObj, resource: 'https://res' } } };
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.authentication.password).toEqual(akvObj);
    });

    test('deserialize: SP Inline cert preserves AKV cert', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 'tenant1', username: 'spid', pfx: akvObj, resource: 'https://res' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.servicePrincipalCert).toEqual(akvObj);
    });

    test('serialize: SP Inline cert writes AKV cert as pfx', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'ServicePrincipal', userTenant: 'tenant1', username: 'spid', pfx: akvObj, resource: 'https://res' } } };
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.authentication.pfx).toEqual(akvObj);
    });

    test('validation: akv-secret with no store is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = { type: 'AzureKeyVaultSecret', store: { referenceName: '', type: 'LinkedServiceReference' }, secretName: 'pw' };
        expect(engine.validateActivity(flat).some(e => /key vault/i.test(e))).toBe(true);
    });

    test('validation: akv-secret with no secretName is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = { type: 'AzureKeyVaultSecret', store: { referenceName: 'AzureKeyVault1', type: 'LinkedServiceReference' }, secretName: '' };
        expect(engine.validateActivity(flat).some(e => /secret name/i.test(e))).toBe(true);
    });

    test('validation: valid AKV password passes', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = akvObj;
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: switching from Basic to MSI clears stale AKV password error', () => {
        // Set up a partial AKV object (would fail Basic validation)
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = { type: 'AzureKeyVaultSecret', store: { referenceName: 'AzureKeyVault1', type: 'LinkedServiceReference' }, secretName: '' };
        // Confirm error appears under Basic
        expect(engine.validateActivity(flat).some(e => /secret name/i.test(e))).toBe(true);
        // Switch to MSI — password field is no longer conditional-visible
        flat.authenticationType = 'MSI';
        flat.resource = 'https://res';
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: SP Credential mode does not require/validate servicePrincipalKey', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'ServicePrincipal';
        flat.servicePrincipalAuthMethod = 'Credential';
        flat.credential = 'MyCred';
        flat.credentialResource = 'https://res';
        // Even with a bad AKV object in servicePrincipalKey, it should not error
        // because servicePrincipalKey is excluded when authMethod is Credential
        flat.servicePrincipalKey = { type: 'AzureKeyVaultSecret', store: { referenceName: '', type: 'LinkedServiceReference' }, secretName: '' };
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });
});

// ─── WebHook ──────────────────────────────────────────────────────────────────

describe('WebHook', () => {
    const base = {
        name: 'Hook1', type: 'WebHook', dependsOn: [], userProperties: [],
        policy: { secureOutput: false, secureInput: false },
        typeProperties: { url: 'https://callback.example.com', method: 'POST', timeout: '00:10:00', body: '{"key":"val"}' },
    };

    test('deserialize: reads url, method, timeout', () => {
        const flat = engine.deserializeActivity(base);
        expect(flat.url).toBe('https://callback.example.com');
        expect(flat.method).toBe('POST');
        expect(flat.timeout).toBe('00:10:00');
    });

    test('deserialize: no auth → authenticationType None', () => {
        const flat = engine.deserializeActivity(base);
        expect(flat.authenticationType).toBe('None');
    });

    test('serialize: writes url, method, timeout', () => {
        const flat = engine.deserializeActivity(base);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.url).toBe('https://callback.example.com');
        expect(out.typeProperties.method).toBe('POST');
        expect(out.typeProperties.timeout).toBe('00:10:00');
    });

    test('serialize: reportStatusOnCallBack omitted when false', () => {
        const flat = engine.deserializeActivity(base);
        flat.reportStatusOnCallBack = false;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.reportStatusOnCallBack).toBeUndefined();
    });

    test('serialize: reportStatusOnCallBack written when true', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, reportStatusOnCallBack: true } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.reportStatusOnCallBack).toBe(true);
    });

    test('serialize: disableCertValidation omitted when false', () => {
        const flat = engine.deserializeActivity(base);
        flat.disableCertValidation = false;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.disableCertValidation).toBeUndefined();
    });

    test('serialize: disableCertValidation written when true', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, disableCertValidation: true } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.disableCertValidation).toBe(true);
    });

    test('round-trip stable (basic)', () => {
        expect(stableFlat(roundTrip(base))).toEqual(stableFlat(engine.deserializeActivity(base)));
    });

    test('headers object → array on deserialize', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, headers: { Authorization: 'Bearer token' } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.headers).toEqual([{ name: 'Authorization', value: 'Bearer token' }]);
    });

    test('headers array → object on serialize', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, headers: { Authorization: 'Bearer token' } } };
        const flat = engine.deserializeActivity(raw);
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.headers).toEqual({ Authorization: 'Bearer token' });
    });

    test('round-trip stable (with headers and reportStatusOnCallBack)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, headers: { 'X-Key': 'val' }, reportStatusOnCallBack: true } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('WebHook Basic auth round-trip stable', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'u', password: 'p' } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validation: missing url is an error', () => {
        const flat = engine.deserializeActivity({ ...base, typeProperties: { method: 'POST', timeout: '00:10:00' } });
        expect(engine.validateActivity(flat).some(e => /url/i.test(e))).toBe(true);
    });

    test('validation: valid activity passes', () => {
        const flat = engine.deserializeActivity(base);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: WebHook POST without body is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.body = '';
        expect(engine.validateActivity(flat).some(e => /body/i.test(e))).toBe(true);
    });

    test('validation: WebHook POST with body passes', () => {
        const flat = engine.deserializeActivity(base);
        flat.body = '{"status":"ok"}';
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });

    test('validation: WebHook header missing name is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: '', value: 'v' }];
        expect(engine.validateActivity(flat).some(e => /name.*required/i.test(e))).toBe(true);
    });

    test('validation: WebHook duplicate headers are an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.headers = [{ name: 'Authorization', value: 'a' }, { name: 'Authorization', value: 'b' }];
        expect(engine.validateActivity(flat).some(e => /duplicate/i.test(e))).toBe(true);
    });

    // ── AKV secret fields ──────────────────────────────────────────────────────
    const hookAkvObj = { type: 'AzureKeyVaultSecret', store: { referenceName: 'AzureKeyVault1', type: 'LinkedServiceReference' }, secretName: 'hook-secret' };

    test('deserialize: WebHook Basic auth preserves AKV password', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'user', password: hookAkvObj } } };
        const flat = engine.deserializeActivity(raw);
        expect(flat.password).toEqual(hookAkvObj);
    });

    test('serialize: WebHook Basic auth writes AKV password', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'user', password: hookAkvObj } } };
        const out = engine.serializeActivity(engine.deserializeActivity(raw));
        expect(out.typeProperties.authentication.password).toEqual(hookAkvObj);
    });

    test('serialize: WebHook strips secretVersion "latest"', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = { ...hookAkvObj, secretVersion: 'latest' };
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.authentication.password.secretVersion).toBeUndefined();
    });

    test('round-trip stable (WebHook Basic auth with AKV password)', () => {
        const raw = { ...base, typeProperties: { ...base.typeProperties, authentication: { type: 'Basic', username: 'user', password: hookAkvObj } } };
        expect(stableFlat(roundTrip(raw))).toEqual(stableFlat(engine.deserializeActivity(raw)));
    });

    test('validation: WebHook akv-secret missing store is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = { type: 'AzureKeyVaultSecret', store: { referenceName: '', type: 'LinkedServiceReference' }, secretName: 'pw' };
        expect(engine.validateActivity(flat).some(e => /key vault/i.test(e))).toBe(true);
    });

    test('validation: WebHook akv-secret missing secretName is an error', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = { type: 'AzureKeyVaultSecret', store: { referenceName: 'AzureKeyVault1', type: 'LinkedServiceReference' }, secretName: '' };
        expect(engine.validateActivity(flat).some(e => /secret name/i.test(e))).toBe(true);
    });

    test('validation: WebHook valid AKV password passes', () => {
        const flat = engine.deserializeActivity(base);
        flat.authenticationType = 'Basic';
        flat.username = 'user';
        flat.password = hookAkvObj;
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });
});

// ─── Copy Activity ─────────────────────────────────────────────────────────────

describe('Copy — isActivityTypeSupported', () => {
    test('Copy is supported', () => {
        expect(engine.isActivityTypeSupported('Copy')).toBe(true);
    });
});

describe('Copy — AzureSqlTable source/sink', () => {
    const raw = {
        name: 'CopySQL',
        type: 'Copy',
        dependsOn: [],
        userProperties: [],
        inputs:  [{ referenceName: 'SrcDS', type: 'DatasetReference' }],
        outputs: [{ referenceName: 'SnkDS', type: 'DatasetReference' }],
        typeProperties: {
            source: {
                type: 'AzureSqlSource',
                sqlReaderQuery: 'SELECT * FROM dbo.Orders',
                queryTimeout: '02:00:00',
                isolationLevel: 'ReadCommitted',
                partitionOption: 'None',
            },
            sink: {
                type: 'AzureSqlSink',
                writeBehavior: 'insert',
                sqlWriterUseTableLock: false,
                disableMetricsCollection: false,
            },
            translator: { type: 'TabularTranslator', typeConversion: true },
            enableStaging: false,
            dataIntegrationUnits: 8,
            parallelCopies: 2,
            enableSkipIncompatibleRow: false,
        },
        policy: { timeout: '0.12:00:00', retry: 0 },
    };

    test('deserialize: dataset refs extracted', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.sourceDataset).toBe('SrcDS');
        expect(flat.sinkDataset).toBe('SnkDS');
    });

    test('deserialize: _sourceObject/_sinkObject stashed', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat._sourceObject).toEqual(raw.typeProperties.source);
        expect(flat._sinkObject).toEqual(raw.typeProperties.sink);
    });

    test('deserialize: translator pass-through', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat._copyTranslator).toEqual(raw.typeProperties.translator);
        expect(flat._copyEnableStaging).toBe(false);
    });

    test('deserialize: typeProperties fields read (dataIntegrationUnits)', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat.dataIntegrationUnits).toBe(8);
        expect(flat.parallelCopies).toBe(2);
    });

    test('serialize: inputs/outputs written from sourceDataset/sinkDataset', () => {
        const flat = engine.deserializeActivity(raw);
        // Set dataset types as webview would
        flat._sourceDatasetType = 'AzureSqlTable';
        flat._sinkDatasetType   = 'AzureSqlTable';
        flat['src_useQuery']    = 'Query';
        flat['src_sqlReaderQuery'] = 'SELECT * FROM dbo.Orders';
        flat['src_queryTimeout']   = '02:00:00';
        flat['snk_writeBehavior']  = 'insert';
        flat['snk_sqlWriterUseTableLock'] = false;
        flat['snk_disableMetricsCollection'] = false;
        const out = engine.serializeActivity(flat);
        expect(out.inputs[0].referenceName).toBe('SrcDS');
        expect(out.outputs[0].referenceName).toBe('SnkDS');
    });

    test('serialize: source object written with correct type', () => {
        const flat = engine.deserializeActivity(raw);
        flat._sourceDatasetType = 'AzureSqlTable';
        flat._sinkDatasetType   = 'AzureSqlTable';
        flat['src_useQuery']       = 'Query';
        flat['src_sqlReaderQuery'] = 'SELECT 1';
        flat['snk_writeBehavior']  = 'insert';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.type).toBe('AzureSqlSource');
        expect(out.typeProperties.source.sqlReaderQuery).toBe('SELECT 1');
    });

    test('serialize: sink object written with correct type', () => {
        const flat = engine.deserializeActivity(raw);
        flat._sourceDatasetType   = 'AzureSqlTable';
        flat._sinkDatasetType     = 'AzureSqlTable';
        flat['src_useQuery']      = 'Table';
        flat['snk_writeBehavior'] = 'insert';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.sink.type).toBe('AzureSqlSink');
    });

    test('serialize: translator pass-through preserved', () => {
        const flat = engine.deserializeActivity(raw);
        flat._sourceDatasetType = 'AzureSqlTable';
        flat._sinkDatasetType   = 'AzureSqlTable';
        flat['src_useQuery']    = 'Table';
        flat['snk_writeBehavior'] = 'insert';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.translator).toEqual(raw.typeProperties.translator);
    });

    test('serialize: dataIntegrationUnits written', () => {
        const flat = engine.deserializeActivity(raw);
        flat._sourceDatasetType = 'AzureSqlTable';
        flat._sinkDatasetType   = 'AzureSqlTable';
        flat['snk_writeBehavior'] = 'insert';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.dataIntegrationUnits).toBe(8);
        expect(out.typeProperties.parallelCopies).toBe(2);
    });

    test('validation: missing sourceDataset is an error', () => {
        const flat = engine.deserializeActivity(raw);
        flat.sourceDataset = '';
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => /source dataset/i.test(e))).toBe(true);
    });

    test('validation: missing sinkDataset is an error', () => {
        const flat = engine.deserializeActivity(raw);
        flat.sinkDataset = '';
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => /sink dataset/i.test(e))).toBe(true);
    });

    test('validation: valid activity has no errors', () => {
        const flat = engine.deserializeActivity(raw);
        expect(engine.validateActivity(flat)).toHaveLength(0);
    });
});

describe('Copy — Parquet source/sink (storage type)', () => {
    const raw = {
        name: 'CopyParquet',
        type: 'Copy',
        dependsOn: [],
        userProperties: [],
        inputs:  [{ referenceName: 'ParquetSrc', type: 'DatasetReference' }],
        outputs: [{ referenceName: 'ParquetSnk', type: 'DatasetReference' }],
        typeProperties: {
            source: {
                type: 'ParquetSource',
                storeSettings: {
                    type: 'AzureBlobFSReadSettings',
                    recursive: true,
                    enablePartitionDiscovery: false,
                },
                formatSettings: { type: 'ParquetReadSettings' },
            },
            sink: {
                type: 'ParquetSink',
                storeSettings: { type: 'AzureBlobFSWriteSettings' },
                formatSettings: { type: 'ParquetWriteSettings' },
            },
            translator: { type: 'TabularTranslator' },
        },
    };

    test('deserialize: _sourceObject stashed', () => {
        const flat = engine.deserializeActivity(raw);
        expect(flat._sourceObject.type).toBe('ParquetSource');
        expect(flat._sinkObject.type).toBe('ParquetSink');
    });

    test('serialize: source/sink written when _sourceDatasetType set', () => {
        const flat = engine.deserializeActivity(raw);
        flat._sourceDatasetType  = 'Parquet';
        flat._sinkDatasetType    = 'Parquet';
        flat._sourceLocationType = 'AzureBlobFSLocation';
        flat._sinkLocationType   = 'AzureBlobFSLocation';
        flat['src_filePathType'] = 'filePathInDataset';
        flat['src_recursive']    = true;
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source.type).toBe('ParquetSource');
        expect(out.typeProperties.source.storeSettings.type).toBe('AzureBlobFSReadSettings');
        expect(out.typeProperties.sink.type).toBe('ParquetSink');
        expect(out.typeProperties.sink.storeSettings.type).toBe('AzureBlobFSWriteSettings');
    });

    test('serialize: falls back to _sourceObject when no _sourceDatasetType', () => {
        const flat = engine.deserializeActivity(raw);
        // No _sourceDatasetType set — should use _sourceObject as-is
        flat._sinkDatasetType = 'Parquet';
        flat._sinkLocationType = 'AzureBlobFSLocation';
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.source).toEqual(raw.typeProperties.source);
    });
});

describe('Copy — AzureSqlDWTable sink writeBehavior', () => {
    test('serialize: BulkInsert → writeBehavior Insert', () => {
        const flat = {
            id: 1, type: 'Copy', name: 'CopyDW',
            sourceDataset: 'SrcDS', sinkDataset: 'SnkDS',
            _sourceDatasetType: 'AzureSqlDWTable',
            _sinkDatasetType: 'AzureSqlDWTable',
            'snk_copyMethod': 'BulkInsert',
            dependsOn: [], userProperties: [],
        };
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.sink.writeBehavior).toBe('Insert');
    });

    test('serialize: Upsert → writeBehavior Upsert', () => {
        const flat = {
            id: 1, type: 'Copy', name: 'CopyDW',
            sourceDataset: 'SrcDS', sinkDataset: 'SnkDS',
            _sourceDatasetType: 'AzureSqlDWTable',
            _sinkDatasetType: 'AzureSqlDWTable',
            'snk_copyMethod': 'Upsert',
            dependsOn: [], userProperties: [],
        };
        const out = engine.serializeActivity(flat);
        expect(out.typeProperties.sink.writeBehavior).toBe('Upsert');
    });
});

// ─── Copy Activity — config-field validation ───────────────────────────────────

describe('Copy — validate: additional-columns empty name blocked', () => {
    test('blocks save when additional-columns row has empty name', () => {
        const flat = {
            type: 'Copy', name: 'TestCopy',
            sourceDataset: 'DS1', sinkDataset: 'DS2',
            _sourceDatasetType: 'Avro', _sinkDatasetType: 'AzureSqlTable',
            src_additionalColumns: [{ name: '', value: '$$FILEPATH' }],
            dependsOn: [], userProperties: [],
        };
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => e.includes('Additional columns') || e.includes('additionalColumns') || e.includes('empty'))).toBe(true);
    });

    test('passes when all additional-columns rows have names', () => {
        const flat = {
            type: 'Copy', name: 'TestCopy',
            sourceDataset: 'DS1', sinkDataset: 'DS2',
            _sourceDatasetType: 'Avro', _sinkDatasetType: 'AzureSqlTable',
            src_additionalColumns: [{ name: 'col1', value: '$$FILEPATH' }],
            dependsOn: [], userProperties: [],
        };
        const errs = engine.validateActivity(flat);
        expect(errs.filter(e => e.toLowerCase().includes('additional'))).toHaveLength(0);
    });
});

describe('Copy — validate: string-list empty item blocked (upsert keys)', () => {
    test('blocks save when upsert key list has an empty entry', () => {
        const flat = {
            type: 'Copy', name: 'TestCopy',
            sourceDataset: 'DS1', sinkDataset: 'DS2',
            _sourceDatasetType: 'AzureSqlTable', _sinkDatasetType: 'AzureSqlTable',
            snk_writeBehavior: 'upsert',
            snk_upsertKeys: [''],
            dependsOn: [], userProperties: [],
        };
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => e.toLowerCase().includes('empty'))).toBe(true);
    });

    test('passes when upsert keys are all non-empty', () => {
        const flat = {
            type: 'Copy', name: 'TestCopy',
            sourceDataset: 'DS1', sinkDataset: 'DS2',
            _sourceDatasetType: 'AzureSqlTable', _sinkDatasetType: 'AzureSqlTable',
            snk_writeBehavior: 'upsert',
            snk_upsertKeys: ['Id', 'Name'],
            dependsOn: [], userProperties: [],
        };
        const errs = engine.validateActivity(flat);
        expect(errs.filter(e => e.toLowerCase().includes('empty'))).toHaveLength(0);
    });
});

describe('Copy — validate: sp-parameters empty name blocked', () => {
    // Use source-side stored procedure parameters (conditional: src_useQuery = 'Stored procedure')
    test('blocks save when sp-parameters has an empty-key entry', () => {
        const flat = {
            type: 'Copy', name: 'TestCopy',
            sourceDataset: 'DS1', sinkDataset: 'DS2',
            _sourceDatasetType: 'AzureSqlTable', _sinkDatasetType: 'AzureSqlTable',
            src_useQuery: 'Stored procedure',
            src_sqlReaderStoredProcedureName: 'sp_read',
            src_storedProcedureParameters: { '': { value: 'foo', type: 'String' } },
            dependsOn: [], userProperties: [],
        };
        const errs = engine.validateActivity(flat);
        expect(errs.some(e => e.toLowerCase().includes('parameter') || e.toLowerCase().includes('empty'))).toBe(true);
    });

    test('passes when all sp-parameter keys are non-empty', () => {
        const flat = {
            type: 'Copy', name: 'TestCopy',
            sourceDataset: 'DS1', sinkDataset: 'DS2',
            _sourceDatasetType: 'AzureSqlTable', _sinkDatasetType: 'AzureSqlTable',
            src_useQuery: 'Stored procedure',
            src_sqlReaderStoredProcedureName: 'sp_read',
            src_storedProcedureParameters: { myParam: { value: 'foo', type: 'String' } },
            dependsOn: [], userProperties: [],
        };
        const errs = engine.validateActivity(flat);
        expect(errs.filter(e => e.toLowerCase().includes('parameter') && e.toLowerCase().includes('empty'))).toHaveLength(0);
    });
});

