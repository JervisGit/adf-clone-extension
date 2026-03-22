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
});
