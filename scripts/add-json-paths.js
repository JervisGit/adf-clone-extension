// One-time script: annotates activity-schemas-v2.json with jsonPath on every field.
// Source of truth: V1 savePipelineToWorkspace() and loadPipelineFromJson().
// Convention:
//   "@fieldName"                  → activity root level (name, description, state, etc.)
//   "policy.fieldName"            → inside activity.policy object
//   "typeProperties.fieldName"    → inside activity.typeProperties
//   null                          → UI-only field; never serialized to JSON
//   "typeProperties.fieldName" + "type":"containerActivities" → recurse
//   "serializeAs":"transformerName" → non-trivial serialization (handled in engine.js Step 3)

'use strict';
const fs = require('fs');
const path = require('path');

const schemaPath = path.join(__dirname, '..', 'src', 'activity-schemas-v2.json');
const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));

// ─── Shared commonProperties jsonPaths ────────────────────────────────────────
// These are the same across every activity type.
const COMMON_JP = {
    name:                  '@name',
    description:           '@description',
    state:                 '@state',
    onInactiveMarkAs:      '@onInactiveMarkAs',
    timeout:               'policy.timeout',         // policy section
    retry:                 'policy.retry',
    retryIntervalInSeconds:'policy.retryIntervalInSeconds',
    secureOutput:          'policy.secureOutput',
    secureInput:           'policy.secureInput',
};

// ─── Activity-specific typeProperties jsonPaths ───────────────────────────────
// key: activityType, value: { fieldName: jsonPath | { jsonPath, type?, serializeAs? } }
const TYPE_JP = {
    Wait: {
        waitTimeInSeconds: 'typeProperties.waitTimeInSeconds',
    },
    Fail: {
        message:   'typeProperties.message',
        errorCode: 'typeProperties.errorCode',
    },
    AppendVariable: {
        variableName: 'typeProperties.variableName',
        value:        'typeProperties.value',
    },
    SetVariable: {
        // variableType and pipelineVariableType are UI-only discriminators
        variableType:        { jsonPath: null, uiOnly: true },
        pipelineVariableType:{ jsonPath: null, uiOnly: true },
        // Pipeline variable path
        variableName:        'typeProperties.variableName',
        value:               'typeProperties.value',
        // Pipeline return value path — complex serialization handled by transformer
        returnValues: {
            jsonPath:    'typeProperties.value',
            serializeAs: 'setVariableReturnValues',
        },
    },
    Filter: {
        items:     'typeProperties.items',
        condition: 'typeProperties.condition',
    },
    ExecutePipeline: {
        pipeline:          'typeProperties.pipeline',
        waitOnCompletion:  'typeProperties.waitOnCompletion',
    },
    ForEach: {
        items:        'typeProperties.items',
        isSequential: 'typeProperties.isSequential',
        batchCount:   'typeProperties.batchCount',
        // activities array — engine recurses into this
        activities: {
            jsonPath: 'typeProperties.activities',
            type:     'containerActivities',
        },
    },
    Until: {
        expression: 'typeProperties.expression',
        // Until has its own timeout inside typeProperties (not policy)
        timeout:    'typeProperties.timeout',
        activities: {
            jsonPath: 'typeProperties.activities',
            type:     'containerActivities',
        },
    },
    IfCondition: {
        expression: 'typeProperties.expression',
        ifTrueActivities: {
            jsonPath: 'typeProperties.ifTrueActivities',
            type:     'containerActivities',
        },
        ifFalseActivities: {
            jsonPath: 'typeProperties.ifFalseActivities',
            type:     'containerActivities',
        },
    },
    Switch: {
        on: 'typeProperties.on',
        cases: {
            jsonPath: 'typeProperties.cases',
            type:     'switchCases',   // engine handles case array + per-case activity recursion
        },
        defaultActivities: {
            jsonPath: 'typeProperties.defaultActivities',
            type:     'containerActivities',
        },
    },
    Lookup: {
        dataset:      'typeProperties.dataset',
        firstRowOnly: 'typeProperties.firstRowOnly',
    },
    Delete: {
        // sourceProperties fields all end up inside typeProperties
        dataset:                 'typeProperties.dataset',
        filePathType:            { jsonPath: null, uiOnly: true }, // controls storeSettings type
        prefix:                  'typeProperties.storeSettings.prefix',
        wildcardFileName:        'typeProperties.storeSettings.wildcardFileName',
        fileListPath:            'typeProperties.storeSettings.fileListPath',
        modifiedDatetimeStart:   'typeProperties.storeSettings.modifiedDatetimeStart',
        modifiedDatetimeEnd:     'typeProperties.storeSettings.modifiedDatetimeEnd',
        recursive:               'typeProperties.storeSettings.recursive',
        maxConcurrentConnections:'typeProperties.maxConcurrentConnections',
    },
    Validation: {
        dataset:  'typeProperties.dataset',
        // Validation has its own timeout inside typeProperties (not policy)
        timeout:  'typeProperties.timeout',
        sleep:    'typeProperties.sleep',
        childItems: {
            jsonPath:    'typeProperties.childItems',
            serializeAs: 'validationChildItems', // converts "ignore"/string booleans
        },
    },
    GetMetadata: {
        dataset:               'typeProperties.dataset',
        fieldList:             'typeProperties.fieldList',
        modifiedDatetimeStart: 'typeProperties.storeSettings.modifiedDatetimeStart',
        modifiedDatetimeEnd:   'typeProperties.storeSettings.modifiedDatetimeEnd',
        skipLineCount:         'typeProperties.storeSettings.skipLineCount',
    },
    SynapseNotebook: {
        notebook:      'typeProperties.notebook',
        parameters:    'typeProperties.parameters',
        sparkPool:     'typeProperties.sparkPool',
        executorSize:  'typeProperties.executorSize',
        driverSize:    'typeProperties.driverSize',
        // dynamicAllocation group → serialized into typeProperties.conf object
        dynamicAllocation: {
            jsonPath:    'typeProperties.conf[spark.dynamicAllocation.enabled]',
            serializeAs: 'synapseNotebookConf',
        },
        minExecutors: {
            jsonPath:    'typeProperties.conf[spark.dynamicAllocation.minExecutors]',
            serializeAs: 'synapseNotebookConf',
        },
        maxExecutors: {
            jsonPath:    'typeProperties.conf[spark.dynamicAllocation.maxExecutors]',
            serializeAs: 'synapseNotebookConf',
        },
        numExecutors: 'typeProperties.numExecutors',
    },
    SparkJob: {
        sparkJob:     'typeProperties.sparkJob',
        sparkPool:    'typeProperties.sparkPool',
        executorSize: 'typeProperties.executorSize',
        driverSize:   'typeProperties.driverSize',
        numExecutors: 'typeProperties.numExecutors',
    },
    Script: {
        // linkedServiceName is at activity root, not typeProperties
        linkedServiceName:           '@linkedServiceName',
        scripts:                     'typeProperties.scripts',
        scriptBlockExecutionTimeout: 'typeProperties.scriptBlockExecutionTimeout',
    },
    SqlServerStoredProcedure: {
        // linkedServiceName is at activity root
        linkedServiceName:          '@linkedServiceName',
        linkedServiceProperties:    { jsonPath: null, uiOnly: true }, // UI config, merged into linkedServiceName.parameters
        storedProcedureName:        'typeProperties.storedProcedureName',
        storedProcedureParameters:  'typeProperties.storedProcedureParameters',
    },
    WebActivity: {
        url:    'typeProperties.url',
        method: 'typeProperties.method',
        body:   'typeProperties.body',
        // Authentication fields are serialized together into typeProperties.authentication
        authenticationType: {
            jsonPath:    'typeProperties.authentication.type',
            serializeAs: 'webAuthentication',
        },
        username:                   { jsonPath: 'typeProperties.authentication.username',                serializeAs: 'webAuthentication' },
        password:                   { jsonPath: 'typeProperties.authentication.password',                serializeAs: 'webAuthentication' },
        resource:                   { jsonPath: 'typeProperties.authentication.resource',                serializeAs: 'webAuthentication' },
        pfx:                        { jsonPath: 'typeProperties.authentication.pfx',                     serializeAs: 'webAuthentication' },
        pfxPassword:                { jsonPath: 'typeProperties.authentication.password',                serializeAs: 'webAuthentication' },
        servicePrincipalAuthMethod: { jsonPath: null, uiOnly: true },
        tenant:                     { jsonPath: 'typeProperties.authentication.tenant',                  serializeAs: 'webAuthentication' },
        servicePrincipalId:         { jsonPath: 'typeProperties.authentication.servicePrincipalId',      serializeAs: 'webAuthentication' },
        servicePrincipalCredentialType: { jsonPath: null, uiOnly: true },
        servicePrincipalKey:        { jsonPath: 'typeProperties.authentication.servicePrincipalKey',     serializeAs: 'webAuthentication' },
        servicePrincipalCert:       { jsonPath: 'typeProperties.authentication.servicePrincipalCert',    serializeAs: 'webAuthentication' },
        servicePrincipalResource:   { jsonPath: 'typeProperties.authentication.resource',                serializeAs: 'webAuthentication' },
        credential:                 { jsonPath: 'typeProperties.authentication.credential',              serializeAs: 'webAuthentication' },
        credentialResource:         { jsonPath: 'typeProperties.authentication.resource',                serializeAs: 'webAuthentication' },
        credentialUserAssigned:     { jsonPath: 'typeProperties.authentication.credential',              serializeAs: 'webAuthentication' },
        headers: 'typeProperties.headers',
        // advancedProperties
        httpRequestTimeout:      'typeProperties.httpRequestTimeout',
        disableAsyncPattern:     'typeProperties.disableAsyncPattern',
        disableCertValidation:   'typeProperties.disableCertValidation',
        datasets:                'typeProperties.datasets',
        linkedServices:          'typeProperties.linkedServices',
    },
    WebHook: {
        url:                'typeProperties.url',
        method:             'typeProperties.method',
        headers:            'typeProperties.headers',
        body:               'typeProperties.body',
        timeout:            'typeProperties.timeout',  // typeProperties for WebHook, not policy
        disableCertValidation:   'typeProperties.disableCertValidation',
        reportStatusOnCallBack:  'typeProperties.reportStatusOnCallBack',
        // Same auth fields as WebActivity
        authenticationType: { jsonPath: 'typeProperties.authentication.type',                    serializeAs: 'webAuthentication' },
        username:           { jsonPath: 'typeProperties.authentication.username',                serializeAs: 'webAuthentication' },
        password:           { jsonPath: 'typeProperties.authentication.password',                serializeAs: 'webAuthentication' },
        resource:           { jsonPath: 'typeProperties.authentication.resource',                serializeAs: 'webAuthentication' },
        pfx:                { jsonPath: 'typeProperties.authentication.pfx',                     serializeAs: 'webAuthentication' },
        pfxPassword:        { jsonPath: 'typeProperties.authentication.password',                serializeAs: 'webAuthentication' },
        servicePrincipalAuthMethod: { jsonPath: null, uiOnly: true },
        tenant:             { jsonPath: 'typeProperties.authentication.tenant',                  serializeAs: 'webAuthentication' },
        servicePrincipalId: { jsonPath: 'typeProperties.authentication.servicePrincipalId',      serializeAs: 'webAuthentication' },
        servicePrincipalCredentialType: { jsonPath: null, uiOnly: true },
        servicePrincipalKey:  { jsonPath: 'typeProperties.authentication.servicePrincipalKey',   serializeAs: 'webAuthentication' },
        servicePrincipalCert: { jsonPath: 'typeProperties.authentication.servicePrincipalCert',  serializeAs: 'webAuthentication' },
        servicePrincipalResource: { jsonPath: 'typeProperties.authentication.resource',          serializeAs: 'webAuthentication' },
        credential:           { jsonPath: 'typeProperties.authentication.credential',            serializeAs: 'webAuthentication' },
        credentialResource:   { jsonPath: 'typeProperties.authentication.resource',              serializeAs: 'webAuthentication' },
        credentialUserAssigned: { jsonPath: 'typeProperties.authentication.credential',          serializeAs: 'webAuthentication' },
    },
    // Copy — source/sink datasets go to inputs[]/outputs[] at activity level;
    // typeProperties fields are serialised via copy-activity-config.json
    Copy: {
        sourceDataset:             { jsonPath: '@inputs[0].referenceName', serializeAs: 'copyDatasetRef' },
        sinkDataset:               { jsonPath: '@outputs[0].referenceName', serializeAs: 'copyDatasetRef' },
        dataIntegrationUnits:      'typeProperties.dataIntegrationUnits',
        parallelCopies:            'typeProperties.parallelCopies',
        enableSkipIncompatibleRow: 'typeProperties.enableSkipIncompatibleRow',
    },
};

// ─── Apply ────────────────────────────────────────────────────────────────────
function applyToFields(fields, typeJp, commonJp) {
    for (const [key, def] of Object.entries(fields)) {
        // Common properties
        if (commonJp && key in commonJp) {
            def.jsonPath = commonJp[key];
            continue;
        }
        // Type-specific
        if (typeJp && key in typeJp) {
            const jp = typeJp[key];
            if (typeof jp === 'string') {
                def.jsonPath = jp;
            } else {
                if ('jsonPath'    in jp) def.jsonPath    = jp.jsonPath;
                if ('type'        in jp) def.type        = jp.type;
                if ('serializeAs' in jp) def.serializeAs = jp.serializeAs;
                if ('uiOnly'      in jp) def.uiOnly      = jp.uiOnly;
            }
        }
        // Fields not in either mapping are left unchanged (may already have jsonPath)
    }
}

let updated = 0;
for (const [actType, actSchema] of Object.entries(schema)) {
    const typeJp = TYPE_JP[actType] || {};

    if (actSchema.commonProperties) {
        applyToFields(actSchema.commonProperties, null, COMMON_JP);
    }
    if (actSchema.typeProperties) {
        applyToFields(actSchema.typeProperties, typeJp, null);
        updated++;
    }
    if (actSchema.sourceProperties) {
        applyToFields(actSchema.sourceProperties, typeJp, null);
    }
    if (actSchema.sinkProperties) {
        applyToFields(actSchema.sinkProperties, typeJp, null);
    }
    if (actSchema.advancedProperties) {
        applyToFields(actSchema.advancedProperties, typeJp, null);
    }
}

fs.writeFileSync(schemaPath, JSON.stringify(schema, null, 2));
console.log(`Done. Annotated ${updated} activity typeProperties blocks.`);
console.log('Transformers needed: synapseNotebookConf, setVariableReturnValues, webAuthentication, validationChildItems, switchCases');
