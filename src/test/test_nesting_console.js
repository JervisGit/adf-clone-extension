// ============================================================
// PASTE THIS ENTIRE SCRIPT INTO THE WEBVIEW DEVTOOLS CONSOLE
// Help -> Toggle Developer Tools, select the webview JS context
// in the dropdown (not the main window), paste and press Enter.
//
// Builds: ForEach -> IfCondition -> every non-container activity
// in the False branch, then logs the final JSON for ADF/Synapse.
// ============================================================

// Step 1: raw activity objects (mirrors what the UI stores in memory)
const _rawActivities = [

    { name: "Filter1", type: "Filter",
      items: "@pipeline().parameters.myList", condition: "@greater(item(), 5)",
      dependsOn: [], userProperties: [] },

    { name: "Lookup1", type: "Lookup",
      dataset: "ADLSAvro1", _datasetType: "Avro", firstRowOnly: true,
      filePathType: "prefix", prefix: "myPrefix/",
      modifiedDatetimeStart: "2026-01-01T00:00:00", recursive: true,
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },

    { name: "GetMetadata1", type: "GetMetadata",
      dataset: "ADLSAvro1", fieldList: ["itemName", "size", "lastModified"],
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },

    { name: "Delete1", type: "Delete",
      dataset: "ADLSAvro1", recursive: true,
      filePathType: "wildcardFilePath", wildcardFileName: "*.avro",
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },

    { name: "SetVariable1", type: "SetVariable",
      variableName: "myVar", value: "@string(42)",
      dependsOn: [], userProperties: [] },

    { name: "AppendVariable1", type: "AppendVariable",
      variableName: "myListVar", value: "@pipeline().parameters.inputItem",
      dependsOn: [], userProperties: [] },

    { name: "ExecutePipeline1", type: "ExecutePipeline",
      pipeline: "SamplePipeline", waitOnCompletion: true,
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },

    { name: "Validation1", type: "Validation",
      dataset: "ADLSAvro1", timeout: "7.00:00:00", sleep: 10, minimumSize: 1024,
      dependsOn: [], userProperties: [] },

    { name: "Fail1", type: "Fail",
      message: "Pipeline failed at this step", errorCode: "ERR_001",
      dependsOn: [], userProperties: [] },

    { name: "Wait1", type: "Wait",
      waitTimeInSeconds: 30,
      dependsOn: [], userProperties: [] },

    { name: "WebActivity1", type: "WebActivity",
      url: "https://myapi.example.com/endpoint", method: "POST", body: '{"key":"value"}',
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },

    { name: "WebHook1", type: "WebHook",
      url: "https://myapi.example.com/webhook", method: "POST",
      body: '{"callbackUrl":"@{activity(WebHook1).output.callBackUri}"}',
      timeout: "10:00:00", dependsOn: [], userProperties: [] },

    { name: "Script1", type: "Script",
      linkedService: "AzureSqlDatabase1", scriptType: "NonQuery",
      scriptContent: "UPDATE dbo.myTable SET status = 1 WHERE id = 1",
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },

    { name: "StoredProc1", type: "SqlServerStoredProcedure",
      linkedService: "AzureSqlDatabase1",
      storedProcedureName: "dbo.usp_ProcessData",
      storedProcedureParameters: JSON.stringify({ param1: { value: "hello", type: "String" } }),
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },

    { name: "SynapseNotebook1", type: "SynapseNotebook",
      notebook: "SampleNotebook", sparkPool: "", executorSize: "Small", executorCount: 2,
      timeout: "0.12:00:00", retry: 0, retryIntervalInSeconds: 30,
      secureOutput: false, secureInput: false, dependsOn: [], userProperties: [] },
];

// Step 2: serialize each leaf via buildNestedActivityTypeProperties
const _leaves = _rawActivities.map(a => {
    const { typeProperties: tp, activityProps: ap } = window._debug.buildNestedActivityTypeProperties(a);
    const c = { name: a.name, type: a.type, dependsOn: a.dependsOn || [], userProperties: a.userProperties || [], typeProperties: tp };
    if (a.state) c.state = a.state;
    Object.assign(c, ap);
    return c;
});

// Step 3: serialize the IfCondition with all leaves in the false branch
const _ifRaw = {
    name: "IfCondition1", type: "IfCondition",
    expression: "@greater(length(item()), 0)",
    ifTrueActivities: [], ifFalseActivities: _leaves,
    dependsOn: [], userProperties: []
};
const { typeProperties: _ifTp } = window._debug.buildNestedActivityTypeProperties(_ifRaw);
const _ifSerialized = { name: "IfCondition1", type: "IfCondition", dependsOn: [], userProperties: [], typeProperties: _ifTp };

// Step 4: wrap in ForEach and run through buildPipelineDataForSave
const _result = window._debug.runTest([{
    name: "ForEach1", type: "ForEach",
    items: "@pipeline().parameters.myList",
    isSequential: false,
    activities: [_ifSerialized],
    dependsOn: [], userProperties: []
}]);

console.log("=== PIPELINE JSON (copy and import into ADF/Synapse) ===");
console.log(JSON.stringify({
    name: "pipeline1",
    properties: { activities: _result.activities, annotations: [], parameters: {}, variables: {} }
}, null, 2));
