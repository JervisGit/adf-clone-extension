/**
 * Config-driven utility functions for the Copy Activity.
 * 
 * Field keys in the config use prefixes to avoid naming collisions when source fields
 * and sink fields are both stored flat on the same activity object:
 *   - Source fields: "src_<fieldname>"  (e.g. src_recursive, src_wildcardFolderPath)
 *   - Sink fields:   "snk_<fieldname>"  (e.g. snk_writeBehavior, snk_writeBatchSize)
 * 
 * jsonPath values are relative to the source or sink object respectively.
 */

const { setValueByPath, getValueByPath } = require('./datasetUtils');

/**
 * Build the source object for a Copy activity from flat form data.
 * @param {Object} formData      - Flat activity object (activity from webview) — field keys use src_ prefix
 * @param {Object} typeConfig    - Entry from copyActivityConfig.datasetTypes[datasetType]
 * @param {string} locationType  - ADF location type string (e.g. "AzureBlobFSLocation"), may be null
 * @param {Object} fallbackObj   - Original _sourceObject to read storeSettings.type from if locationType unknown
 * @returns {Object|null} Properly nested ADF source object, or null if no config
 */
function buildCopySource(formData, typeConfig, locationType, fallbackObj) {
    if (!typeConfig || !typeConfig.sourceTypeName) {
        return fallbackObj ? JSON.parse(JSON.stringify(fallbackObj)) : null;
    }

    const source = { type: typeConfig.sourceTypeName };

    if (typeConfig.hasStoreSettings) {
        // Determine the store-read settings type
        const readType = (locationType && typeConfig.storeReadSettingsTypes && typeConfig.storeReadSettingsTypes[locationType])
            || (fallbackObj && fallbackObj.storeSettings && fallbackObj.storeSettings.type)
            || typeConfig.defaultStoreReadSettings
            || 'AzureBlobFSReadSettings';

        // Include ADF standard defaults for storeSettings (Synapse always writes these)
        source.storeSettings = { type: readType, recursive: true, enablePartitionDiscovery: false };

        // Add format settings if required (e.g. DelimitedTextReadSettings)
        if (typeConfig.formatReadType) {
            source.formatSettings = { type: typeConfig.formatReadType };
        }
    }

    // Map form fields using jsonPath
    const sourceFields = (typeConfig.fields && typeConfig.fields.source) || {};
    for (const [fieldKey, fieldConfig] of Object.entries(sourceFields)) {
        if (!fieldConfig.jsonPath) continue;
        const value = formData[fieldKey];
        // Include all non-null/non-undefined/non-empty values (booleans like false pass through correctly)
        if (value !== undefined && value !== null && value !== '') {
            setValueByPath(source, fieldConfig.jsonPath, value);
        }
    }

    return source;
}

/**
 * Build the sink object for a Copy activity from flat form data.
 * @param {Object} formData      - Flat activity object — field keys use snk_ prefix
 * @param {Object} typeConfig    - Entry from copyActivityConfig.datasetTypes[datasetType]
 * @param {string} locationType  - ADF location type string (e.g. "AzureBlobFSLocation"), may be null
 * @param {Object} fallbackObj   - Original _sinkObject to read storeSettings.type from if locationType unknown
 * @returns {Object|null} Properly nested ADF sink object, or null if no config
 */
function buildCopySink(formData, typeConfig, locationType, fallbackObj) {
    if (!typeConfig || !typeConfig.sinkTypeName) {
        return fallbackObj ? JSON.parse(JSON.stringify(fallbackObj)) : null;
    }

    const sink = { type: typeConfig.sinkTypeName };

    // Apply per-type sink defaults (e.g. writeBehavior, sqlWriterUseTableLock for SQL sinks)
    if (typeConfig.sinkDefaults) {
        Object.assign(sink, typeConfig.sinkDefaults);
    }

    if (typeConfig.hasStoreSettings) {
        const writeType = (locationType && typeConfig.storeWriteSettingsTypes && typeConfig.storeWriteSettingsTypes[locationType])
            || (fallbackObj && fallbackObj.storeSettings && fallbackObj.storeSettings.type)
            || typeConfig.defaultStoreWriteSettings
            || 'AzureBlobFSWriteSettings';

        sink.storeSettings = { type: writeType };

        if (typeConfig.formatWriteType) {
            sink.formatSettings = { type: typeConfig.formatWriteType };
        }
    }

    const sinkFields = (typeConfig.fields && typeConfig.fields.sink) || {};
    for (const [fieldKey, fieldConfig] of Object.entries(sinkFields)) {
        if (!fieldConfig.jsonPath) continue;
        const value = formData[fieldKey];
        if (value !== undefined && value !== null && value !== '') {
            setValueByPath(sink, fieldConfig.jsonPath, value);
        }
    }

    return sink;
}

/**
 * Parse the source object from a loaded Copy activity into flat src_ form data.
 * @param {Object} sourceObj  - The ADF source object (typeProperties.source)
 * @param {Object} typeConfig - Entry from copyActivityConfig.datasetTypes[datasetType]
 * @returns {Object} Flat form data with src_ prefixed keys
 */
function parseCopySourceToForm(sourceObj, typeConfig) {
    if (!typeConfig || !sourceObj) return {};

    const formData = {};
    const sourceFields = (typeConfig.fields && typeConfig.fields.source) || {};

    for (const [fieldKey, fieldConfig] of Object.entries(sourceFields)) {
        if (!fieldConfig.jsonPath) continue;
        const value = getValueByPath(sourceObj, fieldConfig.jsonPath);
        if (value !== undefined) {
            formData[fieldKey] = value;
        } else if (fieldConfig.default !== undefined) {
            formData[fieldKey] = fieldConfig.default;
        }
    }

    return formData;
}

/**
 * Parse the sink object from a loaded Copy activity into flat snk_ form data.
 * @param {Object} sinkObj    - The ADF sink object (typeProperties.sink)
 * @param {Object} typeConfig - Entry from copyActivityConfig.datasetTypes[datasetType]
 * @returns {Object} Flat form data with snk_ prefixed keys
 */
function parseCopySinkToForm(sinkObj, typeConfig) {
    if (!typeConfig || !sinkObj) return {};

    const formData = {};
    const sinkFields = (typeConfig.fields && typeConfig.fields.sink) || {};

    for (const [fieldKey, fieldConfig] of Object.entries(sinkFields)) {
        if (!fieldConfig.jsonPath) continue;
        const value = getValueByPath(sinkObj, fieldConfig.jsonPath);
        if (value !== undefined) {
            formData[fieldKey] = value;
        } else if (fieldConfig.default !== undefined) {
            formData[fieldKey] = fieldConfig.default;
        }
    }

    return formData;
}

module.exports = {
    buildCopySource,
    buildCopySink,
    parseCopySourceToForm,
    parseCopySinkToForm
};
