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
        // Check conditional — skip if the controlling field doesn't match
        if (fieldConfig.conditional) {
            const condVal = formData[fieldConfig.conditional.field];
            const condExpected = fieldConfig.conditional.value;
            const condMet = fieldConfig.conditional.notEmpty
                ? (condVal !== undefined && condVal !== null && condVal !== '')
                : (Array.isArray(condExpected) ? condExpected.includes(condVal) : condVal === condExpected);
            if (!condMet) continue;
        }
        // Check nestedConditional
        if (fieldConfig.nestedConditional) {
            const nCondVal = formData[fieldConfig.nestedConditional.field];
            const nCondExpected = fieldConfig.nestedConditional.value;
            const nCondMet = Array.isArray(nCondExpected) ? nCondExpected.includes(nCondVal) : nCondVal === nCondExpected;
            if (!nCondMet) continue;
        }
        // Check conditionalAll — all conditions must be met (AND logic)
        if (fieldConfig.conditionalAll) {
            const allMet = fieldConfig.conditionalAll.every(cond => {
                const condVal = formData[cond.field];
                return Array.isArray(cond.value) ? cond.value.includes(condVal) : condVal === cond.value;
            });
            if (!allMet) continue;
        }
        let value = formData[fieldKey];
        if (fieldConfig.omitWhenValue !== undefined && value === fieldConfig.omitWhenValue) continue;
        // For filterEmpty arrays (e.g. additional-columns), strip blank-name entries before writing
        if (fieldConfig.filterEmpty && Array.isArray(value)) {
            value = value.filter(item => item[fieldConfig.filterEmpty] && String(item[fieldConfig.filterEmpty]).trim() !== '');
        } else if (fieldConfig.filterEmpty && !Array.isArray(value)) {
            // Stale non-array value (e.g. from old plain-text save) — skip
            continue;
        }
        // Skip empty arrays and empty objects
        const isEmptyArr = Array.isArray(value) && value.length === 0;
        const isEmptyObj = value !== undefined && value !== null && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0;
        if (!isEmptyArr && !isEmptyObj && value !== undefined && value !== null && value !== '') {
            setValueByPath(source, fieldConfig.jsonPath, value);
        } else if (fieldConfig.writeDefault === true && fieldConfig.default !== undefined) {
            setValueByPath(source, fieldConfig.jsonPath, fieldConfig.default);
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
        // Check conditional — skip if the controlling field doesn't match
        if (fieldConfig.conditional) {
            const condVal = formData[fieldConfig.conditional.field];
            const condExpected = fieldConfig.conditional.value;
            const condMet = fieldConfig.conditional.notEmpty
                ? (condVal !== undefined && condVal !== null && condVal !== '')
                : (Array.isArray(condExpected) ? condExpected.includes(condVal) : condVal === condExpected);
            if (!condMet) continue;
        }
        // Check conditionalAll — all conditions must be met (AND logic)
        if (fieldConfig.conditionalAll) {
            const allMet = fieldConfig.conditionalAll.every(cond => {
                const condVal = formData[cond.field];
                return Array.isArray(cond.value) ? cond.value.includes(condVal) : condVal === cond.value;
            });
            if (!allMet) continue;
        }
        const value = formData[fieldKey];
        if (fieldConfig.omitWhenValue !== undefined && value === fieldConfig.omitWhenValue) continue;
        // For noEmpty arrays, filter out blank entries before writing
        let writeValue = (fieldConfig.noEmpty && Array.isArray(value))
            ? value.filter(s => typeof s === 'string' ? s.trim() !== '' : s !== null && s !== undefined)
            : value;
        // For filterEmpty object-arrays, filter and guard against non-array stale values
        if (fieldConfig.filterEmpty && Array.isArray(writeValue)) {
            writeValue = writeValue.filter(item => item[fieldConfig.filterEmpty] && String(item[fieldConfig.filterEmpty]).trim() !== '');
        } else if (fieldConfig.filterEmpty && !Array.isArray(writeValue)) {
            continue;
        }
        if (writeValue !== undefined && writeValue !== null && writeValue !== '') {
            setValueByPath(sink, fieldConfig.jsonPath, writeValue);
        } else if (fieldConfig.writeDefault === true && fieldConfig.default !== undefined) {
            setValueByPath(sink, fieldConfig.jsonPath, fieldConfig.default);
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
