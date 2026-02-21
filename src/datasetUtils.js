/**
 * Utility functions for config-driven dataset editor
 * These functions handle JSON path mapping and other common operations
 */

/**
 * Set a value in a nested object using dot notation path
 * @param {Object} obj - The target object
 * @param {string} path - Dot notation path (e.g., "properties.typeProperties.schema")
 * @param {any} value - The value to set
 * @returns {Object} The modified object
 * 
 * @example
 * const obj = {};
 * setValueByPath(obj, "properties.typeProperties.schema", "dbo");
 * // Result: { properties: { typeProperties: { schema: "dbo" } } }
 */
function setValueByPath(obj, path, value) {
    if (!path) return obj;
    
    const keys = path.split('.');
    let current = obj;
    
    // Navigate/create nested structure
    for (let i = 0; i < keys.length - 1; i++) {
        const key = keys[i];
        if (!current[key] || typeof current[key] !== 'object') {
            current[key] = {};
        }
        current = current[key];
    }
    
    // Set the final value
    const lastKey = keys[keys.length - 1];
    
    // Skip if value is undefined or empty string (unless it's explicitly allowed)
    if (value === undefined || value === null) {
        return obj;
    }
    
    current[lastKey] = value;
    return obj;
}

/**
 * Get a value from a nested object using dot notation path
 * @param {Object} obj - The source object
 * @param {string} path - Dot notation path
 * @returns {any} The value at the path, or undefined if not found
 * 
 * @example
 * const obj = { properties: { typeProperties: { schema: "dbo" } } };
 * getValueByPath(obj, "properties.typeProperties.schema"); // Returns "dbo"
 */
function getValueByPath(obj, path) {
    if (!obj || !path) return undefined;
    
    const keys = path.split('.');
    let current = obj;
    
    for (const key of keys) {
        if (current === undefined || current === null) {
            return undefined;
        }
        current = current[key];
    }
    
    return current;
}

/**
 * Build a complete dataset JSON from form data and configuration
 * @param {Object} formData - Key-value pairs from form inputs
 * @param {Object} datasetConfig - The dataset configuration
 * @param {string} datasetType - Selected dataset type key
 * @param {string} fileType - Selected file type key (optional)
 * @returns {Object} Complete dataset JSON structure
 */
function buildDatasetJson(formData, datasetConfig, datasetType, fileType = null) {
    const result = {
        name: formData.name || 'NewDataset',
        properties: {
            linkedServiceName: {
                referenceName: formData.linkedService || '',
                type: 'LinkedServiceReference'
            },
            annotations: [],
            type: '',  // Will be set based on file type or dataset type
            schema: []
        }
    };
    
    // Determine the type value
    const typeConfig = fileType
        ? datasetConfig.datasetTypes[datasetType]?.fileTypes[fileType]
        : datasetConfig.datasetTypes[datasetType];
    
    if (typeConfig) {
        result.properties.type = typeConfig.typeValue || fileType || datasetType;
    }
    
    // Get all field configurations
    const allFields = {};
    
    // Add common fields
    Object.assign(allFields, datasetConfig.commonFields || {});
    
    // Add dataset-specific fields
    if (fileType && datasetConfig.datasetTypes[datasetType]?.fileTypes[fileType]?.fields) {
        const fileTypeFields = datasetConfig.datasetTypes[datasetType].fileTypes[fileType].fields;
        // Flatten section structure
        for (const section of Object.values(fileTypeFields)) {
            Object.assign(allFields, section);
        }
    } else if (datasetConfig.datasetTypes[datasetType]?.fields) {
        const datasetFields = datasetConfig.datasetTypes[datasetType].fields;
        for (const section of Object.values(datasetFields)) {
            Object.assign(allFields, section);
        }
    }
    
    // Map form data to JSON using jsonPath
    for (const [fieldKey, fieldValue] of Object.entries(formData)) {
        const fieldConfig = allFields[fieldKey];
        
        if (!fieldConfig) continue;
        
        let valueToSet = fieldValue;
        
        // If this field key appears in formData, the frontend already decided it should be written
        // (fields with omitFromJson=true are excluded from formData by the webview before sending)
        let isExplicitOption = false;
        if (fieldConfig.options && Array.isArray(fieldConfig.options)) {
            const matchingOption = fieldConfig.options.find(opt => opt.value === fieldValue);
            if (matchingOption) {
                isExplicitOption = true;
            }
        }
        
        // Handle field type-specific transformations
        if (fieldConfig.type === 'number') {
            valueToSet = parseFloat(fieldValue);
            if (isNaN(valueToSet)) continue;
        } else if (fieldConfig.type === 'boolean') {
            valueToSet = Boolean(fieldValue);
        } else if (fieldConfig.type === 'hidden' && fieldConfig.value !== undefined) {
            // Use configured value for hidden fields
            valueToSet = fieldConfig.value;
        }
        
        // Apply default value if empty and default exists
        // BUT: Don't apply default if user explicitly selected an option (even if value is "")
        if ((valueToSet === '' || valueToSet === undefined) && fieldConfig.default !== undefined && !isExplicitOption) {
            valueToSet = fieldConfig.default;
        }
        
        // Skip empty optional fields (not required, no default, and not an explicit option selection)
        if (!fieldConfig.required && !isExplicitOption && (valueToSet === '' || valueToSet === undefined || valueToSet === null)) {
            continue;
        }
        
        // Set value using jsonPath
        if (fieldConfig.jsonPath) {
            setValueByPath(result, fieldConfig.jsonPath, valueToSet);
        }
    }
    
    // Add hidden fields that aren't in formData (e.g., location type)
    for (const [fieldKey, fieldConfig] of Object.entries(allFields)) {
        if (fieldConfig.type === 'hidden' && fieldConfig.value !== undefined && !formData.hasOwnProperty(fieldKey)) {
            if (fieldConfig.jsonPath) {
                setValueByPath(result, fieldConfig.jsonPath, fieldConfig.value);
            }
        }
    }
    
    return result;
}

/**
 * Extract form data from a dataset JSON for editing
 * @param {Object} datasetJson - The dataset JSON to parse
 * @param {Object} datasetConfig - The dataset configuration
 * @param {string} datasetType - Dataset type key
 * @param {string} fileType - File type key (optional)
 * @returns {Object} Form data object
 */
function parseDatasetJson(datasetJson, datasetConfig, datasetType, fileType = null) {
    const formData = {
        name: datasetJson.name || '',
        linkedService: datasetJson.properties?.linkedServiceName?.referenceName || ''
    };
    
    // Get all field configurations
    const allFields = {};
    
    // Add common fields
    Object.assign(allFields, datasetConfig.commonFields || {});
    
    // Add dataset-specific fields
    if (fileType && datasetConfig.datasetTypes[datasetType]?.fileTypes[fileType]?.fields) {
        const fileTypeFields = datasetConfig.datasetTypes[datasetType].fileTypes[fileType].fields;
        for (const section of Object.values(fileTypeFields)) {
            Object.assign(allFields, section);
        }
    } else if (datasetConfig.datasetTypes[datasetType]?.fields) {
        const datasetFields = datasetConfig.datasetTypes[datasetType].fields;
        for (const section of Object.values(datasetFields)) {
            Object.assign(allFields, section);
        }
    }
    
    // Extract values using jsonPath
    for (const [fieldKey, fieldConfig] of Object.entries(allFields)) {
        if (fieldConfig.jsonPath) {
            const value = getValueByPath(datasetJson, fieldConfig.jsonPath);
            if (value !== undefined) {
                formData[fieldKey] = value;
            } else if (fieldConfig.default !== undefined) {
                formData[fieldKey] = fieldConfig.default;
            }
        }
    }
    
    return formData;
}

/**
 * Validate form data against configuration
 * @param {Object} formData - Form data to validate
 * @param {Object} datasetConfig - Dataset configuration
 * @param {string} datasetType - Dataset type key
 * @param {string} fileType - File type key (optional)
 * @returns {Object} { valid: boolean, errors: string[] }
 */
function validateDatasetForm(formData, datasetConfig, datasetType, fileType = null) {
    const errors = [];
    
    // Get all field configurations
    const allFields = {};
    
    // Add common fields
    Object.assign(allFields, datasetConfig.commonFields || {});
    
    // Add dataset-specific fields
    if (fileType && datasetConfig.datasetTypes[datasetType]?.fileTypes[fileType]?.fields) {
        const fileTypeFields = datasetConfig.datasetTypes[datasetType].fileTypes[fileType].fields;
        for (const section of Object.values(fileTypeFields)) {
            Object.assign(allFields, section);
        }
    } else if (datasetConfig.datasetTypes[datasetType]?.fields) {
        const datasetFields = datasetConfig.datasetTypes[datasetType].fields;
        for (const section of Object.values(datasetFields)) {
            Object.assign(allFields, section);
        }
    }
    
    // Check required fields
    for (const [fieldKey, fieldConfig] of Object.entries(allFields)) {
        if (fieldConfig.required) {
            const value = formData[fieldKey];
            if (value === undefined || value === null || value === '') {
                errors.push(`${fieldConfig.label || fieldKey} is required`);
            }
        }
        
        // Type-specific validation
        if (formData[fieldKey] !== undefined && formData[fieldKey] !== '') {
            if (fieldConfig.type === 'number') {
                const num = parseFloat(formData[fieldKey]);
                if (isNaN(num)) {
                    errors.push(`${fieldConfig.label || fieldKey} must be a valid number`);
                } else {
                    if (fieldConfig.min !== undefined && num < fieldConfig.min) {
                        errors.push(`${fieldConfig.label || fieldKey} must be at least ${fieldConfig.min}`);
                    }
                    if (fieldConfig.max !== undefined && num > fieldConfig.max) {
                        errors.push(`${fieldConfig.label || fieldKey} must be at most ${fieldConfig.max}`);
                    }
                }
            }
        }
    }
    
    return {
        valid: errors.length === 0,
        errors: errors
    };
}

/**
 * Get the appropriate field configuration for a dataset type and file type combination
 * @param {Object} datasetConfig - Dataset configuration
 * @param {string} datasetType - Dataset type key
 * @param {string} fileType - File type key (optional)
 * @returns {Object} Field configuration object organized by sections
 */
function getFieldsConfig(datasetConfig, datasetType, fileType = null) {
    const config = datasetConfig.datasetTypes[datasetType];
    
    if (!config) {
        return {};
    }
    
    if (fileType && config.fileTypes && config.fileTypes[fileType]) {
        return config.fileTypes[fileType].fields || {};
    }
    
    return config.fields || {};
}

/**
 * Check if a field should be visible based on conditional logic
 * @param {Object} fieldConfig - Field configuration
 * @param {Object} formData - Current form data
 * @returns {boolean} Whether the field should be visible
 */
function isFieldVisible(fieldConfig, formData) {
    if (!fieldConfig.conditional) {
        return true;
    }
    
    const { field, value } = fieldConfig.conditional;
    const currentValue = formData[field];
    
    // Handle boolean values
    if (typeof value === 'boolean') {
        return Boolean(currentValue) === value;
    }
    
    // Handle string/other values
    return currentValue === value;
}

// Export for use in both Node.js and browser contexts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        setValueByPath,
        getValueByPath,
        buildDatasetJson,
        parseDatasetJson,
        validateDatasetForm,
        getFieldsConfig,
        isFieldVisible
    };
}
