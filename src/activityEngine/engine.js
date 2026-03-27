'use strict';
// engine.js — schema-driven serialize / deserialize / validate for the V2 pipeline editor.
//
// Design principle: NO if/else chains per activity type.
// All behaviour is driven by activity-schemas-v2.json.
//
// Three operations mirror each other through the same schema:
//   deserializeActivity  raw ADF JSON → flat canvas object
//   serializeActivity    flat canvas object → ADF JSON ready to write to file
//   validateActivity     flat canvas object → array of error strings
//
// Complex field mappings (5 transformers) handle the few places where
// the field layout on disk differs from the flat canvas representation.

const allSchemas = require('../activity-schemas-v2.json');

// ─── Public API ───────────────────────────────────────────────────────────────
module.exports = {
	isActivityTypeSupported,
	deserializeActivity,
	deserializeActivityList,
	serializeActivity,
	serializeActivityList,
	serializePipeline,
	validateActivity,
	validateActivityList,
};

// ─── Supported types ──────────────────────────────────────────────────────────
// All types with complete jsonPath coverage except Copy, which piggybacks on
// copy-activity-config.json and will be wired in a later step.
const SUPPORTED_TYPES = new Set([
	'Wait', 'Fail',
	'AppendVariable', 'SetVariable',
	'Filter', 'ExecutePipeline',
	'ForEach', 'Until', 'IfCondition', 'Switch',
	'Lookup', 'Delete', 'Validation', 'GetMetadata',
	'SynapseNotebook', 'SparkJob',
	'Script', 'SqlServerStoredProcedure',
	'WebActivity', 'WebHook',
	// 'Copy' — added in a later step
]);

function isActivityTypeSupported(type) {
	return SUPPORTED_TYPES.has(type);
}

// ─── Path utilities ───────────────────────────────────────────────────────────
// Supported path formats in jsonPath fields:
//   "@field"                               → obj[field]           (activity root)
//   "policy.field"                         → obj.policy.field
//   "typeProperties.field"                 → obj.typeProperties.field
//   "typeProperties.conf[spark.key.name]"  → obj.typeProperties.conf['spark.key.name']

function parsePath(path) {
	const result = [];
	let i = 0, current = '';
	while (i < path.length) {
		const c = path[i];
		if (c === '[') {
			if (current) { result.push(current); current = ''; }
			const end = path.indexOf(']', i);
			if (end === -1) break;
			result.push(path.slice(i + 1, end));
			i = end + 1;
			if (path[i] === '.') i++;
		} else if (c === '.') {
			if (current) { result.push(current); current = ''; }
			i++;
		} else {
			current += c; i++;
		}
	}
	if (current) result.push(current);
	return result;
}

function getByPath(obj, path) {
	if (!path || obj == null) return undefined;
	if (path.startsWith('@')) return obj[path.slice(1)];
	let cur = obj;
	for (const seg of parsePath(path)) {
		if (cur == null) return undefined;
		cur = cur[seg];
	}
	return cur;
}

function setByPath(obj, path, value) {
	if (!path || value === undefined) return;
	if (path.startsWith('@')) { obj[path.slice(1)] = value; return; }
	const segs = parsePath(path);
	let cur = obj;
	for (let i = 0; i < segs.length - 1; i++) {
		if (cur[segs[i]] == null) cur[segs[i]] = {};
		cur = cur[segs[i]];
	}
	cur[segs[segs.length - 1]] = value;
}

// ─── Deserialize ──────────────────────────────────────────────────────────────
// Takes one raw ADF JSON activity object, returns a flat canvas-friendly object.
// Container nested activities are preserved as raw arrays (editing them is a later step).

function deserializeActivity(raw) {
	const schema = allSchemas[raw.type];
	const flat = { id: Date.now() + Math.random(), type: raw.type };

	if (!schema) {
		// Unknown type — best-effort flatten for display
		flat.name = raw.name || '';
		flat.description = raw.description || '';
		if (raw.typeProperties) Object.assign(flat, raw.typeProperties);
		flat.dependsOn = raw.dependsOn || [];
		flat.userProperties = raw.userProperties || [];
		return flat;
	}

	const FIELD_GROUPS = ['commonProperties', 'typeProperties', 'sourceProperties', 'sinkProperties', 'advancedProperties'];

	// Read each schema field from its jsonPath in the raw object
	for (const group of FIELD_GROUPS) {
		const fields = schema[group];
		if (!fields) continue;
		for (const [key, def] of Object.entries(fields)) {
			// uiOnly fields have no JSON source
			if (def.uiOnly || !def.jsonPath) continue;
			// Container arrays are handled separately below
			if (def.type === 'containerActivities' || def.type === 'switchCases') continue;
			// Fields with serializeAs transformers are read via deserialize transformer, not direct path
			// EXCEPTION: the jsonPath on these fields IS the correct read location, so we still read here.
			// The deserialize transformer then overwrites/adjusts only where needed.
			const value = getByPath(raw, def.jsonPath);
			if (value !== undefined) flat[key] = value;
		}
	}

	// Run deserialize transformers for fields needing adjustment after direct path reads
	runDeserializeTransformers(raw, schema, flat);

	// Default state to Activated when not written in JSON (e.g. TestVariables-style pipelines)
	if (!flat.state) flat.state = 'Activated';
	// Map Inactive → Deactivated so UI radio shows "Deactivated" label
	if (flat.state === 'Inactive') flat.state = 'Deactivated';

	// Container children — preserved as raw JSON arrays until per-container editing is implemented
	for (const group of ['typeProperties']) {
		const fields = schema[group];
		if (!fields) continue;
		for (const [key, def] of Object.entries(fields)) {
			if (def.type === 'containerActivities') {
				flat[key] = getByPath(raw, def.jsonPath) || [];
			} else if (def.type === 'switchCases') {
				flat[key] = getByPath(raw, def.jsonPath) || [];
				flat.defaultActivities = getByPath(raw, 'typeProperties.defaultActivities') || [];
			}
		}
	}

	flat.dependsOn = raw.dependsOn || [];
	flat.userProperties = raw.userProperties || [];
	return flat;
}

function deserializeActivityList(arr) {
	return (arr || []).map(raw => deserializeActivity(raw));
}

// ─── Serialize ────────────────────────────────────────────────────────────────
// Takes a flat canvas activity object, returns a complete ADF JSON activity object.

function serializeActivity(flat) {
	const schema = allSchemas[flat.type];
	if (!schema) return null; // unsupported type — cannot serialize

	const output = { name: flat.name, type: flat.type };

	if (flat.description)       output.description      = flat.description;
	// Normalize state: translate UI "Deactivated" → JSON "Inactive" and emit onInactiveMarkAs
	const isInactive = flat.state === 'Inactive' || flat.state === 'Deactivated';
	if (isInactive) {
		output.state            = 'Inactive';
		output.onInactiveMarkAs = flat.onInactiveMarkAs || 'Succeeded';
	}
	// "Activated" (default) — omit state entirely for cleaner JSON
	output.dependsOn      = flat.dependsOn      || [];
	output.userProperties = flat.userProperties || [];

	const FIELD_GROUPS = ['commonProperties', 'typeProperties', 'sourceProperties', 'sinkProperties', 'advancedProperties'];

	for (const group of FIELD_GROUPS) {
		const fields = schema[group];
		if (!fields) continue;
		for (const [key, def] of Object.entries(fields)) {
			if (def.uiOnly || !def.jsonPath) continue;
			// Transformer fields: skip here, transformer writes the correct structure
			if (def.serializeAs) continue;
			// Container arrays: handled below
			if (def.type === 'containerActivities' || def.type === 'switchCases') continue;
			// Skip fields whose conditional is not met — but ONLY when the conditional field is
			// explicitly set on the flat object. If it's undefined (engine has no webview context),
			// write the field; the transformer will clean up stale wrapper objects if needed.
			if (def.conditional) {
				const condVal = flat[def.conditional.field];
				if (condVal !== undefined && !isConditionMet(def.conditional, flat)) continue;
			}

			let value = flat[key];
			// Strip empty-key entries from KV-type objects before writing
			if ((def.type === 'keyvalue' || def.type === 'storedprocedure-parameters') && value && typeof value === 'object') {
				value = Object.fromEntries(Object.entries(value).filter(([k]) => k.trim() !== ''));
				if (Object.keys(value).length === 0) continue;
			}
			if (value !== undefined && value !== null && value !== '') {
				// Omit boolean fields marked omitWhenFalse when their value is false
				if (def.omitWhenFalse && value === false) continue;
				setByPath(output, def.jsonPath, value);
			}
		}
	}

	// Container children — write back as raw arrays (preserved from deserialize)
	for (const group of ['typeProperties']) {
		const fields = schema[group];
		if (!fields) continue;
		for (const [key, def] of Object.entries(fields)) {
			if (def.type === 'containerActivities') {
				// Preserve raw array as-is (nested editing is a future step)
				setByPath(output, def.jsonPath, flat[key] || []);
			} else if (def.type === 'switchCases') {
				setByPath(output, def.jsonPath, flat[key] || []);
				if (flat.defaultActivities?.length) {
					setByPath(output, 'typeProperties.defaultActivities', flat.defaultActivities);
				}
			}
		}
	}

	// Run serialize transformers last — they overwrite specific fields as needed
	runSerializeTransformers(flat, schema, output);

	// Trim empty wrapper objects produced by setByPath
	if (output.policy      && Object.keys(output.policy).length      === 0) delete output.policy;
	if (output.typeProperties && Object.keys(output.typeProperties).length === 0) delete output.typeProperties;

	return output;
}

function serializeActivityList(activities) {
	return (activities || []).map(a => serializeActivity(a)).filter(Boolean);
}

// ─── Pipeline-level serialize ─────────────────────────────────────────────────
// Builds the complete { name, properties: { activities, parameters, ... } } ADF JSON.
// connections: array of { from, to, condition } canvas connection objects.

function serializePipeline(pipelineData, activities, connections) {
	const withDeps = attachDependsOn(activities, connections);

	// Auto-populate pipeline variables from SetVariable / AppendVariable activities.
	// Only adds entries that don't already exist — never removes or overwrites.
	const pipelineVars = { ...(pipelineData.variables || {}) };
	for (const a of withDeps) {
		if (a.type === 'AppendVariable' && a.variableName && !(a.variableName in pipelineVars)) {
			pipelineVars[a.variableName] = { type: 'Array' };
		} else if (a.type === 'SetVariable' && a.variableName
				&& a.variableType !== 'Pipeline return value'
				&& !(a.variableName in pipelineVars)) {
			pipelineVars[a.variableName] = { type: a.pipelineVariableType || 'String' };
		}
	}

	return {
		name: pipelineData.name || 'pipeline1',
		properties: {
			activities: serializeActivityList(withDeps),
			...(hasKeys(pipelineData.parameters) ? { parameters: pipelineData.parameters } : {}),
			...(hasKeys(pipelineVars)             ? { variables:  pipelineVars             } : {}),
			...(pipelineData.description         ? { description: pipelineData.description } : {}),
			...(pipelineData.concurrency && pipelineData.concurrency !== 1
				? { concurrency: parseInt(pipelineData.concurrency) } : {}),
			annotations: Array.isArray(pipelineData.annotations) ? pipelineData.annotations : [],
			lastPublishTime: new Date().toISOString(),
		}
	};
}

function attachDependsOn(activities, connections) {
	return activities.map(a => ({
		...a,
		dependsOn: (connections || [])
			.filter(c => (c.to?.name ?? c.toName) === a.name)
			.map(c => ({
				activity: c.from?.name ?? c.fromName,
				dependencyConditions: [c.condition || 'Succeeded'],
			})),
	}));
}

function hasKeys(obj) {
	return obj && typeof obj === 'object' && Object.keys(obj).length > 0;
}

// ─── Validate ─────────────────────────────────────────────────────────────────
// Returns an array of human-readable error strings. Empty = valid.

function validateActivity(flat) {
	const schema = allSchemas[flat.type];
	if (!schema) {
		return [`Activity type "${flat.type}" is not yet supported in V2 editor`];
	}

	const errors = [];
	const FIELD_GROUPS = ['commonProperties', 'typeProperties', 'sourceProperties', 'sinkProperties', 'advancedProperties'];

	for (const group of FIELD_GROUPS) {
		const fields = schema[group];
		if (!fields) continue;
		for (const [key, def] of Object.entries(fields)) {
			// Check required fields
			if (def.required) {
				const condOk = !def.conditional || isConditionMet(def.conditional, flat);
				const nestedOk = !def.nestedConditional || isConditionMet(def.nestedConditional, flat);
				if (condOk && nestedOk) {
					const value = flat[key];
					const isEmpty = value === undefined || value === null || value === ''
						|| (Array.isArray(value) && value.length === 0);
					if (isEmpty) {
						errors.push(`"${def.label || key}" is required`);
					}
				}
			}
			// Check KV-type fields for empty parameter names (regardless of required flag)
			if (def.type === 'keyvalue' || def.type === 'storedprocedure-parameters') {
				const value = flat[key];
				if (value && typeof value === 'object') {
					for (const k of Object.keys(value)) {
						if (!k || !k.trim()) {
							errors.push(`"${def.label || key}" has a parameter with an empty name — please fill it in or delete the row`);
							break;
						}
					}
				}
			}
			// Check script-array: each script must have non-empty text and valid parameters
			if (def.type === 'script-array') {
				const scripts = flat[key];
				if (Array.isArray(scripts)) {
					scripts.forEach((s, i) => {
						if (!s.text || !s.text.trim()) {
							errors.push(`Script ${i + 1}: text cannot be empty`);
						}
						// Validate parameters for each script
						if (Array.isArray(s.parameters)) {
							s.parameters.forEach((p, j) => {
								// Name is required
								// if (!p.name || !p.name.trim()) {
								// 	errors.push(`Script ${i + 1} parameter ${j + 1}: name is required`);
								// }
								// For Output/InputOutput and String/Byte[] types, Size is required
								const needsSize = (p.direction === 'Output' || p.direction === 'InputOutput') && (p.type === 'String' || p.type === 'Byte[]');
								if (needsSize && (p.size === undefined || p.size === null || p.size === '' || isNaN(p.size))) {
									errors.push(`Script ${i + 1} parameter ${j + 1}: Size is required for Output/InputOutput String or Byte[]`);
								}
							});
						}
					});
				}
			}
			// Check web-headers: each header must have a non-empty name and value; no duplicate names
			if (def.type === 'web-headers') {
				const headers = flat[key];
				if (Array.isArray(headers)) {
					const seen = new Set();
					headers.forEach((h, i) => {
						if (!h.name || !String(h.name).trim()) {
							errors.push(`Header ${i + 1}: name is required`);
						} else if (!h.value && h.value !== 0) {
							errors.push(`Header ${i + 1} ("${h.name}"): value is required`);
						} else {
							const lower = String(h.name).trim().toLowerCase();
							if (seen.has(lower)) {
								errors.push(`Duplicate header name: "${h.name}"`);
							} else {
								seen.add(lower);
							}
						}
					});
				}
			}
		}
	}
	return errors;
}

// Returns { activityName: [errors] } for all activities that have errors.
function validateActivityList(activities) {
	const allErrors = {};
	// Check for duplicate names
	const nameCount = {};
	for (const a of (activities || [])) if (a.name) nameCount[a.name] = (nameCount[a.name] || 0) + 1;
	for (const [name, count] of Object.entries(nameCount)) {
		if (count > 1) allErrors[name] = [`Duplicate activity name "${name}" — each activity must have a unique name`];
	}
	for (const a of (activities || [])) {
		const errs = validateActivity(a);
		if (errs.length) allErrors[a.name || String(a.id)] = errs;
		// Recurse into container children only when they are already deserialized (flat format).
		// Raw ADF JSON arrays still have typeProperties — skip those to avoid false validation errors.
		for (const key of ['activities', 'ifTrueActivities', 'ifFalseActivities', 'defaultActivities']) {
			if (Array.isArray(a[key]) && a[key].length > 0 && a[key][0].type && !a[key][0].typeProperties) {
				Object.assign(allErrors, validateActivityList(a[key]));
			}
		}
		for (const c of (a.cases || [])) {
			if (Array.isArray(c.activities)) Object.assign(allErrors, validateActivityList(c.activities));
		}
	}
	return allErrors;
}

function isConditionMet(conditional, flat) {
	const val = flat[conditional.field];
	return Array.isArray(conditional.value)
		? conditional.value.includes(val)
		: val === conditional.value;
}

// ─── Transformer registry ─────────────────────────────────────────────────────
// Each transformer is a pair: { serialize(flat, output), deserialize(raw, flat) }
// serialize: called after the main field loop — writes complex structures
// deserialize: called after the main field reads — adjusts values for the canvas

function getUsedTransformerNames(schema) {
	const used = new Set();
	const FIELD_GROUPS = ['commonProperties', 'typeProperties', 'sourceProperties', 'sinkProperties', 'advancedProperties'];
	for (const group of FIELD_GROUPS) {
		const fields = schema[group];
		if (!fields) continue;
		for (const def of Object.values(fields)) {
			if (def.serializeAs) used.add(def.serializeAs);
		}
	}
	return used;
}

function runSerializeTransformers(flat, schema, output) {
	for (const name of getUsedTransformerNames(schema)) {
		TRANSFORMERS[name]?.serialize(flat, output);
	}
}

function runDeserializeTransformers(raw, schema, flat) {
	for (const name of getUsedTransformerNames(schema)) {
		TRANSFORMERS[name]?.deserialize(raw, flat);
	}
}

// ─── Transformers ─────────────────────────────────────────────────────────────
const TRANSFORMERS = {

	// ── 1. SynapseNotebook conf object ────────────────────────────────────────
	// Fields: dynamicAllocation, minExecutors, maxExecutors, numExecutors
	// Disk: typeProperties.conf['spark.dynamicAllocation.*']
	synapseNotebookConf: {
		serialize(flat, output) {
			if (!output.typeProperties) output.typeProperties = {};
			const tp = output.typeProperties;
			tp.snapshot = true;

			// Mirror driverSize from executorSize
			if (flat.executorSize) tp.driverSize = flat.executorSize;

			// Wrap notebook name string into reference object
			if (flat.notebook != null && flat.notebook !== '') {
				tp.notebook = { referenceName: flat.notebook, type: 'NotebookReference' };
			}

			// Wrap sparkPool name string into reference object
			if (flat.sparkPool != null && flat.sparkPool !== '') {
				tp.sparkPool = { referenceName: flat.sparkPool, type: 'BigDataPoolReference' };
			}

			// Build dynamicAllocation conf block
			if (flat.dynamicAllocation === undefined) return;

			const enabled = flat.dynamicAllocation === 'Enabled';
			tp.conf = tp.conf || {};
			tp.conf['spark.dynamicAllocation.enabled'] = enabled;

			if (enabled) {
				if (flat.minExecutors != null && flat.minExecutors !== '')
					tp.conf['spark.dynamicAllocation.minExecutors'] = parseInt(flat.minExecutors);
				if (flat.maxExecutors != null && flat.maxExecutors !== '')
					tp.conf['spark.dynamicAllocation.maxExecutors'] = parseInt(flat.maxExecutors);
			} else {
				if (flat.numExecutors != null && flat.numExecutors !== '') {
					const n = parseInt(flat.numExecutors);
					tp.conf['spark.dynamicAllocation.minExecutors'] = n;
					tp.conf['spark.dynamicAllocation.maxExecutors'] = n;
					tp.numExecutors = n;
				}
			}
		},
		deserialize(raw, flat) {
			const tp = raw.typeProperties;
			if (!tp) return;

			// Unwrap notebook reference object → plain string
			if (tp.notebook && typeof tp.notebook === 'object') {
				const ref = tp.notebook.referenceName;
				flat.notebook = (ref && typeof ref === 'object' && ref.value)
					? ref.value
					: (typeof ref === 'string' ? ref : '');
			}

			// Unwrap sparkPool reference object → plain string
			if (tp.sparkPool && typeof tp.sparkPool === 'object') {
				const ref = tp.sparkPool.referenceName;
				flat.sparkPool = (ref && typeof ref === 'object' && ref.value)
					? ref.value
					: (typeof ref === 'string' ? ref : '');
			}

			// Unpack conf block → dynamicAllocation fields
			const conf = tp.conf;
			if (!conf) return;
			const enabled = conf['spark.dynamicAllocation.enabled'];
			if (enabled !== undefined) flat.dynamicAllocation = enabled ? 'Enabled' : 'Disabled';
			if (conf['spark.dynamicAllocation.minExecutors'] !== undefined)
				flat.minExecutors = conf['spark.dynamicAllocation.minExecutors'];
			if (conf['spark.dynamicAllocation.maxExecutors'] !== undefined)
				flat.maxExecutors = conf['spark.dynamicAllocation.maxExecutors'];
		},
	},

	// ── 1b. SparkJob reference objects ────────────────────────────────────────
	// Fields: sparkJob, sparkPool
	// Disk: typeProperties.sparkJob = { referenceName, type: "SparkJobDefinitionReference" }
	//       typeProperties.sparkPool = { referenceName, type: "BigDataPoolReference" }
	sparkJobRef: {
		serialize(flat, output) {
			if (!output.typeProperties) output.typeProperties = {};
			const tp = output.typeProperties;

			if (flat.sparkJob != null && flat.sparkJob !== '') {
				tp.sparkJob = { referenceName: flat.sparkJob, type: 'SparkJobDefinitionReference' };
			}
			if (flat.sparkPool != null && flat.sparkPool !== '') {
				tp.sparkPool = { referenceName: flat.sparkPool, type: 'BigDataPoolReference' };
			}
		},
		deserialize(raw, flat) {
			const tp = raw.typeProperties;
			if (!tp) return;

			if (tp.sparkJob && typeof tp.sparkJob === 'object') {
				const ref = tp.sparkJob.referenceName;
				flat.sparkJob = typeof ref === 'string' ? ref : (ref?.value || '');
			}
			if (tp.sparkPool && typeof tp.sparkPool === 'object') {
				const ref = tp.sparkPool.referenceName;
				flat.sparkPool = typeof ref === 'string' ? ref : (ref?.value || '');
			}
		},
	},

	// ── 2. SetVariable pipeline return value ──────────────────────────────────
	// Disk format when returnValue: variableName="pipelineReturnValue", setSystemVariable=true, value=[{key,value:{type,content}}]
	setVariableReturnValues: {
		serialize(flat, output) {
			if (flat.variableType !== 'Pipeline return value') return;
			if (!output.typeProperties) output.typeProperties = {};
			delete output.typeProperties.variableName;
			delete output.typeProperties.value;
			output.typeProperties.variableName = 'pipelineReturnValue';
			output.typeProperties.setSystemVariable = true;
			const arr = [];
			for (const [k, item] of Object.entries(flat.returnValues || {})) {
				const v = { key: k, value: { type: item.type } };
				if (item.type !== 'Null') {
					if (item.type === 'Int' || item.type === 'Float') {
						v.value.content = parseFloat(item.value) || 0;
					} else if (item.type === 'Boolean') {
						v.value.content = item.value === 'true' || item.value === true;
					} else {
						v.value.content = item.value || '';
					}
				}
				arr.push(v);
			}
			output.typeProperties.value = arr;
		},
		deserialize(raw, flat) {
			const tp = raw.typeProperties;
			if (!tp) return;
			if (tp.setSystemVariable && tp.variableName === 'pipelineReturnValue') {
				flat.variableType = 'Pipeline return value';
				flat.variableName = undefined;
				const rv = {};
				for (const item of (tp.value || [])) {
					rv[item.key] = { type: item.value?.type, value: item.value?.content };
				}
				flat.returnValues = rv;
			} else {
				flat.variableType = 'Pipeline variable';
			}
		},
	},

	// ── 3. Web auth (WebActivity + WebHook) ───────────────────────────────────
	// Disk: typeProperties.authentication.{type, username, password, ...}
	webAuthentication: {
		serialize(flat, output) {
			if (!flat.authenticationType || flat.authenticationType === 'None') return;
			if (!output.typeProperties) output.typeProperties = {};
			switch (flat.authenticationType) {
				case 'Basic': {
					const auth = { type: 'Basic' };
					if (flat.username) auth.username = flat.username;
					if (flat.password) auth.password = flat.password;
					output.typeProperties.authentication = auth;
					break;
				}
				case 'MSI': {
					const auth = { type: 'MSI' };
					if (flat.resource) auth.resource = flat.resource;
					output.typeProperties.authentication = auth;
					break;
				}
				case 'UserAssignedManagedIdentity': {
					const auth = { type: 'UserAssignedManagedIdentity' };
					if (flat.resource) auth.resource = flat.resource;
					if (flat.credentialUserAssigned) auth.credential = { referenceName: flat.credentialUserAssigned, type: 'CredentialReference' };
					output.typeProperties.authentication = auth;
					break;
				}
				case 'ClientCertificate': {
					const auth = { type: 'ClientCertificate' };
					if (flat.pfx)         auth.pfx      = flat.pfx;
					if (flat.pfxPassword) auth.password = flat.pfxPassword;
					output.typeProperties.authentication = auth;
					break;
				}
				case 'ServicePrincipal': {
					if (flat.servicePrincipalAuthMethod === 'Credential') {
						// Credential method: no `type` field in JSON
						const auth = {};
						if (flat.credential)         auth.credential = { referenceName: flat.credential, type: 'CredentialReference' };
						if (flat.credentialResource) auth.resource   = flat.credentialResource;
						output.typeProperties.authentication = auth;
					} else {
						// Inline method: uses userTenant and username in JSON
						const auth = { type: 'ServicePrincipal' };
						if (flat.tenant)             auth.userTenant  = flat.tenant;
						if (flat.servicePrincipalId) auth.username    = flat.servicePrincipalId;
						const useKey = flat.servicePrincipalCredentialType !== 'Service Principal Certificate';
						if (useKey && flat.servicePrincipalKey)   auth.password = flat.servicePrincipalKey;
						if (!useKey && flat.servicePrincipalCert) auth.pfx      = flat.servicePrincipalCert;
						if (flat.servicePrincipalResource) auth.resource = flat.servicePrincipalResource;
						output.typeProperties.authentication = auth;
					}
					break;
				}
			}
		},
		deserialize(raw, flat) {
			const auth = raw.typeProperties?.authentication;
			if (!auth) { flat.authenticationType = 'None'; return; }

			// ServicePrincipal + Credential method: no `type` field in JSON
			if (!auth.type && auth.credential) {
				flat.authenticationType = 'ServicePrincipal';
				flat.servicePrincipalAuthMethod = 'Credential';
				flat.credential = auth.credential?.referenceName ?? auth.credential;
				if (auth.resource) flat.credentialResource = auth.resource;
				return;
			}

			flat.authenticationType = auth.type || 'None';
			switch (auth.type) {
				case 'Basic':
					if (auth.username) flat.username = auth.username;
					if (auth.password) flat.password = auth.password;
					break;
				case 'MSI':
					if (auth.resource) flat.resource = auth.resource;
					break;
				case 'UserAssignedManagedIdentity':
					if (auth.resource) flat.resource = auth.resource;
					if (auth.credential) flat.credentialUserAssigned = auth.credential?.referenceName ?? auth.credential;
					break;
				case 'ClientCertificate':
					if (auth.pfx)      flat.pfx         = auth.pfx;
					if (auth.password) flat.pfxPassword  = auth.password;
					break;
				case 'ServicePrincipal':
					// Inline method: userTenant + username in JSON
					flat.servicePrincipalAuthMethod = 'Inline';
					if (auth.userTenant)  flat.tenant             = auth.userTenant;
					if (auth.username)    flat.servicePrincipalId = auth.username;
					if (auth.resource)    flat.servicePrincipalResource = auth.resource;
					if (auth.password) {
						flat.servicePrincipalKey          = auth.password;
						flat.servicePrincipalCredentialType = 'Service Principal Key';
					} else if (auth.pfx) {
						flat.servicePrincipalCert           = auth.pfx;
						flat.servicePrincipalCredentialType = 'Service Principal Certificate';
					} else {
						flat.servicePrincipalCredentialType = 'Service Principal Key';
					}
					break;
			}
		},
	},

	// ── 3b. Web headers ───────────────────────────────────────────────────────
	// Disk format: { "HeaderName": "HeaderValue", ... }
	// Flat format: [ { name, value }, ... ]
	webHeaders: {
		serialize(flat, output) {
			const headers = flat.headers;
			if (!Array.isArray(headers) || headers.length === 0) return;
			if (!output.typeProperties) output.typeProperties = {};
			const obj = {};
			for (const h of headers) {
				if (h.name) obj[h.name] = h.value ?? '';
			}
			if (Object.keys(obj).length > 0) output.typeProperties.headers = obj;
		},
		deserialize(raw, flat) {
			const h = raw.typeProperties?.headers;
			if (!h) { flat.headers = []; return; }
			// Already an array (shouldn't happen in real ADF JSON, but handle gracefully)
			if (Array.isArray(h)) { flat.headers = h; return; }
			flat.headers = Object.entries(h).map(([name, value]) => ({ name, value: value ?? '' }));
		},
	},

	// ── 4. Validation childItems ──────────────────────────────────────────────
	// "ignore" (UI default) → omit from JSON; string "true"/"false" → boolean
	validationChildItems: {
		serialize(flat, output) {
			if (!output.typeProperties) output.typeProperties = {};
			if (flat.childItems === undefined || flat.childItems === 'ignore') {
				delete output.typeProperties.childItems;
			} else {
				output.typeProperties.childItems = flat.childItems === 'true' || flat.childItems === true;
			}
		},
		deserialize(raw, flat) {
			const v = raw.typeProperties?.childItems;
			flat.childItems = v === undefined ? 'ignore' : String(v);
		},
	},

	// ── 5. Lookup source block ────────────────────────────────────────────────
	// Builds typeProperties.source from flat fields based on dataset category.
	// SQL:     source.type, source.sqlReaderQuery / sqlReaderStoredProcedureName, queryTimeout
	// Storage: source.type, source.storeSettings (type + path fields), source.formatSettings
	// The direct jsonPath writes on the schema fields already put individual values into
	// source.storeSettings / source.formatSettings — this transformer adds the required
	// type discriminators and cleans up cross-category leftovers.
	lookupSource: {
		serialize(flat, output) {
			if (!output.typeProperties) output.typeProperties = {};
			const tp = output.typeProperties;
			const meta = allSchemas.__meta || {};
			const dsTypeToFormat = meta.datasetTypeToFormatSettings || {};
			const locTypeToStore = meta.locationTypeToStoreSettings || {};

			const cat = flat._datasetCategory || '';
			const dsType = flat._datasetType || '';
			const storeType = flat._storeSettingsType || '';
			const formatType = dsTypeToFormat[dsType] || '';

			if (cat === 'sql') {
				// SQL source: ensure source.type and remove any storage leftovers
				if (!tp.source) tp.source = {};
				tp.source.type = dsType === 'AzureSqlDWTable' ? 'SqlDWSource' : 'AzureSqlSource';
				delete tp.source.storeSettings;
				delete tp.source.formatSettings;
			} else if (cat === 'storage') {
				// Storage source: ensure source.type and inject storeSettings.type / formatSettings.type
				if (!tp.source) tp.source = {};
				tp.source.type = dsType ? dsType + 'Source' : tp.source.type;
			// Remove SQL-only fields that should never appear on a storage source
			delete tp.source.sqlReaderQuery;
			delete tp.source.sqlReaderStoredProcedureName;
			delete tp.source.queryTimeout;
			delete tp.source.isolationLevel;
			delete tp.source.partitionOption;
			delete tp.source.partitionSettings;
			delete tp.source.storedProcedureParameters;
			if (storeType) {
				const epd = flat.enablePartitionDiscovery === true;
				tp.source.storeSettings = { type: storeType, enablePartitionDiscovery: epd, ...(tp.source.storeSettings || {}) };
				if (!epd) delete tp.source.storeSettings.partitionRootPath;
				}
				if (formatType) {
					tp.source.formatSettings = { type: formatType, ...(tp.source.formatSettings || {}) };
				} else {
					delete tp.source.formatSettings;
				}
			}
		},
		deserialize(raw, flat) {
			const tp = raw.typeProperties;
			if (!tp?.source) return;
			const src = tp.source;
			// Infer _datasetCategory (and _datasetType for storage) from source.type
			const sqlSourceTypes = new Set(['AzureSqlSource', 'SqlDWSource', 'SqlServerSource', 'AzureSqlMISource']);
			if (sqlSourceTypes.has(src.type)) {
				if (!flat._datasetCategory) flat._datasetCategory = 'sql';
			} else if (src.storeSettings) {
				if (!flat._datasetCategory) flat._datasetCategory = 'storage';
				if (!flat._datasetType && src.type?.endsWith('Source')) {
					flat._datasetType = src.type.slice(0, -6);
				}
			}
			// Infer storeSettings/formatSettings types
			if (src.storeSettings?.type) flat._storeSettingsType = src.storeSettings.type;
			if (src.formatSettings?.type) flat._formatSettingsType = src.formatSettings.type;
			// Infer filePathType from whichever path field is populated
			const ss = src.storeSettings || {};
			if (ss.prefix !== undefined)                flat.filePathType = 'prefix';
			else if (ss.wildcardFileName !== undefined) flat.filePathType = 'wildcardFilePath';
			else if (ss.fileListPath !== undefined)     flat.filePathType = 'listOfFiles';
			else                                        flat.filePathType = 'filePathInDataset';
			// Infer useQuery for SQL sources
			if (src.sqlReaderStoredProcedureName)       flat.useQuery = 'StoredProcedure';
			else if (src.sqlReaderQuery !== undefined)  flat.useQuery = 'Query';
			else                                        flat.useQuery = 'Table';
			// Unwrap legacy Expression objects → plain string for sqlReaderQuery
			if (flat.sqlReaderQuery && typeof flat.sqlReaderQuery === 'object' && 'value' in flat.sqlReaderQuery) {
				flat.sqlReaderQuery = flat.sqlReaderQuery.value ?? '';
			}
		},
	},

	// ── 6. Delete storeSettings type ─────────────────────────────────────────
	// Writes storeSettings.type and enablePartitionDiscovery from _storeSettingsType.
	// Does NOT touch formatSettings (Delete has no format layer).
	deleteStoreSettings: {
		serialize(flat, output) {
			if (!output.typeProperties) output.typeProperties = {};
			const tp = output.typeProperties;
			const storeType = flat._storeSettingsType || '';
			if (storeType) {
				tp.storeSettings = { type: storeType, enablePartitionDiscovery: false, ...(tp.storeSettings || {}) };
			}
			// enableLogging is always written as false at the typeProperties level
			tp.enableLogging = false;
		},
		deserialize(raw, flat) {
			const tp = raw.typeProperties;
			if (!tp) return;
			if (tp.storeSettings?.type) flat._storeSettingsType = tp.storeSettings.type;
			// Infer filePathType from whichever storeSettings field is populated
			const ss = tp.storeSettings || {};
			if (ss.prefix !== undefined)          flat.filePathType = 'prefix';
			else if (ss.wildcardFileName !== undefined) flat.filePathType = 'wildcardFilePath';
			else if (ss.fileListPath !== undefined)     flat.filePathType = 'listOfFiles';
			else                                        flat.filePathType = 'filePathInDataset';
		},
	},

	// ── 6. GetMetadata storeSettings / formatSettings types ─────────────────────
	// Writes storeSettings.type (e.g. AzureBlobFSReadSettings) and enablePartitionDiscovery,
	// and formatSettings.type (e.g. DelimitedTextReadSettings), from webview-computed hints
	// _storeSettingsType and _formatSettingsType stored on the flat activity.
	getMetadataStoreSettings: {
		serialize(flat, output) {
			if (!output.typeProperties) output.typeProperties = {};
			const tp = output.typeProperties;
			const meta = allSchemas.__meta || {};
			// Resolve types from config maps if not already set by webview
			const locTypeToStore = meta.locationTypeToStoreSettings || {};
			const dsTypeToFormat = meta.datasetTypeToFormatSettings || {};
			const storeType = flat._storeSettingsType || '';
			const formatType = flat._formatSettingsType || '';
			if (storeType) {
				tp.storeSettings = { type: storeType, enablePartitionDiscovery: false, ...(tp.storeSettings || {}) };
			} else {
				// No storage dataset selected — drop any leftover storeSettings
				delete tp.storeSettings;
			}
			if (formatType) {
				tp.formatSettings = { type: formatType, ...(tp.formatSettings || {}) };
			} else {
				// No format type — drop any leftover formatSettings
				delete tp.formatSettings;
			}
		},
		deserialize(raw, flat) {
			const tp = raw.typeProperties;
			if (!tp) return;
			if (tp.storeSettings?.type) flat._storeSettingsType = tp.storeSettings.type;
			if (tp.formatSettings?.type) flat._formatSettingsType = tp.formatSettings.type;
			// Backward compat: migrate skipLineCount from storeSettings to flat (formatSettings path fixed)
			if (tp.storeSettings?.skipLineCount !== undefined && tp.formatSettings?.skipLineCount === undefined) {
				flat.skipLineCount = tp.storeSettings.skipLineCount;
			}
		},
	},

	// ── 6. Copy dataset references ────────────────────────────────────────────
	// sourceDataset → activity.inputs[0].referenceName
	// sinkDataset   → activity.outputs[0].referenceName
	copyDatasetRef: {
		serialize(flat, output) {
			if (flat.sourceDataset) output.inputs  = [{ referenceName: flat.sourceDataset, type: 'DatasetReference' }];
			if (flat.sinkDataset)   output.outputs = [{ referenceName: flat.sinkDataset,   type: 'DatasetReference' }];
		},
		deserialize(raw, flat) {
			if (raw.inputs?.[0]?.referenceName)  flat.sourceDataset = raw.inputs[0].referenceName;
			if (raw.outputs?.[0]?.referenceName) flat.sinkDataset   = raw.outputs[0].referenceName;
		},
	},
};
