# Pipeline Editor Refactoring Plan

## Progress

| Step | Status | Description |
|------|--------|-------------|
| Step 1 | ✅ Done | V2 scaffolding — `pipelineEditorV2.js`, `media/pipelineEditorV2.html/css/js`, extension commands, context menu, canvas/drag-drop/pan/zoom, read-only properties panel |
| Step 2 | ✅ Done | Added `jsonPath` (and `serializeAs`, `uiOnly`, container `type`) to every field in `activity-schemas-v2.json`. Transformers needed in Step 3: `synapseNotebookConf`, `setVariableReturnValues`, `webAuthentication`, `validationChildItems`, `switchCases` |
| Step 3 | ✅ Done | Created `src/activityEngine/engine.js` — schema-driven serialize/deserialize/validate. Wired load/save path in extension host. |
| Step 4 | ✅ Done | Editable + saving for Wait, Fail, SetVariable (incl. pipeline return value kv), AppendVariable. Blocking validation on save. |
| Step 5 | ✅ Done | ExecutePipeline (pipeline-ref select), Filter (expression fields). Pipeline variable select for SetVariable/AppendVariable. |
| Step 6 | ✅ Done | Containers: ForEach, Until, IfCondition, Switch — editable expression/settings fields, Activities tab shows nested activity list (inner canvas editing deferred). |
| Step 6.5 | ✅ Done | Unit tests (Jest) for `engine.js` — 48 tests covering deserialize/serialize round-trips for all types done so far (Steps 1–6), `setVariableReturnValues` transformer (incl. Array/Boolean), `validateActivity` required-field checks. Run with `npm run test:engine`. Nested activity round-trips (ForEach→Until→ForEach) deferred to Step 11.5. |
| Step 7 | ✅ Done | Lookup, Delete, Validation, GetMetadata — dataset `<select>` from datasetList, GetMetadata field list checkboxes, Delete source path options, Source tab rendering. |
| Step 8 | 🔲 | SynapseNotebook, SparkJob |
| Step 9 | 🔲 | Script, SqlServerStoredProcedure |
| Step 10 | 🔲 | WebActivity, WebHook — auth transformer |
| Step 11 | 🔲 | Copy — last; uses copy-activity-config.json |
| Step 11.5 | 🔲 | Container inner canvas editing — ForEach, Until, IfCondition, Switch render their nested activities on a sub-canvas. Enables nested activity round-trip tests from Step 6.5. |
| Step 12 | 🔲 | V2 becomes default, V1 removed |

---

## Approach: Build a Parallel V2 Editor, Not a Refactor

Instead of modifying the existing editor, build a second webview provider (`PipelineEditorV2`) that lives alongside V1. The existing editor stays untouched and continues to work. Activities are migrated to V2 one at a time. Once all activities are covered and verified, V2 becomes the default and V1 is removed.

**Why this is safer than refactoring in-place:**
- Zero risk of breaking V1 during migration
- Each activity can be independently implemented, tested, and shipped
- V1 and V2 can be run side-by-side on the same pipeline file to compare output
- If a V2 activity has a bug, only that activity's schema/handler is in scope

**Rough file structure for V2:**

```
src/
  pipelineEditorV2.js          ← PipelineEditorV2Provider (extension host, ~200 lines)
  activityEngine/
    engine.js                  ← validate / serialize / deserialize, all schema-driven
    fieldRenderers.js          ← one small function per field type
    transformers.js            ← ~5 named transforms for unusual serialization cases
media/
  pipelineEditorV2.html        ← real HTML file, not a template string
  pipelineEditorV2.css
  pipelineEditorV2.js          ← webview script, real .js with IntelliSense and linting
```

The canvas, drag-and-drop, connection drawing, and panel lifecycle from V1 are copied verbatim — those are not the source of bugs.

---

## Why V1 Keeps Breaking

The same per-activity `if (a.type === 'X')` chain exists in **five separate places** in V1:

| Location | What it does | Approx. lines |
|---|---|---|
| `validateActivities()` | checks required fields per type | ~530 |
| `savePipelineToWorkspace()` | serialises each type to JSON | ~440 |
| `generateFormField()` | renders form UI per type | ~2,590 |
| `buildNestedActivityTypeProperties()` | handles container activities | ~450 |
| `loadPipelineFromJson()` | deserialises each type from JSON | ~825 |

Fix a field in one place, and the other four are still wrong — silently. That is the root cause.

The nested activity bug is a specific case: top-level and nested activities go through *different* code paths. The V2 engine uses a single recursive `processActivityList()` that does not care about depth, so the same fix applies everywhere automatically.

---

## How V2 Works: Config-Driven Engine

V2 applies the same approach already working in the dataset editor (`dataset-config.json` + `datasetUtils.js`) to activities.

The one thing currently missing from `activity-schemas.json` is `jsonPath` — the dot-notation path where each field's value lives in the output JSON. Adding that unlocks the full engine:

```
render(activity)      → schema lookup → iterate fields → fieldRenderer[field.type](key, value)
validate(activity)    → schema lookup → iterate required fields → collect errors
serialize(activity)   → schema lookup → iterate fields → write value to jsonPath
deserialize(rawJson)  → schema lookup → iterate fields → read value from jsonPath
```

No `if/else` per activity type. The only bespoke code lives in the JSON schema.

**Example — SynapseNotebook:**

```json
"SynapseNotebook": {
  "typeProperties": {
    "notebook":   { "type": "text",  "label": "Notebook", "required": true,
                    "jsonPath": "typeProperties.notebook" },
    "dynamicAllocation": {
      "type": "radio", "label": "Dynamically allocate executors",
      "jsonPath": "typeProperties.conf[spark.dynamicAllocation.enabled]",
      "serializeAs": "boolFromEnabledDisabled"
    }
  }
}
```

Fields needing non-trivial transformation get a `serializeAs` tag pointing to a named transformer. There are roughly 5 such transformers needed across all 21 activity types — everything else is a direct value write.

**Container activities (ForEach, Until, IfCondition, Switch):**

```json
"ForEach": {
  "typeProperties": {
    "items":        { "type": "expression",        "jsonPath": "typeProperties.items", "required": true },
    "isSequential": { "type": "boolean",           "jsonPath": "typeProperties.isSequential" },
    "activities":   { "type": "containerActivities","jsonPath": "typeProperties.activities" }
  }
}
```

`type: "containerActivities"` tells the engine to recursively call `serializeActivityList()` on that field. This is what fixes the nesting bug at every depth with no special-case code.

---

## Can Schemas Be Inferred Without Manual Checking?

**Mostly yes — V1 code is the source of truth, and unit tests are the automated gate.**

For each activity type, everything needed for the V2 schema is already in V1:

| Schema property | Where to read it in V1 |
|---|---|
| Field keys, labels, types, conditionals | Already in `activity-schemas.json` |
| `jsonPath` for each field | `savePipelineToWorkspace()` — each `if (a.type === 'X')` block shows exactly where values are written |
| Validation rules | `validateActivities()` — one block per type, describes required fields and format rules |
| Deserialisation | `loadPipelineFromJson()` — the inverse of serialisation |
| Auth fields (WebActivity, WebHook) | Currently 200 lines of copy-paste each; extracted into a shared `webAuth` schema fragment |

**The automated gate — one test per activity:**

> Load a known-good pipeline JSON from `/pipeline/` → deserialise with V2 engine → re-serialise → assert output matches the reference file.

The files in `/pipeline/` were saved by V1 and are correct. If V2 can round-trip them without diff, the schema is right — no manual field-by-field inspection needed from you.

**Three activities that need a human look:**
- **Copy** — source/sink mapping already in `copy-activity-config.json`; review that config, not the code
- **Script** — nested `scripts[]` array where each script has its own `parameters[]`
- **SetVariable (pipeline return value)** — `returnValues` → `pipelineReturnValue` is an unusual transformation

All other 18 activity types follow a straightforward field-to-jsonPath pattern and can be done with no manual review.

---

## Migration Order (suggested)

Simple ones first to validate the engine works, complex ones later:

1. `Wait`, `Fail` — trivial, 1–2 fields each
2. `SetVariable`, `AppendVariable` — simple, already well-schema'd
3. `ExecutePipeline`, `Filter` — simple references
4. `ForEach`, `Until`, `IfCondition`, `Switch` — containers; this validates the recursive nesting fix
5. `Lookup`, `Delete`, `Validation`, `GetMetadata` — dataset-referencing activities
6. `SynapseNotebook`, `SparkJob` — Synapse-specific
7. `Script`, `SqlServerStoredProcedure` — linked-service activities
8. `WebActivity`, `WebHook` — auth complexity, but already well-defined in the schema
9. `Copy` — last; most complex, but `copy-activity-config.json` already does most of the work

---

## What Does NOT Change

- Canvas rendering, drag-and-drop, zoom/pan — copied verbatim from V1
- `PipelineEditorProvider` class structure (panel map, dirty state, dispose handling)
- `copy-activity-config.json` — already config-driven, just referenced by V2 engine
- `dataset-config.json`, `datasetUtils.js` — unchanged
- `esbuild.js` build pipeline — no framework change needed
