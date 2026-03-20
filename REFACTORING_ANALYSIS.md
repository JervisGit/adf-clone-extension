# Pipeline Editor Refactoring Plan

## Approach: Build a Parallel V2 Editor, Not a Refactor

Instead of modifying the existing editor, build a second webview provider (`PipelineEditorV2`) that lives alongside the current one. The existing editor stays untouched and continues to work. Activities are migrated to V2 one at a time. Once all activities are covered and verified, V2 becomes the default and V1 is removed.

**Why this is safer:**
- Zero risk of breaking V1 during migration
- Each activity can be independently shipped and tested
- You can compare V1 and V2 output side-by-side on the same pipeline file
- If a migrated activity has a bug, only that activity's handler is in scope — not 11k lines

**Rough file structure for V2:**

```
src/
  pipelineEditorV2.js          ← new PipelineEditorV2Provider (extension host, ~200 lines)
  activityEngine/
    engine.js                  ← validate / serialize / deserialize driven by schema
    fieldRenderers.js          ← one renderer per field type
    activityHandlers.js        ← handlers for activities needing custom logic (Copy, SynapseNotebook)
media/
  pipelineEditorV2.html        ← actual HTML file (not a template string)
  pipelineEditorV2.css
  pipelineEditorV2.js          ← webview script, real .js with IntelliSense
```

The canvas, drag-and-drop, and connection drawing from V1 can be copied verbatim — those are not the problem.

---

## The Core Problem (why V1 keeps breaking)

`pipelineEditor.js` is 11,268 lines. The breakdown is roughly:

| Layer | Lines |
|---|---|
| Extension host (`PipelineEditorProvider` class) | ~800 |
| CSS (inside `getHtmlContent()`) | ~900 |
| HTML markup (inside `getHtmlContent()`) | ~150 |
| Webview JavaScript (inside `getHtmlContent()`) | ~9,400 |

84% of the file is a webview app embedded as a template string. This is what causes the silent bugs — no linting, no IntelliSense, no unit testing is possible on it.

On top of that, the same per-activity-type `if/else` chains exist in **five separate places**:

1. `validateActivities()` — ~530 lines
2. `savePipelineToWorkspace()` — serialisation logic
3. `generateFormField()` — field rendering, ~2,590 lines
4. `buildNestedActivityTypeProperties()` — ~450 lines
5. `loadPipelineFromJson()` — deserialisation

Fix one, and the other four are still wrong. That is why saving/displaying/validating keeps breaking.

---

## Q1: Will this fix nested/deeply nested activity bugs?

**Yes — and this is the most important structural fix.**

**Yes, and here is why.**

The nesting issue exists because there are *separate code paths* for top-level activities vs activities inside a ForEach, Until, IfCondition, or Switch. When a bug is fixed at the top level, the nested path is not updated.

But the underlying schema for an activity does not change based on nesting depth. A `SetVariable` inside a `ForEach` inside an `IfCondition` has exactly the same fields and JSON structure as a top-level `SetVariable`. The container (`ForEach`, `IfCondition`, etc.) simply adds an `activities[]` array field that holds standard activity objects.

If validation, rendering, and serialisation are driven by a schema lookup (`activitySchemas[activity.type]`) rather than by `if/else` chains, then:

- The same logic runs regardless of nesting level
- A recursive call (`processActivities(container.activities)`) handles any depth automatically
- Fixing a bug in `SetVariable`'s schema definition fixes it everywhere, at every nesting level

---

## Q2: Can we make it fully config-driven and infer schemas from the existing code?

**Yes on both counts.**

### What already exists to infer from

Every piece of information needed for the config already lives somewhere in V1. A code-reading pass can extract it mechanically:

| Needed | Where it lives in V1 |
|---|---|
| Field keys and labels | `activity-schemas.json` (`typeProperties`, `commonProperties`) |
| Field types (`text`, `select`, `keyvalue`, etc.) | Same — `type` on each field entry |
| Conditional display logic | Same — `conditional`, `conditionalAll`, `nestedConditional` |
| Validation rules | `validateActivities()` in pipelineEditor.js — one `if (a.type === 'X')` block per type |
| JSON output structure | `savePipelineToWorkspace()` — the `if (a.type === 'X')` blocks show exactly where each field lands |
| Deserialisation mapping | `loadPipelineFromJson()` — the reverse mapping |
| Auth field patterns | WebActivity/WebHook blocks (currently 200 lines of copy-paste each) — extractable into a shared `webAuth` schema fragment |

### What `jsonPath` buys you (the key missing piece)

`dataset-config.json` includes a `jsonPath` on every field: the exact dot-notation path where the value lives in the output JSON (e.g. `properties.typeProperties.location.container`). The dataset editor writes/reads using those paths with no type-specific code.

Adding `jsonPath` to `activity-schemas.json` is what makes the activity engine possible. Example:

```json
"SynapseNotebook": {
  "typeProperties": {
    "notebook": {
      "type": "text", "label": "Notebook", "required": true,
      "jsonPath": "typeProperties.notebook"
    },
    "dynamicAllocation": {
      "type": "radio", "label": "Dynamically allocate executors",
      "jsonPath": "typeProperties.conf.spark.dynamicAllocation.enabled",
      "serializeAs": "boolFromEnabledDisabled"
    }
  }
}
```

Fields that need special transformation (like `dynamicAllocation` → the `conf` object) get a `serializeAs` tag that maps to a named transformer. There are only ~5 distinct transformers needed across all activity types.

### Container activities (ForEach, Until, IfCondition, Switch)

These activity types have a `containerActivities` field in their schema — an array of standard activity objects. The engine handles nesting by recognising this field type and recursing:

```json
"ForEach": {
  "typeProperties": {
    "items":      { "type": "expression", "jsonPath": "typeProperties.items", "required": true },
    "isSequential": { "type": "boolean", "jsonPath": "typeProperties.isSequential" },
    "activities": { "type": "containerActivities", "jsonPath": "typeProperties.activities" }
  }
}
```

`type: "containerActivities"` tells the engine: serialise this field by recursively calling `serializeActivityList()` on it. No special `if (a.type === 'ForEach')` branch needed anywhere — it just works at any depth.

---

## Can schema inference be done without manual validation from you?

**Mostly yes — unit tests carry most of the burden.**

The approach per activity:

1. Read the V1 `savePipelineToWorkspace()` block for that type → extract `jsonPath` values for the schema
2. Read the V1 `validateActivities()` block → add `required` / `validateRule` to schema fields
3. Read the V1 `loadPipelineFromJson()` block → confirm round-trip is correct
4. Write a unit test: load a known good pipeline JSON → deserialise → re-serialise → assert output matches input

That last step is the automated gate. The source of truth is the existing pipeline JSON files in `/pipeline/` — those files are already correct (they were saved by V1). If V2 can round-trip those files without diff, the schema is right with no manual checking needed.

The only activities that need human review are:
- **Copy** — the source/sink config is already in `copy-activity-config.json` and is the most complex
- **Script** — nested `scripts[]` array with parameters inside each script
- **SetVariable with pipeline return value** — the `returnValues` → `pipelineReturnValue` transformation is unusual

Everything else (Wait, Fail, Filter, ForEach, Until, IfCondition, Switch, ExecutePipeline, Lookup, Delete, Validation, GetMetadata, SqlServerStoredProcedure, WebActivity, WebHook, SynapseNotebook, SparkJob) follows a straightforward field-to-jsonPath mapping.

---

**Yes. The foundation already exists.**

`activity-schemas.json` already defines fields with types, labels, `required`, `conditional`, and `options`. `dataset-config.json` goes further and includes `jsonPath` — the exact dot-path of where each field maps in the output JSON.

The dataset editor works because it is entirely driven by `dataset-config.json`:
- **Render**: iterate fields → call a renderer per `type` → done
- **Validate**: iterate fields where `required: true` → check value → done
- **Serialise**: iterate fields → write `value` to `jsonPath` → done
- **Deserialise**: iterate fields → read from `jsonPath` → done

No `if/else` per dataset type. Adding a new dataset type means adding a JSON block.

The same model can be applied to activities. What needs to be added to `activity-schemas.json`:

- `jsonPath` on each field (where it lands in the output `typeProperties`)
- `serializeAs` for fields that need transformation (e.g. `dynamicAllocation` → `conf` object)
- `validateRule` for fields that need format checking (e.g. timeout `HH:MM:SS`, range checks)
- `containerActivities` marker on fields like `activities`, `ifTrueActivities`, `ifFalseActivities` — tells the engine to recurse

Then the engine becomes:

```
renderActivity(type)   → look up schema → iterate fields → call fieldRenderer[field.type]
validateActivity(type) → look up schema → iterate required/validationRule fields → collect errors
serializeActivity(type)→ look up schema → iterate fields → write to jsonPath
deserializeActivity(raw) → look up schema → iterate fields → read from jsonPath
```

No `if/else` per type. The only bespoke code lives in the schema JSON.

---

## What the authentication duplication looks like currently

`WebActivity` and `WebHook` each have ~200 lines of copy-pasted auth validation
(Basic, MSI, ClientCertificate, ServicePrincipal, UserAssignedManagedIdentity).
With schema-driven validation and a shared `validateWebAuth` helper, this collapses to one place.

---

## Proposed Refactoring Steps

### Step 1 — Move webview files out of the template string (no logic changes)
Create `media/pipelineEditor.html`, `media/pipelineEditor.css`, `media/pipelineEditor.js`.  
`getHtmlContent()` reads the HTML file from disk and substitutes webview URIs.  
**Impact:** Unlocks linting, IntelliSense, and formatting on 9,400 lines. Low risk.

### Step 2 — Stop baking schema data into the template at generation time
Replace `${JSON.stringify(activitySchemas)}` inline bake with a `postMessage({ type: 'initSchemas', activitySchemas, ... })`.
Already done for datasets and linked services — same pattern.

### Step 3 — Extend `activity-schemas.json` with `jsonPath` and `validateRule`
No code change yet, just schema authoring. Validates the model before any refactor.

### Step 4 — Replace `validateActivities()` with a schema-driven loop
Single loop: `activitySchemas[a.type]` → check required fields → check validateRules.  
Recursive call handles nested activities. Shared auth/header helpers replace copy-paste.

### Step 5 — Replace `generateFormField()` with a field type renderer registry
Each `type` (`text`, `expression`, `dropdown`, `dataset`, `keyvalue`, etc.) gets one renderer function.  
`conditional` / `conditionalAll` logic extracted into a single `isFieldVisible(prop, activity)` helper.

### Step 6 — Replace `savePipelineToWorkspace()` serialisation with schema-driven `jsonPath` writes
Single loop per activity: iterate fields → write value to `jsonPath` in output object.  
`serializeAs` handles complex mappings (e.g. SynapseNotebook `conf` object).

### Step 7 — Replace `loadPipelineFromJson()` with schema-driven `jsonPath` reads
Mirror of Step 6. Same schema, opposite direction.

---

## What does NOT need to change

- The canvas rendering (`draw()`, drag/drop, connection drawing) — this is fine as-is
- The CSS — well-contained, not a source of bugs
- The `PipelineEditorProvider` class structure — panels map, dirty state, dispose handling are all correct
- The `esbuild.js` build pipeline — no framework change needed
