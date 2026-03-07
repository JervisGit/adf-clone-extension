# Copy Activity – Test & Debug Matrix

## Dataset Types Implemented in Config

| Config Key | Display Name | As Source | As Sink | Storage Layer |
|---|---|---|---|---|
| `AzureSqlTable` | Azure SQL Database | ✅ | ✅ | — |
| `AzureSqlDW` | Azure Synapse Analytics | ✅ | ✅ | — |
| `Parquet` | Parquet | ✅ | ✅ | ADLS / Blob |
| `DelimitedText` | Delimited Text (CSV/TSV) | ✅ | ✅ | ADLS / Blob |
| `Json` | JSON | ✅ | ✅ | ADLS / Blob |
| `Avro` | Avro | ✅ | ✅ | ADLS / Blob |
| `Binary` | Binary | ✅ | ✅ | ADLS / Blob |
| `Orc` | ORC | ✅ | ✅ | ADLS / Blob |
| `Xml` | XML | ✅ | ❌ source-only | ADLS / Blob |
| `Iceberg` | Iceberg | ❌ sink-only | ✅ | ADLS Gen2 only |
| `HttpFile` | HTTP | ✅ | ❌ source-only | HTTP |

> **Azure Synapse Analytics** (`AzureSqlDW`) is implemented in the config but you don't currently have a Synapse-linked dataset in your workspace (`test-synapse-jervis-WorkspaceDefaultSqlServer` is linked service only). You would need a dataset pointing to it.
>
> **Iceberg** (`ADLSIceberg1`) — ADLS Gen2 sink-only. Fields: Max concurrent connections, Block size in MB.
>
> **HTTP** (`HttpFile`) — now implemented. Source-only; request body field only appears when method is POST.
>
> **Excel** — ADF supports Excel as a source-only dataset type for ADLS/Blob. It is not currently in the config and would need to be added as a future gap item. See Known Gaps below.

---

## Test Matrix

Legend: ✅ Tested & working | 🔲 Not yet tested | ⚠️ Known issue | ❌ Not applicable

Source and sink are **fully independent** — the fields rendered for each side depend only on the dataset type of that side, not the other. So each source type needs to be tested once as a source, and each sink type once as a sink. An N×M combination matrix adds no value.

### Sources

#### Database / Warehouse
| Source Dataset Type | Storage | Status |
|---|---|---|
| Azure SQL Database | — | ✅ |
| Azure Synapse Analytics | — | 🔲 |

#### ADLS Gen2
| Source Dataset Type | Status |
|---|---|
| Avro | ✅ |
| Binary | ✅ |
| Delimited Text (CSV/TSV) | ✅ |
| Excel | 🔲 |
| JSON | 🔲 |
| ORC | ✅ |
| Parquet | ✅ |
| XML | 🔲 |

#### Blob Storage
| Source Dataset Type | Status |
|---|---|
| Avro | ✅ |
| Binary | ✅ |
| Delimited Text (CSV/TSV) | ✅ |
| Excel | 🔲 |
| JSON | 🔲 |
| ORC | ✅ |
| Parquet | ✅ |
| XML | 🔲 |

#### Other
| Source Dataset Type | Status |
|---|---|
| HTTP | 🔲 |

### Sinks

#### Database / Warehouse
| Sink Dataset Type | Storage | Status |
|---|---|---|
| Azure SQL Database | — | ✅ |
| Azure Synapse Analytics | — | 🔲 |

#### ADLS Gen2
| Sink Dataset Type | Status |
|---|---|
| Avro | ✅ |
| Binary | ✅ |
| Delimited Text (CSV/TSV) | 🔲 |
| Iceberg | ✅ |
| JSON | 🔲 |
| ORC | ✅ |
| Parquet | ✅ |

#### Blob Storage
| Sink Dataset Type | Status |
|---|---|
| Avro | ✅ |
| Binary | ✅ |
| Delimited Text (CSV/TSV) | 🔲 |
| JSON | 🔲 |
| ORC | ✅ |
| Parquet | ✅ |

---

## What to Test Per Source Type

### Azure SQL Database (source) ✅ Validated
- [x] Use query = **Table** (no extra fields, no max concurrent connections)
- [x] Use query = **Query** → SQL textarea appears, populates/saves correctly
- [x] Use query = **Stored procedure** → SP name + SP parameters grid appear; Physical partitions and Dynamic range greyed out
- [x] Query timeout value writes to JSON by default
- [x] Isolation level dropdown saves correctly
- [x] Partition option = **None** / **Physical partitions** / **Dynamic range** (Table mode)
  - Dynamic range → partition column name + upper/lower bound fields appear
- [x] Additional columns: add row, set name + value ($$COLUMN: or custom), saves as array

### Azure Synapse Analytics (source)
- [ ] Same sub-cases as SQL DB above (same field structure)

### DelimitedText (ADLS + Blob source) ✅ Validated
- [x] File path type = **File path in dataset**
- [x] File path type = **Prefix** (prefix field appears, Blob only) — ADLS correctly omits Prefix option
- [x] File path type = **Wildcard file path** — wildcard folder + file name fields appear; wildcard file name written with `writeDefault`
- [x] File path type = **List of files** — file list path field appears; Recursive, Start time, End time hidden and omitted from JSON
- [x] Recursive hidden and omitted when List of files selected
- [x] Start time / End time hidden when List of files selected
- [x] Skip line count field
- [x] Additional columns — `$$FILEPATH` pre-populated on new rows; blank-name rows filtered
- [x] Max concurrent connections

### Parquet / Avro / ORC / JSON (file sources, ADLS or Blob)
- [ ] File path type = **File path in dataset** (no extra path fields)
- [ ] File path type = **Prefix** (prefix field appears) — *Blob only, not ADLS*
- [ ] File path type = **Wildcard file path** (wildcard folder + file fields appear)
- [ ] File path type = **List of files** (file list path field appears)
- [ ] Recursive toggle saves correctly
- [ ] Delete files after completion toggle
- [ ] Compression (codec + level) fields for applicable formats

### XML-specific (source)
- [ ] Namespace prefixes grid — load/add/remove/save
- [ ] Detection depth
- [ ] Validation XSD path

---

## What to Test Per Sink Type

### Azure SQL Database (sink)
- [ ] Write behaviour = **Insert** → table lock checkbox visible, upsert fields hidden
- [ ] Write behaviour = **Upsert**
  - [ ] Use tempdb = true → interim schema hidden
  - [ ] Use tempdb = false → Interim table schema field appears
  - [ ] Key columns (string-list): add / rename / remove / save round-trip
  - [ ] Empty key columns stripped on save ✅
- [ ] Write behaviour = **Stored procedure**
  - [ ] Table type + Table type parameter name (required) appear
  - [ ] Table lock hidden ✅
  - [ ] SP parameters grid: add / type / null / rename / value / remove
- [ ] Table option = **Use Existing** (default, omitted from JSON)
- [ ] Table option = **Auto Create** (written to JSON)
- [ ] Disable metrics collection toggle

### Azure Synapse Analytics (sink)
- [ ] Same write behaviour sub-cases as SQL DB above
- [ ] Additional Synapse-only fields (COPY command, PolyBase, etc.) — *check if config has these*

### Parquet (ADLS sink) ✅ Validated
- [x] `formatSettings: { type: "ParquetWriteSettings" }` written correctly
- [x] Copy behavior = **None** → omitted from JSON; other values written
- [x] Block size in MB — range validation 4–100 enforced on save
- [x] Max rows per file — empty stays empty (not defaulted to 0); written when set
- [x] File name prefix — only shown when Max rows per file is set; written when non-empty
- [x] Metadata — `additional-columns` style grid; saves as `storeSettings.metadata` array; blank-name rows filtered
- [x] Max concurrent connections

### Avro (ADLS sink) ✅ Validated
- [x] `formatSettings: { type: "AvroWriteSettings" }` written correctly
- [x] Copy behavior = **None** → omitted from JSON; other values written
- [x] Block size in MB — range validation 4–100 enforced on save
- [x] Max rows per file — empty stays empty; written when set
- [x] File name prefix — only shown when Max rows per file is set
- [x] Metadata — `additional-columns` style grid; saves as `storeSettings.metadata` array; blank-name rows filtered
- [x] Max concurrent connections

### ORC (ADLS sink) ✅ Validated
- [x] `formatSettings: { type: "OrcWriteSettings" }` written correctly
- [x] Copy behavior = **None** → omitted from JSON; other values written
- [x] Block size in MB — range validation 4–100 enforced on save
- [x] Max rows per file — empty stays empty; written when set
- [x] File name prefix — only shown when Max rows per file is set
- [x] Metadata — `additional-columns` style grid; saves as `storeSettings.metadata` array; blank-name rows filtered
- [x] Max concurrent connections

### Parquet / Avro / ORC / JSON (file sinks, ADLS or Blob)
- [ ] File path in dataset / wildcard / list of files
- [ ] Compression codec + level
- [ ] Max rows per file

### DelimitedText (sink)
- [ ] Quote all columns toggle
- [ ] File extension field
- [ ] Null value field

---

## Priority Testing Order

High value because they cover most real ETL patterns:

1. ~~**Avro (ADLS) → Azure SQL DB**~~ ← already tested
2. ~~**Azure SQL DB as source**~~ ← validated ✅
3. ~~**Azure SQL DB → Parquet (ADLS)**~~ ← Parquet sink validated ✅
4. ~~**Parquet (ADLS) → Parquet (ADLS)**~~ ← Parquet source validated ✅
5. ~~**Azure SQL DB → Avro (ADLS)**~~ ← Avro sink validated ✅
6. ~~**Azure SQL DB → ORC (ADLS)**~~ ← ORC sink validated ✅
7. ~~**DelimitedText (ADLS) → Azure SQL DB**~~ ← DelimitedText source validated ✅ (ADLS + Blob)
8. **➡️ NEXT: XML (ADLS) → Azure SQL DB** ← XML source (source-only type)
9. **Azure SQL DB → DelimitedText (ADLS)** ← validates DelimitedText sink
10. **Azure SQL DB → JSON (ADLS)** ← validates JSON sink
11. **Azure SQL DB → Synapse** ← requires Synapse dataset setup first
12. **Avro (Blob) → Azure SQL DB** ← tests Blob Avro source variant

---

## Known Gaps / Not Yet Implemented

| Gap | Notes |
|---|---|
| **Excel** dataset type | ADF supports Excel as a source-only format on ADLS/Blob. Not in the config. Needs a new entry similar to `Xml` (source-only, no storeSettings format write type). |
| `Iceberg` sink — sink-only | ADLS Gen2 only. Fields: Max concurrent connections, Block size. No source tab. |
| `HttpFile` source — request body | Only appears when method is POST. Verify conditional rendering works. |
| Azure Synapse Parquet/PolyBase | Synapse sink may have extra options beyond basic SQL (PolyBase, COPY command). |
| Mapping tab | Placeholder only — column mapping not yet implemented. |
| Source partition settings for Synapse | May differ from SQL DB partition fields |
