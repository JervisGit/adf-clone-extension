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
| `ORC` | ORC | ✅ | ✅ | ADLS / Blob |
| `Xml` | XML | ✅ | ❌ source-only | ADLS / Blob |

> **Azure Synapse Analytics** (`AzureSqlDW`) is implemented in the config but you don't currently have a Synapse-linked dataset in your workspace (`test-synapse-jervis-WorkspaceDefaultSqlServer` is linked service only). You would need a dataset pointing to it.
>
> **Iceberg** (`ADLSIceberg1`) — not in the copy-activity-config. Will show no source/sink fields in the Copy editor.

---

## Test Matrix

Legend: ✅ Tested & working | 🔲 Not yet tested | ⚠️ Known issue | ❌ Not applicable

Source and sink are **fully independent** — the fields rendered for each side depend only on the dataset type of that side, not the other. So each source type needs to be tested once as a source, and each sink type once as a sink. An N×M combination matrix adds no value.

### Sources

| Source Dataset Type | Storage | Status |
|---|---|---|
| Azure SQL Database | — | 🔲 |
| Azure Synapse Analytics | — | 🔲 |
| Parquet | ADLS Gen2 | 🔲 |
| Parquet | Blob Storage | 🔲 |
| Delimited Text (CSV/TSV) | ADLS Gen2 | 🔲 |
| JSON | ADLS Gen2 | 🔲 |
| Avro | ADLS Gen2 | ✅ |
| Avro | Blob Storage | 🔲 |
| ORC | ADLS Gen2 | 🔲 |
| XML | ADLS Gen2 | 🔲 |

### Sinks

| Sink Dataset Type | Storage | Status |
|---|---|---|
| Azure SQL Database | — | ✅ |
| Azure Synapse Analytics | — | 🔲 |
| Parquet | ADLS Gen2 | 🔲 |
| Parquet | Blob Storage | 🔲 |
| Delimited Text (CSV/TSV) | ADLS Gen2 | 🔲 |
| JSON | ADLS Gen2 | 🔲 |
| Avro | ADLS Gen2 | 🔲 |
| Avro | Blob Storage | 🔲 |
| ORC | ADLS Gen2 | 🔲 |

---

## What to Test Per Source Type

### Azure SQL Database (source)
- [ ] Use query = **Table** (no extra fields)
- [ ] Use query = **Query** → SQL textarea appears, populates/saves correctly
- [ ] Use query = **Stored procedure** → SP name field appears
- [ ] Query timeout value saves correctly
- [ ] Isolation level dropdown saves correctly
- [ ] Partition option = **None** / **Physical partitions** / **Dynamic range**
  - Dynamic range → partition column name + upper/lower bound fields appear

### Azure Synapse Analytics (source)
- [ ] Same sub-cases as SQL DB above (same field structure)

### Parquet / Avro / ORC / DelimitedText / JSON (file sources, ADLS or Blob)
- [ ] File path type = **File path in dataset** (no extra path fields)
- [ ] File path type = **Prefix** (prefix field appears) — *Blob only, not ADLS*
- [ ] File path type = **Wildcard file path** (wildcard folder + file fields appear)
- [ ] File path type = **List of files** (file list path field appears)
- [ ] Recursive toggle saves correctly
- [ ] Delete files after completion toggle
- [ ] Compression (codec + level) fields for applicable formats

### DelimitedText-specific (source)
- [ ] Skip line count
- [ ] With first row as header on/off
- [ ] Null value field

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

1. **Avro (ADLS) → Azure SQL DB** ← already tested (your current setup)
2. **Azure SQL DB → Parquet (ADLS)** ← DB export, tests SQL source + file sink
3. **Azure SQL DB → Azure SQL DB** ← DB copy/migration pattern
4. **Parquet (ADLS) → Parquet (ADLS)** ← file transform pattern
5. **DelimitedText → Azure SQL DB** ← CSV ingest (common)
6. **XML (ADLS) → Azure SQL DB** ← XML source (source-only type)
7. **Azure SQL DB → Synapse** ← requires Synapse dataset setup first
8. **Avro (Blob) → Azure SQL DB** ← tests Blob storage variant

---

## Known Gaps / Not Yet Implemented

| Gap | Notes |
|---|---|
| `Iceberg` dataset type | Not in copy-activity-config; no source/sink fields rendered |
| `HttpFile` dataset type | In dataset-schemas but not in copy-activity-config |
| Azure Synapse Parquet/PolyBase copy settings | Synapse sink may have extra options beyond basic SQL |
| Source partition settings for Synapse | May differ from SQL DB partition fields |
| Mapping tab | Placeholder only — column mapping not yet implemented |
