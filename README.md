# Track Veil

Anonymize team and run identifiers in track datasets for sharing.

**Supported formats:**
- **Runs**: Rankings in trec-eval `run` format and `Report` JSONL (TREC RAG, RAGTIME, DRAGUN, etc.)
- **Eval**: Output from `trec_eval`, `ir_measures`, `tot` (TREC Tip of the Tongue)
- **Metadata**: Evalbase upload metadata

**Anonymization scheme** (stored in SQLite):
| Original | Anonymized | Example |
|----------|------------|---------|
| team | "T" + 3-digit number | `T042`, `T911` |
| run_id | plantimal name | `koala`, `oak` |

## Installation

```bash
uv pip install -e .
```

## Quick Start

```bash
# Anonymize priority 1 runs (primary use case)
uv run track-veil anonymize \
  -i track-export \
  -o track-anon \
  -m mapping.db \
  --priority "1 (top)"

# View mappings
uv run track-veil show-mapping -m mapping.db

# Reverse lookup
uv run track-veil reverse-lookup -m mapping.db T042-koala
```

Run in interactive mode — the tool will ask for format clarifications and how to handle data errors.

## Input Format

### Variables

Placeholders in `{curly braces}` represent:

| Variable | Description | Examples |
|----------|-------------|----------|
| `{trackname}` | Name of the track | `rag`, `ragtime`, `dragun` |
| `{task}` | Task within the track | `retrieval`, `generation`, `qrels` |
| `{team}` | Team/organization name | `acme-corp`, `university.edu` |
| `{run_id}` | Unique run identifier | `baseline-v1`, `my.run.2` |
| `{judge}` | Judgment method | `trec_eval`, `autoargue` |
| `{priority}` | Upload priority | `1 (top)`, `2`, `3` |

### Directory structure

The input directory must contain `runs/`, `eval/`, and `metadata/` subdirectories:

```
{trackname}-export/
├── runs/{task}/
│   └── {run_id}                       # JSONL report or TSV ranking
├── eval/{task}/
│   └── {run_id}.{judge}               # Evaluation output
└── metadata/{task}/
    └── *.jl                           # Evalbase metadata (any filename)
```

Each directory contains `{task}/` subfolders matching the track's tasks (e.g., RAG: `retrieval`, `generation`; RAGTIME: `mlir`, `repgen`).

### File contents


**Report JSONL**

`runs/{task}/{run_id}` — Report JSONL (one object per line):
```json
{"metadata": {"team_id": "{team}", "run_id": "{run_id}", "topic_id": "1"}, "responses": [{"text": "..."}], ...}
```

**Trec_Eval Run**

`runs/{task}/{run_id}` — TSV ranking (alternative format):
```
{topic}  Q0  {doc_id}  {rank}  {score}  {run_id}
```

**Trec_Eval Output**

`eval/{task}/{run_id}.{judge}` — trec_eval format:
```
map                   all     0.2345
ndcg                  all     0.4567
runid                 all     {run_id}
```

**ir_measures Output**

`eval/{task}/{run_id}.{judge}` — ir_measures format:
```
{run_id}  {topic}  nDCG@10  0.4567
{run_id}  {topic}  AP       0.2345
```

**ToT Output**

`eval/{task}/{run_id}.{judge}` — tot (Tip of the Tongue) format:
```
{run_id}  nDCG@10  {topic}  0.4567
{run_id}  AP       {topic}  0.2345
```


**Evalbase Metadata**

`metadata/{task}/*.jl` — any JSONL file:
```json
{"runtag": "{run_id}", "org": "{team}", "std-priority": "{priority}", ...}
```

Whenever team names are inconsistent, we will prefer information from `runs` directory. The metadata team name will get a separate anonymization name. Use `run_id` as unique identifier that is consistent between both.

### Processing order

`runs/` → `metadata/` → `eval/`

Mappings created from `runs/` are applied consistently across all directories.

## How Anonymization Works

### What gets anonymized

- **Filenames**: `{run_id}.{judge}` → `koala.{judge}`
- **JSONL content**: `team_id`, `run_id` fields in reports and metadata
- **TSV content**: `run_id` columns (auto-detected or interactively confirmed)
- **Email addresses**: Detected and handled interactively

### Mapping persistence

Mappings are stored in a SQLite database (`-m mapping.db`) for:
- Consistency across multiple runs
- Reverse lookup (de-anonymization if needed)
- Reproducibility (same input → same output)

### Filename as source of truth

For `runs/` and `eval/` files, the **filename determines the run_id**. Content values are replaced with the anonymized run_id derived from the filename, even if they differ.

## CLI Commands

### Anonymize

```bash
track-veil anonymize \
  -i <input-dir> \
  -o <output-dir> \
  -m <mapping.db> \
  [--priority "1 (top)"]
```

Run interactively to resolve ambiguities and anonymization options.

### Show-mapping

```bash
track-veil show-mapping -m mapping.db
```

### Reverse-lookup

```bash
track-veil reverse-lookup -m mapping.db T042-koala
```

### Recover-mapping

Recover original mappings from anonymized reports using stored fingerprints:

```bash
uv run track-veil recover-mapping -m mapping.db -i anon_data/runs/ -f table
uv run track-veil recover-mapping -m mapping.db -i anon_data/runs/ -f csv -o recovered.csv
```

## Advanced

### Priority filtering

Filter runs by metadata's `std-priority` field. Only matching runs are included in output.

```bash
--priority "1 (top)"
```

### TSV format detection

The tool auto-detects TSV formats based on:
- **Header rows**: Recognizes `run_id`, `request_id`, `metric`, `value`
- **Column count**: 3 = trec_eval, 4 = tot/ir_measures, 6 with Q0 = ranking
- **Content patterns**: Numeric values indicate data rows, not headers

In interactive mode, detected formats are confirmed with the user and cached per task.

### trec_eval runid handling

For trec_eval format (3 columns: `{measure} {topic} {value}`), the `runid` metric line is anonymized:

```
runid    all    {original}  →  runid    all    {anonymized}
```

Both tab-separated and space-separated files are supported.

### Email handling

Email addresses trigger interactive prompts:
- **Redact**: Replace with `[REDACTED]`
- **Ignore**: Leave as-is
- **Redact all**: Redact all emails in this field for the current task
- **Drop field**: Remove the entire field

Decisions are cached per (task, field) combination.

### Fingerprint-based recovery

During anonymization, content fingerprints (SHA256 of topic_id + report text) are stored. Use `recover-mapping` to match anonymized reports back to original identifiers.
