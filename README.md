# Track Veil

Anonymize team and run identifiers in track datasets for sharing.

**Supported formats:**
- **Runs**: Rankings in trec-eval `run` format, `Report` JSONL (TREC RAG, RAGTIME, DRAGUN, etc.)
  and nugget banks (ragtime26 submission format)
- **Eval**: Output from `trec_eval`, `ir_measures`, `tot` (TREC Tip of the Tongue), and eval JSONL
- **Metadata**: Evalbase upload metadata

Anything else in a JSONL run directory is handled generically: recognized identifying
fields are anonymized and the file is reported for a human to look at.

**Anonymization scheme** (stored in SQLite):
| Original | Anonymized | Example |
|----------|------------|---------|
| team | "T" + 3-digit number | `T042`, `T911` |
| 2025 run_id | plantimal name | `koala`, `oak` |
| 2026 run_id | baby name | `linda`, `john` |

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

**Nugget bank JSONL (ragtime26)**

`runs/{task}/{run_id}` — one nugget bank per line, one line per topic:
```json
{"metadata": {"team_id": "{team}", "topic_id": "1", "run_id": "{run_id}", "run_desc": "..."},
 "nugget_bank": [{"question": "...", "aggregator_type": "AND",
                  "answers": [{"answer": "...", "references": ["{doc_id}"]}]}]}
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
- **JSONL content**: `team_id`, `run_id` fields in reports, nugget banks and metadata
- **TSV content**: `run_id` columns (auto-detected or interactively confirmed)
- **Email addresses**: Detected by value anywhere in a record, handled interactively
- **Free text that names the team or its system**: `run_desc`, `description` → `[REDACTED]`
- **Producer-side payload**: `creator` and `evaldata` are dropped, as is a nugget's or
  a sentence's `metadata` — annotation from whoever built the file, with no place in a
  shared dataset

### Fields with no schema

Records carry open `Dict[str, Any]` bags — `metadata.extra` on a Report, unknown
metadata keys on a ragtime26 nugget bank. Those are walked to any depth and any
recognized identifying key is anonymized:

| Category | Keys |
|----------|------|
| team | `team_id`, `team` |
| run | `run_id`, `runid`, `runtag` |
| redact | `run_desc`, `description`, `std-desc` |
| drop | `creator`, `evaldata` |

A key outside those lists is left alone. Every value is anonymized **exactly once**:
a transformer handles its own declared fields and passes only the open bags to the
generic scan, because mapping a pseudonym again would mint a pseudonym of a pseudonym.

### Second-pass report

After a record has been anonymized, it is scanned again read-only. Anything
identifying found outside the fields the format's own handling covers is written to
`errors.jsonl` as `identifier_found`. A hit there is a gap in the handling, not
something quietly fixed — it is meant to be read.

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

### Select-priority

Copy the run files whose metadata carries a given `std-priority` into a separate
directory — the usual way to reduce an export to the runs a track will actually
assess, *before* anonymizing:

```bash
track-veil select-priority -m meta.jsonl -r runs/ -o prio1/ -p "1 (top)"
track-veil select-priority -m meta.jsonl -r runs/ -o prio1/ -p "1 (top)" --dry-run
```

`--symlink` links instead of copying; `--dry-run` shows what would be selected.
(`anonymize --priority` does the same filtering inline, without a separate copy.)

### Ensure-topics

Filter run files down to a set of topics, read from stdin one id per line. Report
JSONL is filtered on `metadata.topic_id`, ranking TSV on the first column:

```bash
cat topics.txt | track-veil ensure-topics -r runs/ -o filtered/
jq -r '.topic_id' requests.jsonl | track-veil ensure-topics -r runs/ -o filtered/
```

### Info

Statistics about an anonymized dataset — runs, topics, eval files, qrels and
leaderboards. `-m` resolves teams from run ids, `--markdown` emits a table to paste
into a report:

```bash
track-veil info -d data/anon/
track-veil info -d data/anon/ -m mapping.db -v
track-veil info -d data/anon/ --markdown
```

### Generate-datasets-yml

Write a `datasets.yml` describing an anonymized directory — one entry per task, with
the prio-1 runs and the assessed topics taken from the official eval files:

```bash
track-veil generate-datasets-yml -d data/anon/
track-veil generate-datasets-yml -d data/anon/ -o my_datasets.yml --topic-path topics.jsonl
```

### Random-eval

Write a dummy eval file with random scores, so an evaluation pipeline can be
exercised before real assessments exist. Run it on an already-anonymized directory:
run ids come from the filenames in `runs/{task}/`, topics from `--topics` (a plain
id-per-line list or a topics JSONL).

```bash
track-veil random-eval -d data/anon/ --topics rag-topic-list.txt
```

One JSON object per line — `{"run_id", "topic_id", "measure", "value"}` — with a
per-run `"all"` row equal to the mean of that run's topics. Scores are drawn from
`[0, --max-score]` rather than fixed, because identical scores leave the run ranking
undefined. `--seed` makes it reproducible.

## Document tooling (`track-veil-docs`)

A second console script resolves the documents a report cites, and builds the corpus
archives that resolution reads:

```bash
track-veil-docs export-docno -i runs/ --docno-out docnos.txt   # doc ids the reports cite
track-veil-docs ingest -i runs/ -o enriched/ -c corpus.jsonl.gz  # fill in from an archive
track-veil-docs pull   -i runs/ -o enriched/ -c ragtime-mt --host HOST --port PORT
track-veil-docs check  -i enriched/ --docno-out missing.txt    # exit 1 if any are missing
track-veil-docs clean  -i enriched/ -o cleaned/ --remove-docno missing.txt

track-veil-docs build-corpus duckdb -d docnos.txt -o corpus.jsonl.gz --corpus msmarco
track-veil-docs build-corpus routir -d docnos.txt -o corpus.jsonl.gz --corpus ragtime
```

`build-corpus` needs an extra: `uv pip install '.[duckdb]'` or `'.[routir]'`.

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

Email addresses are found **by value** anywhere in a record, so a field does not have
to be called `email`. Each triggers an interactive prompt:
- **Redact**: Replace with `[REDACTED]`
- **Ignore**: Leave as-is
- **Redact all**: Redact all emails in this field for the current task
- **Drop field**: Remove the entire field

Decisions are cached per (task, field) combination.

### Which record type is this?

`runs/{task}/` may hold Reports or nugget banks, and both are JSONL, so the file type
sniff cannot tell them apart. The first record of the first file is inspected, a
format is guessed — from a declared `format_version`, otherwise by trying to validate
against each candidate — and the guess is shown with its evidence for you to confirm:

```
What do the files in runs/nuggets/ hold?
  File: .../runs/nuggets/T3-original-run
  Sample: {"metadata": {"team_id": ..., "nugget_bank": [...
  Record keys: metadata, nugget_bank
  Parses as:   ragtime26_nugget_bank, ragtime25_nugget_bank
  [1] Nugget bank, ragtime26 (metadata + nugget_bank list)
  ...
```

The answer is saved and replayed on the next run.

### Reproducible runs

Interactive answers — TSV layouts, eval filename patterns, record types, email
policies — are saved and replayed, so a run can be repeated without prompts:

```bash
track-veil anonymize -i in/ -o out/ -m mapping.db --save-decisions decisions.yml
track-veil anonymize -i in/ -o out/ -m mapping.db --load-decisions decisions.yml
```

### Re-running over output

Anonymizing an already-anonymized value would map it a second time (`T042` → `T873`)
and silently corrupt the dataset. If a value to anonymize is already one of the
mapping's own pseudonyms — typically because an output directory was used as input —
the tool warns and asks, defaulting to leaving the value alone.

### Fingerprint-based recovery

During anonymization, content fingerprints (SHA256 of topic_id + report text) are stored. Use `recover-mapping` to match anonymized reports back to original identifiers.
