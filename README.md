# TREC Data Anonymization

Anonymize team and run identifiers in TREC datasets for sharing.

## Installation

```bash
uv pip install -e .
```

## Usage

The input directory must **directly contain** `runs/`, `eval/`, and `metadata/` subdirectories:

```
{trackname}-export/     <-- point -i here
├── runs/
├── eval/
└── metadata/
```

```bash
# Anonymize and export only priority 1 runs (primary use case)
uv run trec-anon anonymize \
  -i rag-export \
  -o rag-anon \
  -m mapping.db \
  --priority "1 (top)"

# Show mappings
uv run trec-anon show-mapping -m mapping.db

# Reverse lookup
uv run trec-anon reverse-lookup -m mapping.db Fez-007

# Recover mappings from anonymized reports (if mapping.db is lost)
uv run trec-anon recover-mapping -m mapping.db -i anon_data/runs/

# Example with test data
uv run trec-anon anonymize \
  -i data/fake \
  -o data/anon \
  -m mapping.db \
  --no-interactive \
  --priority "1 (top)"
```



Description of raw TREC datasets, for cleaning and anonymization pipeline.

Below we specify the data layout and how to parse variables from the file name or content info. 


---



## Variables

Each variable is denoted as `{variable}`

**Note:** Variables `team`, `run_id`, and `judge` may contain dots (e.g., `team.org`, `run.v1`, `judge.method`). The filename format `{team}-{run}.{judge}` uses `-` to separate team from run, and the first `.` after the team-run identifier separates the judge extension.

- trackname  # the name of the track, examples: "dragun" | "rag" | "ragtime"
- task       # name of the task run by the track, examples: "rag":  "auggen" | "generation" | "qrels" | "retrieval"  or  "ragtime": "mlir" | "repgen" or "dragun": "qgen" | "repgen"
- run_id     # unique run name submitted by a team (may contain dots)
- team       # team that submitted multiple run_ids (may contain dots)
- judge      # judgment method (may contain dots), examples:
             # "rag": "nist-post-edit"   or
             # "ragtime": "autoargue" or
             # "dragun": "contradictory.results" | "supportive.results"
- priority   # priority of the team and run upload. examples: "1 (top)" | "2" | "3"| ...


## Directory layout

Nested list denotes subdirectory structure. File are either jsonl or "white-space separated value" (TSV/WSV), relevant variables embedded in the contents are indicated in comments.

- {trackname}-export   # export for each track
  - runs                                #  submitted systems (aka runs)
     - {task}                            # directory for each task
       - {run_id}                        #  file for each run, format: "report" | "ranking"
                                         #  "report":  jsonl file with  "{"metadata": {"team_id": "{team}", "run_id": "{run_id}", ...}, ...}"  (load with `report.py#load_report`)
                                         #  "ranking": trec_eval run file: "{topic} Q0 {doc_id} {rank} {score} {run_id}"
  - eval                                 #  per topic/team/run evaluation results
     - {task}
       - {run_id}.{judge}                 # leaderboard in either in format "tot" | "trec_eval" | "ir_measures" | else
                                          # "ir_measures":  "{run_id} {topic} {measure} {value}"
                                          # "trec_eval":    "{topic} {measure} {value}"
                                          # "tot":          "{run_id} {measure} {topic} {value}"
                                          
  - metadata  # data from form upload
     - {task}
        - trec2025-{trackname}-{task}.jl   # json lines with meta information for each run from web upload form. format
                                          # { "runtag": "{run_id}",  "org": "{team}", "std-priority": "{priority}", ...}

    

## Data Cleaning

...todo... (We will do this later)


## Anonymization

The goal is to share runs and eval data while hiding team and run identifiers to preserve anonymity during evaluation.

**Primary use case:** Anonymize and export only priority 1 runs for sharing with the research community.

### What gets anonymized

| Original | Anonymized | Format |
|----------|------------|--------|
| team     | 3-letter CVC code | e.g., "Bax", "Cog", "Fez" |
| run_id   | 3-digit number | e.g., "007", "042", "196" |

Anonymization is applied to:
- **Filenames**: `team1-run1.judge` → `Bax-007.judge`
- **JSONL content**: `team_id`, `run_id` fields in reports and metadata
- **TSV content**: run_id columns (auto-detected or interactively confirmed)
- **Email addresses**: Detected and handled interactively (redact, ignore, or drop field)

### Mapping persistence

Mappings are stored in a SQLite database for:
- Consistency across multiple runs
- Reverse lookup (de-anonymization if needed)
- Reproducibility (same input always produces same output)

### Priority filtering

Runs can be filtered by priority using metadata's `std-priority` field. Only runs matching the specified priority are included in the output.

### TSV format detection

The tool auto-detects TSV file formats based on:
- **Header rows**: Recognizes column names like `run_id`, `request_id`, `metric`, `value`
- **Column count**: 3 columns = trec_eval, 4 columns = tot/ir_measures, 6 columns with Q0 = ranking
- **Content patterns**: Numeric values indicate data rows, not headers

In interactive mode, detected formats are confirmed with the user. Format decisions are cached per task directory.

### Filename as source of truth

For both `runs/` and `eval/` files, the **filename is the source of truth** for the run_id. Even if the file content contains a different run_id value, it will be replaced with the anonymized run_id derived from the filename.

This ensures consistency when:
- Content run_id values are incorrect or inconsistent
- Multiple files need to be processed with the same mapping

### trec_eval format handling

For trec_eval format files (3 columns: `{measure} {topic} {value}`), the special `runid` metric line is anonymized:

```
runid    all    original_run_name  →  runid    all    anonymized_run_id
```

Both tab-separated and space-separated files are supported.

### Fingerprint-based mapping recovery

During anonymization, content fingerprints (SHA256 of topic_id + report text) are stored in the mapping database. If the mapping.db is available but you need to verify mappings against anonymized data:

```bash
# Recover/verify mappings from anonymized reports
uv run trec-anon recover-mapping -m mapping.db -i anon_data/runs/ -f table
uv run trec-anon recover-mapping -m mapping.db -i anon_data/runs/ -f csv -o recovered.csv
```

### Email handling

Email addresses found in data trigger interactive prompts with options:
- **Redact**: Replace with `[REDACTED]`
- **Ignore**: Leave as-is
- **Redact all**: Redact all emails in this field for the current task
- **Drop field**: Remove the entire field containing the email

Decisions are cached per (task, field) combination.

