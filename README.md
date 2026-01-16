# TREC Data Anonymization

Anonymize team and run identifiers in TREC datasets for sharing.

## Installation

```bash
uv pip install -e .
```

## Usage

```bash
# Anonymize a dataset
trec-anon anonymize -i data/raw -o data/anon -m mapping.db

# Show mappings
trec-anon show-mapping -m mapping.db

# Reverse lookup
trec-anon reverse-lookup -m mapping.db Fez-07
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
       - {run_id}                        #  file for each run forat "report"| "qrels" | "ranking"
                                         #  "report":  jsonl file with  "{"metadata": {"team_id": "{team}", "run_id": "{run_id}", ...}, ...}"  (load with `report.py#load_report`)
                                         #  "ranking": trec_eval run file: "{topic} Q0 {doc_id} {rank} {score} {run_id_or_comment}"
                                         #  "qrels": "{topic} Q0 {doc_id} {grade} {is_relevant} {run_id}"
  - eval                                 #  per topic/team/run evaluation results
     - {task}
       - {run_id}.{judge}                 # leaderboard in either in format "tot" | "trec_eval" | "ir_measures" | else
                                          # "ir_measures":  "{run_id} {topic} {measure} {value}"
                                          # "trec_eval":    "{topic} {measure} {value}"
                                          # "tot":          "{run_id} {measure} {topic} {value}"
                                          
  - metadata  # data from form upload
     - {task}
        - trec2025-{rackname}-{task}.jl   # json lines with meta information for each run from web upload form. format
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
| run_id   | 2-digit number | e.g., "07", "42", "93" |

Anonymization is applied to:
- **Filenames**: `team1-run1.judge` → `Bax-07.judge`
- **JSONL content**: `team_id`, `run_id` fields in reports and metadata
- **TSV content**: run_id columns (column depends on format)
- **Email addresses**: Detected and redacted to `[REDACTED]`

### Mapping persistence

Mappings are stored in a SQLite database for:
- Consistency across multiple runs
- Reverse lookup (de-anonymization if needed)
- Reproducibility (same input always produces same output)

### Priority filtering

Runs can be filtered by priority using metadata's `std-priority` field. Only runs matching the specified priority are included in the output.

