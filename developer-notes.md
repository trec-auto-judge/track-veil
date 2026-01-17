# Track Veil — Design Notes

Developer documentation for design decisions and implementation rationale.

For user documentation, see [README.md](README.md).

---

## Design Decisions

### Filename as Source of Truth

**Decision:** The filename determines the `run_id`, not the file content.

**Rationale:**
- Content often has inconsistent or wrong `run_id` values
- Filename is what users see and reference
- Simplifies processing: one authoritative source per file
- Eval files use `{run_id}.{judge}` pattern — filename parsing is already required

**Implementation:** `_extract_run_id_from_filename()` in `pipeline.py`

### Processing Order: runs → metadata → eval

**Decision:** Process directories in this specific order.

**Rationale:**
- `runs/` establishes the canonical team/run mappings from Report JSONL
- `metadata/` may have different team names (`org` field) — we warn but don't override
- `eval/` only does lookups — all mappings must exist by this point

**Implementation:** `run()` method in `AnonymizationPipeline`

### Interactive-Only Mode

**Decision:** Removed `--no-interactive` option.

**Rationale:**
- Too many ambiguities in real data (TSV formats, email handling, filename patterns)
- Non-interactive defaults often wrong, leading to silent data corruption
- Better to fail explicitly than produce bad output
- Not worth maintaining untested code paths

### Anonymization Scheme

**Decision:** Teams = `T` + 3-digit number, Runs = plantimal names

**Rationale:**
- `T###` format: clearly artificial, easy to grep, 999 teams sufficient
- Plantimal names: memorable, easy to discuss ("the koala run"), pronounceable
- Both are deterministic per seed — same input produces same output

**Implementation:** `pseudonyms.py` — `generate_team_pool()`, `generate_plantimal_pool()`

### Collision Handling

**Decision:** On UNIQUE constraint collision, record the conflicting name and retry with next pseudonym.

**Rationale:**
- Can happen when mixing old mapping.db with new data
- Better to skip and continue than fail entirely
- `invalidated_names` table tracks skipped names for debugging

**Implementation:** `get_or_create_run()` and `get_or_create_team()` in `mapping.py`

### Team Name Mismatch Warning

**Decision:** When `runs/` has `team_id=X` but `metadata/` has `org=Y` for same run, warn but anonymize both separately.

**Rationale:**
- Teams sometimes submit with inconsistent names
- Can't know which is "correct" — both might be valid
- Warning lets user investigate
- Only warn once per (team, task) to avoid spam

**Implementation:** `store_run_team()`, `get_run_team()` in `MappingStore`

---

## Filename Parsing Assumptions

### The Problem

Variables `{run_id}`, `{team}`, and `{judge}` can all contain dots and dashes, making filename parsing ambiguous:
- `my.run.v1.judge.method` — where does run_id end and judge begin?
- `team-name-run-name` — where does team end and run begin?

### Where We Make Assumptions

| Context | Pattern | Assumption | Resolution |
|---------|---------|------------|------------|
| `runs/{task}/{filename}` | `{run_id}` | Filename IS the run_id | No parsing needed |
| `eval/{task}/{filename}` | `{run_id}.{judge}` | Both may contain dots | Use metadata to find known run_ids, match longest prefix |
| `metadata/{task}/{filename}` | Any | No restrictions | Process all files |

### How Eval Filename Parsing Works

For `eval/` files with pattern `{run_id}.{judge}`:

1. **If metadata was processed:** We have a list of known `run_id` values from `runtag` fields
2. **Match longest known run_id:** Sort known IDs by length descending, find first that matches filename prefix
3. **Extract judge:** Everything after the matched run_id + first dot

**Example:** File `my.run.v1.nist.edit`, known run_ids = `["my.run.v1", "other"]`
- Matches `my.run.v1` → judge = `nist.edit`

**Fallback (no metadata):** Cannot reliably parse — interactive prompt asks user to specify the pattern.

### Where We Don't Make Assumptions

| Item | Assumption? | Notes |
|------|-------------|-------|
| Metadata filenames | None | Any file in `metadata/{task}/` is processed |
| Run file extensions | None | Files in `runs/{task}/` have no required extension |
| Whitespace in TSV | Flexible | Both tabs and spaces accepted, `.split()` used |
| Character encoding | UTF-8 | Standard assumption |

### Interactive Resolution

When automatic parsing fails or is ambiguous, we prompt the user:
- **Eval filename pattern:** Ask for judge suffix or manual specification
- **TSV format:** Show detected format, ask for confirmation
- **Email handling:** Ask how to handle each unique case

**Implementation:** `_ask_eval_filename_pattern()`, `_ask_tsv_format()` in `pipeline.py`

---

## Interactive Dialogs

Users encounter these prompts during anonymization. Decisions are often cached to avoid repeated prompts.

### 1. TSV Format Selection

**When:** Processing a TSV file in `runs/` or `eval/` for the first time in a task.

**Shows:**
```
Detected format: ir_measures ({run_id} {topic} {measure} {value})
Confidence: high
Sample: myrun  1  nDCG@10  0.4567

Which format is this?
  1. ir_measures ({run_id} {topic} {measure} {value})
  2. tot ({run_id} {measure} {topic} {value})
  3. trec_eval ({measure} {topic} {value})
  4. ranking ({topic} Q0 {doc_id} {rank} {score} {run_id})
```

**Caching:** Per (task, directory_type). Once confirmed for `eval/task1/`, applies to all files in that directory.

**Implementation:** `_ask_tsv_format()` in `pipeline.py`

---

### 2. Eval Filename Pattern

**When:** Processing `eval/` directory and need to determine how to split `{run_id}.{judge}`.

**Shows:**
```
How should eval filenames be parsed?
  1. Use suffix '.trec_eval' (e.g., 'myrun.trec_eval' → run_id='myrun')
  2. Use suffix '.autoargue' (from other files in directory)
  3. Enter custom suffix
  4. Enter run_id manually for each file
```

Options are generated from:
- Known run_ids from metadata (matched against filenames)
- Common suffixes found in the directory

**Caching:** Per task directory. Suffix applies to all files in `eval/{task}/`.

**Implementation:** `_ask_eval_filename_pattern()` in `pipeline.py`

---

### 3. Manual Run ID Entry

**When:** User chose "Enter run_id manually" in filename pattern dialog.

**Shows:**
```
Enter run_id for file: complex.dotted.filename.judge.method
run_id: _
```

**Caching:** None — asked for each file.

**Implementation:** `_ask_manual_run_id()` in `pipeline.py`

---

### 4. Email Handling

**When:** An email address is detected in JSONL content.

**Shows:**
```
Email detected in metadata/task1/meta.jl
  Field: contact_email
  Value: researcher@university.edu

How should this email field be handled?
  1. Redact this email ([REDACTED])
  2. Ignore (keep as-is)
  3. Redact all emails in 'contact_email' for this task
  4. Drop entire field
```

**Caching:** Per (task, field_path). "Redact all" applies to all emails in that field for the task.

**Implementation:** `get_email_action()` in `pipeline.py`

---

### 5. Unknown Run ID in Eval

**When:** An eval file references a run_id that wasn't found in `runs/` directory.

**Shows:**
```
[WARNING] eval/task1/unknown-run.judge: run_id 'unknown-run' not found in mappings

How to proceed?
  1. Skip this file
  2. Create mapping for this run_id
  3. Skip entire task directory
```

**Caching:** None — asked for each unknown run_id.

**Implementation:** Anonymous prompt in `_process_eval_task()` in `pipeline.py`

---

### 6. Data Repair (Malformed Fields)

**When:** A JSONL field has unexpected type (e.g., string instead of list).

**Shows:**
```
Malformed field in runs/task1/myrun:42
  Field: responses
  Expected: list
  Got: str = "This should have been a list..."
  Team: acme-corp

How to handle?
  1. Wrap in list
  2. Skip this line
  3. Set to empty list

Remember this fix? [Y]es for all, [t]eam only, [n]o: _
```

**Caching:** Stored in `repairs.db`. Can be:
- Global (applies to all teams)
- Team-specific (only for the specified team)

**Implementation:** `_try_repair_field()` in `transformers.py`, `repairs.py`

---

## TSV Format Detection

### Supported Formats

| Format | Columns | Pattern |
|--------|---------|---------|
| trec_eval | 3 | `{measure} {topic} {value}` |
| ir_measures | 4 | `{run_id} {topic} {measure} {value}` |
| tot | 4 | `{run_id} {measure} {topic} {value}` |
| ranking | 6 | `{topic} Q0 {doc_id} {rank} {score} {run_id}` |

### Detection Heuristics

1. **Header detection:** Check if first row contains known column names (`run_id`, `metric`, `topic_id`, etc.)
2. **Column count:** Narrow down format candidates
3. **Q0 marker:** 6 columns with "Q0" in column 1 = ranking format
4. **Topic position:** Distinguish ir_measures vs tot by which column looks like topic IDs

**Implementation:** `detect_tsv_format()` in `transformers.py`

---

## Fingerprint Recovery

### Purpose

Allow recovering original team/run mappings from anonymized reports if `mapping.db` is available but you need to verify which anonymized file corresponds to which original.

### How It Works

1. During anonymization: compute SHA256 of `(topic_id, report_text)` for each report line
2. Store fingerprint → original mapping in `report_fingerprints` table
3. Later: compute same fingerprint from anonymized report, lookup in DB

**Implementation:** `compute_report_fingerprint()`, `store_fingerprint()`, `lookup_fingerprint()` in `mapping.py`

---

## File Structure

```
track_veil_lib/
├── __init__.py           # Version
└── anonymizer/
    ├── __init__.py       # Public exports
    ├── cli.py            # Click commands
    ├── pipeline.py       # Main orchestration
    ├── mapping.py        # SQLite storage, fingerprints
    ├── pseudonyms.py     # Name generation (T###, plantimals)
    ├── transformers.py   # JSONL/TSV transformation, format detection
    ├── repairs.py        # Data repair suggestions
    └── errors.py         # Error collection, email actions
```

---

## Future Considerations

- **Batch email decisions:** Currently per-field, could add "redact all emails everywhere"
- **Format override flag:** Allow forcing TSV format without interactive confirmation
- **Incremental processing:** Skip already-processed files based on fingerprints
- **Dry-run improvements:** Currently minimal — could show more detail
