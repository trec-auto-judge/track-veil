"""File format transformers for anonymization.

Handles:
- Report JSONL (team_id, run_id in metadata)
- Metadata JSONL (runtag, org, email warning)
- TSV/WSV files (various track formats)
"""

import json
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from .mapping import MappingStore, compute_report_fingerprint
from .repairs import RepairRule, RepairStore, suggest_repair_options
from .errors import ErrorCollector, IssueType, EmailAction
from ..report import Report

# Type alias for email handler callback
# Signature: (task: str, field_path: str, email: str, file_path: Path) -> EmailAction
EmailHandler = Callable[[str, str, str, Path], EmailAction]


EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)


class TsvFormat(str, Enum):
    """Known TSV/WSV formats."""
    TOT = "tot"                  # {run_id} {measure} {topic} {value}
    IR_MEASURES = "ir_measures"  # {run_id} {topic} {measure} {value}
    TREC_EVAL = "trec_eval"      # {topic} {measure} {value}
    RANKING = "ranking"          # {topic} Q0 {doc_id} {rank} {score} {run_id}
    UNKNOWN = "unknown"


@dataclass
class TsvFormatHint:
    """Hint about detected TSV format."""
    likely_format: TsvFormat
    confidence: str  # "high", "medium", "low"
    reason: str
    run_id_columns: List[int]  # Which columns contain run_id


def detect_tsv_format(lines: List[str]) -> TsvFormatHint:
    """Detect TSV format based on column count, headers, and content.

    Returns a hint - user should confirm.
    """
    if not lines:
        return TsvFormatHint(TsvFormat.UNKNOWN, "low", "Empty file", [])

    # Parse first few non-empty lines
    sample_rows = []
    for line in lines[:10]:
        line = line.strip()
        if line and not line.startswith("#"):
            parts = line.split()
            sample_rows.append(parts)

    if not sample_rows:
        return TsvFormatHint(TsvFormat.UNKNOWN, "low", "No data rows", [])

    # Check if first row looks like a header
    # Known column names (case-insensitive)
    RUN_ID_NAMES = {"run_id", "runtag", "run", "runid", "system"}
    TOPIC_ID_NAMES = {"topic_id", "request_id", "query_id", "narrative_id",
                      "topicid", "queryid"}  # Removed "topic", "qid", "query" - too common as values
    METRIC_NAMES = {"metric", "measure", "eval_metric"}
    VALUE_NAMES = {"value", "score", "result"}
    ALL_HEADER_NAMES = RUN_ID_NAMES | TOPIC_ID_NAMES | METRIC_NAMES | VALUE_NAMES

    first_row_lower = [col.lower() for col in sample_rows[0]]

    # Only treat first row as header if it clearly looks like one:
    # - Contains known header column names
    # - Does NOT contain obvious numeric values (floats are clearly data)
    def is_numeric(val: str) -> bool:
        try:
            float(val)
            return True
        except ValueError:
            return False

    # If any column is numeric, this is data, not a header
    has_numeric = any(is_numeric(col) for col in sample_rows[0])

    # Check if first row contains known header names
    has_run_col = any(col in RUN_ID_NAMES for col in first_row_lower)
    has_topic_col = any(col in TOPIC_ID_NAMES for col in first_row_lower)
    has_metric_col = any(col in METRIC_NAMES for col in first_row_lower)

    # Only treat as header if we have header-like names AND no numeric values
    if not has_numeric and (has_run_col or (has_topic_col and has_metric_col)):
        # This looks like a header row - find run_id column indices
        run_id_cols = [i for i, col in enumerate(first_row_lower) if col in RUN_ID_NAMES]

        if run_id_cols:
            # Determine format based on columns present
            header_desc = ", ".join(sample_rows[0])
            if has_metric_col:
                return TsvFormatHint(
                    TsvFormat.IR_MEASURES,
                    "high",
                    f"Header detected: {header_desc}",
                    run_id_cols,
                )
            else:
                return TsvFormatHint(
                    TsvFormat.RANKING,
                    "high",
                    f"Header detected: {header_desc}",
                    run_id_cols,
                )
        elif has_topic_col and has_metric_col:
            # Has topic and metric but no run_id - trec_eval style
            return TsvFormatHint(
                TsvFormat.TREC_EVAL,
                "high",
                f"Header detected (no run_id): {', '.join(sample_rows[0])}",
                [],
            )

    col_counts = [len(row) for row in sample_rows]
    typical_cols = max(set(col_counts), key=col_counts.count)

    # Check for topic-like values (integers or numeric strings)
    def looks_like_topic(val: str) -> bool:
        try:
            int(val)
            return True
        except ValueError:
            return False

    def looks_like_float(val: str) -> bool:
        try:
            float(val)
            return True
        except ValueError:
            return False

    # 3 columns: trec_eval format (no run_id)
    if typical_cols == 3:
        return TsvFormatHint(
            TsvFormat.TREC_EVAL,
            "high",
            "3 columns matches trec_eval: {topic} {measure} {value}",
            [],
        )

    # 4 columns: tot or ir_measures (both have run_id in col 0)
    if typical_cols == 4:
        # Both formats have run_id in col 0
        # tot: {run_id} {measure} {topic} {value}
        # ir_measures: {run_id} {topic} {measure} {value}
        # Difference: col 1 vs col 2 is topic
        first_row = sample_rows[0]
        if looks_like_topic(first_row[1]):
            return TsvFormatHint(
                TsvFormat.IR_MEASURES,
                "medium",
                "4 columns, col 1 looks like topic: ir_measures format",
                [0],
            )
        elif looks_like_topic(first_row[2]):
            return TsvFormatHint(
                TsvFormat.TOT,
                "medium",
                "4 columns, col 2 looks like topic: tot format",
                [0],
            )
        else:
            return TsvFormatHint(
                TsvFormat.TOT,
                "low",
                "4 columns, assuming tot format (run_id in col 0)",
                [0],
            )

    # 6 columns: ranking format {topic} Q0 {doc_id} {rank} {score} {run_id}
    if typical_cols == 6:
        first_row = sample_rows[0]
        # Check if col 1 is "Q0" (common in TREC formats)
        if len(first_row) > 1 and first_row[1] == "Q0":
            return TsvFormatHint(
                TsvFormat.RANKING,
                "medium",
                "6 columns with Q0: ranking format",
                [5],
            )
        return TsvFormatHint(
            TsvFormat.UNKNOWN,
            "low",
            f"6 columns but no Q0 marker",
            [5],  # Assume run_id in last column
        )

    return TsvFormatHint(
        TsvFormat.UNKNOWN,
        "low",
        f"Unusual column count: {typical_cols}",
        [],
    )


class ReportTransformer:
    """Transform Report JSONL files (runs/)."""

    def __init__(
        self,
        mapping: MappingStore,
        repairs: RepairStore,
        errors: ErrorCollector,
        interactive: bool = True,
        ask_fn: Optional[Callable[[str, List[Tuple[str, Any]]], int]] = None,
        email_handler: Optional[EmailHandler] = None,
    ):
        self.mapping = mapping
        self.repairs = repairs
        self.errors = errors
        self.interactive = interactive
        self.ask_fn = ask_fn or self._default_ask
        self.email_handler = email_handler
        self._current_task: str = ""  # Set by caller before processing
        self._warned_run_mismatches: set = set()  # Track warned mismatches to avoid repeats

    def _default_ask(self, prompt: str, options: List[Tuple[str, Any]]) -> int:
        """Default interactive prompt."""
        print(f"\n{prompt}")
        for i, (desc, _) in enumerate(options, 1):
            print(f"  [{i}] {desc}")
        while True:
            try:
                choice = int(input("Choice: ")) - 1
                if 0 <= choice < len(options):
                    return choice
            except ValueError:
                pass
            print("Invalid choice, try again.")

    def _check_field_type(
        self,
        data: Dict,
        field_path: str,
        expected_type: type,
        file_path: Path,
        line_num: int,
        team_id: Optional[str] = None,
    ) -> Tuple[Any, bool]:
        """Check field type and repair if needed.

        Returns (value, should_skip_record).
        """
        parts = field_path.split(".")
        obj = data
        for part in parts[:-1]:
            if part not in obj:
                return None, False
            obj = obj[part]

        field_name = parts[-1]
        if field_name not in obj:
            return None, False

        value = obj[field_name]
        if isinstance(value, expected_type):
            return value, False

        # Type mismatch - try to repair (check team-specific rules first)
        rule = self.repairs.get_rule(field_path, value, team_id=team_id)
        if rule:
            repaired, skip = rule.apply(value)
            if not skip:
                obj[field_name] = repaired
            return repaired, skip

        # No rule - ask user or log error
        if self.interactive:
            print(f"\nMalformed field in {file_path}:{line_num}")
            print(f"  Field: {field_path}")
            print(f"  Expected: {expected_type.__name__}")
            print(f"  Got: {type(value).__name__} = {json.dumps(value)[:100]}")
            if team_id:
                print(f"  Team: {team_id}")

            options = suggest_repair_options(field_path, value, expected_type.__name__)
            choice = self.ask_fn("How to handle?", options)
            desc, rule = options[choice]

            # Ask if should remember
            remember = input("Remember this fix? [Y]es for all, [t]eam only, [n]o:").strip().lower()
            if remember == "t" and team_id:
                rule.team_id = team_id
                self.repairs.save_rule(rule, sample_value=value)
            elif remember != "n":
                self.repairs.save_rule(rule, sample_value=value)

            repaired, skip = rule.apply(value)
            if not skip:
                obj[field_name] = repaired
            return repaired, skip
        else:
            # Non-interactive: log error
            self.errors.add_issue(
                IssueType.MALFORMED_FIELD,
                file_path,
                line_num,
                field_path,
                f"Expected {expected_type.__name__}, got {type(value).__name__}",
                original_value=value,
            )
            return value, False

    def transform_line(
        self,
        line: str,
        file_path: Path,
        line_num: int,
        expected_run_id: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        """Transform a single JSONL line.

        Args:
            line: The JSONL line to transform
            file_path: Source file for error reporting
            line_num: Line number for error reporting
            expected_run_id: If provided, verify metadata.run_id matches this value

        Returns (transformed_json, fingerprint_info) where fingerprint_info
        is a dict with keys: fingerprint, original_team, original_run,
                             topic_id, anon_team, anon_run
        Returns (None, None) if record should be skipped.
        """
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            self.errors.add_issue(
                IssueType.PARSE_ERROR,
                file_path,
                line_num,
                None,
                f"JSON parse error: {e}",
                original_value=line[:200],
            )
            return None, None

        fingerprint_info = None

        # Check and repair metadata fields
        if "metadata" in data:
            meta = data["metadata"]

            # Always drop creator field (contains identifying info)
            if "creator" in meta:
                del meta["creator"]

            # Get team_id early for team-scoped repair rules
            current_team = meta.get("team_id", "")

            # Check narrative field type (common issue)
            if "narrative" in meta:
                _, skip = self._check_field_type(
                    data, "metadata.narrative", str, file_path, line_num,
                    team_id=current_team
                )
                if skip:
                    self.errors.add_skipped_record(
                        file_path, line_num, "Skipped due to malformed narrative", data
                    )
                    return None, None

            # Capture original values BEFORE anonymization for fingerprinting
            original_team = current_team
            content_run_id = meta.get("run_id", "")

            # Filename is the source of truth for run_id
            # If expected_run_id (filename) is provided, use it instead of content's run_id
            if expected_run_id:
                if content_run_id and content_run_id != expected_run_id:
                    # Only warn once per (file, content_run_id, expected_run_id) combination
                    warn_key = (file_path.name, content_run_id, expected_run_id)
                    if warn_key not in self._warned_run_mismatches:
                        self._warned_run_mismatches.add(warn_key)
                        print(f"  [WARNING] {file_path.name}: metadata.run_id '{content_run_id}' "
                              f"doesn't match filename '{expected_run_id}' - using filename")
                original_run = expected_run_id
            else:
                original_run = content_run_id

            # Parse into Report model to get correctly resolved topic_id and text
            try:
                report = Report.model_validate(data)
                topic_id = report.metadata.topic_id
                report_text = report.get_text()
            except Exception:
                # If Report validation fails, skip fingerprinting but continue
                topic_id = ""
                report_text = ""

            # Anonymize team_id
            anon_team = ""
            if "team_id" in meta and meta["team_id"]:
                anon_team = self.mapping.get_or_create_team(original_team)
                meta["team_id"] = anon_team

            # Anonymize run_id (use original_run which is authoritative - from filename if provided)
            anon_run = ""
            if original_run:
                anon_run = self.mapping.get_or_create_run(original_run)
                meta["run_id"] = anon_run

                # Store run→team association for cross-source consistency checks
                if original_team:
                    self.mapping.store_run_team(original_run, original_team)

            # Compute fingerprint if we have all required data
            if original_team and original_run and topic_id and report_text:
                fingerprint = compute_report_fingerprint(topic_id, report_text)
                fingerprint_info = {
                    "fingerprint": fingerprint,
                    "original_team": original_team,
                    "original_run": original_run,
                    "topic_id": topic_id,
                    "anon_team": anon_team,
                    "anon_run": anon_run,
                }

            # Check for email addresses
            self._scan_for_emails(meta, "metadata", file_path, line_num)

        return json.dumps(data, separators=(",", ":")), fingerprint_info

    def _scan_for_emails(
        self,
        obj: Any,
        path: str,
        file_path: Path,
        line_num: int,
        parent: Any = None,
        parent_key: Any = None,
    ) -> bool:
        """Recursively scan for email addresses and optionally redact/drop.

        Args:
            obj: The object to scan
            path: JSON path for display (e.g., "metadata.creator.contact")
            file_path: Source file for error reporting
            line_num: Line number for error reporting
            parent: Parent object (dict or list) for in-place modification
            parent_key: Key or index in parent for in-place modification

        Returns:
            True if this field should be dropped from parent
        """
        if isinstance(obj, str):
            emails = EMAIL_PATTERN.findall(obj)
            for email in emails:
                self.errors.add_email_warning(file_path, line_num, path, email)

                # Check with handler if we should redact/drop
                if self.email_handler and parent is not None:
                    action = self.email_handler(
                        self._current_task, path, email, file_path
                    )
                    if action == EmailAction.REDACT:
                        # Redact in place
                        redacted = EMAIL_PATTERN.sub("[REDACTED]", parent[parent_key])
                        parent[parent_key] = redacted
                    elif action == EmailAction.DROP_FIELD:
                        return True  # Signal to drop this field
            return False
        elif isinstance(obj, dict):
            keys_to_drop = []
            for key, value in obj.items():
                should_drop = self._scan_for_emails(
                    value, f"{path}.{key}", file_path, line_num,
                    parent=obj, parent_key=key
                )
                if should_drop:
                    keys_to_drop.append(key)
            for key in keys_to_drop:
                del obj[key]
            return False
        elif isinstance(obj, list):
            indices_to_drop = []
            for i, item in enumerate(obj):
                should_drop = self._scan_for_emails(
                    item, f"{path}[{i}]", file_path, line_num,
                    parent=obj, parent_key=i
                )
                if should_drop:
                    indices_to_drop.append(i)
            # Remove in reverse order to preserve indices
            for i in reversed(indices_to_drop):
                del obj[i]
            return False
        return False

    def transform_file(
        self,
        input_path: Path,
        output_path: Path,
        expected_run_id: Optional[str] = None,
    ) -> int:
        """Transform a Report JSONL file. Returns number of lines processed.

        Args:
            input_path: Source JSONL file
            output_path: Destination file
            expected_run_id: If provided, verify metadata.run_id matches (typically the filename)
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with open(input_path, "r") as fin, open(output_path, "w") as fout:
            for line_num, line in enumerate(fin, 1):
                line = line.strip()
                if not line:
                    continue
                result, fingerprint_info = self.transform_line(
                    line, input_path, line_num, expected_run_id
                )
                if result:
                    fout.write(result + "\n")
                    count += 1
                    # Store fingerprint if available
                    if fingerprint_info:
                        self.mapping.store_fingerprint(**fingerprint_info)
        return count


class MetadataTransformer:
    """Transform Metadata JSONL files (metadata/)."""

    def __init__(
        self,
        mapping: MappingStore,
        errors: ErrorCollector,
        email_handler: Optional[EmailHandler] = None,
    ):
        self.mapping = mapping
        self.errors = errors
        self.email_handler = email_handler
        self._current_task: str = ""  # Set by caller before processing
        self._warned_runs: set = set()  # Track which runs we've warned about
        self._warned_team_mismatches: set = set()  # Track (org, task) pairs we've warned about

    def transform_line(
        self,
        line: str,
        file_path: Path,
        line_num: int,
    ) -> Optional[str]:
        """Transform a single metadata JSONL line."""
        try:
            data = json.loads(line)
        except json.JSONDecodeError as e:
            self.errors.add_issue(
                IssueType.PARSE_ERROR,
                file_path,
                line_num,
                None,
                f"JSON parse error: {e}",
                original_value=line[:200],
            )
            return None

        # Anonymize org (team) - check for mismatch with runs/ team
        if "org" in data and data["org"]:
            org = data["org"]
            runtag = data.get("runtag", "")

            # Check if this run has a different team from runs/
            if runtag:
                expected_team = self.mapping.get_run_team(runtag)
                if expected_team and expected_team != org:
                    warn_key = (org, self._current_task)
                    if warn_key not in self._warned_team_mismatches:
                        self._warned_team_mismatches.add(warn_key)
                        print(f"  [WARNING] Team mismatch for run '{runtag}': "
                              f"metadata.org='{org}' vs runs.team_id='{expected_team}'")

            data["org"] = self.mapping.get_or_create_team(org)

        # Anonymize runtag (run_id)
        # Warn if creating new mapping - run_id should normally come from runs/
        if "runtag" in data and data["runtag"]:
            original_run = data["runtag"]
            existing = self.mapping.get_run(original_run)
            if existing is None and original_run not in self._warned_runs:
                print(f"  [WARNING] Creating run mapping from metadata (not seen in runs/): {original_run}")
                self._warned_runs.add(original_run)
            data["runtag"] = self.mapping.get_or_create_run(original_run)

        # Check for email field
        if "email" in data and data["email"]:
            email = data["email"]
            self.errors.add_email_warning(file_path, line_num, "email", email)

            # Check with handler for action
            action = EmailAction.REDACT  # Default to redact
            if self.email_handler:
                action = self.email_handler(
                    self._current_task, "email", email, file_path
                )

            if action == EmailAction.REDACT:
                data["email"] = "[REDACTED]"
            elif action == EmailAction.DROP_FIELD:
                del data["email"]

        return json.dumps(data, separators=(",", ":"))

    def transform_file(
        self,
        input_path: Path,
        output_path: Path,
    ) -> int:
        """Transform a Metadata JSONL file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with open(input_path, "r") as fin, open(output_path, "w") as fout:
            for line_num, line in enumerate(fin, 1):
                line = line.strip()
                if not line:
                    continue
                result = self.transform_line(line, input_path, line_num)
                if result:
                    fout.write(result + "\n")
                    count += 1
        return count


class TsvTransformer:
    """Transform TSV/WSV files (eval/ and runs/)."""

    def __init__(
        self,
        mapping: MappingStore,
        errors: ErrorCollector,
    ):
        self.mapping = mapping
        self.errors = errors

    def transform_file(
        self,
        input_path: Path,
        output_path: Path,
        run_id_columns: List[int],
        create_if_missing: bool = False,
    ) -> Tuple[int, List[str]]:
        """Transform a TSV file, anonymizing run_id in specified columns.

        Args:
            input_path: Source TSV file
            output_path: Destination file
            run_id_columns: Which columns contain run_id
            create_if_missing: If True, create new mappings for unknown run_ids (for runs/).
                             If False, track unknown run_ids and return them (for eval/).

        Returns:
            Tuple of (lines_processed, list_of_unknown_run_ids)
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(input_path, "r") as fin:
            lines = fin.readlines()

        count = 0
        unknown_run_ids: List[str] = []

        with open(output_path, "w") as fout:
            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    fout.write(line)
                    continue

                parts = stripped.split()
                for col_idx in run_id_columns:
                    if col_idx < len(parts):
                        original = parts[col_idx]
                        anon, is_unknown = self._anonymize_value(original, create_if_missing)
                        parts[col_idx] = anon
                        if is_unknown and original not in unknown_run_ids:
                            unknown_run_ids.append(original)

                fout.write("\t".join(parts) + "\n")
                count += 1

        return count, unknown_run_ids

    def _anonymize_value(self, value: str, create_if_missing: bool) -> Tuple[str, bool]:
        """Anonymize a run_id value.

        Args:
            value: The run_id value to anonymize
            create_if_missing: If True, create mapping if not found

        Returns:
            Tuple of (anonymized_value, is_unknown).
            If is_unknown is True, the value was not found and wasn't created.
        """
        # Check if value matches a known run_id
        existing = self.mapping.get_run(value)
        if existing is not None:
            return existing, False

        # Not found
        if create_if_missing:
            return self.mapping.get_or_create_run(value), False
        else:
            # Return original value and flag as unknown
            return value, True


def anonymize_filename(
    filename: str,
    mapping: MappingStore,
) -> str:
    """Anonymize run_id in a filename.

    Run files typically have format: {run_id} (no extension, no team prefix)
    The entire filename IS the run_id.

    If not already mapped, creates a new mapping.
    """
    return mapping.get_or_create_run(filename)


def anonymize_eval_filename(
    filename: str,
    mapping: MappingStore,
) -> Optional[str]:
    """Anonymize eval filename with format {run_id}.{judge}.

    Eval files have format: {run_id}.{judge}
    Run_id may contain dots, so we do prefix matching against known run_ids.
    We match the longest known run_id that is a prefix of the filename.

    Example: "my.run.v1.nist-edit" -> "007.nist-edit" (if "my.run.v1" is known)

    Returns None if no matching run_id is found (caller should handle this error).
    """
    run_map = mapping.get_all_run_mappings()

    # Sort by length descending to match longest prefix first
    sorted_runs = sorted(run_map.keys(), key=len, reverse=True)

    for orig_run in sorted_runs:
        # Check if filename starts with run_id followed by a dot
        prefix = orig_run + "."
        if filename.startswith(prefix):
            anon_run = run_map[orig_run]
            # Replace run_id prefix, keep the rest (.judge extension)
            return anon_run + filename[len(orig_run):]

    return None  # No match found - caller should handle this error
