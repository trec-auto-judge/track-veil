"""File format transformers for anonymization.

Handles:
- Report JSONL (team_id, run_id in metadata)
- Metadata JSONL (runtag, org, email warning)
- TSV/WSV files (various TREC formats)
"""

import json
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from .mapping import MappingStore
from .repairs import RepairRule, RepairStore, suggest_repair_options
from .errors import ErrorCollector, IssueType, EmailAction

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
    QRELS = "qrels"              # {topic} Q0 {doc_id} {grade} {is_relevant} {run_id}
    UNKNOWN = "unknown"


@dataclass
class TsvFormatHint:
    """Hint about detected TSV format."""
    likely_format: TsvFormat
    confidence: str  # "high", "medium", "low"
    reason: str
    run_id_columns: List[int]  # Which columns contain run_id


def detect_tsv_format(lines: List[str]) -> TsvFormatHint:
    """Detect TSV format based on column count and content.

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

    # 6 columns: ranking or qrels (both have run_id in col 5)
    if typical_cols == 6:
        first_row = sample_rows[0]
        # Check if col 1 is "Q0" (common in TREC formats)
        if len(first_row) > 1 and first_row[1] == "Q0":
            # Both ranking and qrels have Q0 in col 1
            # ranking: {topic} Q0 {doc_id} {rank} {score} {run_id}
            # qrels: {topic} Q0 {doc_id} {grade} {is_relevant} {run_id}
            # Difference: col 4 is float (score) vs int (is_relevant 0/1)
            if looks_like_float(first_row[4]) and not looks_like_topic(first_row[4]):
                return TsvFormatHint(
                    TsvFormat.RANKING,
                    "medium",
                    "6 columns with Q0, col 4 looks like score: ranking format",
                    [5],
                )
            else:
                return TsvFormatHint(
                    TsvFormat.QRELS,
                    "medium",
                    "6 columns with Q0: qrels format",
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


def anonymize_run_id_in_string(
    value: str,
    mapping: MappingStore,
) -> str:
    """Replace team-run patterns in a string.

    Handles patterns like "team1-run1" -> "Fez-07"
    """
    # Get all known mappings
    team_map = mapping.get_all_team_mappings()
    run_map = mapping.get_all_run_mappings()

    result = value

    # Replace team-run combinations
    for orig_team, anon_team in team_map.items():
        for orig_run, anon_run in run_map.items():
            # Pattern: team-run (with various separators)
            for sep in ["-", "_", "."]:
                old = f"{orig_team}{sep}{orig_run}"
                new = f"{anon_team}{sep}{anon_run}"
                result = result.replace(old, new)

    # Also replace standalone occurrences
    for orig_team, anon_team in team_map.items():
        result = result.replace(orig_team, anon_team)
    for orig_run, anon_run in run_map.items():
        # Be careful with short run names - only replace if looks like identifier
        if len(orig_run) > 2:  # Skip very short strings
            result = result.replace(orig_run, anon_run)

    return result


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

        # Type mismatch - try to repair
        rule = self.repairs.get_rule(field_path, value)
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

            options = suggest_repair_options(field_path, value, expected_type.__name__)
            choice = self.ask_fn("How to handle?", options)
            desc, rule = options[choice]

            # Ask if should remember
            remember = input("Remember this fix for similar patterns? [Y/n]: ").strip().lower()
            if remember != "n":
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
    ) -> Optional[str]:
        """Transform a single JSONL line. Returns None if record should be skipped."""
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

        # Check and repair metadata fields
        if "metadata" in data:
            meta = data["metadata"]

            # Always drop creator field (contains identifying info)
            if "creator" in meta:
                del meta["creator"]

            # Check narrative field type (common issue)
            if "narrative" in meta:
                _, skip = self._check_field_type(
                    data, "metadata.narrative", str, file_path, line_num
                )
                if skip:
                    self.errors.add_skipped_record(
                        file_path, line_num, "Skipped due to malformed narrative", data
                    )
                    return None

            # Anonymize team_id
            if "team_id" in meta and meta["team_id"]:
                orig_team = meta["team_id"]
                meta["team_id"] = self.mapping.get_or_create_team(orig_team)

            # Anonymize run_id
            if "run_id" in meta and meta["run_id"]:
                orig_run = meta["run_id"]
                meta["run_id"] = self.mapping.get_or_create_run(orig_run)

            # Check for email addresses
            self._scan_for_emails(meta, "metadata", file_path, line_num)

        return json.dumps(data, separators=(",", ":"))

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
    ) -> int:
        """Transform a Report JSONL file. Returns number of lines processed."""
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

        # Anonymize org (team)
        if "org" in data and data["org"]:
            data["org"] = self.mapping.get_or_create_team(data["org"])

        # Anonymize runtag (run_id)
        if "runtag" in data and data["runtag"]:
            data["runtag"] = self.mapping.get_or_create_run(data["runtag"])

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
    """Transform TSV/WSV files (eval/)."""

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
    ) -> int:
        """Transform a TSV file, anonymizing run_id in specified columns."""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(input_path, "r") as fin:
            lines = fin.readlines()

        count = 0
        with open(output_path, "w") as fout:
            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    fout.write(line)
                    continue

                parts = stripped.split()
                for col_idx in run_id_columns:
                    if col_idx < len(parts):
                        # Handle team-run patterns
                        original = parts[col_idx]
                        parts[col_idx] = self._anonymize_value(original)

                fout.write("\t".join(parts) + "\n")
                count += 1

        return count

    def _anonymize_value(self, value: str) -> str:
        """Anonymize a value that might be team-run or just run_id."""
        # Check for team-run pattern (e.g., "team1-run1")
        for sep in ["-", "_", "."]:
            if sep in value:
                parts = value.split(sep, 1)
                if len(parts) == 2:
                    team_part, run_part = parts
                    anon_team = self.mapping.get_or_create_team(team_part)
                    anon_run = self.mapping.get_or_create_run(run_part)
                    return f"{anon_team}{sep}{anon_run}"

        # Might be just run_id
        return self.mapping.get_or_create_run(value)


def anonymize_filename(
    filename: str,
    mapping: MappingStore,
) -> str:
    """Anonymize team/run patterns in a filename."""
    result = filename

    # Get existing mappings
    team_map = mapping.get_all_team_mappings()
    run_map = mapping.get_all_run_mappings()

    # Replace team-run combinations first (more specific)
    for orig_team, anon_team in team_map.items():
        for orig_run, anon_run in run_map.items():
            for sep in ["-", "_", "."]:
                old = f"{orig_team}{sep}{orig_run}"
                new = f"{anon_team}{sep}{anon_run}"
                result = result.replace(old, new)

    return result
