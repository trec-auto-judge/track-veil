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

from .field_scan import REDACTED, anonymize_fields, scan_for_missed_fields
from .mapping import MappingStore, compute_report_fingerprint
from .repairs import RepairRule, RepairStore, suggest_repair_options
from .errors import ErrorCollector, IssueType, EmailAction
from autojudge_base.report import Report


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
    CUSTOM = "custom"            # User-specified columns
    UNKNOWN = "unknown"          # Skip file


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


def scan_for_emails(
    obj: Any,
    path: str,
    file_path: Path,
    line_num: Optional[int],
    *,
    errors: ErrorCollector,
    email_handler: Optional[EmailHandler],
    task: str = "",
    parent: Any = None,
    parent_key: Any = None,
    declared_fields: Tuple[str, ...] = (),
) -> bool:
    """Recursively find email addresses by value and apply the email policy.

    Addresses are matched with EMAIL_PATTERN wherever they occur, so this does not
    depend on a field being named "email". Each one is reported, then the handler
    decides: REDACT rewrites in place, DROP_FIELD removes the field, IGNORE leaves
    it. The handler is where the answer is remembered per task and field.

    ``declared_fields`` names keys that hold an address by declaration, complementing
    the pattern: matching needs the value to LOOK like an address, so an obfuscated or
    malformed one ("a.person AT uni DOT edu") slips past. Under a declared name no
    pattern is needed, and REDACT takes the whole value rather than a matched span.

    Returns True if the field this was called on should be dropped by its parent.
    """
    if isinstance(obj, str):
        declared = isinstance(parent_key, str) and any(
            parent_key.lower() == name.lower() for name in declared_fields
        )
        # Under a declared name the value IS the address, whatever it looks like.
        addresses = EMAIL_PATTERN.findall(obj) or ([obj] if declared else [])

        for email in addresses:
            errors.add_email_warning(file_path, line_num, path, email)

            if email_handler and parent is not None:
                action = email_handler(task, path, email, file_path)
                if action == EmailAction.REDACT:
                    # A declared field goes whole - the rest of its value is the
                    # address's context, not text worth keeping. Elsewhere only the
                    # address goes, so the surrounding sentence survives.
                    parent[parent_key] = (
                        REDACTED if declared
                        else EMAIL_PATTERN.sub(REDACTED, parent[parent_key])
                    )
                elif action == EmailAction.DROP_FIELD:
                    return True
        return False

    if isinstance(obj, dict):
        keys_to_drop = []
        for key, value in obj.items():
            if scan_for_emails(
                value, f"{path}.{key}" if path else key, file_path, line_num,
                errors=errors, email_handler=email_handler, task=task,
                parent=obj, parent_key=key, declared_fields=declared_fields,
            ):
                keys_to_drop.append(key)
        for key in keys_to_drop:
            del obj[key]
        return False

    if isinstance(obj, list):
        indices_to_drop = []
        for index, item in enumerate(obj):
            if scan_for_emails(
                item, f"{path}[{index}]", file_path, line_num,
                errors=errors, email_handler=email_handler, task=task,
                parent=obj, parent_key=index, declared_fields=declared_fields,
            ):
                indices_to_drop.append(index)
        for index in reversed(indices_to_drop):
            del obj[index]
        return False

    return False


def resolve_run_identity(
    mapping: MappingStore,
    *,
    original_team: str,
    content_run_id: str,
    expected_run_id: Optional[str],
    file_path: Path,
    warned: set,
) -> Tuple[str, str, str]:
    """Apply the filename-is-source-of-truth rule and mint the pseudonyms.

    The filename names the run; a differing run_id in the content is a warning
    (once per file/content/filename combination), not an override. Records the
    run -> team association so later sources can be cross-checked.

    Returns (original_run, anon_team, anon_run); the pseudonyms are "" when there
    was nothing to anonymize. Callers do their own field access, which is the only
    part that differs between a dict-based and a model-based transformer.
    """
    if expected_run_id:
        if content_run_id and content_run_id != expected_run_id:
            warn_key = (file_path.name, content_run_id, expected_run_id)
            if warn_key not in warned:
                warned.add(warn_key)
                print(f"  [WARNING] {file_path.name}: metadata.run_id '{content_run_id}' "
                      f"doesn't match filename '{expected_run_id}' - using filename")
        original_run = expected_run_id
    else:
        original_run = content_run_id

    anon_team = mapping.get_or_create_team(original_team) if original_team else ""

    anon_run = ""
    if original_run:
        anon_run = mapping.get_or_create_run(original_run)
        if original_team:
            mapping.store_run_team(original_run, original_team)

    return original_run, anon_team, anon_run


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
            if "run_desc" in meta:
                meta["run_desc"] = REDACTED
                
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

            # Free text and identifying blobs named by ReportMetaData
            if meta.get("description"):
                meta["description"] = REDACTED
            meta.pop("evaldata", None)

            # Parse into Report model to get correctly resolved topic_id and text
            try:
                report = Report.model_validate(data)
                topic_id = report.metadata.topic_id
                report_text = report.get_text()
            except Exception:
                # If Report validation fails, skip fingerprinting but continue
                topic_id = ""
                report_text = ""

            original_run, anon_team, anon_run = resolve_run_identity(
                self.mapping,
                original_team=original_team,
                content_run_id=content_run_id,
                expected_run_id=expected_run_id,
                file_path=file_path,
                warned=self._warned_run_mismatches,
            )
            if anon_team and meta.get("team_id"):
                meta["team_id"] = anon_team
            if anon_run:
                meta["run_id"] = anon_run

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

        # The generic bags, whether or not the record had metadata. The declared
        # fields are handled above and deliberately NOT passed here, so no value is
        # anonymized twice (see field_scan, "anonymize each value exactly once").
        #
        # No expected_run_id: the filename names THIS run, but a run_id inside a
        # generic bag may reference another one (a baseline, a parent run), which
        # must keep its own identity.
        self._clean_generic_fields(data)

        # Second pass: anything identifying outside the fields ReportMetaData names
        # is a gap in the handling above, not something to fix quietly.
        self._report_missed_fields(data, file_path, line_num)

        return json.dumps(data, separators=(",", ":")), fingerprint_info


    #: Paths this transformer owns, so the read-only second pass does not report
    #: them back as gaps. Everything else it finds is a genuine miss.
    HANDLED_PATHS = (
        "metadata.team_id", "metadata.run_id", "metadata.run_desc",
        "metadata.description", "metadata.creator", "metadata.evaldata",
        "metadata.extra",
    )

    #: The one Dict[str, Any] bag that is kept and scanned: an open field with no
    #: schema, so only the generic field scan can say anything about it.
    SCANNED_BAGS = ("extra",)

    #: Dict[str, Any] bags dropped outright - producer-side payload and AutoJudge's
    #: own evaluation data, neither of which belongs in a shared dataset. Dropped
    #: rather than scanned, so there is nothing left to anonymize.
    DROPPED_BAGS = ("metadata", "evaldata")

    def _clean_generic_fields(self, data: Dict[str, Any]) -> None:
        """Handle the Report's Dict[str, Any] bags: drop most, scan metadata.extra.

        Only open bags are passed to the field scan; everything the schema names is
        handled in transform_line and must not be scanned as well, or it would be
        anonymized twice.
        """
        meta = data.get("metadata")
        if isinstance(meta, dict):
            for field_name in self.SCANNED_BAGS:
                if isinstance(meta.get(field_name), dict):
                    anonymize_fields(meta[field_name], self.mapping)

        data.pop("evaldata", None)  # report-level; metadata.evaldata is dropped above

        for sentences_key in ("responses", "answer"):
            for sentence in data.get(sentences_key) or []:
                if isinstance(sentence, dict):
                    for field_name in self.DROPPED_BAGS:
                        sentence.pop(field_name, None)

    def _report_missed_fields(self, data: Any, file_path: Path, line_num: int) -> None:
        for action in scan_for_missed_fields(data, self.HANDLED_PATHS):
            self.errors.add_issue(
                IssueType.IDENTIFIER_FOUND,
                file_path,
                line_num,
                action.path,
                f"Identifying field '{action.key}' ({action.category.value}) found at "
                f"{action.path}, which the Report handling does not cover",
                original_value=action.original,
            )

    def _scan_for_emails(
        self,
        obj: Any,
        path: str,
        file_path: Path,
        line_num: int,
        parent: Any = None,
        parent_key: Any = None,
    ) -> bool:
        """Find email addresses and apply the email policy (see scan_for_emails)."""
        return scan_for_emails(
            obj, path, file_path, line_num,
            errors=self.errors, email_handler=self.email_handler,
            task=self._current_task, parent=parent, parent_key=parent_key,
        )

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
        with open(input_path, "rt", encoding="utf-8") as fin, open(output_path, "wt", encoding="utf-8") as fout:
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


class NuggetBankTransformer:
    """Transform ragtime26 nugget bank JSONL files.

    Works on the typed model rather than raw JSON: each line is loaded as a
    ``Ragtime26NuggetBank`` and written back through ``write_ragtime26_nugget_banks``,
    so the traversal is the format's own and nothing here has to know the wire shape.
    Note the writer stamps ``format_version`` onto every line, so output is not
    byte-identical to the submission -- the anonymized fields change anyway.

    Per nugget bank:
      - ``metadata.team_id`` and ``metadata.run_id`` are anonymized, with the filename
        as the source of truth for run_id (as for Reports)
      - ``metadata.run_desc`` is redacted -- it is free text naming the team's system
      - unknown ``metadata`` keys survive (Ragtime26NuggetMetadata allows extras) but
        are put through the generic field scan, so a team named under some other key
        is anonymized rather than published
      - nugget-, answer- and reference-level ``metadata`` is dropped: producer-side
        annotation with no place in a shared dataset

    Questions, answers and references themselves are left untouched.
    """

    #: The whole metadata object is handled: declared fields by _anonymize_metadata,
    #: unknown ones by the field scan below. So the read-only second pass reports
    #: only identifying data found OUTSIDE metadata.
    HANDLED_PATHS = ("metadata",)

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
        self._warned_run_mismatches: set = set()

    def _anonymize_metadata(
        self,
        bank,
        file_path: Path,
        expected_run_id: Optional[str] = None,
    ) -> None:
        """Anonymize the run metadata of one nugget bank, in place."""
        meta = bank.metadata

        if meta.run_desc is not None:
            meta.run_desc = REDACTED

        _run, anon_team, anon_run = resolve_run_identity(
            self.mapping,
            original_team=meta.team_id or "",
            content_run_id=meta.run_id or "",
            expected_run_id=expected_run_id,
            file_path=file_path,
            warned=self._warned_run_mismatches,
        )
        if anon_team:
            meta.team_id = anon_team
        if anon_run:
            meta.run_id = anon_run

    @staticmethod
    def _drop_annotations(bank) -> int:
        """Drop nugget/answer/reference metadata. Returns how many were dropped.

        Setting them to None is enough: the writer dumps with exclude_none, so the
        keys disappear from the output.
        """
        dropped = 0
        for nugget in bank.nuggets_as_list():
            if nugget.metadata is not None:
                nugget.metadata = None
                dropped += 1
            for answer in nugget.answers or []:
                if answer.metadata is not None:
                    answer.metadata = None
                    dropped += 1
                for reference in answer.references or []:
                    # references are doc-id strings or Reference objects
                    if not isinstance(reference, str) and reference.metadata is not None:
                        reference.metadata = None
                        dropped += 1
        return dropped

    def transform_file(
        self,
        input_path: Path,
        output_path: Path,
        expected_run_id: Optional[str] = None,
    ) -> Tuple[int, int]:
        """Transform a nugget bank JSONL file.

        Returns (nugget_banks_written, annotations_dropped).
        """
        # _load_jsonl rather than load_ragtime26_nugget_banks_from_file: the public
        # loader picks its parser from the filename, and these files are named after
        # the run, with no .jsonl extension.
        from autojudge_base.nugget_data import (
            Ragtime26NuggetBank, Ragtime26NuggetBanks, Ragtime26NuggetMetadata,
            write_ragtime26_nugget_banks,
        )
        from autojudge_base.nugget_data.io import _load_jsonl

        try:
            with open(input_path, "rt", encoding="utf-8") as f:
                nugget_banks = _load_jsonl(f, Ragtime26NuggetBank, Ragtime26NuggetBanks)
        except Exception as e:
            self.errors.add_issue(
                IssueType.PARSE_ERROR,
                input_path,
                None,
                None,
                f"Could not load as ragtime26 nugget banks: {e}",
            )
            return 0, 0

        # A record with no nugget_bank key is rejected by the model itself
        # (Ragtime26NuggetBank.nugget_bank is required), so a Report handed to this
        # transformer fails to load rather than writing out an empty nugget bank.
        dropped = 0
        for bank in nugget_banks.banks.values():
            self._anonymize_metadata(bank, input_path, expected_run_id)
            dropped += self._drop_annotations(bank)

            # The one open bag here: metadata is extra="allow", so unknown keys are
            # preserved and nothing above has looked at them. Scan those keys only -
            # the declared ones were handled by _anonymize_metadata and must not be
            # anonymized twice. Nugget/answer/reference metadata needs no scan; it is
            # dropped outright by _drop_annotations.
            declared = set(Ragtime26NuggetMetadata.model_fields)
            dumped = bank.metadata.model_dump(exclude_none=True)
            unknown = {k: v for k, v in dumped.items() if k not in declared}

            if unknown:
                anonymize_fields(unknown, self.mapping)
                scan_for_emails(
                    unknown, "metadata", input_path, None,
                    errors=self.errors, email_handler=self.email_handler,
                    task=self._current_task,
                )
                kept = {k: v for k, v in dumped.items() if k in declared}
                bank.metadata = Ragtime26NuggetMetadata.model_validate(
                    {**kept, **unknown}
                )
            # Second pass: metadata allows unknown keys and nuggets carry free text,
            # so anything identifying outside the three fields handled above is a gap.
            for action in scan_for_missed_fields(
                bank.model_dump(exclude_none=True), self.HANDLED_PATHS
            ):
                self.errors.add_issue(
                    IssueType.IDENTIFIER_FOUND,
                    input_path,
                    None,
                    action.path,
                    f"Identifying field '{action.key}' ({action.category.value}) found "
                    f"at {action.path} in topic {bank.query_id}, which the ragtime26 "
                    f"handling does not cover",
                    original_value=action.original,
                )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_ragtime26_nugget_banks(nugget_banks, output_path, format="jsonl")
        return len(nugget_banks.banks), dropped


class RawJsonlTransformer:
    """Best-effort anonymization of a JSONL file whose format is not recognized.

    The fallback for when no format could be guessed, or the typed loader refused
    the file: rather than dropping it from the dataset, anonymize the identifying
    fields that can be found in a top-level ``metadata`` object and pass the rest
    through.

    This is deliberately shallow. The record's structure is unknown, so identifying
    data nested anywhere else is NOT found -- which in an anonymization tool is a
    leak, not an inconvenience. Every file handled this way is recorded as an
    UNKNOWN_FORMAT issue so it shows up in the error report for a human to look at.
    """

    def __init__(
        self,
        mapping: MappingStore,
        errors: ErrorCollector,
        email_handler: Optional[EmailHandler] = None,
    ):
        self.mapping = mapping
        self.errors = errors
        self.email_handler = email_handler
        self._current_task: str = ""

    def transform_file(
        self,
        input_path: Path,
        output_path: Path,
        expected_run_id: Optional[str] = None,
    ) -> int:
        """Anonymize what can be found in each record's metadata. Returns lines written."""
        self.errors.add_issue(
            IssueType.UNKNOWN_FORMAT,
            input_path,
            None,
            None,
            "Unrecognized JSONL format: anonymized the metadata fields that were "
            "recognized and passed the rest through unexamined. Identifying data "
            "elsewhere in these records would NOT have been removed.",
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0

        with open(input_path, "rt", encoding="utf-8") as fin, \
                open(output_path, "wt", encoding="utf-8") as fout:
            for line_num, line in enumerate(fin, 1):
                stripped = line.strip()
                if not stripped:
                    continue

                try:
                    data = json.loads(stripped)
                except json.JSONDecodeError as e:
                    self.errors.add_issue(
                        IssueType.PARSE_ERROR, input_path, line_num, None,
                        f"JSON parse error, line copied unchanged: {e}",
                        original_value=stripped[:200],
                    )
                    fout.write(line)
                    continue

                # Deep scan: no schema to go by, so every level is examined
                anonymize_fields(data, self.mapping, expected_run_id)
                scan_for_emails(
                    data, "", input_path, line_num,
                    errors=self.errors, email_handler=self.email_handler,
                    task=self._current_task,
                )

                fout.write(json.dumps(data, separators=(",", ":")) + "\n")
                count += 1

        return count


#: Metadata field names, by role.
#:
#: Tracks rename these slightly from one year to the next, so each role is a LIST of
#: spellings: when a new track calls the team something else, add the name here rather
#: than reworking the handling below. EVERY listed name a record carries is handled,
#: not just the first - a record spelling the team two ways holds two identifying
#: values, and leaving either behind publishes it.

#: The team.
ORG_FIELDS: Tuple[str, ...] = ("org",)

#: The run.
RUN_FIELDS: Tuple[str, ...] = ("runtag",)

#: Free-text description fields are NOT listed here. They are redacted whole by the
#: generic scan, via FIELDS_BY_CATEGORY[REDACT] in field_scan -- which is the single
#: place to add another one. Substituting the team and run names into them instead was
#: tried and dropped: it only removes the names we happen to know, and a description of
#: a team's own system identifies it in ways no substitution reaches.

#: Fields that hold an email address by declaration. Handed to ``scan_for_emails`` as
#: its ``declared_fields``, so the two directions COMPLEMENT each other: the pattern
#: finds an address under any name, and these names find an address the pattern would
#: not match ("a.person AT uni DOT edu", a trailing typo).
EMAIL_FIELDS: Tuple[str, ...] = ("email",)


def present_string_fields(data: Dict[str, Any], names: Tuple[str, ...]) -> List[str]:
    """Every one of ``names`` the record carries as a non-empty string.

    A value of some other shape (a list where a team name was expected) is left out
    deliberately: it is not something to anonymize by guessing, so it falls through to
    the generic scan and, failing that, is reported by the second pass.
    """
    return [name for name in names if isinstance(data.get(name), str) and data[name]]


class MetadataTransformer:
    """Transform Metadata JSONL files (metadata/).

    A metadata record is an open ``Dict[str, Any]``. Two roles are handled here by name
    -- see ORG_FIELDS and RUN_FIELDS -- because each needs something only this
    transformer can do:

    - the team is the one place metadata and ``runs/`` can be cross-checked against
      each other, and a disagreement means runs/ wins (see _resolve_team)
    - the run warns when it has to mint a mapping for a run ``runs/`` never contained

    Everything else goes through the same shared machinery as the Report and raw-JSONL
    paths: ``anonymize_fields`` at any depth -- which is what redacts the free-text
    description fields -- ``scan_for_emails`` by value (told which names hold an
    address outright, see EMAIL_FIELDS), and a read-only second pass reporting whatever
    neither of them resolved. There is no schema to enumerate here, so "everything
    else" is literally every other key.
    """

    def __init__(
        self,
        mapping: MappingStore,
        errors: ErrorCollector,
        email_handler: Optional[EmailHandler] = None,
    ):
        self.mapping = mapping
        self.errors = errors
        # Without a configured policy an address is redacted, which is what this
        # transformer has always done: metadata files carry submitter addresses as a
        # matter of course, so silence is not the safe default here.
        self.email_handler = email_handler or (lambda *_args: EmailAction.REDACT)
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

        # The keys this record actually uses for each role. These are the keys handled
        # by name below, and so the keys kept out of the generic scan: passing them
        # there as well would anonymize each value twice (see field_scan, "anonymize
        # each value exactly once").
        org_fields = present_string_fields(data, ORG_FIELDS)
        run_fields = present_string_fields(data, RUN_FIELDS)
        handled: Tuple[str, ...] = tuple(org_fields + run_fields)

        # Read the run names before anonymizing anything: the mismatch check below
        # looks them up in the mapping, which is keyed by the original.
        original_runs: List[str] = [data[field] for field in run_fields]

        # Anonymize org (team), with runs/ as the source of truth for the spelling.
        orgs: List[Tuple[str, str]] = []
        for field in org_fields:
            original_org = self._resolve_team(field, data[field], original_runs)
            anon_org = self.mapping.get_or_create_team(original_org)
            data[field] = anon_org
            orgs.append((original_org, anon_org))

        # Anonymize runtag (run_id)
        runs: List[Tuple[str, str]] = []
        for field, original_run in zip(run_fields, original_runs):
            self._warn_on_unseen_run(original_run)
            anon_run = self.mapping.get_or_create_run(original_run)
            data[field] = anon_run
            runs.append((original_run, anon_run))

        # Store run->team relationship for later lookups (e.g., info command)
        for original_run, anon_run in runs:
            for original_org, anon_org in orgs:
                self.mapping.store_run_team(original_run, original_org,
                                            anon_run, anon_org)

        # Everything the handled keys do not cover, at any depth. This is where the
        # free-text description fields get redacted, via FIELDS_BY_CATEGORY[REDACT].
        cleaned = self._clean_generic_fields(data, handled)

        # Addresses are found by VALUE, so one sitting in the description or in a
        # nested field is caught too - not only a field literally named "email". The
        # declared names catch the other direction: a value under one of them that no
        # pattern would match.
        scan_for_emails(
            data, "", file_path, line_num,
            errors=self.errors, email_handler=self.email_handler,
            task=self._current_task, declared_fields=EMAIL_FIELDS,
        )

        self._report_missed_fields(data, handled + cleaned, file_path, line_num)

        return json.dumps(data, separators=(",", ":"))

    def _resolve_team(self, field: str, metadata_org: str, runtags: List[str]) -> str:
        """The team that owns this record, with runs/ as the source of truth.

        A metadata file may spell the team differently from the reports themselves
        ("iastate" where runs/ said "isu"). Anonymizing both spellings mints two
        pseudonyms and publishes one team as two, and leaves run_team_mappings
        disagreeing with the team_id inside the reports -- so the spelling runs/
        recorded for the same run wins, and the metadata one is replaced. This is the
        rule resolve_run_identity already applies to run_id, where the filename wins
        over the content.

        Warns once per (metadata spelling, task). Returns the name to anonymize; the
        metadata spelling is still substituted out of free text by the caller.
        """
        for runtag in runtags:
            runs_team = self.mapping.get_run_team(runtag)
            if not runs_team or runs_team == metadata_org:
                continue

            warn_key = (metadata_org, self._current_task)
            if warn_key not in self._warned_team_mismatches:
                self._warned_team_mismatches.add(warn_key)
                print(f"  [WARNING] Team mismatch for run '{runtag}': "
                      f"metadata.{field}='{metadata_org}' vs "
                      f"runs.team_id='{runs_team}' - using runs.team_id")
            return runs_team

        # runs/ never recorded a team for these runs, so metadata is all there is.
        return metadata_org

    def _warn_on_unseen_run(self, original_run: str) -> None:
        """Warn before minting a run mapping: run_id should normally come from runs/."""
        if self.mapping.get_run(original_run) is None and original_run not in self._warned_runs:
            print(f"  [WARNING] Creating run mapping from metadata (not seen in runs/): {original_run}")
            self._warned_runs.add(original_run)

    def _clean_generic_fields(
        self,
        data: Dict[str, Any],
        handled: Tuple[str, ...],
    ) -> Tuple[str, ...]:
        """Deep-scan every key this transformer did not handle by name.

        The scan runs over a *view* of the unhandled entries, so the handled ones are
        not anonymized a second time. The view shares its dicts and lists with
        ``data``, so nested edits land directly; top-level scalars the scan replaced,
        and keys it dropped, are written back afterwards - surviving keys keep their
        original position.

        No ``expected_run_id``: the record's own runtag names THIS run, but a run_id
        sitting in some other field may reference a different one (a baseline, a
        parent run), which has to keep its own identity.

        Returns the paths the scan acted on, so the second pass does not report them
        back as gaps.
        """
        generic: Dict[str, Any] = {
            key: value for key, value in data.items() if key not in handled
        }
        actions = anonymize_fields(generic, self.mapping)

        for key in [k for k in data if k not in handled and k not in generic]:
            del data[key]      # the scan dropped it outright (creator, evaldata)
        data.update(generic)   # scalars it replaced; surviving keys keep their place

        return tuple(action.path for action in actions)

    def _report_missed_fields(
        self,
        data: Any,
        handled: Tuple[str, ...],
        file_path: Path,
        line_num: int,
    ) -> None:
        """Identifying fields that neither pass above resolved.

        The generic scan covers every path outside the handled keys, so a hit here
        means it recognised the key and declined the value - a ``team_id`` holding a
        list, say, which ``anonymize_fields`` skips as not-a-string. Reported rather
        than repaired: rewriting a value of an unexpected shape is how data gets
        corrupted.
        """
        for action in scan_for_missed_fields(data, handled):
            self.errors.add_issue(
                IssueType.IDENTIFIER_FOUND,
                file_path,
                line_num,
                action.path,
                f"Identifying field '{action.key}' ({action.category.value}) found at "
                f"{action.path}, which the metadata handling did not anonymize",
                original_value=action.original,
            )

    def transform_file(
        self,
        input_path: Path,
        output_path: Path,
    ) -> int:
        """Transform a Metadata JSONL file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with open(input_path, "rt", encoding="utf-8") as fin, open(output_path, "wt", encoding="utf-8") as fout:
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

        with open(input_path, "rt", encoding="utf-8") as fin:
            lines = fin.readlines()

        count = 0
        unknown_run_ids: List[str] = []

        with open(output_path, "wt", encoding="utf-8") as fout:
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
