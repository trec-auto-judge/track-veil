"""Orchestrates the anonymization pipeline.

Directory structure expected:
    {input}/
        runs/{task}/{run_id}        # Report JSONL
        eval/{task}/{run_id}.{judge} # TSV eval results
        metadata/{task}/*.jl        # Metadata JSONL
"""

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from .mapping import MappingStore
from .repairs import RepairStore
from .errors import ErrorCollector, EmailAction
from .transformers import (
    ReportTransformer,
    MetadataTransformer,
    TsvTransformer,
    TsvFormat,
    TsvFormatHint,
    detect_tsv_format,
    anonymize_filename,
)


@dataclass
class PipelineConfig:
    """Configuration for the anonymization pipeline."""
    input_dir: Path
    output_dir: Path
    mapping_db: Path

    # Directory names (can be overridden)
    runs_dir: str = "runs"
    eval_dir: str = "eval"
    metadata_dir: str = "metadata"

    # Behavior
    interactive: bool = True
    dry_run: bool = False

    # Filtering
    priority_filter: Optional[str] = None  # e.g., "1 (top)"

    # TSV format overrides: {file_pattern: TsvFormat}
    tsv_formats: Dict[str, TsvFormat] = field(default_factory=dict)


@dataclass
class PipelineStats:
    """Statistics from pipeline run."""
    files_processed: int = 0
    lines_processed: int = 0
    teams_anonymized: int = 0
    runs_anonymized: int = 0
    files_filtered: int = 0  # Skipped due to priority filter
    errors: int = 0
    warnings: int = 0


class AnonymizationPipeline:
    """Main pipeline for anonymizing TREC data.

    Usage:
        config = PipelineConfig(
            input_dir=Path("data/raw"),
            output_dir=Path("data/anon"),
            mapping_db=Path("mapping.db"),
        )
        pipeline = AnonymizationPipeline(config)
        stats = pipeline.run()
    """

    def __init__(
        self,
        config: PipelineConfig,
        ask_fn: Optional[Callable[[str, List[Tuple[str, object]]], int]] = None,
    ):
        self.config = config
        self.ask_fn = ask_fn or self._default_ask
        self.stats = PipelineStats()

        # Priority map: {(team, run) -> priority} or {run_id -> priority}
        self._priority_map: Dict[str, str] = {}

        # Task format cache: {task_dir -> (TsvFormat, run_id_columns)}
        # All files in a task directory share the same format
        self._task_format_cache: Dict[Path, Tuple[TsvFormat, List[int]]] = {}

        # Eval filename pattern cache: {task_dir -> judge_suffix}
        # e.g., ".qrel_eval", ".nist-edit", or None if filenames don't follow {run_id}.{judge}
        self._eval_filename_cache: Dict[Path, Optional[str]] = {}

        # Email policy cache: {(task, field_path) -> EmailAction}
        # Stores decisions for "redact_all" per task+field combination
        self._email_policy_cache: Dict[Tuple[str, str], EmailAction] = {}
        # Track one example per (task, field) for display
        self._email_examples: Dict[Tuple[str, str], Tuple[str, str]] = {}  # -> (email, file_path)

        # Initialize stores
        self.mapping = MappingStore(config.mapping_db)
        self.repairs = RepairStore(config.mapping_db)
        self.errors = ErrorCollector()

        # Initialize transformers with email handler callback
        self.report_transformer = ReportTransformer(
            self.mapping,
            self.repairs,
            self.errors,
            interactive=config.interactive,
            ask_fn=self.ask_fn,
            email_handler=self.get_email_action,
        )
        self.metadata_transformer = MetadataTransformer(
            self.mapping,
            self.errors,
            email_handler=self.get_email_action,
        )
        self.tsv_transformer = TsvTransformer(
            self.mapping,
            self.errors,
        )

    def _default_ask(self, prompt: str, options: List[Tuple[str, object]]) -> int:
        """Default interactive prompt."""
        print(f"\n{prompt}")
        for i, (desc, _) in enumerate(options, 1):
            print(f"  [{i}] {desc}")
        while True:
            try:
                choice = int(input("Choice: ")) - 1
                if 0 <= choice < len(options):
                    return choice
            except (ValueError, EOFError):
                pass
            print("Invalid choice, try again.")

    def _ask_tsv_format(
        self,
        file_path: Path,
        hint: TsvFormatHint,
        sample_lines: List[str] = None,
    ) -> Tuple[TsvFormat, List[int]]:
        """Ask user to confirm TSV format."""
        if not self.config.interactive:
            # Use hint
            return hint.likely_format, hint.run_id_columns

        task_name = file_path.parent.name
        print(f"\nDetected TSV format:")
        print(f"  Task: {task_name}")
        print(f"  File: {file_path}")

        # Show first data line as sample
        if sample_lines:
            for line in sample_lines:
                line = line.strip()
                if line and not line.startswith("#"):
                    # Truncate if too long
                    if len(line) > 80:
                        line = line[:77] + "..."
                    print(f"  Sample: {line}")
                    break

        print(f"  Hint: {hint.likely_format.value} ({hint.confidence} confidence)")
        print(f"  Reason: {hint.reason}")

        options = [
            (f"{fmt.value} - run_id in column(s) {self._format_columns(fmt)}", fmt)
            for fmt in TsvFormat
            if fmt != TsvFormat.UNKNOWN
        ]
        options.append(("Skip this file", None))

        # Put likely format first
        for i, (desc, fmt) in enumerate(options):
            if fmt == hint.likely_format:
                options.insert(0, options.pop(i))
                break

        choice = self.ask_fn("Which format is this?", options)
        selected_format = options[choice][1]

        if selected_format is None:
            return TsvFormat.UNKNOWN, []

        return selected_format, self._get_run_id_columns(selected_format)

    def _format_columns(self, fmt: TsvFormat) -> str:
        """Get column description for format."""
        cols = self._get_run_id_columns(fmt)
        if not cols:
            return "none"
        return ", ".join(str(c) for c in cols)

    def _get_run_id_columns(self, fmt: TsvFormat) -> List[int]:
        """Get which columns contain run_id for a format."""
        if fmt == TsvFormat.TOT:
            return [0]
        elif fmt == TsvFormat.IR_MEASURES:
            return [0]
        elif fmt == TsvFormat.RANKING:
            return [5]
        else:
            return []

    def _ask_eval_filename_pattern(
        self,
        task_dir: Path,
        sample_file: Path,
    ) -> Optional[str]:
        """Ask user about eval filename pattern for this task.

        Returns:
            - Judge suffix string (e.g., ".qrel_eval") for pattern-based extraction
            - "MANUAL" marker to indicate run_id should be entered per file
            - "SKIP" marker to skip the entire task
        """
        if not self.config.interactive:
            # Non-interactive: try to detect common patterns
            return self._detect_judge_suffix(sample_file.name)

        print(f"\nEval filename pattern:")
        print(f"  Task: {task_dir.name}")
        print(f"  File: {sample_file}")
        print(f"  Example filename: {sample_file.name}")
        print(f"\nEval files typically follow the pattern: {{run_id}}.{{judge}}")
        print(f"The run_id may contain dots, so we need to know the judge suffix.")

        # Try to detect and suggest
        detected = self._detect_judge_suffix(sample_file.name)
        if detected:
            print(f"  Detected suffix: '{detected}'")

        options = [
            ("Enter judge suffix (e.g., .qrel_eval, .nist-edit)", "enter_suffix"),
            ("Enter run_id manually for each file", "manual"),
            ("Skip this task", "skip"),
        ]

        choice = self.ask_fn("How should filenames be parsed?", options)
        action = options[choice][1]

        if action == "skip":
            return "SKIP"
        elif action == "manual":
            return "MANUAL"
        elif action == "enter_suffix":
            suffix = input(f"Enter judge suffix (including dot, e.g., '.qrel_eval'): ").strip()
            # Empty suffix is valid - means filename is exactly the run_id
            if suffix and not suffix.startswith("."):
                suffix = "." + suffix
            return suffix

        return "SKIP"

    def _ask_manual_run_id(self, filename: str) -> Optional[str]:
        """Ask user to enter run_id for a specific file.

        Returns run_id or None to skip the file.
        """
        print(f"\nEnter run_id for file: {filename}")
        print(f"  (or press Enter to skip this file)")
        run_id = input("run_id: ").strip()
        return run_id if run_id else None

    def _get_filename_suffix(self, filename: str, run_id: str) -> str:
        """Get the suffix part of a filename by removing the run_id prefix.

        E.g., filename='team1-run1.judge', run_id='team1-run1' -> '.judge'
        """
        if filename.startswith(run_id):
            return filename[len(run_id):]
        return ""

    def _copy_trec_eval_with_anon_runid(
        self, input_file: Path, output_file: Path, anon_run_id: str
    ) -> int:
        """Copy trec_eval format file, replacing 'runid' metric values with anonymized run_id.

        trec_eval format: topic_id<TAB>metric<TAB>value
        The 'runid' metric line has the run_id as the value (3rd column).

        Returns number of lines processed.
        """
        output_file.parent.mkdir(parents=True, exist_ok=True)
        lines_processed = 0

        with open(input_file, "r") as f_in, open(output_file, "w") as f_out:
            for line in f_in:
                lines_processed += 1
                stripped = line.rstrip("\n\r")
                if not stripped:
                    f_out.write(line)
                    continue

                parts = stripped.split()  # Handle both tabs and spaces
                # Check for runid metric line: "runid <topic> <value>"
                if len(parts) >= 3 and parts[0].lower() == "runid":
                    parts[2] = anon_run_id
                    f_out.write("\t".join(parts) + "\n")
                else:
                    f_out.write(line)

        return lines_processed

    def _detect_judge_suffix(self, filename: str) -> Optional[str]:
        """Try to detect judge suffix from filename.

        Common patterns: .qrel_eval, .nist-edit, .judge, .eval
        """
        # Check for common suffixes
        common_suffixes = [".qrel_eval", ".nist-edit", ".nist_edit", ".judge", ".eval"]
        for suffix in common_suffixes:
            if filename.endswith(suffix):
                return suffix
        return None

    def _extract_run_id_from_eval_filename(
        self,
        filename: str,
        judge_suffix: Optional[str],
    ) -> Optional[str]:
        """Extract run_id from eval filename using known judge suffix.

        Args:
            filename: The eval filename (e.g., "aa.bb.cc.qrel_eval")
            judge_suffix: The judge suffix (e.g., ".qrel_eval") or None

        Returns:
            The run_id (e.g., "aa.bb.cc") or None if can't extract
        """
        if judge_suffix is None:
            return None

        if not filename.endswith(judge_suffix):
            return None

        # Remove suffix to get run_id
        run_id = filename[:-len(judge_suffix)]
        return run_id if run_id else None

    def get_email_action(
        self,
        task: str,
        field_path: str,
        email: str,
        file_path: Path,
    ) -> EmailAction:
        """Get action for an email address, prompting if needed.

        Args:
            task: Task directory name (e.g., "task1")
            field_path: JSON path to the field (e.g., "metadata.creator.contact")
            email: The email address found
            file_path: File where email was found

        Returns:
            EmailAction indicating whether to redact or ignore
        """
        cache_key = (task, field_path)

        # Check if we already have a policy for this task+field
        if cache_key in self._email_policy_cache:
            return self._email_policy_cache[cache_key]

        # Store first example for this task+field
        if cache_key not in self._email_examples:
            self._email_examples[cache_key] = (email, str(file_path))

        # Non-interactive mode: default to redact
        if not self.config.interactive:
            return EmailAction.REDACT

        # Interactive: prompt user with example
        example_email, example_file = self._email_examples[cache_key]
        # Mask middle of email for display
        masked = self._mask_email(example_email)

        print(f"\nEmail found in {task}/ ({field_path}):")
        print(f"  Example: {masked}")
        print(f"  File: {example_file}")

        options = [
            ("Redact all (for this field in this task)", EmailAction.REDACT_ALL),
            ("Drop field (remove entire field for this task)", EmailAction.DROP_FIELD),
            ("Redact this one", EmailAction.REDACT),
            ("Ignore all (leave as-is for this field in this task)", EmailAction.IGNORE),
        ]

        choice = self.ask_fn("How should this email field be handled?", options)
        action = options[choice][1]

        # Cache persistent decisions for this task+field
        if action in (EmailAction.REDACT_ALL, EmailAction.DROP_FIELD, EmailAction.IGNORE):
            self._email_policy_cache[cache_key] = action

        # For reporting purposes, map REDACT_ALL to REDACT
        if action == EmailAction.REDACT_ALL:
            return EmailAction.REDACT
        return action

    def _mask_email(self, email: str) -> str:
        """Mask middle of email for display."""
        if "@" not in email:
            return email[:3] + "***"
        local, domain = email.split("@", 1)
        if len(local) <= 2:
            masked_local = local[0] + "***"
        else:
            masked_local = local[:2] + "***"
        return f"{masked_local}@{domain}"

    def _scan_metadata_for_priority(self, meta_dir: Path):
        """Scan metadata files to build priority map.

        Maps run identifiers (runtag) to their priority values.
        Keys are stored as run_id to match filenames (which are just {run_id}).
        """
        import json

        for task_dir in meta_dir.iterdir():
            if not task_dir.is_dir():
                continue

            for meta_file in task_dir.iterdir():
                if meta_file.is_dir():
                    continue

                try:
                    with open(meta_file, "r") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                data = json.loads(line)
                                run = data.get("runtag", "")
                                priority = data.get("std-priority", "")

                                if run:
                                    # Store with run_id key to match filenames
                                    self._priority_map[run] = priority
                            except json.JSONDecodeError:
                                continue
                except IOError:
                    continue

    def _extract_run_id_from_filename(self, filename: str) -> Optional[str]:
        """Extract the run_id from a filename.

        Run files: filename IS the run_id
        Eval files: {run_id}.{judge} where run_id may contain dots

        Uses known run identifiers from metadata to find the correct match.
        """
        # If we have metadata, match against known identifiers
        if self._priority_map:
            # Sort by length descending to match longest first
            known_ids = sorted(self._priority_map.keys(), key=len, reverse=True)
            for run_id in known_ids:
                if filename == run_id or filename.startswith(run_id + "."):
                    return run_id

        # Fallback: for run files, filename IS the run_id
        # For eval files without metadata, we can't reliably extract run_id
        # since run_id may contain dots
        return filename

    def _should_include_file(self, filename: str) -> bool:
        """Check if file should be included based on priority filter.

        Returns True if no filter is set, or if file matches the filter.
        """
        if not self.config.priority_filter:
            return True

        # Extract run_id from filename (handles dots in team/run/judge)
        run_id = self._extract_run_id_from_filename(filename)
        if not run_id:
            return False

        # Look up in priority map
        priority = self._priority_map.get(run_id, "")
        return priority == self.config.priority_filter

    def run(self) -> PipelineStats:
        """Run the full anonymization pipeline."""
        input_dir = self.config.input_dir
        output_dir = self.config.output_dir

        print(f"Anonymizing: {input_dir} -> {output_dir}")
        print(f"Mapping DB: {self.config.mapping_db}")

        # If priority filter is set, scan metadata first to build priority map
        if self.config.priority_filter:
            meta_input = input_dir / self.config.metadata_dir
            if meta_input.exists():
                print(f"\nScanning metadata for priority filter: {self.config.priority_filter}")
                self._scan_metadata_for_priority(meta_input)
                print(f"  Found {len(self._priority_map)} runs with priority info")
            else:
                print(f"\nWarning: Priority filter set but no metadata directory found")

        # Process runs directory first (establishes run_id mappings)
        runs_input = input_dir / self.config.runs_dir
        if runs_input.exists():
            self._process_runs(runs_input, output_dir / self.config.runs_dir)

        # Process metadata directory second (can provide team_id for runs)
        meta_input = input_dir / self.config.metadata_dir
        if meta_input.exists():
            self._process_metadata(meta_input, output_dir / self.config.metadata_dir)

        # Process eval directory last (strictly lookup - all mappings should exist)
        eval_input = input_dir / self.config.eval_dir
        if eval_input.exists():
            self._process_eval(eval_input, output_dir / self.config.eval_dir)

        # Update stats
        mapping_stats = self.mapping.get_stats()
        self.stats.teams_anonymized = mapping_stats["teams"]
        self.stats.runs_anonymized = mapping_stats["runs"]
        self.stats.errors = len([
            i for i in self.errors.issues
            if i.issue_type.value in ("malformed_field", "parse_error")
        ])
        self.stats.warnings = len(self.errors.issues) - self.stats.errors

        return self.stats

    def _detect_file_format(self, file_path: Path) -> str:
        """Detect if file is JSONL or TSV based on first line.

        Returns: "jsonl" or "tsv"
        """
        import json
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    json.loads(line)
                    return "jsonl"
                except json.JSONDecodeError:
                    return "tsv"
        return "jsonl"  # Empty file, default to jsonl

    def _process_runs(self, input_dir: Path, output_dir: Path):
        """Process runs/ directory (Report JSONL or TSV ranking files)."""
        print(f"\nProcessing runs: {input_dir}")

        for task_dir in input_dir.iterdir():
            if not task_dir.is_dir():
                continue

            self._process_runs_task(task_dir, input_dir, output_dir)

    def _process_runs_task(self, task_dir: Path, input_dir: Path, output_dir: Path):
        """Process a single task directory in runs/."""
        # Set current task context for email handler
        self.report_transformer._current_task = task_dir.name

        # Track format for this task (all files in a task should be same format)
        task_format = None
        task_tsv_cols = None

        for run_file in task_dir.iterdir():
            if run_file.is_dir():
                continue

            # Check priority filter
            if not self._should_include_file(run_file.name):
                rel_path = run_file.relative_to(input_dir)
                print(f"  [filtered] {rel_path}")
                self.stats.files_filtered += 1
                continue

            rel_path = run_file.relative_to(input_dir)

            if self.config.dry_run:
                print(f"  [dry-run] Would process: {rel_path}")
                continue

            # Detect format on first file
            if task_format is None:
                task_format = self._detect_file_format(run_file)
                if task_format == "tsv":
                    # Detect TSV format and get run_id columns
                    with open(run_file, "r") as f:
                        sample_lines = f.readlines()[:20]
                    hint = detect_tsv_format(sample_lines)
                    fmt, task_tsv_cols = self._ask_tsv_format(
                        run_file, hint, sample_lines
                    )
                    if fmt != TsvFormat.UNKNOWN:
                        print(f"  (Detected {fmt.value} format for runs in {task_dir.name}/)")

            # FILENAME IS THE SOURCE OF TRUTH for run_id
            # Create mapping from filename FIRST, before processing content
            anon_filename = anonymize_filename(run_file.name, self.mapping)

            # Process based on format
            temp_output = output_dir / rel_path
            if task_format == "jsonl":
                # For JSONL, filename IS the run_id - verify content matches
                lines = self.report_transformer.transform_file(
                    run_file, temp_output, expected_run_id=run_file.name
                )
            else:
                # TSV format (ranking file)
                if task_tsv_cols:
                    # For runs/, filename is source of truth - replace content run_ids
                    # with the anonymized run_id from the filename
                    lines = self._copy_tsv_with_replaced_run_id(
                        run_file, temp_output, task_tsv_cols, anon_filename
                    )
                else:
                    # Unknown format, just copy
                    temp_output.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(run_file, temp_output)
                    lines = sum(1 for _ in open(run_file))

            self.stats.files_processed += 1
            self.stats.lines_processed += lines

            # Rename output file with anonymized name
            if anon_filename != run_file.name:
                final_output = temp_output.parent / anon_filename
                temp_output.rename(final_output)
                print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
            else:
                # This shouldn't happen since we just created the mapping above
                print(f"  {rel_path}")

    def _process_eval(self, input_dir: Path, output_dir: Path):
        """Process eval/ directory (TSV files)."""
        print(f"\nProcessing eval: {input_dir}")

        for task_dir in input_dir.iterdir():
            if not task_dir.is_dir():
                continue

            self._process_eval_task(task_dir, input_dir, output_dir)

    def _handle_unknown_eval_run_id_value(
        self,
        eval_file: Path,
        rel_path: Path,
        extracted_run_id: str,
    ) -> Optional[str]:
        """Handle eval file where extracted run_id wasn't found in mapping.

        Returns anonymized run_id or None to skip.
        """
        print(f"\nRun_id '{extracted_run_id}' not found in mapping.")
        print(f"This typically means:")
        print(f"  - This run wasn't included in runs/ directory")
        print(f"  - The judge suffix was incorrect (wrong run_id extracted)")

        options = [
            ("Skip this file", "skip"),
            ("Create mapping for this run_id", "create"),
            ("Skip entire task directory", "skip_task"),
        ]

        choice = self.ask_fn("How to proceed?", options)
        action = options[choice][1]

        if action == "skip":
            return None
        elif action == "skip_task":
            raise StopIteration("skip_task")  # Signal to skip entire task
        elif action == "create":
            # Create mapping and return anonymized run_id
            anon_run = self.mapping.get_or_create_run(extracted_run_id)
            print(f"  Created mapping: {extracted_run_id} -> {anon_run}")
            return anon_run

        return None

    def _copy_tsv_with_replaced_run_id(
        self,
        input_path: Path,
        output_path: Path,
        run_id_cols: List[int],
        replacement_run_id: str,
    ) -> int:
        """Copy TSV file, replacing all values in run_id columns with a single value.

        Args:
            input_path: Source TSV file
            output_path: Destination file
            run_id_cols: Which columns contain run_id values to replace
            replacement_run_id: The value to use for all run_id columns

        Returns:
            Number of data lines processed
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with open(input_path, "r") as fin, open(output_path, "w") as fout:
            for line in fin:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    fout.write(line)
                    continue
                parts = stripped.split()

                # Replace run_id in specified columns
                for col_idx in run_id_cols:
                    if col_idx < len(parts):
                        parts[col_idx] = replacement_run_id

                # Special case: trec_eval "runid" metric line
                # Format: topic  metric  value  where metric="runid"
                if len(parts) >= 3 and parts[1].lower() == "runid":
                    parts[2] = replacement_run_id

                fout.write("\t".join(parts) + "\n")
                count += 1
        return count

    def _process_eval_task(self, task_dir: Path, input_dir: Path, output_dir: Path):
        """Process a single task directory in eval/. May retry on user request."""
        try:
            self._process_eval_task_inner(task_dir, input_dir, output_dir)
        except StopIteration as e:
            if str(e) == "skip_task":
                # User requested to skip entire task
                task_output_dir = output_dir / task_dir.name
                if task_output_dir.exists():
                    shutil.rmtree(task_output_dir)
                print(f"  Skipped task '{task_dir.name}'")

    def _process_eval_task_inner(self, task_dir: Path, input_dir: Path, output_dir: Path):
        """Inner implementation of eval task processing."""
        # Get list of files first to allow asking about patterns on first file
        eval_files = [f for f in task_dir.iterdir() if not f.is_dir()]
        if not eval_files:
            return

        for eval_file in eval_files:
            # Check priority filter
            if not self._should_include_file(eval_file.name):
                rel_path = eval_file.relative_to(input_dir)
                print(f"  [filtered] {rel_path}")
                self.stats.files_filtered += 1
                continue

            rel_path = eval_file.relative_to(input_dir)

            # Check if we have a cached format for this task directory
            if task_dir in self._task_format_cache:
                fmt, run_id_cols = self._task_format_cache[task_dir]
            else:
                # Detect format for first file in this task
                with open(eval_file, "r") as f:
                    sample_lines = f.readlines()[:20]

                hint = detect_tsv_format(sample_lines)

                # Check for override
                override_key = str(rel_path)
                if override_key in self.config.tsv_formats:
                    fmt = self.config.tsv_formats[override_key]
                    run_id_cols = self._get_run_id_columns(fmt)
                else:
                    fmt, run_id_cols = self._ask_tsv_format(eval_file, hint, sample_lines)

                # Cache format for this task directory (even if no run_id columns, like trec_eval)
                if fmt != TsvFormat.UNKNOWN:
                    self._task_format_cache[task_dir] = (fmt, run_id_cols)
                    if self.config.interactive:
                        print(f"  (Using {fmt.value} format for all files in {task_dir.name}/)")

            if fmt == TsvFormat.UNKNOWN:
                print(f"  [skip] {rel_path} (unknown format)")
                continue

            if self.config.dry_run:
                print(f"  [dry-run] Would process: {rel_path} as {fmt.value}")
                continue

            # Get filename pattern (judge suffix) for this task - ask once per task
            if task_dir not in self._eval_filename_cache:
                judge_suffix = self._ask_eval_filename_pattern(task_dir, eval_file)
                self._eval_filename_cache[task_dir] = judge_suffix

                # Handle SKIP - skip the entire task
                if judge_suffix == "SKIP":
                    print(f"  Skipping task '{task_dir.name}'")
                    return

                if judge_suffix and judge_suffix != "MANUAL" and self.config.interactive:
                    print(f"  (Using filename pattern '{{run_id}}{judge_suffix}' for {task_dir.name}/)")
            else:
                judge_suffix = self._eval_filename_cache[task_dir]
                # Already cached SKIP - return
                if judge_suffix == "SKIP":
                    return

            # Extract run_id from filename
            if judge_suffix == "MANUAL":
                # Ask user for run_id per file
                extracted_run_id = self._ask_manual_run_id(eval_file.name)
                if extracted_run_id is None:
                    print(f"  [skip] {rel_path} (no run_id provided)")
                    continue
            else:
                # Use judge suffix pattern
                extracted_run_id = self._extract_run_id_from_eval_filename(eval_file.name, judge_suffix)

            # For known formats without run_id columns (like trec_eval),
            # copy the file but still anonymize the filename
            if not run_id_cols:
                if extracted_run_id is None:
                    print(f"  [skip] {rel_path} (can't extract run_id from filename)")
                    continue

                # Look up run_id in mapping
                anon_run = self.mapping.get_run(extracted_run_id)
                if anon_run is None:
                    print(f"  [WARNING] {rel_path}: run_id '{extracted_run_id}' not found in mapping")
                    if self.config.interactive:
                        anon_run = self._handle_unknown_eval_run_id_value(
                            eval_file, rel_path, extracted_run_id
                        )
                    if anon_run is None:
                        print(f"  [skip] {rel_path} (unknown run_id)")
                        continue

                temp_output = output_dir / rel_path

                # Copy file, replacing 'runid' metric values with anonymized run_id
                lines = self._copy_trec_eval_with_anon_runid(eval_file, temp_output, anon_run)
                self.stats.files_processed += 1
                self.stats.lines_processed += lines

                # Construct anonymized filename
                if judge_suffix == "MANUAL":
                    # Derive suffix from original filename and manual run_id
                    file_suffix = self._get_filename_suffix(eval_file.name, extracted_run_id)
                else:
                    file_suffix = judge_suffix
                anon_filename = anon_run + file_suffix
                final_output = temp_output.parent / anon_filename
                temp_output.rename(final_output)
                print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
                continue

            # Process content (anonymize run_id columns)
            temp_output = output_dir / rel_path

            # For eval files: ALWAYS replace run_id columns with filename's anonymized run_id
            # (filename is the source of truth, ignore whatever values are in the content)
            if extracted_run_id and run_id_cols:
                anon_run = self.mapping.get_run(extracted_run_id)
                if anon_run:
                    lines = self._copy_tsv_with_replaced_run_id(
                        eval_file, temp_output, run_id_cols, anon_run
                    )
                    self.stats.files_processed += 1
                    self.stats.lines_processed += lines

                    # Anonymize filename
                    if judge_suffix:
                        if judge_suffix == "MANUAL":
                            file_suffix = self._get_filename_suffix(eval_file.name, extracted_run_id)
                        else:
                            file_suffix = judge_suffix
                        anon_filename = anon_run + file_suffix
                        final_output = temp_output.parent / anon_filename
                        temp_output.rename(final_output)
                        print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
                    else:
                        print(f"  {rel_path}")
                    continue
                else:
                    print(f"  [ERROR] Filename run_id '{extracted_run_id}' not in mapping")
                    continue

            # Fallback: no extracted_run_id or no run_id_cols - try to anonymize using content values
            lines, unknown_run_ids = self.tsv_transformer.transform_file(
                eval_file, temp_output, run_id_cols,
                create_if_missing=False,
            )

            if unknown_run_ids:
                print(f"  [WARNING] Unknown run_id(s) in content: {unknown_run_ids[:3]}")

            self.stats.files_processed += 1
            self.stats.lines_processed += lines

            # Try to anonymize filename if we have a pattern
            if extracted_run_id is not None and judge_suffix is not None:
                anon_run = self.mapping.get_run(extracted_run_id)
                if anon_run is not None:
                    if judge_suffix == "MANUAL":
                        file_suffix = self._get_filename_suffix(eval_file.name, extracted_run_id)
                    else:
                        file_suffix = judge_suffix
                    anon_filename = anon_run + file_suffix
                    final_output = temp_output.parent / anon_filename
                    temp_output.rename(final_output)
                    print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
                else:
                    print(f"  {rel_path} (filename run_id '{extracted_run_id}' not in mapping)")
            else:
                print(f"  {rel_path}")

    def _process_metadata(self, input_dir: Path, output_dir: Path):
        """Process metadata/ directory (Metadata JSONL files)."""
        import json

        print(f"\nProcessing metadata: {input_dir}")

        for task_dir in input_dir.iterdir():
            if not task_dir.is_dir():
                continue

            # Set current task context for email handler
            self.metadata_transformer._current_task = task_dir.name

            for meta_file in task_dir.iterdir():
                if meta_file.is_dir():
                    continue

                rel_path = meta_file.relative_to(input_dir)

                if self.config.dry_run:
                    print(f"  [dry-run] Would process: {rel_path}")
                    continue

                # If priority filter is set, do line-by-line filtering
                if self.config.priority_filter:
                    output_file = output_dir / rel_path
                    output_file.parent.mkdir(parents=True, exist_ok=True)
                    lines_written = 0
                    lines_filtered = 0

                    with open(meta_file, "r") as fin, open(output_file, "w") as fout:
                        for line_num, line in enumerate(fin, 1):
                            line = line.strip()
                            if not line:
                                continue

                            # Check if this line matches priority filter
                            try:
                                data = json.loads(line)
                                run = data.get("runtag", "")

                                if not self._should_include_file(run):
                                    lines_filtered += 1
                                    continue
                            except json.JSONDecodeError:
                                pass  # Let transformer handle parse errors

                            # Transform and write
                            result = self.metadata_transformer.transform_line(
                                line, meta_file, line_num
                            )
                            if result:
                                fout.write(result + "\n")
                                lines_written += 1

                    self.stats.files_processed += 1
                    self.stats.lines_processed += lines_written
                    if lines_filtered > 0:
                        print(f"  {rel_path} ({lines_filtered} lines filtered)")
                    else:
                        print(f"  {rel_path}")
                else:
                    output_file = output_dir / rel_path
                    lines = self.metadata_transformer.transform_file(meta_file, output_file)
                    self.stats.files_processed += 1
                    self.stats.lines_processed += lines
                    print(f"  {rel_path}")

    def write_error_report(self, output_path: Path):
        """Write error/warning report to file."""
        self.errors.write_report(output_path)

    def print_summary(self):
        """Print pipeline summary."""
        print("\n" + "=" * 50)
        print("Anonymization Complete")
        print("=" * 50)
        print(f"Files processed:    {self.stats.files_processed}")
        print(f"Lines processed:    {self.stats.lines_processed}")
        print(f"Teams anonymized:   {self.stats.teams_anonymized}")
        print(f"Runs anonymized:    {self.stats.runs_anonymized}")
        if self.stats.files_filtered > 0:
            print(f"Files filtered:     {self.stats.files_filtered}")
        print(f"Errors:             {self.stats.errors}")
        print(f"Warnings:           {self.stats.warnings}")

        if self.errors.issues:
            self.errors.print_summary()

    def close(self):
        """Clean up resources."""
        self.mapping.close()
        self.repairs.close()
