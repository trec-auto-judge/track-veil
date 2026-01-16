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
    anonymize_eval_filename,
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
        elif fmt == TsvFormat.QRELS:
            return [5]
        else:
            return []

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

        # Process runs directory
        runs_input = input_dir / self.config.runs_dir
        if runs_input.exists():
            self._process_runs(runs_input, output_dir / self.config.runs_dir)

        # Process eval directory
        eval_input = input_dir / self.config.eval_dir
        if eval_input.exists():
            self._process_eval(eval_input, output_dir / self.config.eval_dir)

        # Process metadata directory
        meta_input = input_dir / self.config.metadata_dir
        if meta_input.exists():
            self._process_metadata(meta_input, output_dir / self.config.metadata_dir)

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
        unrenamed_files: List[Path] = []

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

            # Process based on format
            temp_output = output_dir / rel_path
            if task_format == "jsonl":
                lines = self.report_transformer.transform_file(run_file, temp_output)
            else:
                # TSV format (ranking file)
                if task_tsv_cols:
                    lines = self.tsv_transformer.transform_file(
                        run_file, temp_output, task_tsv_cols
                    )
                else:
                    # Unknown format, just copy
                    temp_output.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(run_file, temp_output)
                    lines = sum(1 for _ in open(run_file))

            self.stats.files_processed += 1
            self.stats.lines_processed += lines

            # Rename output file with anonymized name
            anon_filename = anonymize_filename(run_file.name, self.mapping)
            if anon_filename != run_file.name:
                final_output = temp_output.parent / anon_filename
                temp_output.rename(final_output)
                print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
            else:
                # For JSONL files, unrenamed is unexpected (mapping should be created)
                # For TSV files, unrenamed means filename didn't match any run_id
                if task_format == "jsonl":
                    print(f"  {rel_path}")
                else:
                    print(f"  [WARNING] {rel_path} - filename not anonymized!")
                    unrenamed_files.append(run_file)

        # Check for unrenamed files (only relevant for TSV run files)
        if unrenamed_files and self.config.interactive:
            self._handle_unrenamed_run_files(task_dir, input_dir, output_dir, unrenamed_files)

    def _handle_unrenamed_run_files(
        self,
        task_dir: Path,
        input_dir: Path,
        output_dir: Path,
        unrenamed_files: List[Path],
    ):
        """Handle run files that weren't renamed."""
        print(f"\n{'='*60}")
        print(f"WARNING: {len(unrenamed_files)} run file(s) in task '{task_dir.name}' were not anonymized!")
        print(f"This may indicate:")
        print(f"  - Filename doesn't match the run_id in the file content")
        print(f"  - Wrong TSV format was selected")
        print(f"\nAffected files:")
        for f in unrenamed_files[:5]:
            print(f"  - {f.name}")
        if len(unrenamed_files) > 5:
            print(f"  ... and {len(unrenamed_files) - 5} more")

        options = [
            ("Continue anyway", "continue"),
            ("Delete output for this task and skip", "skip"),
        ]

        choice = self.ask_fn("How to proceed?", options)
        action = options[choice][1]

        if action == "skip":
            task_output_dir = output_dir / task_dir.name
            if task_output_dir.exists():
                shutil.rmtree(task_output_dir)
            print(f"  Skipped task '{task_dir.name}'")

    def _process_eval(self, input_dir: Path, output_dir: Path):
        """Process eval/ directory (TSV files)."""
        print(f"\nProcessing eval: {input_dir}")

        for task_dir in input_dir.iterdir():
            if not task_dir.is_dir():
                continue

            self._process_eval_task(task_dir, input_dir, output_dir)

    def _process_eval_task(self, task_dir: Path, input_dir: Path, output_dir: Path):
        """Process a single task directory in eval/. May retry on user request."""
        unrenamed_files: List[Path] = []

        for eval_file in task_dir.iterdir():
            if eval_file.is_dir():
                continue

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

            # For known formats without run_id columns (like trec_eval),
            # copy the file but still anonymize the filename
            if not run_id_cols:
                temp_output = output_dir / rel_path
                temp_output.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(eval_file, temp_output)
                self.stats.files_processed += 1

                # Eval filename format: {run_id}.{judge}
                anon_filename = anonymize_eval_filename(eval_file.name, self.mapping)
                if anon_filename != eval_file.name:
                    final_output = temp_output.parent / anon_filename
                    temp_output.rename(final_output)
                    print(f"  {rel_path} -> {rel_path.parent / anon_filename} (content unchanged)")
                else:
                    print(f"  [WARNING] {rel_path} - filename not anonymized!")
                    unrenamed_files.append(eval_file)
                continue

            # Process content (anonymize run_id columns)
            temp_output = output_dir / rel_path
            lines = self.tsv_transformer.transform_file(
                eval_file, temp_output, run_id_cols
            )
            self.stats.files_processed += 1
            self.stats.lines_processed += lines

            # Rename output file (eval format: {run_id}.{judge})
            anon_filename = anonymize_eval_filename(eval_file.name, self.mapping)
            if anon_filename != eval_file.name:
                final_output = temp_output.parent / anon_filename
                temp_output.rename(final_output)
                print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
            else:
                print(f"  [WARNING] {rel_path} - filename not anonymized!")
                unrenamed_files.append(eval_file)

        # Check for unrenamed files and offer retry
        if unrenamed_files and self.config.interactive:
            self._handle_unrenamed_files(task_dir, input_dir, output_dir, unrenamed_files)

    def _handle_unrenamed_files(
        self,
        task_dir: Path,
        input_dir: Path,
        output_dir: Path,
        unrenamed_files: List[Path],
    ):
        """Handle files that weren't renamed - warn user and offer retry."""
        print(f"\n{'='*60}")
        print(f"WARNING: {len(unrenamed_files)} file(s) in task '{task_dir.name}' were not anonymized!")
        print(f"This may indicate:")
        print(f"  - Wrong TSV format was selected")
        print(f"  - Input filenames don't match expected {{run_id}}.{{judge}} pattern")
        print(f"  - Run files weren't processed first (mappings not established)")
        print(f"\nAffected files:")
        for f in unrenamed_files[:5]:  # Show first 5
            print(f"  - {f.name}")
        if len(unrenamed_files) > 5:
            print(f"  ... and {len(unrenamed_files) - 5} more")

        options = [
            ("Continue anyway (files copied but not anonymized)", "continue"),
            ("Retry this task with different format selection", "retry"),
            ("Delete output for this task and skip", "skip"),
        ]

        choice = self.ask_fn("How to proceed?", options)
        action = options[choice][1]

        if action == "retry":
            # Clear cache and output for this task
            if task_dir in self._task_format_cache:
                del self._task_format_cache[task_dir]

            # Remove output directory for this task
            task_output_dir = output_dir / task_dir.name
            if task_output_dir.exists():
                shutil.rmtree(task_output_dir)

            print(f"\nRetrying task '{task_dir.name}'...")
            self._process_eval_task(task_dir, input_dir, output_dir)

        elif action == "skip":
            # Remove output directory for this task
            task_output_dir = output_dir / task_dir.name
            if task_output_dir.exists():
                shutil.rmtree(task_output_dir)
            print(f"  Skipped task '{task_dir.name}'")

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
