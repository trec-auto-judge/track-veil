"""Orchestrates the anonymization pipeline.

Directory structure expected:
    {input}/
        runs/{task}/{run_id}        # Report JSONL
        eval/{task}/{run_id}.{judge} # TSV eval results
        metadata/{task}/*.jl        # Metadata JSONL
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from .mapping import MappingStore
from .repairs import RepairStore
from .errors import ErrorCollector
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

        # Initialize stores
        self.mapping = MappingStore(config.mapping_db)
        self.repairs = RepairStore(config.mapping_db)
        self.errors = ErrorCollector()

        # Initialize transformers
        self.report_transformer = ReportTransformer(
            self.mapping,
            self.repairs,
            self.errors,
            interactive=config.interactive,
            ask_fn=self.ask_fn,
        )
        self.metadata_transformer = MetadataTransformer(
            self.mapping,
            self.errors,
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
    ) -> Tuple[TsvFormat, List[int]]:
        """Ask user to confirm TSV format."""
        if not self.config.interactive:
            # Use hint
            return hint.likely_format, hint.run_id_columns

        print(f"\nDetected TSV format for {file_path.name}:")
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

    def _scan_metadata_for_priority(self, meta_dir: Path):
        """Scan metadata files to build priority map.

        Maps run identifiers to their priority values.
        Keys are stored as "{team}-{run}" to match filenames.
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
                                team = data.get("org", "")
                                run = data.get("runtag", "")
                                priority = data.get("std-priority", "")

                                if team and run:
                                    # Store with team-run key to match filenames
                                    key = f"{team}-{run}"
                                    self._priority_map[key] = priority
                            except json.JSONDecodeError:
                                continue
                except IOError:
                    continue

    def _extract_run_id_from_filename(self, filename: str) -> Optional[str]:
        """Extract the {team}-{run} part from a filename.

        Filenames have format: {team}-{run} or {team}-{run}.{judge}
        where team, run, and judge can all contain dots.

        Uses known run identifiers from metadata to find the correct split.
        """
        # If we have metadata, match against known identifiers
        if self._priority_map:
            # Sort by length descending to match longest first
            known_ids = sorted(self._priority_map.keys(), key=len, reverse=True)
            for run_id in known_ids:
                if filename == run_id or filename.startswith(run_id + "."):
                    return run_id

        # Fallback: assume no dots in team-run (first part before dot)
        # This handles cases where metadata isn't available
        if "." in filename:
            return filename.split(".")[0]
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

    def _process_runs(self, input_dir: Path, output_dir: Path):
        """Process runs/ directory (Report JSONL files)."""
        print(f"\nProcessing runs: {input_dir}")

        for task_dir in input_dir.iterdir():
            if not task_dir.is_dir():
                continue

            for run_file in task_dir.iterdir():
                if run_file.is_dir():
                    continue

                # Check priority filter
                if not self._should_include_file(run_file.name):
                    rel_path = run_file.relative_to(input_dir)
                    print(f"  [filtered] {rel_path}")
                    self.stats.files_filtered += 1
                    continue

                # Determine output filename (anonymize after processing content)
                rel_path = run_file.relative_to(input_dir)

                if self.config.dry_run:
                    print(f"  [dry-run] Would process: {rel_path}")
                    continue

                # Process content first to populate mappings
                temp_output = output_dir / rel_path
                lines = self.report_transformer.transform_file(run_file, temp_output)
                self.stats.files_processed += 1
                self.stats.lines_processed += lines

                # Now rename output file with anonymized name
                anon_filename = anonymize_filename(run_file.name, self.mapping)
                if anon_filename != run_file.name:
                    final_output = temp_output.parent / anon_filename
                    temp_output.rename(final_output)
                    print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
                else:
                    print(f"  {rel_path}")

    def _process_eval(self, input_dir: Path, output_dir: Path):
        """Process eval/ directory (TSV files)."""
        print(f"\nProcessing eval: {input_dir}")

        for task_dir in input_dir.iterdir():
            if not task_dir.is_dir():
                continue

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

                # Detect or lookup format
                with open(eval_file, "r") as f:
                    sample_lines = f.readlines()[:20]

                hint = detect_tsv_format(sample_lines)

                # Check for override
                override_key = str(rel_path)
                if override_key in self.config.tsv_formats:
                    fmt = self.config.tsv_formats[override_key]
                    run_id_cols = self._get_run_id_columns(fmt)
                else:
                    fmt, run_id_cols = self._ask_tsv_format(eval_file, hint)

                if fmt == TsvFormat.UNKNOWN or not run_id_cols:
                    print(f"  [skip] {rel_path} (no run_id columns)")
                    continue

                if self.config.dry_run:
                    print(f"  [dry-run] Would process: {rel_path} as {fmt.value}")
                    continue

                # Process content
                temp_output = output_dir / rel_path
                lines = self.tsv_transformer.transform_file(
                    eval_file, temp_output, run_id_cols
                )
                self.stats.files_processed += 1
                self.stats.lines_processed += lines

                # Rename output file
                anon_filename = anonymize_filename(eval_file.name, self.mapping)
                if anon_filename != eval_file.name:
                    final_output = temp_output.parent / anon_filename
                    temp_output.rename(final_output)
                    print(f"  {rel_path} -> {rel_path.parent / anon_filename}")
                else:
                    print(f"  {rel_path}")

    def _process_metadata(self, input_dir: Path, output_dir: Path):
        """Process metadata/ directory (Metadata JSONL files)."""
        import json

        print(f"\nProcessing metadata: {input_dir}")

        for task_dir in input_dir.iterdir():
            if not task_dir.is_dir():
                continue

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
                                team = data.get("org", "")
                                run = data.get("runtag", "")
                                key = f"{team}-{run}"

                                if not self._should_include_file(key):
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
