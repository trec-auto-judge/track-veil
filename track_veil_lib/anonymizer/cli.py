"""Command-line interface for track data anonymization."""

import click
from pathlib import Path
from typing import List, Optional

from .mapping import MappingStore
from .pipeline import AnonymizationPipeline, PipelineConfig


@click.group()
@click.version_option(version="0.4.5")
def cli():
    """Track Veil - Data Anonymization Tool.

    Anonymize team and run identifiers in track datasets while preserving
    data structure for sharing.
    """
    pass


@cli.command()
@click.option(
    "--input", "-i",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Input directory containing track data",
)
@click.option(
    "--output", "-o",
    "output_dir",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Output directory for anonymized data",
)
@click.option(
    "--mapping", "-m",
    "mapping_db",
    type=click.Path(dir_okay=False, path_type=Path),
    default="mapping.db",
    help="SQLite database for storing mappings (default: mapping.db)",
)
@click.option(
    "--runs-dir",
    default="runs",
    help="Name of runs subdirectory (default: runs)",
)
@click.option(
    "--eval-dir",
    default="eval",
    help="Name of eval subdirectory (default: eval)",
)
@click.option(
    "--metadata-dir",
    default="metadata",
    help="Name of metadata subdirectory (default: metadata)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without making changes",
)
@click.option(
    "--error-report",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Write error report to this file (default: {output}/errors.jsonl)",
)
@click.option(
    "--priority", "-p",
    "priority_filter",
    default=None,
    help='Only process runs with this priority (e.g., "1 (top)")',
)
@click.option(
    "--save-decisions",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Save interactive decisions to YAML file for reproducible runs",
)
@click.option(
    "--load-decisions",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Load decisions from YAML file (skips interactive prompts)",
)
def anonymize(
    input_dir: Path,
    output_dir: Path,
    mapping_db: Path,
    runs_dir: str,
    eval_dir: str,
    metadata_dir: str,
    dry_run: bool,
    error_report: Optional[Path],
    priority_filter: Optional[str],
    save_decisions: Optional[Path],
    load_decisions: Optional[Path],
):
    """Anonymize a track dataset.

    Replaces team and run identifiers with random pseudonyms.
    Mappings are stored in a SQLite database for consistency and
    potential de-anonymization.

    Example:
        trec-anon anonymize -i data/raw -o data/anon -m mapping.db
    """
    config = PipelineConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        mapping_db=mapping_db,
        runs_dir=runs_dir,
        eval_dir=eval_dir,
        metadata_dir=metadata_dir,
        dry_run=dry_run,
        priority_filter=priority_filter,
        save_decisions=save_decisions,
        load_decisions=load_decisions,
    )

    pipeline = AnonymizationPipeline(config)
    try:
        pipeline.run()
        pipeline.print_summary()

        # Write error report
        if error_report is None:
            error_report = output_dir / "errors.jsonl"
        if pipeline.errors.issues:
            pipeline.write_error_report(error_report)
            click.echo(f"\nError report written to: {error_report}")
    finally:
        pipeline.close()


@cli.command("show-mapping")
@click.option(
    "--mapping", "-m",
    "mapping_db",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="SQLite database containing mappings",
)
@click.option(
    "--format", "-f",
    "output_format",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format (default: table)",
)
def show_mapping(mapping_db: Path, output_format: str):
    """Show current anonymization mappings.

    Example:
        trec-anon show-mapping -m mapping.db
        trec-anon show-mapping -m mapping.db -f json
    """
    import json

    with MappingStore(mapping_db) as store:
        teams = store.get_all_team_mappings()
        runs = store.get_all_run_mappings()
        stats = store.get_stats()

        if output_format == "json":
            data = {
                "teams": teams,
                "runs": runs,
                "stats": stats,
            }
            click.echo(json.dumps(data, indent=2))

        elif output_format == "csv":
            click.echo("type,original,anonymized")
            for orig, anon in sorted(teams.items()):
                click.echo(f"team,{orig},{anon}")
            for orig, anon in sorted(runs.items()):
                click.echo(f"run,{orig},{anon}")

        else:  # table
            click.echo("\nTeam Mappings:")
            click.echo("-" * 40)
            if teams:
                for orig, anon in sorted(teams.items()):
                    click.echo(f"  {orig:20} -> {anon}")
            else:
                click.echo("  (none)")

            click.echo("\nRun Mappings:")
            click.echo("-" * 40)
            if runs:
                for orig, anon in sorted(runs.items()):
                    click.echo(f"  {orig:20} -> {anon}")
            else:
                click.echo("  (none)")

            click.echo("\nStatistics:")
            click.echo("-" * 40)
            click.echo(f"  Teams mapped:      {stats['teams']}")
            click.echo(f"  Runs mapped:       {stats['runs']}")
            click.echo(f"  Fingerprints:      {stats['fingerprints']}")
            click.echo(f"  Teams remaining:   {stats['teams_remaining']}")
            click.echo(f"  Runs remaining:    {stats['runs_remaining']}")


@cli.command("reverse-lookup")
@click.option(
    "--mapping", "-m",
    "mapping_db",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="SQLite database containing mappings",
)
@click.argument("anonymized_value")
def reverse_lookup(mapping_db: Path, anonymized_value: str):
    """Look up the original value for an anonymized identifier.

    Example:
        trec-anon reverse-lookup -m mapping.db Fez
        trec-anon reverse-lookup -m mapping.db Fez-07
    """
    with MappingStore(mapping_db) as store:
        teams = store.get_all_team_mappings()
        runs = store.get_all_run_mappings()

        # Reverse the mappings
        teams_rev = {v: k for k, v in teams.items()}
        runs_rev = {v: k for k, v in runs.items()}

        # Check if it's a compound value (team-run)
        for sep in ["-", "_", "."]:
            if sep in anonymized_value:
                parts = anonymized_value.split(sep, 1)
                if len(parts) == 2:
                    anon_team, anon_run = parts
                    orig_team = teams_rev.get(anon_team, "???")
                    orig_run = runs_rev.get(anon_run, "???")
                    click.echo(f"{anonymized_value} -> {orig_team}{sep}{orig_run}")
                    return

        # Check teams
        if anonymized_value in teams_rev:
            click.echo(f"{anonymized_value} (team) -> {teams_rev[anonymized_value]}")
            return

        # Check runs
        if anonymized_value in runs_rev:
            click.echo(f"{anonymized_value} (run) -> {runs_rev[anonymized_value]}")
            return

        click.echo(f"No mapping found for: {anonymized_value}", err=True)


@cli.command("recover-mapping")
@click.option(
    "--mapping", "-m",
    "mapping_db",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="SQLite database containing fingerprints from anonymization",
)
@click.option(
    "--input", "-i",
    "input_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Anonymized report file (JSONL) or directory",
)
@click.option(
    "--output", "-o",
    "output_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output file for recovered mappings (default: stdout)",
)
@click.option(
    "--format", "-f",
    "output_format",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--glob", "-g",
    "file_glob",
    default="*",
    help="Glob pattern(s) for files, comma-separated (default: '*' for all files)",
)
@click.option(
    "--source", "-s",
    "source_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=None,
    help="Directory with freshly anonymized files to export FROM (required with --export)",
)
@click.option(
    "--export", "-e",
    "export_dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Export matched files from --source to this directory",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Show debug output",
)
@click.option(
    "--debug-topic",
    "debug_topic",
    default=None,
    help="Debug a specific topic: compare unmatched -i text against -s files",
)
def recover_mapping(
    mapping_db: Path,
    input_path: Path,
    output_path: Optional[Path],
    output_format: str,
    file_glob: str,
    source_dir: Optional[Path],
    export_dir: Optional[Path],
    verbose: bool,
    debug_topic: Optional[str],
):
    """Recover original team/run mappings from anonymized reports.

    Uses content fingerprints stored during anonymization to match
    anonymized reports back to their original identifiers.

    Example:
        trec-anon recover-mapping -m mapping.db -i anon_reports.jsonl
        trec-anon recover-mapping -m mapping.db -i anon_data/runs/ -f csv -o mappings.csv
    """
    import json as json_module
    from io import StringIO
    from typing import Dict, List

    from .mapping import MappingStore, compute_report_fingerprint
    from autojudge_base.report import Report

    with MappingStore(mapping_db) as store:
        results: List[Dict] = []
        unmatched: List[Dict] = []

        # Check fingerprint count in DB
        stats = store.get_stats()
        if verbose:
            click.echo(f"[DEBUG] Database: {mapping_db}", err=True)
            click.echo(f"[DEBUG] Fingerprints in DB: {stats['fingerprints']}", err=True)

        if stats['fingerprints'] == 0:
            click.echo("Warning: No fingerprints stored in database. "
                      "Was anonymization run with fingerprint storage enabled?", err=True)

        # Collect files using glob pattern(s)
        if input_path.is_dir():
            files: List[Path] = []
            for pattern in file_glob.split(","):
                pattern = pattern.strip()
                files.extend(input_path.rglob(pattern))
            files = sorted(set(files))  # dedupe and sort
        else:
            files = [input_path]

        if verbose:
            click.echo(f"[DEBUG] Input path: {input_path}", err=True)
            click.echo(f"[DEBUG] Glob pattern: {file_glob}", err=True)
            click.echo(f"[DEBUG] Files found: {len(files)}", err=True)
            for f in files[:10]:
                click.echo(f"[DEBUG]   - {f}", err=True)
            if len(files) > 10:
                click.echo(f"[DEBUG]   ... and {len(files) - 10} more", err=True)

        if not files:
            click.echo(f"No files found matching pattern '{file_glob}' in {input_path}", err=True)
            return

        lines_processed = 0
        lines_parsed = 0
        lines_skipped_json = 0
        lines_skipped_model = 0
        lines_skipped_empty = 0
        matched_files: set[Path] = set()

        for file_path in files:
            if verbose:
                click.echo(f"[DEBUG] Processing file: {file_path}", err=True)

            file_lines = 0
            file_parsed = 0
            with open(file_path, "rt", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    file_lines += 1
                    lines_processed += 1

                    try:
                        data = json_module.loads(line)
                    except json_module.JSONDecodeError as e:
                        lines_skipped_json += 1
                        if verbose and lines_skipped_json <= 3:
                            click.echo(f"[DEBUG]   Line {line_num}: JSON decode error: {e}", err=True)
                        continue

                    # Parse into Report model for correct field resolution
                    try:
                        report = Report.model_validate(data)
                        topic_id = report.metadata.topic_id
                        report_text = report.get_text()
                        anon_team = report.metadata.team_id or ""
                        anon_run = report.metadata.run_id or ""
                    except Exception as e:
                        lines_skipped_model += 1
                        if verbose and lines_skipped_model <= 3:
                            click.echo(f"[DEBUG]   Line {line_num}: Report model error: {e}", err=True)
                        continue

                    if not (topic_id and report_text):
                        lines_skipped_empty += 1
                        if verbose and lines_skipped_empty <= 3:
                            click.echo(f"[DEBUG]   Line {line_num}: Missing topic_id or report_text", err=True)
                        continue

                    lines_parsed += 1
                    file_parsed += 1

                    # Compute fingerprint and lookup
                    fingerprint = compute_report_fingerprint(topic_id, report_text)
                    match = store.lookup_fingerprint(fingerprint)

                    if match:
                        matched_files.add(file_path)
                        results.append({
                            "file": str(file_path),
                            "line": line_num,
                            "topic_id": topic_id,
                            "anon_team": anon_team,
                            "anon_run": anon_run,
                            "original_team": match["original_team"],
                            "original_run": match["original_run"],
                        })
                    else:
                        unmatched.append({
                            "file": str(file_path),
                            "line": line_num,
                            "topic_id": topic_id,
                            "anon_team": anon_team,
                            "anon_run": anon_run,
                            "fingerprint": fingerprint,
                            "report_text_preview": report_text[:100] if report_text else "",
                        })

            if verbose:
                click.echo(f"[DEBUG]   Lines: {file_lines}, Parsed: {file_parsed}", err=True)

        if verbose:
            click.echo(f"[DEBUG] Summary:", err=True)
            click.echo(f"[DEBUG]   Total lines processed: {lines_processed}", err=True)
            click.echo(f"[DEBUG]   Lines parsed as Report: {lines_parsed}", err=True)
            click.echo(f"[DEBUG]   Skipped (JSON error): {lines_skipped_json}", err=True)
            click.echo(f"[DEBUG]   Skipped (Model error): {lines_skipped_model}", err=True)
            click.echo(f"[DEBUG]   Skipped (empty fields): {lines_skipped_empty}", err=True)
            click.echo(f"[DEBUG]   Matched fingerprints: {len(results)}", err=True)
            click.echo(f"[DEBUG]   Unmatched fingerprints: {len(unmatched)}", err=True)
            click.echo(f"[DEBUG]   Files with matches: {len(matched_files)}", err=True)

            # Show details for unmatched entries
            if unmatched:
                click.echo(f"[DEBUG] Unmatched entry details:", err=True)
                seen_topics: set[str] = set()
                for i, u in enumerate(unmatched[:20]):  # limit to first 20
                    click.echo(f"[DEBUG]   [{i+1}] file={u['file']}", err=True)
                    click.echo(f"[DEBUG]       topic_id={repr(u['topic_id'])}", err=True)
                    click.echo(f"[DEBUG]       anon_run={u['anon_run']}", err=True)
                    click.echo(f"[DEBUG]       fingerprint={u['fingerprint'][:16]}...", err=True)
                    click.echo(f"[DEBUG]       text_preview={repr(u['report_text_preview'])}", err=True)

                    # Show what's in DB for this topic_id (once per topic)
                    topic_id = u['topic_id']
                    if topic_id not in seen_topics:
                        seen_topics.add(topic_id)
                        db_entries = store.get_fingerprints_by_topic(topic_id)
                        if db_entries:
                            click.echo(f"[DEBUG]       DB has {len(db_entries)} entries for topic_id={repr(topic_id)}:", err=True)
                            for db_e in db_entries[:3]:  # show first 3
                                click.echo(f"[DEBUG]         - fp={db_e['fingerprint'][:16]}... run={db_e['original_run']}", err=True)
                            if len(db_entries) > 3:
                                click.echo(f"[DEBUG]         ... and {len(db_entries) - 3} more", err=True)
                        else:
                            click.echo(f"[DEBUG]       DB has NO entries for topic_id={repr(topic_id)}", err=True)

                if len(unmatched) > 20:
                    click.echo(f"[DEBUG]   ... and {len(unmatched) - 20} more unmatched", err=True)

        # Debug topic: compare unmatched -i text against -s files
        if debug_topic and source_dir:
            click.echo(f"\n[DEBUG-TOPIC] Analyzing topic_id={repr(debug_topic)}", err=True)

            # Get unmatched entries for this topic with full text
            unmatched_for_topic: list[dict] = []
            for file_path in files:
                try:
                    with open(file_path, "rt", encoding="utf-8") as fh:
                        for line_num, line in enumerate(fh, 1):
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                data = json_module.loads(line)
                                report = Report.model_validate(data)
                                if report.metadata.topic_id == debug_topic:
                                    report_text = report.get_text()
                                    fp = compute_report_fingerprint(debug_topic, report_text)
                                    if not store.lookup_fingerprint(fp):
                                        unmatched_for_topic.append({
                                            "file": str(file_path),
                                            "anon_run": report.metadata.run_id or "",
                                            "text": report_text,
                                            "words": set(report_text.lower().split()),
                                        })
                            except Exception:
                                continue
                except Exception:
                    continue

            click.echo(f"[DEBUG-TOPIC] Found {len(unmatched_for_topic)} unmatched -i entries for topic", err=True)

            # Scan source files for this topic
            source_entries: list[dict] = []
            source_files_list: list[Path] = []
            for pattern in file_glob.split(","):
                pattern = pattern.strip()
                source_files_list.extend(source_dir.rglob(pattern))
            source_files_list = sorted(set(source_files_list))

            for src_path in source_files_list:
                try:
                    with open(src_path, "rt", encoding="utf-8") as fh:
                        for line in fh:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                data = json_module.loads(line)
                                report = Report.model_validate(data)
                                if report.metadata.topic_id == debug_topic:
                                    report_text = report.get_text()
                                    source_entries.append({
                                        "file": str(src_path),
                                        "anon_run": report.metadata.run_id or "",
                                        "text": report_text,
                                        "words": set(report_text.lower().split()),
                                    })
                            except Exception:
                                continue
                except Exception:
                    continue

            click.echo(f"[DEBUG-TOPIC] Found {len(source_entries)} -s entries for topic", err=True)

            # Compare each unmatched -i entry against -s entries by word overlap
            for i, u_entry in enumerate(unmatched_for_topic[:5]):  # limit to 5
                click.echo(f"\n[DEBUG-TOPIC] Unmatched -i [{i+1}]: {u_entry['anon_run']}", err=True)
                click.echo(f"[DEBUG-TOPIC]   text (first 80): {repr(u_entry['text'][:80])}", err=True)

                # Find best matches by Jaccard similarity
                similarities: list[tuple[float, dict]] = []
                for s_entry in source_entries:
                    intersection = len(u_entry['words'] & s_entry['words'])
                    union = len(u_entry['words'] | s_entry['words'])
                    jaccard = intersection / union if union > 0 else 0.0
                    similarities.append((jaccard, s_entry))

                similarities.sort(key=lambda x: -x[0])  # descending

                click.echo(f"[DEBUG-TOPIC]   Best matches in -s:", err=True)
                for sim, s_entry in similarities[:3]:
                    click.echo(f"[DEBUG-TOPIC]     {sim:.2%} overlap: {s_entry['anon_run']}", err=True)
                    click.echo(f"[DEBUG-TOPIC]       text (first 80): {repr(s_entry['text'][:80])}", err=True)

                    # Show side-by-side diff for high-overlap matches
                    if sim >= 0.8:
                        import subprocess
                        import tempfile
                        import textwrap

                        u_text = u_entry['text']
                        s_text = s_entry['text']

                        click.echo(f"[DEBUG-TOPIC]       --- Side-by-side diff (-i left, -s right) ---", err=True)
                        click.echo(f"[DEBUG-TOPIC]       Lengths: -i={len(u_text)}, -s={len(s_text)}", err=True)

                        # Wrap text at 60 chars so diff can show line-by-line differences
                        u_wrapped = "\n".join(textwrap.wrap(u_text, width=60))
                        s_wrapped = "\n".join(textwrap.wrap(s_text, width=60))

                        # Write to temp files and run diff -y
                        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f_i:
                            f_i.write(u_wrapped)
                            f_i_path = f_i.name
                        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f_s:
                            f_s.write(s_wrapped)
                            f_s_path = f_s.name

                        try:
                            result = subprocess.run(
                                ['diff', '-y', '--width=300', '--suppress-common-lines', f_i_path, f_s_path],
                                capture_output=True,
                                text=True
                            )
                            # Show all diff output (only differing lines)
                            diff_lines = result.stdout.splitlines()
                            if diff_lines:
                                for line in diff_lines:
                                    click.echo(f"[DIFF] {line}", err=True)
                            else:
                                click.echo(f"[DIFF] No differences found (texts identical)", err=True)
                        except FileNotFoundError:
                            click.echo(f"[DEBUG-TOPIC]       (diff command not found, showing raw texts)", err=True)
                            click.echo(f"[DEBUG-TOPIC]       -i text:\n{u_text[:500]}...", err=True)
                            click.echo(f"[DEBUG-TOPIC]       -s text:\n{s_text[:500]}...", err=True)
                        finally:
                            import os
                            os.unlink(f_i_path)
                            os.unlink(f_s_path)

        elif debug_topic and not source_dir:
            click.echo("Warning: --debug-topic requires --source to compare against", err=True)

        # Export matched files from source directory if requested
        if export_dir:
            if not source_dir:
                click.echo("Error: --source is required when using --export", err=True)
                return

            import shutil

            # TODO: Discuss whether to expose direct export of matched input files
            # (files from -i with lost keys) via separate flag like --export-input.
            # Currently we only export from --source.

            # Build fingerprint set by re-reading matched files from input
            matched_fingerprints: set[str] = set()
            for file_path in matched_files:
                with open(file_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json_module.loads(line)
                            report = Report.model_validate(data)
                            fp = compute_report_fingerprint(
                                report.metadata.topic_id,
                                report.get_text()
                            )
                            matched_fingerprints.add(fp)
                        except Exception:
                            continue

            if verbose:
                click.echo(f"[DEBUG] Matched fingerprints to export: {len(matched_fingerprints)}", err=True)
                click.echo(f"[DEBUG] Scanning source directory: {source_dir}", err=True)

            # Scan source directory and find files with matching fingerprints
            source_files: list[Path] = []
            for pattern in file_glob.split(","):
                pattern = pattern.strip()
                source_files.extend(source_dir.rglob(pattern))
            source_files = sorted(set(source_files))

            if verbose:
                click.echo(f"[DEBUG] Source files found: {len(source_files)}", err=True)

            # Find source files that contain matching fingerprints
            export_dir.mkdir(parents=True, exist_ok=True)
            exported_count = 0
            for src_path in source_files:
                file_has_match = False
                try:
                    with open(src_path, "rt", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                data = json_module.loads(line)
                                report = Report.model_validate(data)
                                fp = compute_report_fingerprint(
                                    report.metadata.topic_id,
                                    report.get_text()
                                )
                                if fp in matched_fingerprints:
                                    file_has_match = True
                                    break
                            except Exception:
                                continue
                except Exception:
                    continue

                if file_has_match:
                    rel_path = src_path.relative_to(source_dir)
                    dst_path = export_dir / rel_path
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_path, dst_path)
                    exported_count += 1
                    if verbose:
                        click.echo(f"[DEBUG] Exported: {rel_path}", err=True)

            click.echo(f"Exported {exported_count} files from {source_dir} to {export_dir}")

        # Output results
        output = _format_recovery_results(results, unmatched, output_format)

        if output_path:
            with open(output_path, "wt", encoding="utf-8") as f:
                f.write(output)
            click.echo(f"Results written to: {output_path}")
            click.echo(f"Matched: {len(results)}, Unmatched: {len(unmatched)}")
        else:
            click.echo(output)


def _format_recovery_results(
    results: list,
    unmatched: list,
    output_format: str,
) -> str:
    """Format recovery results for output."""
    import json as json_module
    from io import StringIO

    out = StringIO()

    if output_format == "json":
        data = {"matched": results, "unmatched": unmatched}
        out.write(json_module.dumps(data, indent=2))

    elif output_format == "csv":
        out.write("topic_id,anon_team,anon_run,original_team,original_run,file,line\n")
        for r in results:
            out.write(f"{r['topic_id']},{r['anon_team']},{r['anon_run']},"
                     f"{r['original_team']},{r['original_run']},"
                     f"{r['file']},{r['line']}\n")
        if unmatched:
            out.write("\n# Unmatched entries:\n")
            for r in unmatched:
                out.write(f"# {r['topic_id']},{r['anon_team']},{r['anon_run']},"
                         f"UNMATCHED,UNMATCHED,{r['file']},{r['line']}\n")

    else:  # table
        out.write("\nRecovered Mappings:\n")
        out.write("=" * 80 + "\n")
        if results:
            pass
            # out.write(f"{'Topic':<15} {'Anon Team':<15} {'Anon Run':<10} "
            #          f"{'Orig Team':<15} {'Orig Run':<10}\n")
            # out.write("-" * 80 + "\n")
            # for r in results:
            #     out.write(f"{r['topic_id']:<15} {r['anon_team']:<15} "
            #              f"{r['anon_run']:<10} {r['original_team']:<15} "
            #              f"{r['original_run']:<10}\n")
        else:
            out.write("  (no matches found)\n")

        if unmatched:
            out.write(f"\nUnmatched: {len(unmatched)} reports\n")

        out.write(f"\nSummary: {len(results)} matched, {len(unmatched)} unmatched\n")

    return out.getvalue()


def _scan_metadata_for_priority(metadata_file: Path, priority_value: str) -> set[str]:
    """Scan metadata JSONL file and return runtags matching the given priority."""
    import json

    matching_runtags: set[str] = set()

    try:
        with open(metadata_file, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    runtag = data.get("runtag", "")
                    priority = data.get("std-priority", "")

                    if runtag and priority == priority_value:
                        matching_runtags.add(runtag)
                except json.JSONDecodeError:
                    continue
    except IOError as e:
        raise click.ClickException(f"Cannot read metadata file: {e}")

    return matching_runtags


def _find_run_file(runtag: str, runs_dir: Path) -> Optional[Path]:
    """Find a run file in the runs directory.

    Returns run_file path if found, None otherwise.
    """
    run_file = runs_dir / runtag
    if run_file.exists() and run_file.is_file():
        return run_file
    return None


def _copy_run_file(
    run_file: Path,
    output_dir: Path,
    symlink: bool,
) -> Path:
    """Copy or symlink a run file to the output directory. Returns output path."""
    import shutil

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / run_file.name

    if symlink:
        if out_file.exists() or out_file.is_symlink():
            out_file.unlink()
        out_file.symlink_to(run_file.resolve())
    else:
        shutil.copy2(run_file, out_file)

    return out_file


@cli.command("select-priority")
@click.option(
    "--metadata", "-m",
    "metadata_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Metadata JSONL file with runtag and std-priority fields",
)
@click.option(
    "--runs", "-r",
    "runs_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Input runs directory containing run files",
)
@click.option(
    "--output", "-o",
    "output_dir",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Output directory for selected files",
)
@click.option(
    "--priority", "-p",
    "priority_value",
    default="1 (top)",
    help="Priority value to match (default: '1 (top)')",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be copied without making changes",
)
@click.option(
    "--symlink", "-s",
    is_flag=True,
    help="Create symlinks instead of copying files",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Show debug information",
)
def select_priority(
    metadata_file: Path,
    runs_dir: Path,
    output_dir: Path,
    priority_value: str,
    dry_run: bool,
    symlink: bool,
    verbose: bool,
):
    """Select run files by priority and copy to output directory.

    Scans metadata file for entries matching the given std-priority value
    and copies the corresponding run files from the runs directory to the output.

    Example:
        track-veil select-priority -m meta.jsonl -r runs/ -o prio1/ -p "1 (top)"
        track-veil select-priority -m meta.jsonl -r runs/ -o prio1/ -p "1 (highest)" --dry-run
    """
    if verbose:
        click.echo(f"Metadata file: {metadata_file.resolve()}")
        click.echo(f"Runs directory: {runs_dir.resolve()}")
        click.echo(f"Output directory: {output_dir.resolve()}")
        click.echo(f"Priority filter: '{priority_value}'")
        files_in_runs = sorted([f.name for f in runs_dir.iterdir() if f.is_file()])
        click.echo(f"Files in runs dir ({len(files_in_runs)}): {files_in_runs[:10]}{'...' if len(files_in_runs) > 10 else ''}")

    matching_runtags = _scan_metadata_for_priority(metadata_file, priority_value)
    click.echo(f"Found {len(matching_runtags)} runtags with priority '{priority_value}'")

    if verbose:
        click.echo(f"Runtags: {sorted(matching_runtags)}")

    if not matching_runtags:
        click.echo("No matching runs found. Nothing to do.")
        return

    copied = 0
    not_found = []

    for runtag in sorted(matching_runtags):
        run_file = _find_run_file(runtag, runs_dir)
        if run_file is None:
            not_found.append(runtag)
            continue

        if dry_run:
            click.echo(f"  [dry-run] {runtag}")
        else:
            out_file = _copy_run_file(run_file, output_dir, symlink)
            click.echo(f"  {runtag} -> {out_file}")

        copied += 1

    # Summary
    click.echo(f"\nSummary:")
    click.echo(f"  Matching runtags: {len(matching_runtags)}")
    click.echo(f"  Files {'would be ' if dry_run else ''}copied: {copied}")
    if not_found:
        click.echo(f"  Not found in runs/: {len(not_found)}")
        shown = not_found[:10] if len(not_found) > 10 else not_found
        for tag in shown:
            click.echo(f"    - {tag}")
        if len(not_found) > 10:
            click.echo(f"    ... and {len(not_found) - 10} more")


def _read_topic_ids_from_stdin() -> set[str]:
    """Read topic IDs from stdin, one per line."""
    import sys
    topic_ids: set[str] = set()
    for line in sys.stdin:
        line = line.strip()
        if line:
            topic_ids.add(line)
    return topic_ids


def _filter_report_jsonl(input_path: Path, output_path: Path, topic_ids: set[str]) -> tuple[int, int, int]:
    """Filter Report JSONL file to only include matching topic_ids.

    Only keeps the first report per topic (drops duplicates).

    Returns (kept_lines, duplicates_dropped, total_lines).
    """
    import json
    kept = 0
    duplicates = 0
    total = 0
    seen_topics: set[str] = set()

    with open(input_path, 'rt', encoding="utf-8") as inf, open(output_path, 'wt', encoding="utf-8") as outf:
        for line in inf:
            total += 1
            line_stripped = line.strip()
            if not line_stripped:
                continue
            try:
                data = json.loads(line_stripped)
                # Check metadata.topic_id or top-level topic_id
                topic_id = None
                if isinstance(data.get("metadata"), dict):
                    topic_id = str(data["metadata"].get("topic_id", ""))
                if not topic_id:
                    topic_id = str(data.get("topic_id", ""))
                if not topic_id:
                    # Also check narrative_id
                    if isinstance(data.get("metadata"), dict):
                        topic_id = str(data["metadata"].get("narrative_id", ""))
                    if not topic_id:
                        topic_id = str(data.get("narrative_id", ""))

                if topic_id in topic_ids:
                    if topic_id in seen_topics:
                        duplicates += 1
                    else:
                        seen_topics.add(topic_id)
                        outf.write(line)
                        kept += 1
            except json.JSONDecodeError:
                # Skip malformed lines
                continue

    return kept, duplicates, total


def _filter_ranking_tsv(input_path: Path, output_path: Path, topic_ids: set[str]) -> tuple[int, int]:
    """Filter Ranking TSV file to only include matching topic_ids.

    Ranking format: {topic} Q0 {doc_id} {rank} {score} {run_id}
    Topic is column 0.

    Returns (kept_lines, total_lines).
    """
    kept = 0
    total = 0

    with open(input_path, 'rt', encoding="utf-8") as inf, open(output_path, 'wt', encoding="utf-8") as outf:
        for line in inf:
            total += 1
            line_stripped = line.strip()
            if not line_stripped or line_stripped.startswith('#'):
                outf.write(line)  # Preserve comments/empty lines
                continue

            parts = line_stripped.split()
            if len(parts) >= 1:
                topic_id = parts[0]
                if topic_id in topic_ids:
                    outf.write(line)
                    kept += 1

    return kept, total


def _detect_file_type(file_path: Path) -> str:
    """Detect if file is 'jsonl' or 'tsv' based on content."""
    try:
        with open(file_path, 'rt', encoding="utf-8") as f:
            first_line = f.readline().strip()
            if not first_line:
                return "unknown"
            if first_line.startswith('{'):
                return "jsonl"
            # Check if it looks like ranking TSV (6 columns, col 1 is "Q0")
            parts = first_line.split()
            if len(parts) >= 6 and parts[1] == "Q0":
                return "tsv"
            # Default to TSV for other whitespace-separated formats
            if len(parts) >= 2:
                return "tsv"
    except Exception:
        pass
    return "unknown"


def _parse_official_eval_jsonl(eval_path: Path, task_name: str) -> dict:
    """Parse official eval JSONL files for a task and extract stats.

    Looks for files matching: eval/*.{task_name}.official.eval.jsonl

    Returns dict with:
        - topics: set of topic_ids (excluding "all" aggregates)
        - measures: set of measure names
        - runs: set of run_ids
        - lines: total line count
        - files: list of matched file paths
    """
    import json

    result = {
        "topics": set(),
        "measures": set(),
        "runs": set(),
        "lines": 0,
        "files": [],
    }

    eval_pattern = f"*.{task_name}.official.eval.jsonl"
    official_eval_files = list(eval_path.glob(eval_pattern)) if eval_path.exists() else []
    result["files"] = official_eval_files

    for eval_file in official_eval_files:
        try:
            with open(eval_file, mode="rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    result["lines"] += 1
                    try:
                        data = json.loads(line)
                        topic_id = data.get("topic_id")
                        # Skip aggregate rows (topic_id == "all")
                        if topic_id and topic_id != "all":
                            result["topics"].add(str(topic_id))
                        measure = data.get("measure")
                        if measure:
                            result["measures"].add(measure)
                        run_id = data.get("run_id")
                        if run_id:
                            result["runs"].add(run_id)
                    except json.JSONDecodeError:
                        pass
        except IOError:
            pass

    return result


def _parse_metadata_for_prio1(data_dir: Path, task_name: str) -> dict:
    """Parse metadata files for a task and extract prio1 runs.

    Looks for files matching: metadata/{task_name}/*.jl

    Returns dict with:
        - prio1_runs: list of run_ids with std-priority "1 (top)" or "1 (highest)"
        - all_runs: list of all run_ids in metadata
        - files: list of matched file paths
    """
    import json

    result = {
        "prio1_runs": [],
        "all_runs": [],
        "files": [],
    }

    metadata_task_dir = data_dir / "metadata" / task_name
    metadata_files = list(metadata_task_dir.glob("*.jl")) if metadata_task_dir.exists() else []
    result["files"] = metadata_files

    for metadata_path in metadata_files:
        try:
            with open(metadata_path, mode="rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        run_id = data.get("runtag") or data.get("run_id")
                        if run_id:
                            result["all_runs"].append(run_id)
                            # prio1 runs have std-priority == "1 (top)" or "1 (highest)"
                            prio = data.get("std-priority", data.get("std-prio", ""))
                            if prio in ("1", "1 (top)", "1 (highest)"):
                                result["prio1_runs"].append(run_id)
                    except json.JSONDecodeError:
                        pass
        except IOError:
            pass

    return result


@cli.command("ensure-topics")
@click.option(
    "--runs", "-r",
    "runs_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Input runs directory containing run files (Report JSONL or Ranking TSV)",
)
@click.option(
    "--output", "-o",
    "output_dir",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Output directory for filtered files",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be done without making changes",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Show detailed progress",
)
def ensure_topics(
    runs_dir: Path,
    output_dir: Path,
    dry_run: bool,
    verbose: bool,
):
    """Filter run files to only include specified topics.

    Reads topic IDs from stdin (one per line) and filters each run file
    to only include entries for those topics.

    Supports:
    - Report JSONL files (filters by metadata.topic_id)
    - Ranking TSV files (filters by first column)

    Example:
        cat topics.txt | track-veil ensure-topics -r runs/ -o filtered/
        jq -r '.topic_id' requests.jsonl | track-veil ensure-topics -r runs/ -o filtered/
    """
    import sys

    # Check if stdin has data
    if sys.stdin.isatty():
        raise click.ClickException(
            "No topic IDs provided on stdin. Pipe topic IDs (one per line) to this command.\n"
            "Example: cat topics.txt | track-veil ensure-topics -r runs/ -o filtered/"
        )

    topic_ids = _read_topic_ids_from_stdin()
    click.echo(f"Read {len(topic_ids)} topic IDs from stdin")

    if verbose:
        shown = sorted(topic_ids)[:10]
        click.echo(f"Topics: {shown}{'...' if len(topic_ids) > 10 else ''}")

    if not topic_ids:
        click.echo("No topic IDs provided. Nothing to do.")
        return

    # Find all files in runs directory
    run_files = sorted([f for f in runs_dir.iterdir() if f.is_file()])
    click.echo(f"Found {len(run_files)} files in {runs_dir}")

    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    stats = {"jsonl": 0, "tsv": 0, "unknown": 0, "empty": 0, "kept_lines": 0, "duplicates": 0, "total_lines": 0}

    for run_file in run_files:
        file_type = _detect_file_type(run_file)
        stats[file_type] = stats.get(file_type, 0) + 1

        if file_type == "unknown":
            if verbose:
                click.echo(f"  [skip] {run_file.name} (unknown format)")
            continue

        out_file = output_dir / run_file.name

        if dry_run:
            click.echo(f"  [dry-run] {run_file.name} ({file_type})")
            continue

        if file_type == "jsonl":
            kept, duplicates, total = _filter_report_jsonl(run_file, out_file, topic_ids)
            stats["duplicates"] += duplicates
        else:  # tsv
            kept, total = _filter_ranking_tsv(run_file, out_file, topic_ids)
            duplicates = 0

        stats["kept_lines"] += kept
        stats["total_lines"] += total

        dropped = total - kept - duplicates

        # Remove output file if all lines were dropped
        if kept == 0:
            out_file.unlink(missing_ok=True)
            stats["empty"] += 1
            if verbose:
                click.echo(f"  [empty] {run_file.name}: all {total} lines dropped, not written")
        elif verbose:
            dup_msg = f", {duplicates} duplicates" if duplicates else ""
            click.echo(f"  {run_file.name}: {kept} kept, {dropped} dropped{dup_msg} (of {total})")

    # Summary
    click.echo(f"\nSummary:")
    click.echo(f"  Topic IDs: {len(topic_ids)}")
    click.echo(f"  Files processed: {stats['jsonl']} JSONL, {stats['tsv']} TSV, {stats['unknown']} skipped")
    if not dry_run:
        written = stats['jsonl'] + stats['tsv'] - stats['unknown'] - stats['empty']
        click.echo(f"  Files written: {written} ({stats['empty']} empty, not written)")
        dup_msg = f" ({stats['duplicates']} duplicate topics dropped)" if stats['duplicates'] else ""
        click.echo(f"  Lines kept: {stats['kept_lines']}/{stats['total_lines']}{dup_msg}")
        click.echo(f"  Output: {output_dir}")


@cli.command("info")
@click.option(
    "--data", "-d",
    "data_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Anonymized data directory to analyze",
)
@click.option(
    "--mapping", "-m",
    "mapping_db",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Mapping database to look up teams from run_ids",
)
@click.option(
    "--runs-dir",
    default="runs",
    help="Name of runs subdirectory (default: runs)",
)
@click.option(
    "--eval-dir",
    default="eval",
    help="Name of eval subdirectory (default: eval)",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Show detailed per-task breakdown",
)
@click.option(
    "--markdown", "--md",
    is_flag=True,
    help="Output as GitHub-flavored markdown table",
)
def info_command(
    data_dir: Path,
    mapping_db: Optional[Path],
    runs_dir: str,
    eval_dir: str,
    verbose: bool,
    markdown: bool,
):
    """Show statistics about an anonymized dataset.

    Reports on runs, topics, eval files, qrels, and leaderboards.

    Example:
        track-veil info -d data/anon/
        track-veil info -d data/anon/ -m mapping.db -v
    """
    import json
    from collections import Counter, defaultdict

    click.echo(f"Dataset: {data_dir.resolve()}")

    # Load mapping DB if provided (for run->team lookups)
    run_to_team: dict[str, str] = {}
    if mapping_db:
        mapping = MappingStore(mapping_db)
        run_to_team = mapping.get_anon_run_to_team()
        click.echo(f"Mapping DB: {mapping_db} ({len(run_to_team)} run->team mappings)")

    click.echo()

    # Overall stats
    total_runs = 0
    total_topics: set[str] = set()
    total_teams: set[str] = set()
    total_lines = 0

    # Per-task stats
    task_stats: dict[str, dict] = defaultdict(lambda: {
        "run_files": 0,      # Number of run files
        "run_ids": set(),    # Unique run_ids (from content or filename)
        "prio1_runs": [],    # Priority 1 run_ids (from metadata)
        "teams": set(),      # Unique teams (JSONL only)
        "topics": set(),
        "lines": 0,
        "format": "unknown",
    })

    # === RUNS DIRECTORY ===
    runs_path = data_dir / runs_dir
    if runs_path.exists():
        click.echo(f"=== Runs ({runs_dir}/) ===")

        for task_dir in sorted(runs_path.iterdir()):
            if not task_dir.is_dir():
                continue

            task_name = task_dir.name
            run_files = [f for f in task_dir.iterdir() if f.is_file()]

            # Get prio1 runs from metadata
            metadata_data = _parse_metadata_for_prio1(data_dir, task_name)
            task_stats[task_name]["prio1_runs"] = metadata_data["prio1_runs"]

            for run_file in run_files:
                task_stats[task_name]["run_files"] += 1
                total_runs += 1

                # Filename is the run_id for anonymized data
                run_id = run_file.name
                task_stats[task_name]["run_ids"].add(run_id)

                # Look up team from mapping DB
                # Try full filename first, then stem (without extension)
                team = run_to_team.get(run_id) or run_to_team.get(run_file.stem)
                if team:
                    task_stats[task_name]["teams"].add(team)
                    total_teams.add(team)

                # Detect format and extract stats
                file_format = _detect_file_type(run_file)
                task_stats[task_name]["format"] = file_format

                if file_format == "jsonl":
                    try:
                        with open(run_file, mode="rt", encoding="utf-8") as f:
                            for line in f:
                                task_stats[task_name]["lines"] += 1
                                total_lines += 1
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    data = json.loads(line)
                                    # Extract topic_id and team_id
                                    topic_id = None
                                    if isinstance(data.get("metadata"), dict):
                                        topic_id = str(data["metadata"].get("topic_id", ""))
                                        team_id = data["metadata"].get("team_id", "")
                                        if team_id:
                                            task_stats[task_name]["teams"].add(team_id)
                                            total_teams.add(team_id)
                                    if topic_id:
                                        task_stats[task_name]["topics"].add(topic_id)
                                        total_topics.add(topic_id)
                                except json.JSONDecodeError:
                                    pass
                    except IOError:
                        pass
                elif file_format == "tsv":
                    # Ranking TSV: topic in column 0, run_id in column 5
                    try:
                        with open(run_file, mode="rt", encoding="utf-8") as f:
                            for line in f:
                                task_stats[task_name]["lines"] += 1
                                total_lines += 1
                                parts = line.strip().split()
                                if len(parts) >= 6:
                                    task_stats[task_name]["topics"].add(parts[0])
                                    total_topics.add(parts[0])
                    except IOError:
                        pass

        # Print runs summary
        click.echo(f"  Total run files: {total_runs}")
        if total_teams:
            click.echo(f"  Total teams: {len(total_teams)}")
        elif not run_to_team:
            click.echo(f"  Total teams: (needs mapping DB for TSV files)")
        else:
            click.echo(f"  Total teams: 0")
        click.echo(f"  Total topics: {len(total_topics)}")
        click.echo(f"  Total lines: {total_lines}")

        if verbose:
            click.echo()
            for task_name in sorted(task_stats.keys()):
                ts = task_stats[task_name]
                team_info = f", teams={len(ts['teams'])}"
                click.echo(f"  [{task_name}] runs={ts['run_files']}{team_info}, "
                          f"topics={len(ts['topics'])}, lines={ts['lines']}, format={ts['format']}")
        click.echo()

    # === EVAL DIRECTORY ===
    eval_path = data_dir / eval_dir
    eval_files_total = 0
    eval_stats: dict[str, dict] = defaultdict(lambda: {
        "files": 0,
        "lines": 0,
        "topics": set(),      # Assessed topics
        "docs": set(),        # Assessed documents (qrels only)
        "measures": set(),
        "runs": set(),
        "label_counts": Counter(),
        "values": [],
    })

    if eval_path.exists():
        click.echo(f"=== Eval ({eval_dir}/) ===")

        # First, scan for official eval JSONL files at eval root: *.{task}.official.eval.jsonl
        for task_name in task_stats.keys():
            official_data = _parse_official_eval_jsonl(eval_path, task_name)
            if official_data["files"]:
                eval_stats[task_name]["files"] += len(official_data["files"])
                eval_files_total += len(official_data["files"])
                eval_stats[task_name]["lines"] += official_data["lines"]
                eval_stats[task_name]["topics"].update(official_data["topics"])
                eval_stats[task_name]["measures"].update(official_data["measures"])
                eval_stats[task_name]["runs"].update(official_data["runs"])

        # Also scan task subdirectories for other eval files (qrels, leaderboards, etc.)
        for task_dir in sorted(eval_path.iterdir()):
            if not task_dir.is_dir():
                continue

            task_name = task_dir.name
            eval_files = [f for f in task_dir.iterdir() if f.is_file()]

            for eval_file in eval_files:
                eval_stats[task_name]["files"] += 1
                eval_files_total += 1

                # Parse eval file (could be qrels or leaderboard)
                # Try to detect format from content
                try:
                    with open(eval_file, mode="rt", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line or line.startswith("#"):
                                continue
                            eval_stats[task_name]["lines"] += 1
                            parts = line.split()

                            # Qrels format: topic_id 0 doc_id relevance
                            if len(parts) == 4 and parts[1] == "0":
                                eval_stats[task_name]["topics"].add(parts[0])
                                eval_stats[task_name]["docs"].add(parts[2])
                                try:
                                    label = int(parts[3])
                                    eval_stats[task_name]["label_counts"][label] += 1
                                except ValueError:
                                    pass

                            # Leaderboard formats (3-4 columns)
                            elif len(parts) == 3:
                                # trec_eval: measure topic value
                                eval_stats[task_name]["measures"].add(parts[0])
                                eval_stats[task_name]["topics"].add(parts[1])
                                try:
                                    eval_stats[task_name]["values"].append(float(parts[2]))
                                except ValueError:
                                    pass

                            elif len(parts) == 4:
                                # tot/ir_measures: run_id topic/measure measure/topic value
                                eval_stats[task_name]["runs"].add(parts[0])
                                try:
                                    eval_stats[task_name]["values"].append(float(parts[3]))
                                except ValueError:
                                    pass

                except IOError:
                    pass

        # Aggregate totals across tasks
        total_assessed_topics: set[str] = set()
        total_assessed_docs: set[str] = set()
        total_eval_lines = 0
        for es in eval_stats.values():
            total_assessed_topics.update(es['topics'])
            total_assessed_docs.update(es['docs'])
            total_eval_lines += es['lines']

        # Print eval summary
        click.echo(f"  Total files: {eval_files_total}")
        click.echo(f"  Total lines: {total_eval_lines}")
        click.echo(f"  Assessed topics: {len(total_assessed_topics)}")
        if total_assessed_docs:
            click.echo(f"  Assessed docs: {len(total_assessed_docs)}")

        if verbose:
            click.echo()
            for task_name in sorted(eval_stats.keys()):
                es = eval_stats[task_name]
                click.echo(f"  [{task_name}]")
                click.echo(f"    files={es['files']}, lines={es['lines']}")

                if es['topics']:
                    click.echo(f"    assessed topics={len(es['topics'])}")
                if es['docs']:
                    click.echo(f"    assessed docs={len(es['docs'])}")
                if es['measures']:
                    click.echo(f"    measures={len(es['measures'])}: {sorted(es['measures'])[:5]}{'...' if len(es['measures']) > 5 else ''}")
                if es['runs']:
                    click.echo(f"    runs={len(es['runs'])}")

                # Qrels label distribution
                if es['label_counts']:
                    labels = sorted(es['label_counts'].keys())
                    click.echo(f"    qrels labels: min={min(labels)}, max={max(labels)}")
                    click.echo(f"    distribution: {dict(sorted(es['label_counts'].items()))}")

                # Value range for leaderboards
                if es['values']:
                    click.echo(f"    value range: [{min(es['values']):.4f}, {max(es['values']):.4f}]")

        click.echo()

    # === Markdown table output ===
    if markdown:
        click.echo()
        click.echo("| track | task | teams | runs | prio1 | topics | assessed_topics | total_reports |")
        click.echo("|:------------:|:---------:|-----:|-----:|------:|-------:|----------------:|--------------:|")

        # Combine task_stats and eval_stats
        all_tasks = set(task_stats.keys()) | set(eval_stats.keys())
        for task_name in sorted(all_tasks):
            ts = task_stats.get(task_name, {"run_files": 0, "prio1_runs": [], "teams": set(), "topics": set(), "lines": 0})
            es = eval_stats.get(task_name, {"topics": set(), "lines": 0})

            # Try to extract track from task name (e.g., "trec2025-rag-generation" -> "rag")
            # or use data_dir name as track
            track = data_dir.name

            teams = len(ts.get("teams", set()))
            runs = ts.get("run_files", 0)
            prio1 = len(ts.get("prio1_runs", []))
            topics = len(ts.get("topics", set()))
            assessed_topics = len(es.get("topics", set()))
            total_reports = ts.get("lines", 0)

            click.echo(f"| {track} | {task_name} | {teams} | {runs} | {prio1} | {topics} | {assessed_topics} | {total_reports} |")

        click.echo()
        return

    # === TODO notes ===
    click.echo("=== TODO (not yet implemented) ===")
    click.echo("  - Cross-file deduplication stats")


@cli.command("generate-datasets-yml")
@click.option(
    "--data", "-d",
    "data_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Anonymized data directory to scan",
)
@click.option(
    "--output", "-o",
    "output_file",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Output file (default: datasets.yml in data directory)",
)
@click.option(
    "--runs-dir",
    default="runs",
    help="Name of runs subdirectory (default: runs)",
)
@click.option(
    "--eval-dir",
    default="eval",
    help="Name of eval subdirectory (default: eval)",
)
@click.option(
    "--topic-path",
    default=None,
    type=click.Path(dir_okay=False, path_type=Path),
    help="Path to the topic file",
)
def generate_datasets_yml(
    data_dir: Path,
    output_file: Optional[Path],
    runs_dir: str,
    eval_dir: str,
    topic_path: Optional[Path]=None,
):
    """Generate datasets.yml for use with run_all_datasets.py.

    Scans an anonymized data directory and creates a datasets.yml file
    with one entry per task, extracting assessed_topics from qrels.

    Example:
        track-veil generate-datasets-yml -d data/anon/
        track-veil generate-datasets-yml -d data/anon/ -o my_datasets.yml
    """
    import yaml

    if output_file is None:
        output_file = data_dir / "datasets.yml"

    datasets = []

    # Scan runs directory for task names
    runs_path = data_dir / runs_dir
    eval_path = data_dir / eval_dir

    if not runs_path.exists():
        raise click.ClickException(f"Runs directory not found: {runs_path}")

    for task_dir in sorted(runs_path.iterdir()):
        if not task_dir.is_dir():
            continue

        task_name = task_dir.name

        # Collect run IDs from filenames
        run_ids = sorted([f.name for f in task_dir.iterdir() if f.is_file()])

        # Extract prio1_runs from metadata file
        metadata_data = _parse_metadata_for_prio1(data_dir, task_name)
        prio1_runs = metadata_data["prio1_runs"]
        click.echo(f"  [DEBUG] Looking for metadata at: {data_dir}/metadata/{task_name}/*.jl", err=True)
        click.echo(f"  [DEBUG] Found metadata files: {[f.name for f in metadata_data['files']]}", err=True)
        click.echo(f"  [DEBUG] Found {len(prio1_runs)} prio1 runs: {prio1_runs}", err=True)

        # Extract assessed topics from official eval JSONL (if exists)
        # Pattern: eval/*.{task}.official.eval.jsonl (at eval root, not in subdirs)
        official_data = _parse_official_eval_jsonl(eval_path, task_name)
        click.echo(f"  [DEBUG] Looking for eval at: {eval_path}/*.{task_name}.official.eval.jsonl", err=True)
        click.echo(f"  [DEBUG] Found eval files: {[f.name for f in official_data['files']]}", err=True)
        assessed_topics = sorted(official_data["topics"])

        topic_path_rel = topic_path.relative_to(data_dir) if topic_path else None
        dataset_entry = {
            "name": task_name,
            "responses": str(task_dir.relative_to(data_dir)),
            "topics":  str(topic_path_rel) if topic_path_rel is not None else  "TODO: path to topics JSONL file", 
            "prio1_runs": prio1_runs,
            "assessed_topics": assessed_topics,
        }
        datasets.append(dataset_entry)

        click.echo(f"  {task_name}: {len(run_ids)} runs, {len(prio1_runs)} prio1, {len(assessed_topics)} assessed topics")

    # Write YAML
    output_data = {"datasets": datasets}

    with open(output_file, "wt", encoding="utf-8") as f:
        f.write("# Generated by track-veil generate-datasets-yml\n")
        # f.write("# TODO: Fill in 'topics' paths\n\n")
        yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)

    click.echo(f"\nGenerated {output_file} with {len(datasets)} dataset(s)")
    click.echo("NOTE: You must manually fill in 'topics' paths")


# Topic id field names, in priority order, for JSONL topic files.
# request_id is canonical (autojudge_base.request.Request); the others appear in
# RAG/RAGTIME/DRAGUN topic files.
TOPIC_ID_FIELDS = ("request_id", "topic_id", "query_id", "narrative_id", "id")


def _read_topic_ids_from_file(path: Path) -> List[str]:
    """Read topic ids from a plain list or a topics JSONL file.

    Each line is either a bare topic id or a JSON object, detected per line:
    a line parsing to a JSON object yields its first field named in
    TOPIC_ID_FIELDS, anything else is used verbatim. Numeric ids parse as JSON
    but are not objects, so they stay bare ids.

    Blank lines and '#' comments are ignored. Order is preserved and duplicates
    are dropped, so the generated file has exactly one row per run/topic pair.
    """
    import json

    ids: List[str] = []
    with open(path, "rt", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                ids.append(line)  # plain topic id
                continue

            if not isinstance(record, dict):
                ids.append(line)  # e.g. a numeric topic id
                continue

            for key in TOPIC_ID_FIELDS:
                if key in record:
                    ids.append(str(record[key]))
                    break
            else:
                raise click.ClickException(
                    f"{path}:{line_number}: JSON topic record has no id field "
                    f"(tried {', '.join(TOPIC_ID_FIELDS)}); keys present: "
                    f"{', '.join(sorted(record)) or '(none)'}"
                )

    return list(dict.fromkeys(ids))


def _random_eval_leaderboard(
    run_ids: List[str],
    topic_ids: List[str],
    metric: str,
    rng,
    max_score: float,
):
    """Build a Leaderboard of random per-topic scores.

    The per-run "all" rows are computed by LeaderboardBuilder.build() as the mean
    of that run's per-topic scores, so the aggregate always agrees with its parts.
    """
    from autojudge_base import LeaderboardBuilder, LeaderboardSpec, MeasureSpec

    builder = LeaderboardBuilder(
        LeaderboardSpec(measures=(MeasureSpec(metric, dtype=float),))
    )
    for run_id in run_ids:
        for topic_id in topic_ids:
            builder.add(
                run_id=run_id,
                topic_id=topic_id,
                values={metric: round(rng.uniform(0.0, max_score), 4)},
            )
    return builder.build()


@cli.command("random-eval")
@click.option(
    "--data", "-d",
    "data_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Anonymized data directory containing runs/ and eval/",
)
@click.option(
    "--topics",
    "topic_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="File with topic ids, one per line",
)
@click.option(
    "--task",
    default=None,
    help="Only generate for this task (default: every task in runs/)",
)
@click.option(
    "--metric",
    default="random",
    show_default=True,
    help="Measure name written into the measure column",
)
@click.option(
    "--filename",
    "output_name",
    default="random.eval.jsonl",
    show_default=True,
    help="Name of the eval file written into eval/{task}/",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed, for reproducible output (default: nondeterministic)",
)
@click.option(
    "--max-score",
    type=float,
    default=10.0,
    show_default=True,
    help="Scores are drawn uniformly from [0, MAX_SCORE]",
)
@click.option(
    "--runs-dir",
    default="runs",
    help="Name of runs subdirectory (default: runs)",
)
@click.option(
    "--eval-dir",
    default="eval",
    help="Name of eval subdirectory (default: eval)",
)
def random_eval(
    data_dir: Path,
    topic_path: Path,
    task: Optional[str],
    metric: str,
    output_name: str,
    seed: Optional[int],
    max_score: float,
    runs_dir: str,
    eval_dir: str,
):
    """Write a dummy eval JSONL file with random scores.

    Carries no information, but is well-formed, so an evaluation pipeline can be
    exercised before real assessments exist. Scores are drawn uniformly from
    [0, --max-score] rather than fixed, because identical scores leave the run
    ranking undefined. Each run's "all" row is the mean of its own topic scores.

    Output is one JSON object per line - {"run_id", "topic_id", "measure",
    "value"} - the format meta-evaluate reads back:

        {"run_id": "adela", "topic_id": "rag2026-0", "measure": "random", "value": 8.7}

    Run ids come from the filenames in runs/{task}/; topics come from --topics.
    Intended for an already-anonymized directory, so the run ids written out are
    pseudonyms.

    Example:
        track-veil random-eval -d data/anon/ --topics rag-topic-list.txt
    """
    import random

    runs_path = data_dir / runs_dir
    if not runs_path.exists():
        raise click.ClickException(f"Runs directory not found: {runs_path}")

    topic_ids = _read_topic_ids_from_file(topic_path)
    if not topic_ids:
        raise click.ClickException(f"No topic ids found in {topic_path}")

    task_dirs = sorted(d for d in runs_path.iterdir() if d.is_dir())
    if task is not None:
        task_dirs = [d for d in task_dirs if d.name == task]
        if not task_dirs:
            raise click.ClickException(f"Task not found in {runs_path}: {task}")
    if not task_dirs:
        raise click.ClickException(f"No task directories found in {runs_path}")

    rng = random.Random(seed)
    total_rows = 0

    for task_dir in task_dirs:
        run_ids = sorted(f.name for f in task_dir.iterdir() if f.is_file())
        if not run_ids:
            click.echo(f"  [skip] {task_dir.name} (no run files)")
            continue

        leaderboard = _random_eval_leaderboard(
            run_ids, topic_ids, metric, rng, max_score
        )

        output_dir = data_dir / eval_dir / task_dir.name
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / output_name

        leaderboard.write(output_file, format="jsonl")

        total_rows += len(leaderboard.entries)
        click.echo(
            f"  {task_dir.name}: {len(run_ids)} runs x {len(topic_ids)} topics "
            f"-> {output_file}"
        )

    click.echo(f"\nWrote {total_rows} rows")


def main():
    cli()


if __name__ == "__main__":
    main()
