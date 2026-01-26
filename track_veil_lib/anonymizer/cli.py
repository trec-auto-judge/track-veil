"""Command-line interface for track data anonymization."""

import click
from pathlib import Path
from typing import Optional

from .mapping import MappingStore
from .pipeline import AnonymizationPipeline, PipelineConfig


@click.group()
@click.version_option(version="0.1.0")
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
    from ..report import Report

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
            with open(file_path, "r") as f:
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
                    with open(file_path, "r") as fh:
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
                    with open(src_path, "r") as fh:
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
                with open(file_path, "r") as f:
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
                    with open(src_path, "r") as f:
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
            with open(output_path, "w") as f:
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
        with open(metadata_file, "r") as f:
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


def main():
    cli()


if __name__ == "__main__":
    main()
