"""Command-line interface for TREC data anonymization."""

import click
from pathlib import Path
from typing import Optional

from .mapping import MappingStore
from .pipeline import AnonymizationPipeline, PipelineConfig


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """TREC Data Anonymization Tool.

    Anonymize team and run identifiers in TREC datasets while preserving
    data structure for sharing.
    """
    pass


@cli.command()
@click.option(
    "--input", "-i",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Input directory containing TREC data",
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
    "--no-interactive",
    is_flag=True,
    help="Run non-interactively (skip prompts, log errors)",
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
    no_interactive: bool,
    dry_run: bool,
    error_report: Optional[Path],
    priority_filter: Optional[str],
):
    """Anonymize a TREC dataset.

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
        interactive=not no_interactive,
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


def main():
    cli()


if __name__ == "__main__":
    main()
