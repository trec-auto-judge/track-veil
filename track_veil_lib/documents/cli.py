"""Console script for track-veil document tooling.

Exposes the document resolver commands (pull, ingest, export-docno, check, clean)
plus the corpus archive builders under a `build-corpus` subgroup:

    track-veil-docs ingest -i runs/ -o enriched/ -c corpus.jsonl.gz
    track-veil-docs check -i enriched/ --docno-out missing.txt
    track-veil-docs clean -i enriched/ -o cleaned/ --remove-docno missing.txt
    track-veil-docs build-corpus routir --corpus msmarco -d ids.txt -o corpus.jsonl.gz
    track-veil-docs build-corpus duckdb --corpus msmarco -d ids.txt -o corpus.jsonl.gz

The builder modules keep their own argparse interfaces (they are copied verbatim
from trec25/data-cleaning), so `build-corpus routir --help` shows argparse's help
and accepts exactly the flags those scripts always accepted. Their imports happen
inside the command bodies, so the CLI loads without the optional `routir` /
`duckdb` extras installed.
"""

import sys

import click

from .document_resolver import cli as main


@main.group("build-corpus")
def build_corpus():
    """Build a corpus archive (jsonl.gz) from a list of document IDs."""
    pass


def _delegate(module_name: str, subcommand: str, args) -> None:
    """Run a copied argparse script's main() with the given arguments."""
    from importlib import import_module

    try:
        module = import_module(f".{module_name}", package=__package__)
    except ImportError as e:
        raise click.ClickException(
            f"`build-corpus {subcommand}` needs the '{subcommand}' extra: "
            f"uv pip install '.[{subcommand}]'  ({e})"
        ) from e

    saved_argv = sys.argv
    sys.argv = [f"track-veil-docs build-corpus {subcommand}", *args]
    try:
        module.main()
    finally:
        sys.argv = saved_argv


@build_corpus.command(
    "routir",
    context_settings=dict(ignore_unknown_options=True, help_option_names=[]),
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def routir(args):
    """Build a corpus archive using Routir. Requires the 'routir' extra.

    Pass --help for the full option list.
    """
    _delegate("build_corpus_archive_from_routir", "routir", args)


@build_corpus.command(
    "duckdb",
    context_settings=dict(ignore_unknown_options=True, help_option_names=[]),
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def duckdb(args):
    """Build a corpus archive using DuckDB. Requires the 'duckdb' extra.

    Pass --help for the full option list.
    """
    _delegate("build_corpus_archive_from_duckdb", "duckdb", args)


if __name__ == "__main__":
    main()
