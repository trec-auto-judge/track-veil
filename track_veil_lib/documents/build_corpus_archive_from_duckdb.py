#!/usr/bin/env python3
"""
Uses DuckDB to build a corpus archive (jsonl.gz) from a list of document IDs.

Usage:
    python build_corpus_archive_from_duckdb.py --corpus msmarco --corpus-path ./path/to/routir/segments.jsonl --doc-ids ids.txt --output corpus.jsonl.gz

Note: This script has no trec_auto_judge dependencies. Use document_resolver.py export-docno
to extract doc_ids from reports.
"""

import argparse
import gzip
import json
import sys
from pathlib import Path
from typing import Sequence, List, Set

import duckdb

BATCH_SIZE = 1000

CORPUS_DEFAULTS = {
    "msmarco": "/exp/scale25/rag/docs/msmarco_v2.1_doc_segmented",
    "neuclir": "/exp/scale25/neuclir/docs/mlir.mt.jsonl",
    "ragtime": "/exp/scale25/ragtime/docs/mlir.mt.jsonl",
}

# Document ID field name varies by corpus
DOCNO_FIELDS = {
    "msmarco": "docid",
    "neuclir": "id",
    "ragtime": "id",
}


def get_corpus_glob(corpus_path: str) -> str:
    """Convert corpus path to glob pattern for DuckDB.

    If path is a directory, returns glob for all json-like files.
    If path is a file, returns the path as-is.
    """
    p = Path(corpus_path)
    if p.is_dir():
        # Find all json-like files in directory
        extensions = ["*.json", "*.jl", "*.jsonl", "*.json.gz", "*.jl.gz", "*.jsonl.gz"]
        for ext in extensions:
            if list(p.glob(ext)):
                return str(p / ext)
        # Fallback
        return str(p / "*.jsonl.gz")
    return corpus_path


def get_last_docno(output_path: Path) -> str | None:
    """Read the last docno from an existing output file."""
    open_fn = gzip.open if str(output_path).endswith(".gz") else open
    last_docno = None
    try:
        with open_fn(output_path, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    doc = json.loads(line)
                    last_docno = doc.get("docno")
    except (FileNotFoundError, EOFError):
        pass
    return last_docno


def format_preview(
    items: Sequence[str],
    limit: int = 10,
    separator: str = ", ",
) -> str:
    """Format a list of items with preview and 'more' suffix."""
    preview = separator.join(items[:limit])
    if len(items) > limit:
        if "\n" in separator:
            more_prefix = separator
        else:
            more_prefix = " "
        preview += f"{more_prefix}... ({len(items) - limit} more)"
    return preview


def get_default_db_path(corpus_path: str) -> Path:
    """Get default database path based on corpus path."""
    p = Path(corpus_path)
    if p.is_dir():
        return p / "corpus.duckdb"
    # For file/glob patterns, put db in parent directory
    return p.parent / "corpus.duckdb"


def ensure_index_exists(
    db_path: Path,
    corpus_path: str,
    docno_field: str,
) -> None:
    """Create and populate the database if it doesn't exist."""
    if db_path.exists():
        return

    print(f"Creating index at {db_path}...")
    con = duckdb.connect(str(db_path))

    # Load all documents into a persistent table
    con.execute(f"""
        CREATE TABLE corpus AS
        SELECT * FROM read_json_auto('{corpus_path}', ignore_errors=true)
    """)

    # Create index on the docno field
    con.execute(f"CREATE INDEX idx_docno ON corpus({docno_field})")

    row_count = con.execute("SELECT COUNT(*) FROM corpus").fetchone()[0]
    print(f"Indexed {row_count} documents")
    con.close()


def fetch_documents_duckdb(
    db_path: Path,
    doc_ids: List[str],
    docno_field: str = "docid",
) -> tuple[List[dict], Set[str]]:
    """Fetch documents from corpus using DuckDB.

    Args:
        db_path: Path to the DuckDB database file
        doc_ids: List of document IDs to fetch
        docno_field: Name of the document ID field in the JSON (default: "docid")

    Returns:
        Tuple of (list of documents, set of missing doc_ids)
    """
    if not doc_ids:
        return [], set()

    con = duckdb.connect(str(db_path), read_only=True)
    con.execute("CREATE TEMP TABLE requested_ids (docno VARCHAR)")
    con.executemany("INSERT INTO requested_ids VALUES (?)", [(d,) for d in doc_ids])

    query = f"""
        SELECT corpus.*
        FROM corpus
        INNER JOIN requested_ids ON corpus.{docno_field} = requested_ids.docno
    """

    try:
        result = con.execute(query).fetchall()
        columns = [desc[0] for desc in con.description]
    except Exception as e:
        print(f"DuckDB query error: {e}", file=sys.stderr)
        con.close()
        return [], set(doc_ids)

    # Convert to list of dicts
    documents = []
    found_ids: Set[str] = set()
    for row in result:
        doc = dict(zip(columns, row))
        # Normalize docno field
        if docno_field in doc and docno_field != "docno":
            doc["docno"] = doc[docno_field]
        found_ids.add(doc.get("docno") or doc.get(docno_field))
        documents.append(doc)

    missing = set(doc_ids) - found_ids
    con.close()

    return documents, missing


def process_batch(
    doc_ids_batch: List[str],
    db_path: Path,
    docno_field: str,
) -> tuple[List[dict], List[str]]:
    """Process a batch of doc_ids using DuckDB."""
    documents, missing = fetch_documents_duckdb(db_path, doc_ids_batch, docno_field)
    return documents, list(missing)


def main():
    parser = argparse.ArgumentParser(
        description="Build corpus archive from document IDs using DuckDB"
    )
    parser.add_argument(
        "--corpus", "-c",
        choices=["msmarco", "neuclir", "ragtime"],
        required=True,
        help="Corpus type",
    )
    parser.add_argument(
        "--doc-ids", "-d",
        type=Path,
        required=True,
        help="File with one doc_id per line",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        required=True,
        help="Output jsonl or jsonl.gz file",
    )
    parser.add_argument(
        "--corpus-path",
        type=str,
        default=None,
        help="Override default corpus path",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Path to DuckDB database file (default: corpus.duckdb in corpus directory)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Batch size for processing (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last docno in existing output file",
    )

    args = parser.parse_args()

    corpus_path = args.corpus_path or CORPUS_DEFAULTS[args.corpus]
    corpus_glob = get_corpus_glob(corpus_path)
    docno_field = DOCNO_FIELDS[args.corpus]
    db_path = args.db_path or get_default_db_path(corpus_path)

    print(f"Using corpus: {args.corpus} at {corpus_glob}")
    print(f"Database: {db_path}")

    # Ensure index exists (creates if needed)
    ensure_index_exists(db_path, corpus_glob, docno_field)

    # Load doc_ids
    with open(args.doc_ids, encoding="utf-8") as f:
        doc_ids = [line.strip() for line in f if line.strip()]
    print(f"Loaded {len(doc_ids)} doc_ids")

    # Handle resume
    file_mode = "wt"
    start_idx = 0
    if args.resume and args.output.exists():
        last_docno = get_last_docno(args.output)
        if last_docno:
            try:
                last_idx = doc_ids.index(last_docno)
                start_idx = last_idx + 1
                file_mode = "at"
                print(f"Resuming after docno {last_docno} (index {last_idx}), skipping {start_idx} docs")
            except ValueError:
                print(f"WARNING: Last docno '{last_docno}' not found in doc_ids, starting from beginning")

    if start_idx >= len(doc_ids):
        print("All doc_ids already processed.")
        return

    doc_ids = doc_ids[start_idx:]
    print(f"Processing {len(doc_ids)} remaining doc_ids")

    # Process in batches
    total_found = 0
    all_missing: List[str] = []
    batch_size = args.batch_size
    num_batches = (len(doc_ids) + batch_size - 1) // batch_size

    open_fn = gzip.open if str(args.output).endswith(".gz") else open
    with open_fn(args.output, file_mode, encoding="utf-8") as out:
        for batch_num in range(num_batches):
            start = batch_num * batch_size
            end = min(start + batch_size, len(doc_ids))
            batch = doc_ids[start:end]

            print(f"Processing batch {batch_num + 1}/{num_batches} (docs {start + 1}-{end}/{len(doc_ids)})...")

            documents, missing = process_batch(batch, db_path, docno_field)

            for doc in documents:
                out.write(json.dumps(doc, default=str) + "\n")

            total_found += len(documents)
            all_missing.extend(missing)
            out.flush()

    print(f"\nDone. Found {total_found}/{len(doc_ids)} documents.")
    if all_missing:
        print(f"Missing {len(all_missing)} docs: {format_preview(all_missing, limit=5)}", file=sys.stderr)

    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()