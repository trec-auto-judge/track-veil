#!/usr/bin/env python3
"""
Uses Routir to build a corpus archive (jsonl.gz) from a list of document IDs.

Usage:
    python build_corpus_archive.py --corpus msmarco --corpur-path ./path/to/routir/segments.jsonl --doc-ids ids.txt --output corpus.jsonl.gz

Note: This script has no trec_auto_judge dependencies. Use documents_for_reports_from_archive.py
to extract doc_ids from reports (export-docno command).
"""

import argparse
import gc
import gzip
import json
import sys
from pathlib import Path
from typing import Dict, Any, Sequence, List, Tuple, TextIO

BATCH_SIZE = 1000

CORPUS_DEFAULTS = {
    "msmarco": "/exp/scale25/rag/docs/msmarco_v2.1_doc_segmented",
    "neuclir": "/exp/scale25/neuclir/docs/mlir.mt.jsonl",
    "ragtime": "/exp/scale25/ragtime/docs/mlir.mt.jsonl",
}


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


def get_file_finder(corpus: str, corpus_path: str):
    """Get the appropriate file finder for the corpus type."""
    if corpus == "msmarco":
        from routir.utils.file_io import MSMARCOSegOffset
        return MSMARCOSegOffset(corpus_path)
    else:  # neuclir, ragtime
        from routir.utils.file_io import OffsetFile
        return OffsetFile(corpus_path)


def fetch_document(file_finder, doc_id: str) -> Dict[str, Any] | None:
    """Fetch a document and return parsed JSON. Returns None if not found or empty."""
    raw = file_finder[doc_id]
    if not raw:
        return None
    # MSMARCOSegOffset returns JSON string, OffsetFile may return parsed dict
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            print(f"DEBUG: doc_id={doc_id}, type(raw)={type(raw)}, len={len(raw)}, raw={repr(raw)[:200]}", file=sys.stderr)
            raise e
    return raw


def process_batch(
    doc_ids_batch: List[str],
    corpus: str,
    corpus_path: str,
    out_file: TextIO,
) -> Tuple[int, List[str]]:
    """Process a batch of doc_ids, creating/closing file_finder for each batch."""
    file_finder = get_file_finder(corpus, corpus_path)
    found = 0
    missing = []
    try:
        for doc_id in doc_ids_batch:
            try:
                doc = fetch_document(file_finder, doc_id)
                if doc:
                    doc["docno"] = doc_id
                    out_file.write(json.dumps(doc) + "\n")
                    found += 1
                else:
                    missing.append(doc_id)
            except (KeyError, IndexError):
                missing.append(doc_id)
    finally:
        if hasattr(file_finder, "close"):
            file_finder.close()
        del file_finder
        gc.collect()
    return found, missing


def main():
    parser = argparse.ArgumentParser(
        description="Build corpus archive from document IDs"
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
    print(f"Using corpus: {args.corpus} at {corpus_path}")

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

            found, missing = process_batch(batch, args.corpus, corpus_path, out)
            total_found += found
            all_missing.extend(missing)
            out.flush()

    print(f"\nDone. Found {total_found}/{len(doc_ids)} documents.")
    if all_missing:
        print(f"Missing {len(all_missing)} docs: {format_preview(all_missing, limit=5)}", file=sys.stderr)

    print(f"Output: {args.output}")


if __name__ == "__main__":
    # # Quick debug: uncomment to test single docid lookup
    # from routir.utils.file_io import MSMARCOSegOffset
    # ff = MSMARCOSegOffset("/home/dietz/trec-auto-judge/datacleaning2/routir_corpora/msmarco_v2.1_doc_segmented")
    # # raw = ff["msmarco_v2.1_doc_36_614618375#3_1234312863"]  # known good
    # raw = ff["msmarco_v2.1_doc_22_1648697797#2_2364448606"]
    # print(f"type={type(raw)}, raw={repr(raw)[:500]}")
    # exit()

    main()