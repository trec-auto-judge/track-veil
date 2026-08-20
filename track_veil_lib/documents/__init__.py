"""Document resolution and corpus archive building for track data sets.

Modules are copied verbatim from trec25/data-cleaning:

- ``document_resolver``: resolve Report citations to Documents
  (pull / ingest / export-docno / check / clean)
- ``build_corpus_archive_from_routir``: build a corpus archive via Routir
  (requires the ``routir`` extra)
- ``build_corpus_archive_from_duckdb``: build a corpus archive via DuckDB
  (requires the ``duckdb`` extra)

The ``track-veil-docs`` console script (see ``cli``) exposes all three.
"""
