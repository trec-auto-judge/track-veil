"""Tests for the document resolver (track_veil_lib.documents).

Covers the corpus-archive path end to end: export-docno -> ingest -> check ->
clean -> check, plus the payload-shape handling in ArchiveDocumentResolver.
The remote (HTTP) resolver is not covered here - it needs a document service.
"""

import gzip
import json
from pathlib import Path

import pytest
from autojudge_base.report import Report, load_report

from track_veil_lib.documents.document_resolver import (
    ArchiveDocumentResolver,
    populate_report_documents,
    process_directory,
    process_directory_check,
    process_directory_clean,
    process_directory_export,
)


def make_report(topic_id: str = "topic-1", references=("docA", "docB")) -> dict:
    """A Rag24-style report: citations are indices into references."""
    return {
        "metadata": {"team_id": "t1", "run_id": "r1", "topic_id": topic_id},
        "references": list(references),
        "responses": [
            {"text": "Sentence one.", "citations": [0]},
            {"text": "Sentence two.", "citations": [1]},
        ],
    }


def write_reports(path: Path, reports) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wt", encoding="utf-8") as f:
        for report in reports:
            f.write(json.dumps(report) + "\n")


def write_corpus(path: Path, docs) -> None:
    """Write a corpus archive; gzipped iff the path ends in .gz."""
    open_fn = gzip.open if str(path).endswith(".gz") else open
    with open_fn(path, "wt", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc) + "\n")


@pytest.fixture
def reports_dir(tmp_path):
    """A directory holding one report that cites docA and docB."""
    input_dir = tmp_path / "reports"
    write_reports(input_dir / "r1.jsonl", [make_report()])
    return input_dir


@pytest.fixture
def corpus_gz(tmp_path):
    """A corpus archive containing only docA - docB is deliberately missing."""
    path = tmp_path / "corpus.jsonl.gz"
    write_corpus(path, [{"docno": "docA", "text": "Body of document A."}])
    return path


class TestCreateDoc:
    """Payload shapes ArchiveDocumentResolver must accept."""

    def test_string_payload(self):
        doc = ArchiveDocumentResolver._create_doc("docA", "raw text")
        assert doc.id == "docA"
        assert doc.text == "raw text"

    def test_id_and_text(self):
        doc = ArchiveDocumentResolver._create_doc("docA", {"id": "docA", "text": "body"})
        assert doc.id == "docA"
        assert doc.text == "body"

    def test_segment_key(self):
        doc = ArchiveDocumentResolver._create_doc("docA", {"docno": "docA", "segment": "seg body"})
        assert doc.id == "docA"
        assert doc.text == "seg body"

    def test_docno_fills_in_missing_id(self):
        doc = ArchiveDocumentResolver._create_doc("ignored", {"docno": "docA", "text": "body"})
        assert doc.id == "docA"

    def test_no_text_returns_none(self):
        assert ArchiveDocumentResolver._create_doc("docA", {"id": "docA"}) is None


class TestArchiveDocumentResolver:
    """Corpus loading and resolution."""

    def test_resolves_present_and_reports_missing(self, corpus_gz):
        resolver = ArchiveDocumentResolver([corpus_gz])
        resolved, failed = resolver.resolve({"docA", "docB"})
        assert set(resolved) == {"docA"}
        assert failed == {"docB"}

    def test_plain_jsonl_corpus(self, tmp_path):
        path = tmp_path / "corpus.jsonl"
        write_corpus(path, [{"docno": "docA", "text": "body"}])
        resolver = ArchiveDocumentResolver([path])
        resolved, failed = resolver.resolve({"docA"})
        assert set(resolved) == {"docA"}
        assert failed == set()

    def test_docid_and_id_are_accepted_as_docno(self, tmp_path):
        path = tmp_path / "corpus.jsonl"
        write_corpus(path, [
            {"docid": "docA", "text": "a"},
            {"id": "docB", "text": "b"},
        ])
        resolver = ArchiveDocumentResolver([path])
        resolved, _failed = resolver.resolve({"docA", "docB"})
        assert set(resolved) == {"docA", "docB"}

    def test_document_without_identifier_raises(self, tmp_path):
        path = tmp_path / "corpus.jsonl"
        write_corpus(path, [{"text": "no identifier"}])
        with pytest.raises(ValueError):
            ArchiveDocumentResolver([path])

    def test_empty_request_resolves_nothing(self, corpus_gz):
        resolver = ArchiveDocumentResolver([corpus_gz])
        assert resolver.resolve(set()) == ({}, set())

    def test_close_clears_corpus(self, corpus_gz):
        resolver = ArchiveDocumentResolver([corpus_gz])
        resolver.close()
        assert resolver.corpus == {}


class TestPopulateReportDocuments:
    """In-place enrichment of a single report."""

    def test_populates_documents(self, corpus_gz):
        report = Report.model_validate(make_report())
        failed = populate_report_documents(report, ArchiveDocumentResolver([corpus_gz]))
        assert set(report.documents) == {"docA"}
        assert failed == {"docB"}

    def test_skips_already_present(self, corpus_gz):
        report = Report.model_validate(make_report())
        populate_report_documents(report, ArchiveDocumentResolver([corpus_gz]))

        class ExplodingResolver:
            def resolve(self, doc_ids):
                raise AssertionError(f"should not re-resolve {doc_ids}")

            def close(self):
                pass

        # docB is still unresolved, so it is still requested; docA must not be.
        with pytest.raises(AssertionError, match="docB"):
            populate_report_documents(report, ExplodingResolver())


class TestProcessDirectory:
    """Directory-level drivers, as the CLI subcommands use them."""

    def test_export_docno(self, reports_dir, tmp_path):
        docno_out = tmp_path / "docnos.txt"
        process_directory_export(reports_dir, docno_out)
        assert docno_out.read_text(encoding="utf-8").split() == ["docA", "docB"]

    def test_ingest_then_check_then_clean(self, reports_dir, corpus_gz, tmp_path):
        enriched = tmp_path / "enriched"
        resolver = ArchiveDocumentResolver([corpus_gz])

        ok = process_directory(reports_dir, enriched, resolver)
        assert ok is False  # docB could not be resolved

        report = load_report(enriched / "r1.jsonl")[0]
        assert set(report.documents) == {"docA"}

        missing_out = tmp_path / "missing.txt"
        assert process_directory_check(enriched, docno_out=missing_out) is False
        assert missing_out.read_text(encoding="utf-8").split() == ["docB"]

        cleaned = tmp_path / "cleaned"
        process_directory_clean(enriched, cleaned, remove_docids={"docB"})

        # docB is gone and the surviving Rag24 index still points at docA.
        report = load_report(cleaned / "r1.jsonl")[0]
        assert report.references == ["docA"]
        assert [s.citations for s in report.responses] == [[0], []]
        assert process_directory_check(cleaned) is True

    def test_clean_by_corpus_membership(self, reports_dir, corpus_gz, tmp_path):
        cleaned = tmp_path / "cleaned"
        process_directory_clean(reports_dir, cleaned, corpus_paths=[corpus_gz])
        report = load_report(cleaned / "r1.jsonl")[0]
        assert report.references == ["docA"]

    def test_skip_existing_files(self, reports_dir, corpus_gz, tmp_path):
        enriched = tmp_path / "enriched"
        enriched.mkdir()
        (enriched / "r1.jsonl").write_text("sentinel\n", encoding="utf-8")

        process_directory(
            reports_dir, enriched, ArchiveDocumentResolver([corpus_gz]),
            skip_existing_files=True,
        )
        assert (enriched / "r1.jsonl").read_text(encoding="utf-8") == "sentinel\n"

    def test_empty_input_directory_is_not_a_failure(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        assert process_directory(empty, tmp_path / "out", ArchiveDocumentResolver([])) is True


class TestCli:
    """Command wiring of the track-veil-docs console script."""

    def test_commands_are_registered(self):
        from track_veil_lib.documents.cli import main

        assert set(main.commands) >= {
            "pull", "ingest", "export-docno", "check", "clean", "build-corpus",
        }
        assert set(main.commands["build-corpus"].commands) == {"routir", "duckdb"}

    def test_export_docno_via_cli(self, reports_dir, tmp_path):
        from click.testing import CliRunner

        from track_veil_lib.documents.cli import main

        docno_out = tmp_path / "docnos.txt"
        result = CliRunner().invoke(
            main, ["export-docno", "--input", str(reports_dir), "--docno-out", str(docno_out)]
        )
        assert result.exit_code == 0, result.output
        assert docno_out.read_text(encoding="utf-8").split() == ["docA", "docB"]

    def test_check_exits_nonzero_when_documents_missing(self, reports_dir):
        from click.testing import CliRunner

        from track_veil_lib.documents.cli import main

        result = CliRunner().invoke(main, ["check", "--input", str(reports_dir)])
        assert result.exit_code == 1
