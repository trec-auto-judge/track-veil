"""Tests for NuggetBankTransformer (ragtime26 nugget bank anonymization)."""

import json

from track_veil_lib.anonymizer.errors import ErrorCollector, IssueType
from track_veil_lib.anonymizer.mapping import MappingStore
from track_veil_lib.anonymizer.transformers import NuggetBankTransformer


def bank_line(
    team_id="example-team",
    topic_id="topic-1",
    run_id="content-run-id",
    run_desc="names the team's system",
    extra_metadata=None,
    nugget_metadata=None,
    answer_metadata=None,
):
    """One ragtime26 nugget bank line, with annotation at every level."""
    metadata = {
        "team_id": team_id,
        "topic_id": topic_id,
        "run_id": run_id,
        "run_desc": run_desc,
    }
    metadata.update(extra_metadata or {})

    answer = {"answer": "An answer", "references": ["doc-1", "doc-2"]}
    if answer_metadata is not None:
        answer["metadata"] = answer_metadata

    nugget = {"question": "A question?", "aggregator_type": "AND", "answers": [answer]}
    if nugget_metadata is not None:
        nugget["metadata"] = nugget_metadata

    return {"metadata": metadata, "nugget_bank": [nugget]}


def write_bank_file(directory, name="T3-original-run", lines=None):
    """Write a run-named nugget bank file (no extension, like runs/)."""
    path = directory / name
    lines = lines if lines is not None else [bank_line()]
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")
    return path


def transform(tmp_path, source, expected_run_id=None, mapping=None):
    """Run the transformer, returning (result, output records, errors)."""
    output = tmp_path / "out" / source.name
    errors = ErrorCollector()

    def run(store):
        transformer = NuggetBankTransformer(store, errors)
        return transformer.transform_file(source, output, expected_run_id=expected_run_id)

    if mapping is not None:
        result = run(mapping)
    else:
        with MappingStore(tmp_path / "map.db", seed=42) as store:
            result = run(store)

    records = []
    if output.exists():
        records = [
            json.loads(line)
            for line in output.read_text(encoding="utf-8").splitlines() if line.strip()
        ]
    return result, records, errors


class TestMetadataAnonymization:
    """The three identifying fields, and what must survive alongside them."""

    def test_team_id_is_anonymized(self, tmp_path):
        source = write_bank_file(tmp_path)
        _, records, _ = transform(tmp_path, source)

        assert records[0]["metadata"]["team_id"] != "example-team"
        assert records[0]["metadata"]["team_id"]

    def test_run_desc_is_redacted(self, tmp_path):
        source = write_bank_file(tmp_path)
        _, records, _ = transform(tmp_path, source)

        assert records[0]["metadata"]["run_desc"] == "[REDACTED]"

    def test_topic_id_is_preserved(self, tmp_path):
        """Topics are not anonymized - they are shared across all runs."""
        source = write_bank_file(tmp_path)
        _, records, _ = transform(tmp_path, source)

        assert records[0]["metadata"]["topic_id"] == "topic-1"

    def test_unknown_metadata_keys_survive(self, tmp_path):
        """The spec allows other metadata fields; they are a participant's data."""
        source = write_bank_file(
            tmp_path, lines=[bank_line(extra_metadata={"submitted_at": "2026-01-01"})]
        )
        _, records, _ = transform(tmp_path, source)

        assert records[0]["metadata"]["submitted_at"] == "2026-01-01"

    def test_filename_is_source_of_truth_for_run_id(self, tmp_path):
        """The anonymized run_id comes from the filename, not the content."""
        source = write_bank_file(tmp_path, name="T3-original-run")

        with MappingStore(tmp_path / "map.db", seed=42) as store:
            expected = store.get_or_create_run("T3-original-run")
            _, records, _ = transform(
                tmp_path, source, expected_run_id="T3-original-run", mapping=store
            )

        assert records[0]["metadata"]["run_id"] == expected

    def test_run_id_mismatch_warns(self, tmp_path, capsys):
        source = write_bank_file(tmp_path, name="T3-original-run")
        transform(tmp_path, source, expected_run_id="T3-original-run")

        assert "doesn't match filename" in capsys.readouterr().out


class TestAnnotationRemoval:
    """Producer-side annotation must not reach a shared dataset."""

    def test_nugget_and_answer_metadata_are_dropped(self, tmp_path):
        source = write_bank_file(tmp_path, lines=[bank_line(
            nugget_metadata={"origin": "internal"},
            answer_metadata={"supported": True},
        )])
        (written, dropped), records, _ = transform(tmp_path, source)

        assert (written, dropped) == (1, 2)
        nugget = records[0]["nugget_bank"][0]
        assert "metadata" not in nugget
        assert "metadata" not in nugget["answers"][0]

    def test_nothing_dropped_when_there_is_no_annotation(self, tmp_path):
        source = write_bank_file(tmp_path)
        (written, dropped), _, _ = transform(tmp_path, source)

        assert (written, dropped) == (1, 0)

    def test_reference_metadata_is_dropped(self, tmp_path):
        """References may be Reference objects rather than bare doc ids."""
        line = bank_line()
        line["nugget_bank"][0]["answers"][0]["references"] = [
            {"doc_id": "doc-1", "metadata": {"quote_expanded": "internal"}}
        ]
        source = write_bank_file(tmp_path, lines=[line])
        (_, dropped), records, _ = transform(tmp_path, source)

        assert dropped == 1
        assert "metadata" not in records[0]["nugget_bank"][0]["answers"][0]["references"][0]


class TestContentPreserved:
    """Everything that is not identifying must come through untouched."""

    def test_questions_answers_and_references_are_untouched(self, tmp_path):
        source = write_bank_file(tmp_path)
        _, records, _ = transform(tmp_path, source)

        nugget = records[0]["nugget_bank"][0]
        assert nugget["question"] == "A question?"
        assert nugget["aggregator_type"] == "AND"
        assert nugget["answers"][0]["answer"] == "An answer"
        assert nugget["answers"][0]["references"] == ["doc-1", "doc-2"]

    def test_format_version_is_stamped(self, tmp_path):
        """The writer adds it; output is deliberately not byte-identical to input."""
        source = write_bank_file(tmp_path)
        _, records, _ = transform(tmp_path, source)

        assert records[0]["format_version"] == "RAGTIME26"

    def test_every_topic_is_written(self, tmp_path):
        source = write_bank_file(tmp_path, lines=[
            bank_line(topic_id="topic-1"), bank_line(topic_id="topic-2"),
        ])
        (written, _), records, _ = transform(tmp_path, source)

        assert written == 2
        assert {r["metadata"]["topic_id"] for r in records} == {"topic-1", "topic-2"}


class TestMappingConsistency:
    """Pseudonyms must agree with the rest of the dataset."""

    def test_same_team_gets_the_same_pseudonym(self, tmp_path):
        source = write_bank_file(tmp_path, lines=[
            bank_line(topic_id="topic-1"), bank_line(topic_id="topic-2"),
        ])
        _, records, _ = transform(tmp_path, source)

        assert len({r["metadata"]["team_id"] for r in records}) == 1

    def test_reuses_an_existing_mapping(self, tmp_path):
        """A team already mapped from runs/ keeps its pseudonym here."""
        source = write_bank_file(tmp_path, name="T3-original-run")

        with MappingStore(tmp_path / "map.db", seed=42) as store:
            existing_team = store.get_or_create_team("example-team")
            _, records, _ = transform(tmp_path, source, mapping=store)

        assert records[0]["metadata"]["team_id"] == existing_team


class TestFailures:
    """A bad file is reported, not crashed on."""

    def test_unparseable_file_is_recorded(self, tmp_path):
        source = tmp_path / "T3-original-run"
        source.write_text("this is not json\n", encoding="utf-8")

        (written, dropped), records, errors = transform(tmp_path, source)

        assert (written, dropped) == (0, 0)
        assert records == []
        assert any(i.issue_type == IssueType.PARSE_ERROR for i in errors.issues)

    def test_wrong_shape_is_recorded(self, tmp_path):
        """A Report JSONL handed to this transformer must fail loudly."""
        source = tmp_path / "T3-original-run"
        source.write_text(json.dumps({
            "metadata": {"team_id": "t", "run_id": "r", "topic_id": "1"},
            "responses": [{"text": "not a nugget bank", "citations": []}],
        }) + "\n", encoding="utf-8")

        (written, _), _, errors = transform(tmp_path, source)

        assert written == 0
        assert any(i.issue_type == IssueType.PARSE_ERROR for i in errors.issues)
