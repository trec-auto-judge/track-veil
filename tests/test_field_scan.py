"""Tests for the deep field scan and the Report metadata handling built on it."""

import json

import pytest

from track_veil_lib.anonymizer.errors import ErrorCollector, IssueType
from track_veil_lib.anonymizer.field_scan import (
    REDACTED, FieldCategory, anonymize_fields, iter_json_fields,
    scan_fields, scan_for_missed_fields,
)
from track_veil_lib.anonymizer.mapping import MappingStore
from track_veil_lib.anonymizer.repairs import RepairStore
from track_veil_lib.anonymizer.transformers import ReportTransformer, resolve_run_identity


@pytest.fixture
def mapping(tmp_path):
    with MappingStore(tmp_path / "map.db", seed=42) as store:
        yield store


# ============ the traversal itself ============


class TestTraversal:

    def test_walks_through_lists_and_nested_dicts(self):
        node = {"a": [{"b": {"c": 1}}]}
        paths = [path for path, _container, _key in iter_json_fields(node)]

        assert paths == ["a", "a[0].b", "a[0].b.c"]

    def test_finds_identifying_fields_at_any_depth(self):
        node = {"responses": [{"metadata": {"team_id": "example-team"}}]}

        found = scan_fields(node)

        # "metadata" itself is not identifying - only the keys inside it are
        assert [(a.path, a.category) for a in found] == [
            ("responses[0].metadata.team_id", FieldCategory.TEAM),
        ]

    def test_key_matching_is_case_insensitive(self):
        assert [a.category for a in scan_fields({"Team_ID": "x"})] == [FieldCategory.TEAM]

    def test_scan_does_not_modify(self):
        node = {"team_id": "example-team", "run_desc": "text"}
        before = json.dumps(node, sort_keys=True)

        scan_fields(node)

        assert json.dumps(node, sort_keys=True) == before

    def test_unlisted_keys_are_left_alone(self, mapping):
        node = {"question": "A question?", "value": 0.5, "topic_id": "topic-1"}

        anonymize_fields(node, mapping)

        assert node == {"question": "A question?", "value": 0.5, "topic_id": "topic-1"}


class TestAnonymizeFields:

    def test_each_category_is_handled(self, mapping):
        node = {
            "team_id": "example-team",
            "run_id": "example-run",
            "run_desc": "names the system",
            "creator": {"contact": "someone"},
        }

        anonymize_fields(node, mapping)

        assert node["team_id"] != "example-team"
        assert node["run_id"] != "example-run"
        assert node["run_desc"] == REDACTED
        assert "creator" not in node

    def test_acts_at_depth(self, mapping):
        node = {"outer": [{"inner": {"team_id": "example-team"}}]}

        anonymize_fields(node, mapping)

        assert node["outer"][0]["inner"]["team_id"] != "example-team"

    def test_expected_run_id_overrides_content(self, mapping):
        """The filename names the run, so content run ids map through it."""
        node = {"run_id": "content-run"}

        anonymize_fields(node, mapping, expected_run_id="filename-run")

        assert node["run_id"] == mapping.get_run("filename-run")

    def test_same_original_gets_the_same_pseudonym(self, mapping):
        first = {"team_id": "example-team"}
        second = {"deep": {"team": "example-team"}}

        anonymize_fields(first, mapping)
        anonymize_fields(second, mapping)

        assert first["team_id"] == second["deep"]["team"]


class TestScanForMissedFields:

    def test_handled_paths_are_not_reported(self):
        node = {"metadata": {"team_id": "anonymized-already"}}

        assert scan_for_missed_fields(node, ("metadata.team_id",)) == []

    def test_prefix_covers_the_subtree(self):
        node = {"metadata": {"extra": {"team_id": "x", "deep": {"run_id": "y"}}}}

        assert scan_for_missed_fields(node, ("metadata.extra",)) == []

    def test_fields_elsewhere_are_reported(self):
        node = {"metadata": {"team_id": "x"}, "responses": [{"metadata": {"team": "y"}}]}

        missed = scan_for_missed_fields(node, ("metadata.team_id",))

        assert [a.path for a in missed] == ["responses[0].metadata.team"]


# ============ Report metadata handling ============


def report_line(**metadata):
    meta = {"team_id": "example-team", "run_id": "content-run", "topic_id": "topic-1"}
    meta.update(metadata)
    return {"metadata": meta, "responses": [{"text": "A sentence.", "citations": []}]}


def transform_report(tmp_path, mapping, record, expected_run_id="filename-run"):
    errors = ErrorCollector()
    transformer = ReportTransformer(
        mapping, RepairStore(tmp_path / "map.db"), errors, interactive=False,
    )
    out, fingerprint = transformer.transform_line(
        json.dumps(record), tmp_path / expected_run_id, 1, expected_run_id=expected_run_id
    )
    return json.loads(out) if out else None, fingerprint, errors


class TestReportMetadataFields:
    """The field list: team_id, run_id, description, creator, extra, run_desc, evaldata."""

    def test_description_is_redacted(self, tmp_path, mapping):
        out, _, _ = transform_report(tmp_path, mapping, report_line(description="names the system"))
        assert out["metadata"]["description"] == REDACTED

    def test_evaldata_is_dropped(self, tmp_path, mapping):
        out, _, _ = transform_report(tmp_path, mapping, report_line(evaldata={"score": 1}))
        assert "evaldata" not in out["metadata"]

    def test_run_desc_is_redacted_and_creator_dropped(self, tmp_path, mapping):
        out, _, _ = transform_report(
            tmp_path, mapping, report_line(run_desc="text", creator={"contact": "someone"})
        )
        assert out["metadata"]["run_desc"] == REDACTED
        assert "creator" not in out["metadata"]

    def test_extra_is_deep_scanned(self, tmp_path, mapping):
        out, _, _ = transform_report(tmp_path, mapping, report_line(
            extra={"nested": {"team_id": "example-team", "run_desc": "text"}}
        ))

        nested = out["metadata"]["extra"]["nested"]
        assert nested["team_id"] != "example-team"
        assert nested["run_desc"] == REDACTED

    def test_extra_run_id_keeps_its_own_identity(self, tmp_path, mapping):
        """Regression: a run referenced inside extra is not this file's run.

        The filename names THIS run; a baseline named in extra must not be
        rewritten to it, or two different runs become indistinguishable.
        """
        out, _, _ = transform_report(
            tmp_path, mapping, report_line(extra={"baseline_run": {"run_id": "other-run"}}),
            expected_run_id="filename-run",
        )

        extra_run = out["metadata"]["extra"]["baseline_run"]["run_id"]
        assert extra_run == mapping.get_run("other-run")
        assert extra_run != out["metadata"]["run_id"]

    def test_team_and_run_are_anonymized_with_filename_winning(self, tmp_path, mapping):
        out, _, _ = transform_report(tmp_path, mapping, report_line())

        assert out["metadata"]["team_id"] == mapping.get_team("example-team")
        assert out["metadata"]["run_id"] == mapping.get_run("filename-run")

    def test_fingerprint_survives_the_reordering(self, tmp_path, mapping):
        """Regression: run/team resolution moved after Report.model_validate."""
        _, fingerprint, _ = transform_report(tmp_path, mapping, report_line())

        assert fingerprint is not None
        assert fingerprint["original_team"] == "example-team"
        assert fingerprint["original_run"] == "filename-run"
        assert fingerprint["anon_team"] and fingerprint["anon_run"]


class TestReportSecondPass:

    def test_identifying_field_in_an_unhandled_bag_is_reported(self, tmp_path, mapping):
        """A stray top-level key is neither dropped nor scanned, so it is reported."""
        record = report_line()
        record["submitter_info"] = {"team": "example-team"}

        _, _, errors = transform_report(tmp_path, mapping, record)

        found = [i for i in errors.issues if i.issue_type == IssueType.IDENTIFIER_FOUND]
        assert any("submitter_info.team" in (i.field_path or "") for i in found)

    def test_sentence_bags_are_dropped(self, tmp_path, mapping):
        """Sentence metadata/evaldata are producer payload - dropped, not scanned."""
        record = report_line()
        record["responses"][0]["metadata"] = {"team_id": "example-team"}
        record["responses"][0]["evaldata"] = {"score": 1}

        out, _, errors = transform_report(tmp_path, mapping, record)

        assert "metadata" not in out["responses"][0]
        assert "evaldata" not in out["responses"][0]
        # Dropped before the second pass runs, so there is nothing left to report
        found = [i for i in errors.issues if i.issue_type == IssueType.IDENTIFIER_FOUND]
        assert not any("responses" in (i.field_path or "") for i in found)

    def test_report_level_evaldata_is_dropped(self, tmp_path, mapping):
        record = report_line()
        record["evaldata"] = {"team_id": "example-team"}

        out, _, _ = transform_report(tmp_path, mapping, record)

        assert "evaldata" not in out

    def test_handled_metadata_fields_are_not_reported(self, tmp_path, mapping):
        _, _, errors = transform_report(tmp_path, mapping, report_line())

        assert [i for i in errors.issues if i.issue_type == IssueType.IDENTIFIER_FOUND] == []


class TestResolveRunIdentity:

    def test_filename_wins_over_content(self, tmp_path, mapping):
        original_run, _anon_team, anon_run = resolve_run_identity(
            mapping, original_team="example-team", content_run_id="content-run",
            expected_run_id="filename-run", file_path=tmp_path / "filename-run", warned=set(),
        )

        assert original_run == "filename-run"
        assert anon_run == mapping.get_run("filename-run")

    def test_content_used_when_no_filename_given(self, tmp_path, mapping):
        original_run, _t, _r = resolve_run_identity(
            mapping, original_team="", content_run_id="content-run",
            expected_run_id=None, file_path=tmp_path / "f", warned=set(),
        )

        assert original_run == "content-run"

    def test_mismatch_warns_once(self, tmp_path, mapping, capsys):
        warned = set()
        for _ in range(3):
            resolve_run_identity(
                mapping, original_team="example-team", content_run_id="content-run",
                expected_run_id="filename-run", file_path=tmp_path / "filename-run",
                warned=warned,
            )

        assert capsys.readouterr().out.count("doesn't match filename") == 1
