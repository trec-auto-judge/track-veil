"""Tests for MetadataTransformer (metadata/*.jl anonymization).

A metadata record has no schema: three roles are handled by name (ORG_FIELDS,
RUN_FIELDS) and everything else -- any key, any depth -- goes through the
same shared machinery the Report and raw-JSONL paths use. These tests pin both halves,
and the seam between them:

- the by-name half keeps its cross-checks (team mismatch, unseen run) and its
  substitute-don't-redact treatment of the standard description
- the generic half reaches nested identifying fields and finds emails BY VALUE, so an
  address outside a field named "email" no longer ships
- neither half touches what the other handled, so no value is anonymized twice
"""
import json

import pytest

from track_veil_lib.anonymizer.errors import EmailAction, ErrorCollector, IssueType
from track_veil_lib.anonymizer.mapping import MappingStore
from track_veil_lib.anonymizer.transformers import MetadataTransformer

ORG, RUN = "example-org", "run1"


def redact_emails(task, field_path, email, file_path):
    """The email policy under test: every address is redacted."""
    return EmailAction.REDACT


def meta_line(**overrides):
    """One metadata record, in the shape the fake dataset uses."""
    record = {
        "runtag": RUN,
        "org": ORG,
        "email": "researcher@university.edu",
        "date": "2025-08-14",
        "task": "trec2025-rag-generation",
        "std-priority": "1 (top)",
    }
    record.update(overrides)
    return record


def transform(tmp_path, records, email_handler=redact_emails):
    """Run the transformer over `records`.

    Returns (output records, errors, pseudonyms) where `pseudonyms` holds what the
    mapping stored for ORG and RUN - read, never created, so a lookup here cannot
    mask a value the transformer failed to map.
    """
    source = tmp_path / "meta"
    source.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8"
    )
    output = tmp_path / "out" / "meta"
    errors = ErrorCollector()

    with MappingStore(tmp_path / "map.db", seed=42) as store:
        transformer = MetadataTransformer(store, errors, email_handler=email_handler)
        transformer.transform_file(source, output)

        out = [
            json.loads(line)
            for line in output.read_text(encoding="utf-8").splitlines() if line.strip()
        ]
        pseudonyms = {"run": store.get_run(RUN), "team": store.get_team(ORG)}

    return out, errors, pseudonyms


def run_one(tmp_path, **overrides):
    """The common case: one record, returning it with the errors and pseudonyms."""
    out, errors, pseudonyms = transform(tmp_path, [meta_line(**overrides)])
    return out[0], errors, pseudonyms


# ============ the fields handled by name ============


class TestNamedFields:
    """org and runtag keep the handling only this transformer can do."""

    def test_org_and_runtag_are_anonymized(self, tmp_path):
        record, _, _ = run_one(tmp_path)

        assert record["org"] != ORG and record["org"]
        assert record["runtag"] != RUN and record["runtag"]

    def test_unrelated_fields_survive_untouched(self, tmp_path):
        record, _, _ = run_one(tmp_path)

        assert record["date"] == "2025-08-14"
        assert record["task"] == "trec2025-rag-generation"
        assert record["std-priority"] == "1 (top)"

    def test_each_value_is_anonymized_exactly_once(self, tmp_path):
        """A pseudonym re-anonymized would map T873 -> T412 and look fine anyway."""
        record, _, store = run_one(tmp_path)

        assert record["runtag"] == store["run"]
        assert record["org"] == store["team"]

    def test_a_run_the_runs_directory_never_had_is_still_mapped(self, tmp_path):
        """It warns (stdout), but the run must not ship under its real name."""
        record, _, _ = run_one(tmp_path)

        assert record["runtag"] != RUN


class TestTeamMismatch:
    """runs/ is the source of truth when metadata spells the team differently."""

    RUNS_TEAM = "isu"
    META_ORG = "iastate"

    def _transform(self, tmp_path, **overrides):
        """One record whose org disagrees with the team runs/ recorded for its run."""
        source = tmp_path / "meta"
        record = meta_line(org=self.META_ORG, **overrides)
        source.write_text(json.dumps(record) + "\n", encoding="utf-8")
        errors = ErrorCollector()

        with MappingStore(tmp_path / "map.db", seed=42) as store:
            # runs/ is processed first and records the run -> team association.
            runs_anon = store.get_or_create_team(self.RUNS_TEAM)
            store.store_run_team(RUN, self.RUNS_TEAM)

            transformer = MetadataTransformer(store, errors, email_handler=redact_emails)
            transformer.transform_file(source, tmp_path / "out" / "meta")

            out = json.loads((tmp_path / "out" / "meta").read_text(encoding="utf-8"))
            stray = store.get_team(self.META_ORG)
        return out, runs_anon, stray

    def test_the_runs_team_pseudonym_is_used(self, tmp_path):
        record, runs_anon, _ = self._transform(tmp_path)

        assert record["org"] == runs_anon

    def test_the_metadata_spelling_gets_no_pseudonym_of_its_own(self, tmp_path):
        """Otherwise one real team is published as two."""
        _, _, stray = self._transform(tmp_path)

        assert stray is None

    def test_an_agreeing_org_is_unaffected(self, tmp_path):
        source = tmp_path / "meta"
        source.write_text(json.dumps(meta_line(org=self.RUNS_TEAM)) + "\n", encoding="utf-8")
        errors = ErrorCollector()

        with MappingStore(tmp_path / "map.db", seed=42) as store:
            runs_anon = store.get_or_create_team(self.RUNS_TEAM)
            store.store_run_team(RUN, self.RUNS_TEAM)
            MetadataTransformer(store, errors, email_handler=redact_emails).transform_file(
                source, tmp_path / "out" / "meta"
            )
            record = json.loads((tmp_path / "out" / "meta").read_text(encoding="utf-8"))

        assert record["org"] == runs_anon

    def test_metadata_stands_alone_when_runs_recorded_no_team(self, tmp_path):
        """No source of truth to defer to, so the metadata spelling is the team."""
        record, _, _ = run_one(tmp_path, org=self.META_ORG)

        assert record["org"] not in (self.META_ORG, "")


class TestDescriptionsAreRedacted:
    """Free text in which a team describes its own system goes whole.

    Substituting the team and run names into it was tried and dropped: it removes only
    the names we happen to know, while a system description identifies its author
    through the system it describes.
    """

    @pytest.mark.parametrize("field", ["std-desc", "rag-top-k", "run_desc", "description"])
    def test_a_description_field_is_redacted(self, tmp_path, field):
        record, _, _ = run_one(tmp_path, **{field: "Dense retrieval, 2 stages."})

        assert record[field] == "[REDACTED]"

    def test_nothing_of_the_original_text_survives(self, tmp_path):
        record, _, _ = run_one(
            tmp_path, **{"std-desc": f"{RUN}_v2 by {ORG}, our in-house system"}
        )

        assert RUN not in record["std-desc"]
        assert ORG not in record["std-desc"]
        assert "in-house" not in record["std-desc"]

    def test_it_is_redacted_even_without_a_runtag(self, tmp_path):
        """The old code skipped std-desc entirely unless a runtag was present."""
        out, _, _ = transform(tmp_path, [{"org": ORG, "std-desc": f"Built by {ORG}."}])

        assert out[0]["std-desc"] == "[REDACTED]"

    def test_a_nested_description_is_reached_too(self, tmp_path):
        record, _, _ = run_one(tmp_path, submitter={"run_desc": "Our system"})

        assert record["submitter"]["run_desc"] == "[REDACTED]"


# ============ the generic deep scan ============


class TestDeepFieldScan:
    """Identifying keys below the top level, which the by-name handling cannot see."""

    def test_a_nested_team_id_is_anonymized(self, tmp_path):
        record, _, _ = run_one(tmp_path, submitter={"team_id": "other-team"})

        assert record["submitter"]["team_id"] != "other-team"

    def test_a_nested_creator_is_dropped(self, tmp_path):
        record, _, _ = run_one(tmp_path, submitter={"creator": "A. Person"})

        assert "creator" not in record["submitter"]

    def test_a_top_level_creator_is_dropped(self, tmp_path):
        """Top-level scalars live in the scan's view and must be written back."""
        record, _, _ = run_one(tmp_path, creator="A. Person")

        assert "creator" not in record

    def test_a_top_level_run_desc_is_redacted(self, tmp_path):
        record, _, _ = run_one(tmp_path, run_desc="Our system, by name")

        assert record["run_desc"] == "[REDACTED]"

    def test_a_run_id_in_another_field_keeps_its_own_identity(self, tmp_path):
        """It may reference a baseline or parent run, not the record's own run."""
        record, _, _ = run_one(tmp_path, baseline={"run_id": "some-other-run"})

        assert record["baseline"]["run_id"] not in ("some-other-run", record["runtag"])

    def test_identifying_fields_inside_a_list_are_reached(self, tmp_path):
        record, _, _ = run_one(tmp_path, contributors=[{"team": "other-team"}])

        assert record["contributors"][0]["team"] != "other-team"


class TestEmailsFoundByValue:
    """An address is identifying wherever it sits, not only in a field named email."""

    def test_the_email_field_is_still_redacted(self, tmp_path):
        record, _, _ = run_one(tmp_path)

        assert "@" not in record["email"]

    def test_a_declared_field_goes_even_when_no_pattern_matches(self, tmp_path):
        """The name says it is an address; obfuscation must not buy a free pass."""
        record, _, _ = run_one(tmp_path, email="a.person AT uni DOT edu")

        assert record["email"] == "[REDACTED]"

    def test_a_declared_field_is_replaced_whole(self, tmp_path):
        """Not just the matched span: the rest of the value is the address's context."""
        record, _, _ = run_one(tmp_path, email="Alex Person <alex@uni.edu>")

        assert record["email"] == "[REDACTED]"

    def test_an_address_outside_a_declared_field_keeps_its_context(self, tmp_path):
        """The complement: elsewhere only the address goes, the sentence survives."""
        record, _, _ = run_one(tmp_path, notes="Ask alex@uni.edu about reruns.")

        assert record["notes"] == "Ask [REDACTED] about reruns."

    def test_the_email_field_is_redacted_without_a_configured_policy(self, tmp_path):
        """Metadata carries submitter addresses as a matter of course."""
        out, _, _ = transform(tmp_path, [meta_line()], email_handler=None)

        assert "@" not in out[0]["email"]

    def test_an_address_in_a_description_goes_with_the_redaction(self, tmp_path):
        record, _, _ = run_one(
            tmp_path, **{"std-desc": "Questions to a.person@uni.edu please."}
        )

        assert record["std-desc"] == "[REDACTED]"

    def test_an_address_in_a_nested_field_is_redacted(self, tmp_path):
        record, _, _ = run_one(tmp_path, submitter={"contact": "b@uni.edu"})

        assert "@" not in record["submitter"]["contact"]

    def test_every_address_is_reported(self, tmp_path):
        _, errors, _ = run_one(tmp_path, submitter={"contact": "b@uni.edu"})

        found = {entry["email"] for entry in errors.email_addresses}
        assert found == {"researcher@university.edu", "b@uni.edu"}

    def test_dropping_removes_the_field(self, tmp_path):
        out, _, _ = transform(
            tmp_path,
            [meta_line()],
            email_handler=lambda *_a: EmailAction.DROP_FIELD,
        )

        assert "email" not in out[0]


# ============ the read-only second pass ============


class TestMissedFields:

    def test_an_identifying_field_of_the_wrong_shape_is_reported(self, tmp_path):
        """anonymize_fields skips a non-string value; it must not vanish silently."""
        _, errors, _ = run_one(tmp_path, team_id=["one", "two"])

        missed = [i for i in errors.issues if i.issue_type == IssueType.IDENTIFIER_FOUND]
        assert [i.field_path for i in missed] == ["team_id"]

    def test_the_wrong_shaped_value_is_left_alone(self, tmp_path):
        record, _, _ = run_one(tmp_path, team_id=["one", "two"])

        assert record["team_id"] == ["one", "two"]

    def test_fields_the_scan_handled_are_not_reported_back(self, tmp_path):
        _, errors, _ = run_one(tmp_path, submitter={"team_id": "other-team"})

        assert not [i for i in errors.issues if i.issue_type == IssueType.IDENTIFIER_FOUND]

    def test_a_clean_record_reports_nothing(self, tmp_path):
        _, errors, _ = run_one(tmp_path)

        assert not [i for i in errors.issues if i.issue_type == IssueType.IDENTIFIER_FOUND]


# ============ malformed input ============


class TestParseErrors:

    def test_an_unparseable_line_is_reported(self, tmp_path):
        source = tmp_path / "meta"
        source.write_text("{not json\n", encoding="utf-8")
        errors = ErrorCollector()

        with MappingStore(tmp_path / "map.db", seed=42) as store:
            MetadataTransformer(store, errors).transform_file(source, tmp_path / "out")

        assert any(i.issue_type == IssueType.PARSE_ERROR for i in errors.issues)
