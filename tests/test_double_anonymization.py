"""Guard against a value being anonymized twice within a run.

``get_or_create_team``/``get_or_create_run`` are not idempotent over their own
output: hand one a pseudonym and it mints a pseudonym *of the pseudonym*, and the
mapping gains a bogus entry. Nothing in a single pass should do that, but the
failure is quiet -- the output still looks anonymized -- so it needs pinning.

The general detector is that the mapping's originals and its pseudonyms must be
disjoint sets: if any pseudonym also appears as an original, something fed output
back in. Each test below drives a transformer over data where the same team and run
appear in several places, which is where a double pass would show up.
"""

import json

import pytest

from track_veil_lib.anonymizer.errors import ErrorCollector
from track_veil_lib.anonymizer.field_scan import anonymize_fields
from track_veil_lib.anonymizer.mapping import MappingStore
from track_veil_lib.anonymizer.repairs import RepairStore
from track_veil_lib.anonymizer.transformers import (
    NuggetBankTransformer, RawJsonlTransformer, ReportTransformer,
)

TEAM = "example-team"
RUN = "example-run"


@pytest.fixture
def mapping(tmp_path):
    with MappingStore(tmp_path / "map.db", seed=42) as store:
        yield store


def assert_no_pseudonym_reused_as_original(mapping):
    """The core check: nothing anonymized has itself been anonymized again."""
    teams = mapping.get_all_team_mappings()
    runs = mapping.get_all_run_mappings()

    reused_teams = set(teams.values()) & set(teams.keys())
    reused_runs = set(runs.values()) & set(runs.keys())

    assert not reused_teams, f"team pseudonym re-anonymized as an original: {reused_teams}"
    assert not reused_runs, f"run pseudonym re-anonymized as an original: {reused_runs}"


def write_lines(path, records):
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8"
    )
    return path


class TestReportTransformer:

    def transform(self, tmp_path, mapping, record):
        transformer = ReportTransformer(
            mapping, RepairStore(tmp_path / "map.db"), ErrorCollector(), interactive=False,
        )
        out, _ = transformer.transform_line(
            json.dumps(record), tmp_path / RUN, 1, expected_run_id=RUN
        )
        return json.loads(out)

    def test_team_named_in_several_places_maps_once(self, tmp_path, mapping):
        """metadata.team_id and metadata.extra both name the same team."""
        out = self.transform(tmp_path, mapping, {
            "metadata": {
                "team_id": TEAM, "run_id": RUN, "topic_id": "topic-1",
                "extra": {"nested": {"team": TEAM}},
            },
            "responses": [{"text": "A sentence.", "citations": []}],
        })

        anon_team = mapping.get_team(TEAM)
        assert out["metadata"]["team_id"] == anon_team
        assert out["metadata"]["extra"]["nested"]["team"] == anon_team
        assert len(mapping.get_all_team_mappings()) == 1
        assert_no_pseudonym_reused_as_original(mapping)

    def test_run_maps_from_the_original(self, tmp_path, mapping):
        out = self.transform(tmp_path, mapping, {
            "metadata": {"team_id": TEAM, "run_id": RUN, "topic_id": "topic-1"},
            "responses": [{"text": "A sentence.", "citations": []}],
        })

        assert out["metadata"]["run_id"] == mapping.get_run(RUN)
        assert_no_pseudonym_reused_as_original(mapping)

    def test_many_records_do_not_accumulate_mappings(self, tmp_path, mapping):
        """Every line names the same team and run; one mapping each is correct."""
        for topic in ("topic-1", "topic-2", "topic-3"):
            self.transform(tmp_path, mapping, {
                "metadata": {"team_id": TEAM, "run_id": RUN, "topic_id": topic},
                "responses": [{"text": "A sentence.", "citations": []}],
            })

        assert len(mapping.get_all_team_mappings()) == 1
        assert len(mapping.get_all_run_mappings()) == 1
        assert_no_pseudonym_reused_as_original(mapping)


class TestNuggetBankTransformer:

    def bank(self, topic_id="topic-1", **extra_metadata):
        metadata = {"team_id": TEAM, "topic_id": topic_id,
                    "run_id": RUN, "run_desc": "names the system"}
        metadata.update(extra_metadata)
        return {
            "metadata": metadata,
            "nugget_bank": [{"question": "A question?", "aggregator_type": "AND",
                             "answers": [{"answer": "An answer", "references": ["doc-1"]}]}],
        }

    def run(self, tmp_path, mapping, records):
        source = write_lines(tmp_path / RUN, records)
        errors = ErrorCollector()
        NuggetBankTransformer(mapping, errors).transform_file(
            source, tmp_path / "out" / RUN, expected_run_id=RUN
        )
        written = [
            json.loads(line)
            for line in (tmp_path / "out" / RUN).read_text(encoding="utf-8").splitlines()
        ]
        return written, errors

    def test_every_topic_shares_one_pseudonym(self, tmp_path, mapping):
        records, _ = self.run(
            tmp_path, mapping, [self.bank("topic-1"), self.bank("topic-2")]
        )

        assert {r["metadata"]["team_id"] for r in records} == {mapping.get_team(TEAM)}
        assert len(mapping.get_all_team_mappings()) == 1
        assert len(mapping.get_all_run_mappings()) == 1
        assert_no_pseudonym_reused_as_original(mapping)

    def test_unknown_metadata_key_is_anonymized_once(self, tmp_path, mapping):
        """A team named under an unknown key is anonymized, not published.

        metadata is extra="allow", so `team` survives parsing. The declared fields
        are handled by hand and the unknown ones by the field scan, and the two sets
        do not overlap -- so `team` gets the *same* pseudonym as team_id and the
        mapping gains exactly one entry.
        """
        records, errors = self.run(tmp_path, mapping, [self.bank(team=TEAM)])

        anon_team = mapping.get_team(TEAM)
        assert records[0]["metadata"]["team_id"] == anon_team
        assert records[0]["metadata"]["team"] == anon_team

        # Handled, so the read-only second pass must not report it back as a gap
        reported = [i for i in errors.issues if i.issue_type.value == "identifier_found"]
        assert not any((i.field_path or "").startswith("metadata") for i in reported)

        assert len(mapping.get_all_team_mappings()) == 1
        assert_no_pseudonym_reused_as_original(mapping)


class TestRawJsonlTransformer:

    def test_team_repeated_at_depth_maps_once(self, tmp_path, mapping):
        """The raw path scans the whole record, so a repeated team is the risk case."""
        source = write_lines(tmp_path / RUN, [{
            "metadata": {"team_id": TEAM, "run_id": RUN},
            "nested": {"deeper": [{"team": TEAM}, {"runtag": RUN}]},
        }])
        transformer = RawJsonlTransformer(mapping, ErrorCollector())

        transformer.transform_file(source, tmp_path / "out" / RUN, expected_run_id=RUN)

        record = json.loads((tmp_path / "out" / RUN).read_text(encoding="utf-8").strip())
        anon_team = mapping.get_team(TEAM)
        assert record["metadata"]["team_id"] == anon_team
        assert record["nested"]["deeper"][0]["team"] == anon_team
        assert len(mapping.get_all_team_mappings()) == 1
        assert_no_pseudonym_reused_as_original(mapping)


class TestFieldScan:

    def test_repeated_calls_are_stable(self, mapping):
        """Anonymizing two records naming the same team must not chain pseudonyms."""
        first = {"team_id": TEAM}
        second = {"team_id": TEAM}

        anonymize_fields(first, mapping)
        anonymize_fields(second, mapping)

        assert first["team_id"] == second["team_id"] == mapping.get_team(TEAM)
        assert len(mapping.get_all_team_mappings()) == 1
        assert_no_pseudonym_reused_as_original(mapping)

    def test_re_anonymizing_output_is_refused(self, mapping, capsys):
        """Feeding output back in is warned about and left alone, not mapped again.

        With no handler MappingStore keeps the value as it is; the pipeline passes
        a handler that asks. Either way the mapping must not gain a second entry.
        """
        record = {"team_id": TEAM}
        anonymize_fields(record, mapping)
        pseudonym = record["team_id"]

        anonymize_fields(record, mapping)  # feed the output back in

        assert record["team_id"] == pseudonym
        assert len(mapping.get_all_team_mappings()) == 1
        assert "already the pseudonym" in capsys.readouterr().out
        assert_no_pseudonym_reused_as_original(mapping)

    def test_handler_can_allow_re_anonymizing(self, tmp_path):
        """When the handler says yes, the second mapping is made deliberately."""
        with MappingStore(
            tmp_path / "map.db", seed=42,
            on_pseudonym_reuse=lambda kind, value, original: True,
        ) as store:
            record = {"team_id": TEAM}
            anonymize_fields(record, store)
            pseudonym = record["team_id"]

            anonymize_fields(record, store)

            assert record["team_id"] != pseudonym
            assert len(store.get_all_team_mappings()) == 2
