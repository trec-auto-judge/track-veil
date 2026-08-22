"""Which eval files come from manual assessments.

Two things in a dataset depend on this and nothing else: `assessed_topics`, and the
`truth` file a meta-evaluation correlates against. Neither can be read off a filename
-- "official" says who published an evaluation, not what produced it, and an official
eval computed by an automatic judge is not an assessment -- so the question is put to
the user and the answer is kept.

These tests also pin the layout the scan accepts, which is the bug that started this:
the anonymizer writes eval/{task}/, mirroring metadata/{task}/, but the scan looked
only at the eval root with the task in the filename, and reported "0 assessed topics"
rather than "I looked in the wrong place".
"""
from track_veil_lib.anonymizer.cli import (
    _discover_eval_candidates,
    _resolve_manual_evals,
)
from track_veil_lib.anonymizer.decisions import DecisionsStore

TASK = "generation"


def write(path, topic_id="101"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '{"topic_id": "%s", "measure": "ndcg", "run_id": "r1"}\n' % topic_id,
        encoding="utf-8",
    )
    return path


def never_asks(*args, **kwargs):
    raise AssertionError("asked the user when the answer was already determined")


def resolve(tmp_path, decisions=None, interactive=False):
    return _resolve_manual_evals(tmp_path / "eval", TASK, decisions, interactive)


class TestDiscovery:
    """Both layouts are read; finding a file says nothing about what produced it."""

    def test_the_task_subdirectory_layout_is_found(self, tmp_path):
        """eval/{task}/ - what the anonymizer actually writes."""
        wanted = write(tmp_path / "eval" / TASK / "umbrela.official.eval.jsonl")

        assert _discover_eval_candidates(tmp_path / "eval", TASK) == [wanted]

    def test_the_flat_layout_is_still_found(self, tmp_path):
        """eval/*.{task}.* - the older exports."""
        wanted = write(tmp_path / "eval" / f"umbrela.{TASK}.official.eval.jsonl")

        assert _discover_eval_candidates(tmp_path / "eval", TASK) == [wanted]

    def test_another_tasks_files_are_not_candidates(self, tmp_path):
        write(tmp_path / "eval" / "retrieval" / "umbrela.official.eval.jsonl")

        assert _discover_eval_candidates(tmp_path / "eval", TASK) == []

    def test_a_missing_eval_directory_is_not_an_error(self, tmp_path):
        assert _discover_eval_candidates(tmp_path / "eval", TASK) == []


class TestNothingIsAssumed:
    """The name is never the answer, and neither is silence."""

    def test_a_file_named_official_is_not_assumed_to_be_manual(self, tmp_path, monkeypatch):
        """An official evaluation may well be computed by an automatic judge."""
        monkeypatch.setattr("click.confirm", never_asks)
        write(tmp_path / "eval" / TASK / "umbrela.official.eval.jsonl")

        manual, ignored = resolve(tmp_path, DecisionsStore(), interactive=False)

        assert manual == []
        assert [f.name for f in ignored] == ["umbrela.official.eval.jsonl"]

    def test_nothing_is_recorded_when_nothing_was_asked(self, tmp_path, monkeypatch):
        """An assumption is not a decision; recording one would replay it as if it were."""
        monkeypatch.setattr("click.confirm", never_asks)
        write(tmp_path / "eval" / TASK / "umbrela.official.eval.jsonl")
        decisions = DecisionsStore()

        resolve(tmp_path, decisions, interactive=False)

        assert decisions.get_manual_evals(TASK) is None

    def test_no_candidates_at_all_asks_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("click.confirm", never_asks)

        assert resolve(tmp_path, DecisionsStore(), interactive=True) == ([], [])


class TestAsking:

    def test_the_chosen_file_is_manual_and_the_rest_are_not(self, tmp_path, monkeypatch):
        write(tmp_path / "eval" / TASK / "nist.eval.jsonl")
        write(tmp_path / "eval" / TASK / "autojudge.eval.jsonl")
        monkeypatch.setattr("click.confirm", lambda prompt, **kw: "nist" in prompt)

        manual, ignored = resolve(tmp_path, DecisionsStore(), interactive=True)

        assert [f.name for f in manual] == ["nist.eval.jsonl"]
        assert [f.name for f in ignored] == ["autojudge.eval.jsonl"]

    def test_the_answer_is_recorded(self, tmp_path, monkeypatch):
        write(tmp_path / "eval" / TASK / "nist.eval.jsonl")
        monkeypatch.setattr("click.confirm", lambda prompt, **kw: True)
        decisions = DecisionsStore()

        resolve(tmp_path, decisions, interactive=True)

        assert decisions.get_manual_evals(TASK) == ["nist.eval.jsonl"]

    def test_answering_none_is_recorded_as_a_real_answer(self, tmp_path, monkeypatch):
        """Otherwise the same question comes back on every run."""
        write(tmp_path / "eval" / TASK / "autojudge.eval.jsonl")
        monkeypatch.setattr("click.confirm", lambda prompt, **kw: False)
        decisions = DecisionsStore()

        resolve(tmp_path, decisions, interactive=True)

        assert decisions.get_manual_evals(TASK) == []

    def test_the_official_marker_only_sets_the_default(self, tmp_path, monkeypatch):
        """It preselects the answer for the user; it does not decide it."""
        write(tmp_path / "eval" / TASK / "umbrela.official.eval.jsonl")
        write(tmp_path / "eval" / TASK / "random.eval.jsonl")
        defaults = {}

        def record(prompt, **kw):
            defaults[prompt.split("/")[-1].split(" ")[0]] = kw.get("default")
            return kw.get("default")

        monkeypatch.setattr("click.confirm", record)
        resolve(tmp_path, DecisionsStore(), interactive=True)

        assert defaults["umbrela.official.eval.jsonl"] is True
        assert defaults["random.eval.jsonl"] is False


class TestReplay:
    """A stored answer is replayed, and replaces asking entirely."""

    def test_a_stored_choice_is_used_without_asking(self, tmp_path, monkeypatch):
        monkeypatch.setattr("click.confirm", never_asks)
        write(tmp_path / "eval" / TASK / "nist.eval.jsonl")
        write(tmp_path / "eval" / TASK / "autojudge.eval.jsonl")
        decisions = DecisionsStore()
        decisions.set_manual_evals(TASK, ["nist.eval.jsonl"])

        manual, ignored = resolve(tmp_path, decisions, interactive=True)

        assert [f.name for f in manual] == ["nist.eval.jsonl"]
        assert [f.name for f in ignored] == ["autojudge.eval.jsonl"]

    def test_a_stored_none_is_not_confused_with_never_asked(self, tmp_path, monkeypatch):
        monkeypatch.setattr("click.confirm", never_asks)
        write(tmp_path / "eval" / TASK / "umbrela.official.eval.jsonl")
        decisions = DecisionsStore()
        decisions.set_manual_evals(TASK, [])

        manual, ignored = resolve(tmp_path, decisions, interactive=True)

        assert manual == []
        assert [f.name for f in ignored] == ["umbrela.official.eval.jsonl"]

    def test_a_stored_file_that_is_gone_is_simply_absent(self, tmp_path, monkeypatch):
        monkeypatch.setattr("click.confirm", never_asks)
        decisions = DecisionsStore()
        decisions.set_manual_evals(TASK, ["nist.eval.jsonl"])

        assert resolve(tmp_path, decisions, interactive=True) == ([], [])


class TestPersistence:

    def test_manual_evals_survive_a_save_load_round_trip(self, tmp_path):
        decisions = DecisionsStore()
        decisions.set_manual_evals(TASK, ["nist.eval.jsonl"])
        decisions.set_manual_evals("retrieval", [])
        path = tmp_path / "decisions.yml"

        decisions.save(path)
        loaded = DecisionsStore.load(path)

        assert loaded.get_manual_evals(TASK) == ["nist.eval.jsonl"]
        assert loaded.get_manual_evals("retrieval") == []
        assert loaded.get_manual_evals("never-seen") is None
