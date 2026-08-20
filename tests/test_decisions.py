"""Tests for saving and replaying anonymization decisions.

Covers the decisions store round-trip (including the per-directory file_type) and
the pipeline behaviour that feeds it: runs formats, eval formats, JSONL eval
directories, and replay via --load-decisions.
"""

import json

import pytest

from track_veil_lib.anonymizer.decisions import DecisionsStore, FormatDecision
from track_veil_lib.anonymizer.pipeline import AnonymizationPipeline, PipelineConfig
from track_veil_lib.anonymizer.transformers import TsvFormat


def build_dataset(root, *, eval_jsonl=True, eval_tsv=True):
    """Create an input tree with a TSV runs dir and eval dirs of both types."""
    inp = root / "input"
    (inp / "runs" / "gen").mkdir(parents=True)
    (inp / "metadata" / "gen").mkdir(parents=True)

    # runs/gen: TSV ranking, run_id in column 5
    (inp / "runs" / "gen" / "run1").write_text(
        "\n".join(f"topic{i} Q0 doc{i} {i} 0.9 run1" for i in range(3)) + "\n",
        encoding="utf-8",
    )
    (inp / "metadata" / "gen" / "meta.jl").write_text(
        json.dumps({"runtag": "run1", "org": "TeamX"}) + "\n", encoding="utf-8"
    )

    if eval_tsv:
        (inp / "eval" / "gen").mkdir(parents=True)
        (inp / "eval" / "gen" / "run1").write_text(
            "ndcg\tall\t0.5\nrunid\tall\trun1\n", encoding="utf-8"
        )

    if eval_jsonl:
        (inp / "eval" / "official").mkdir(parents=True)
        (inp / "eval" / "official" / "run1").write_text(
            json.dumps({"run_id": "run1", "topic_id": "42", "value": 0.46}) + "\n"
            + json.dumps({"run_id": "run1", "team_id": "TeamX", "topic_id": "all"}) + "\n",
            encoding="utf-8",
        )

    return inp


def run_pipeline(root, inp, out_name, **kwargs):
    config = PipelineConfig(
        input_dir=inp,
        output_dir=root / out_name,
        mapping_db=root / "map.db",
        interactive=False,
        **kwargs,
    )
    AnonymizationPipeline(config).run()
    return root / out_name


def only_file(directory):
    """The single file in a directory (eval dirs hold one run each here)."""
    files = sorted(p for p in directory.iterdir() if p.is_file())
    assert len(files) == 1, f"expected one file in {directory}, got {files}"
    return files[0]


class TestFormatDecision:
    """Serialization of a single decision."""

    def test_tsv_round_trip(self):
        fd = FormatDecision(
            format="ranking", run_id_cols=[5], team_cols=[], has_header=False,
        )
        restored = FormatDecision.from_dict(fd.to_dict())
        assert restored == fd
        assert restored.file_type == "tsv"

    def test_jsonl_omits_tsv_layout(self):
        fd = FormatDecision(file_type="jsonl", filename_pattern=".official")
        d = fd.to_dict()
        assert d["file_type"] == "jsonl"
        assert "format" not in d
        assert "run_id_cols" not in d
        assert FormatDecision.from_dict(d) == fd

    def test_legacy_dict_without_file_type_defaults_to_tsv(self):
        fd = FormatDecision.from_dict({"format": "trec_eval", "run_id_cols": []})
        assert fd.file_type == "tsv"
        assert fd.format == "trec_eval"


class TestDecisionsStore:
    """Store-level accessors and YAML round-trip."""

    def test_save_load_round_trip(self, tmp_path):
        store = DecisionsStore()
        store.set_runs_format("gen", TsvFormat.RANKING, [5], [], False)
        store.set_eval_format("gen", TsvFormat.TREC_EVAL, [], [], False, ".qrel_eval")
        store.set_eval_format("official", None, [], [], False, "", file_type="jsonl")
        store.set_manual_run_id("gen", "weird_file", "run7")

        path = tmp_path / "decisions.yml"
        store.save(path)
        loaded = DecisionsStore.load(path)

        assert loaded.get_runs_format("gen") == (TsvFormat.RANKING, [5], [], False)
        assert loaded.get_eval_format("gen")[0] == TsvFormat.TREC_EVAL
        assert loaded.get_eval_file_type("official") == "jsonl"
        assert loaded.get_manual_run_id("gen", "weird_file") == "run7"

    def test_jsonl_has_no_tsv_format_but_keeps_pattern(self, tmp_path):
        store = DecisionsStore()
        store.set_eval_format("official", None, [], [], False, ".eval", file_type="jsonl")

        path = tmp_path / "decisions.yml"
        store.save(path)
        loaded = DecisionsStore.load(path)

        # No TSV layout to report...
        assert loaded.get_eval_format("official") is None
        # ...but the filename pattern must still replay.
        assert loaded.get_eval_filename_pattern("official") == ".eval"

    def test_empty_filename_pattern_survives(self, tmp_path):
        """An empty pattern means "filename is the run_id" - it must not be lost."""
        store = DecisionsStore()
        store.set_eval_format("official", None, [], [], False, "", file_type="jsonl")

        path = tmp_path / "decisions.yml"
        store.save(path)

        assert DecisionsStore.load(path).get_eval_filename_pattern("official") == ""

    def test_unknown_task_returns_none(self):
        store = DecisionsStore()
        assert store.get_runs_format("nope") is None
        assert store.get_eval_format("nope") is None
        assert store.get_eval_file_type("nope") is None
        assert store.get_eval_filename_pattern("nope") is None


class TestExtractRunIdFromEvalFilename:
    """Filename -> run_id, including the no-suffix layouts."""

    @pytest.mark.parametrize("filename,suffix,expected", [
        ("run1", "", "run1"),                        # empty suffix: filename IS the run_id
        ("a.b.c", "", "a.b.c"),
        ("run1", None, "run1"),                      # no known suffix: whole filename
        ("run1.qrel_eval", ".qrel_eval", "run1"),
        ("a.b.c.qrel_eval", ".qrel_eval", "a.b.c"),
        ("run1.other", ".qrel_eval", None),          # suffix mismatch
        (".qrel_eval", ".qrel_eval", None),          # suffix only, nothing left
    ])
    def test_cases(self, filename, suffix, expected):
        extract = AnonymizationPipeline._extract_run_id_from_eval_filename
        assert extract(None, filename, suffix) == expected


class TestPipelineSavesDecisions:
    """What lands in decisions.yml after a run."""

    def test_runs_and_eval_formats_are_saved(self, tmp_path):
        inp = build_dataset(tmp_path)
        decisions = tmp_path / "decisions.yml"
        run_pipeline(tmp_path, inp, "out", save_decisions=decisions)

        store = DecisionsStore.load(decisions)

        # runs format (was previously never recorded)
        assert store.get_runs_format("gen") == (TsvFormat.RANKING, [5], [], False)
        # eval TSV format
        assert store.get_eval_format("gen")[0] == TsvFormat.TREC_EVAL
        # eval JSONL directory recorded by file type, with no TSV layout
        assert store.get_eval_file_type("official") == "jsonl"
        assert store.get_eval_format("official") is None

    def test_file_type_recorded_for_jsonl_runs_directory(self, tmp_path):
        inp = tmp_path / "input"
        (inp / "runs" / "gen").mkdir(parents=True)
        (inp / "runs" / "gen" / "run1").write_text(json.dumps({
            "metadata": {"team_id": "TeamX", "run_id": "run1", "topic_id": "42"},
            "responses": [{"text": "Hello.", "citations": ["docA"]}],
        }) + "\n", encoding="utf-8")

        decisions = tmp_path / "decisions.yml"
        run_pipeline(tmp_path, inp, "out", save_decisions=decisions)

        assert DecisionsStore.load(decisions).get_runs_file_type("gen") == "jsonl"


class TestJsonlEvalAnonymization:
    """JSONL eval directories: fields inside the JSON, not TSV columns."""

    def test_run_id_and_team_are_rewritten(self, tmp_path):
        inp = build_dataset(tmp_path)
        out = run_pipeline(tmp_path, inp, "out")

        result = only_file(out / "eval" / "official")
        records = [json.loads(line) for line in result.read_text(encoding="utf-8").splitlines()]

        anon_run = result.name  # filename is the anonymized run_id
        assert [r["run_id"] for r in records] == [anon_run, anon_run]
        assert "run1" not in result.read_text(encoding="utf-8")

        # team_id was mapped (it exists in the mapping via runs/metadata)
        assert records[1]["team_id"] != "TeamX"

    def test_json_stays_valid(self, tmp_path):
        """Regression: the TSV copier used to split JSON lines on whitespace."""
        inp = build_dataset(tmp_path)
        out = run_pipeline(tmp_path, inp, "out")

        for line in only_file(out / "eval" / "official").read_text(encoding="utf-8").splitlines():
            json.loads(line)  # raises if the record was mangled

    def test_unparseable_line_is_copied_and_reported(self, tmp_path):
        inp = build_dataset(tmp_path, eval_tsv=False)
        target = inp / "eval" / "official" / "run1"
        target.write_text(
            json.dumps({"run_id": "run1", "value": 1}) + "\nnot json at all\n",
            encoding="utf-8",
        )

        out = run_pipeline(tmp_path, inp, "out")
        lines = only_file(out / "eval" / "official").read_text(encoding="utf-8").splitlines()
        assert "not json at all" in lines


class TestFilenamePatternSentinels:
    """"SKIP" and "MANUAL" are decisions too, and must survive a round-trip."""

    def _interactive(self, root, inp, out_name, choice, decisions_kwargs):
        """Run interactively with a scripted answer to every menu."""
        config = PipelineConfig(
            input_dir=inp,
            output_dir=root / out_name,
            mapping_db=root / "map.db",
            interactive=True,
            **decisions_kwargs,
        )
        pipeline = AnonymizationPipeline(config, ask_fn=lambda prompt, options: choice(prompt, options))
        pipeline.run()
        return root / out_name

    def test_skip_is_saved_and_replayed(self, tmp_path):
        inp = build_dataset(tmp_path, eval_tsv=False)
        decisions = tmp_path / "decisions.yml"

        # "Skip this task" is the third option of the filename-pattern menu
        def choose_skip(prompt, options):
            return 2 if "filenames" in prompt.lower() else 0

        self._interactive(tmp_path, inp, "out1", choose_skip, {"save_decisions": decisions})
        assert DecisionsStore.load(decisions).get_eval_filename_pattern("official") == "SKIP"

        # Replayed without asking: the task is skipped again
        def refuse(prompt, options):
            raise AssertionError(f"should not have prompted: {prompt}")

        out2 = self._interactive(tmp_path, inp, "out2", refuse, {"load_decisions": decisions})
        assert not (out2 / "eval" / "official").exists()

    def test_manual_run_ids_are_saved_and_replayed(self, tmp_path, monkeypatch):
        inp = build_dataset(tmp_path, eval_tsv=False)
        decisions = tmp_path / "decisions.yml"

        # "Enter run_id manually for each file" is the second option
        def choose_manual(prompt, options):
            return 1 if "filenames" in prompt.lower() else 0

        monkeypatch.setattr("builtins.input", lambda *a, **k: "run1")
        self._interactive(tmp_path, inp, "out1", choose_manual, {"save_decisions": decisions})

        store = DecisionsStore.load(decisions)
        assert store.get_eval_filename_pattern("official") == "MANUAL"
        assert store.get_manual_run_id("official", "run1") == "run1"

        # Replay must not call input() again
        monkeypatch.setattr("builtins.input", lambda *a, **k: pytest.fail("prompted again"))

        def refuse(prompt, options):
            raise AssertionError(f"should not have prompted: {prompt}")

        out2 = self._interactive(tmp_path, inp, "out2", refuse, {"load_decisions": decisions})
        assert only_file(out2 / "eval" / "official").exists()


class TestReplayDecisions:
    """--load-decisions reproduces a run without asking again."""

    def test_second_pass_matches_first(self, tmp_path):
        inp = build_dataset(tmp_path)
        decisions = tmp_path / "decisions.yml"

        first = run_pipeline(tmp_path, inp, "out1", save_decisions=decisions)
        second = run_pipeline(tmp_path, inp, "out2", load_decisions=decisions)

        for task in ("gen", "official"):
            a = only_file(first / "eval" / task)
            b = only_file(second / "eval" / task)
            assert a.name == b.name
            assert a.read_text(encoding="utf-8") == b.read_text(encoding="utf-8")
