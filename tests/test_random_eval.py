"""Tests for `track-veil random-eval`.

The generated file must carry no information but be well-formed: eval JSONL
records, one per run/topic pair plus an "all" row per run whose score is the
mean of that run's topics, and scores spread out enough to rank the runs.

The record shape must match autojudge_evaluate.eval_results.io - that is what
produces the official *.eval.jsonl files and what meta-evaluate reads back.
"""

import json
from collections import defaultdict

import pytest
from click.testing import CliRunner

from track_veil_lib.anonymizer.cli import cli, _read_topic_ids_from_file


def build_anon_dir(root, runs=("Caf-672", "Lux-196", "Zug-665"), task="task1"):
    """A minimal anonymized directory: runs/{task}/{run_id} files."""
    task_dir = root / "anon" / "runs" / task
    task_dir.mkdir(parents=True)
    for run_id in runs:
        (task_dir / run_id).write_text("{}\n", encoding="utf-8")
    return root / "anon"


def write_topics(path, topics=("topic1", "topic2", "topic3")):
    path.write_text("\n".join(topics) + "\n", encoding="utf-8")
    return path


def run_command(data_dir, topics, *extra):
    result = CliRunner().invoke(
        cli, ["random-eval", "-d", str(data_dir), "--topics", str(topics), *extra]
    )
    assert result.exit_code == 0, result.output
    return result


def parse(path):
    """Return (records, {run_id: {topic_id: score}}) from an eval JSONL file."""
    records = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
    ]
    rows = defaultdict(dict)
    for record in records:
        rows[record["run_id"]][record["topic_id"]] = float(record["value"])
    return records, rows


@pytest.fixture
def dataset(tmp_path):
    data_dir = build_anon_dir(tmp_path)
    topics = write_topics(tmp_path / "topics.txt")
    return data_dir, topics


class TestFileShape:
    """Columns, rows and placement."""

    def test_written_to_eval_task_directory(self, dataset):
        data_dir, topics = dataset
        run_command(data_dir, topics)
        assert (data_dir / "eval" / "task1" / "random.eval.jsonl").exists()

    def test_jsonl_record_shape(self, dataset):
        """Must match autojudge_evaluate.eval_results.io, which meta-evaluate reads."""
        data_dir, topics = dataset
        run_command(data_dir, topics, "--seed", "42")
        path = data_dir / "eval" / "task1" / "random.eval.jsonl"

        records, _ = parse(path)
        for record in records:
            assert list(record) == ["run_id", "topic_id", "measure", "value"]
            assert isinstance(record["value"], float)

        # No header line, and one JSON object per line
        first = path.read_text(encoding="utf-8").splitlines()[0]
        assert json.loads(first)["measure"] == "random"

    def test_one_row_per_run_topic_pair_plus_all(self, dataset):
        data_dir, topics = dataset
        run_command(data_dir, topics)
        _, rows = parse(data_dir / "eval" / "task1" / "random.eval.jsonl")

        assert set(rows) == {"Caf-672", "Lux-196", "Zug-665"}
        for topics_of_run in rows.values():
            assert set(topics_of_run) == {"all", "topic1", "topic2", "topic3"}

    def test_measure_name_is_configurable(self, dataset):
        data_dir, topics = dataset
        run_command(data_dir, topics, "--metric", "ndcg_fake", "--filename", "x.jsonl")
        records, _ = parse(data_dir / "eval" / "task1" / "x.jsonl")
        assert {r["measure"] for r in records} == {"ndcg_fake"}

    def test_run_ids_come_from_run_filenames(self, tmp_path):
        data_dir = build_anon_dir(tmp_path, runs=("only-run",))
        topics = write_topics(tmp_path / "topics.txt")
        run_command(data_dir, topics)
        _, rows = parse(data_dir / "eval" / "task1" / "random.eval.jsonl")
        assert set(rows) == {"only-run"}


class TestScores:
    """The properties that make the dummy file usable for an evaluation run."""

    def test_all_row_is_the_mean_of_its_topics(self, dataset):
        data_dir, topics = dataset
        run_command(data_dir, topics)
        _, rows = parse(data_dir / "eval" / "task1" / "random.eval.jsonl")

        for run_id, scores in rows.items():
            per_topic = [v for t, v in scores.items() if t != "all"]
            # Exact: the aggregate is written unrounded, so it is precisely the
            # mean of the per-topic values the file contains.
            assert scores["all"] == pytest.approx(
                sum(per_topic) / len(per_topic), abs=1e-9
            ), f"aggregate disagrees with its topics for {run_id}"

    def test_scores_are_within_range(self, dataset):
        data_dir, topics = dataset
        run_command(data_dir, topics, "--max-score", "10.0")
        _, rows = parse(data_dir / "eval" / "task1" / "random.eval.jsonl")

        for scores in rows.values():
            for value in scores.values():
                assert 0.0 <= value <= 10.0

    def test_runs_are_rankable(self, dataset):
        """All-equal scores would leave the ranking undefined - the point of randomizing."""
        data_dir, topics = dataset
        run_command(data_dir, topics, "--seed", "42")
        _, rows = parse(data_dir / "eval" / "task1" / "random.eval.jsonl")

        aggregates = [scores["all"] for scores in rows.values()]
        assert len(set(aggregates)) == len(aggregates)

    def test_seed_makes_output_reproducible(self, dataset):
        data_dir, topics = dataset
        target = data_dir / "eval" / "task1" / "random.eval.jsonl"

        run_command(data_dir, topics, "--seed", "42")
        first = target.read_text(encoding="utf-8")
        run_command(data_dir, topics, "--seed", "42")
        assert target.read_text(encoding="utf-8") == first

        run_command(data_dir, topics, "--seed", "7")
        assert target.read_text(encoding="utf-8") != first


class TestTopicList:
    """--topics parsing."""

    def test_order_preserved_and_duplicates_dropped(self, tmp_path):
        data_dir = build_anon_dir(tmp_path, runs=("r1",))
        topics = tmp_path / "topics.txt"
        topics.write_text("b\na\nb\n\n# comment\nc\n", encoding="utf-8")

        run_command(data_dir, topics)
        records, _ = parse(data_dir / "eval" / "task1" / "random.eval.jsonl")
        # Per-topic rows keep the topic-file order; the builder appends aggregates last
        assert [r["topic_id"] for r in records] == ["b", "a", "c", "all"]

    def test_jsonl_topics_use_the_id_field(self, tmp_path):
        """Regression: whole JSON lines were used as topic ids."""
        data_dir = build_anon_dir(tmp_path, runs=("r1",))
        topics = tmp_path / "topics.jsonl"
        topics.write_text(
            json.dumps({"request_id": "rag2026-0", "title": "a long question ..."}) + "\n"
            + json.dumps({"request_id": "rag2026-1", "title": "another"}) + "\n",
            encoding="utf-8",
        )

        run_command(data_dir, topics)
        _, rows = parse(data_dir / "eval" / "task1" / "random.eval.jsonl")
        assert set(rows["r1"]) == {"all", "rag2026-0", "rag2026-1"}

    @pytest.mark.parametrize("record,expected", [
        ({"request_id": "r-1"}, "r-1"),
        ({"topic_id": "t-1"}, "t-1"),
        ({"query_id": "q-1"}, "q-1"),
        ({"narrative_id": 2024}, "2024"),   # ints are stringified
        ({"id": "i-1"}, "i-1"),
    ])
    def test_supported_id_fields(self, tmp_path, record, expected):
        topics = tmp_path / "topics.jsonl"
        topics.write_text(json.dumps(record) + "\n", encoding="utf-8")
        assert _read_topic_ids_from_file(topics) == [expected]

    def test_bare_and_numeric_ids_still_work(self, tmp_path):
        """A numeric id parses as JSON but is not an object - keep it verbatim."""
        topics = tmp_path / "topics.txt"
        topics.write_text("42\nplain-topic\n", encoding="utf-8")
        assert _read_topic_ids_from_file(topics) == ["42", "plain-topic"]

    def test_json_record_without_id_field_is_an_error(self, tmp_path):
        data_dir = build_anon_dir(tmp_path, runs=("r1",))
        topics = tmp_path / "topics.jsonl"
        topics.write_text(json.dumps({"title": "no id here"}) + "\n", encoding="utf-8")

        result = CliRunner().invoke(
            cli, ["random-eval", "-d", str(data_dir), "--topics", str(topics)]
        )
        assert result.exit_code != 0
        assert "no id field" in result.output

    def test_empty_topic_file_is_an_error(self, tmp_path):
        data_dir = build_anon_dir(tmp_path, runs=("r1",))
        topics = tmp_path / "topics.txt"
        topics.write_text("\n# nothing here\n", encoding="utf-8")

        result = CliRunner().invoke(
            cli, ["random-eval", "-d", str(data_dir), "--topics", str(topics)]
        )
        assert result.exit_code != 0
        assert "No topic ids" in result.output


class TestTaskSelection:
    """Which task directories get a file."""

    def test_all_tasks_by_default(self, tmp_path):
        data_dir = build_anon_dir(tmp_path, runs=("r1",), task="task1")
        (data_dir / "runs" / "task2").mkdir()
        (data_dir / "runs" / "task2" / "r2").write_text("{}\n", encoding="utf-8")
        topics = write_topics(tmp_path / "topics.txt")

        run_command(data_dir, topics)
        assert (data_dir / "eval" / "task1" / "random.eval.jsonl").exists()
        assert (data_dir / "eval" / "task2" / "random.eval.jsonl").exists()

    def test_single_task_selection(self, tmp_path):
        data_dir = build_anon_dir(tmp_path, runs=("r1",), task="task1")
        (data_dir / "runs" / "task2").mkdir()
        (data_dir / "runs" / "task2" / "r2").write_text("{}\n", encoding="utf-8")
        topics = write_topics(tmp_path / "topics.txt")

        run_command(data_dir, topics, "--task", "task1")
        assert (data_dir / "eval" / "task1" / "random.eval.jsonl").exists()
        assert not (data_dir / "eval" / "task2").exists()

    def test_unknown_task_is_an_error(self, dataset):
        data_dir, topics = dataset
        result = CliRunner().invoke(cli, [
            "random-eval", "-d", str(data_dir), "--topics", str(topics), "--task", "nope",
        ])
        assert result.exit_code != 0
        assert "Task not found" in result.output

    def test_missing_runs_directory_is_an_error(self, tmp_path):
        data_dir = tmp_path / "anon"
        data_dir.mkdir()
        topics = write_topics(tmp_path / "topics.txt")

        result = CliRunner().invoke(
            cli, ["random-eval", "-d", str(data_dir), "--topics", str(topics)]
        )
        assert result.exit_code != 0
        assert "Runs directory not found" in result.output
