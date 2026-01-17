"""Integration tests for the anonymization pipeline."""

import json
import pytest
import tempfile
import shutil
from pathlib import Path

from track_veil_lib.anonymizer.pipeline import AnonymizationPipeline, PipelineConfig
from track_veil_lib.anonymizer.mapping import MappingStore


@pytest.fixture
def temp_dirs():
    """Create temporary input/output directories."""
    input_dir = Path(tempfile.mkdtemp())
    output_dir = Path(tempfile.mkdtemp())
    db_path = Path(tempfile.mktemp(suffix=".db"))

    yield input_dir, output_dir, db_path

    # Cleanup
    shutil.rmtree(input_dir, ignore_errors=True)
    shutil.rmtree(output_dir, ignore_errors=True)
    db_path.unlink(missing_ok=True)


class TestMultiTaskProcessing:
    """Test that multiple task directories are processed correctly."""

    def test_multiple_eval_tasks_processed(self, temp_dirs):
        """All task directories in eval/ should appear in output."""
        input_dir, output_dir, db_path = temp_dirs

        # Create input structure with two tasks
        (input_dir / "runs" / "task1").mkdir(parents=True)
        (input_dir / "runs" / "task2").mkdir(parents=True)
        (input_dir / "eval" / "task1").mkdir(parents=True)
        (input_dir / "eval" / "task2").mkdir(parents=True)

        # Create report files (JSONL) - filename is just {run_id}
        report1 = {
            "metadata": {"team_id": "team1", "run_id": "run1", "topic_id": "t1"},
            "responses": [{"text": "Response text for task1"}],
        }
        report2 = {
            "metadata": {"team_id": "team2", "run_id": "run2", "topic_id": "t2"},
            "responses": [{"text": "Response text for task2"}],
        }
        (input_dir / "runs" / "task1" / "run1").write_text(
            json.dumps(report1) + "\n"
        )
        (input_dir / "runs" / "task2" / "run2").write_text(
            json.dumps(report2) + "\n"
        )

        # Create eval files with headers (so they get detected correctly)
        # Content may have run_id values that get anonymized
        eval_content1 = "run_id\ttopic_id\tmetric\tvalue\nrun1\t1\tndcg\t0.5\n"
        eval_content2 = "run_id\ttopic_id\tmetric\tvalue\nrun2\t2\tndcg\t0.6\n"
        (input_dir / "eval" / "task1" / "results.tsv").write_text(eval_content1)
        (input_dir / "eval" / "task2" / "results.tsv").write_text(eval_content2)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Verify both task directories exist in output
        assert (output_dir / "eval" / "task1").exists()
        assert (output_dir / "eval" / "task2").exists()
        assert (output_dir / "runs" / "task1").exists()
        assert (output_dir / "runs" / "task2").exists()

    def test_eval_files_anonymized(self, temp_dirs):
        """Eval TSV files should have run_id anonymized."""
        input_dir, output_dir, db_path = temp_dirs

        # Create minimal structure
        (input_dir / "runs" / "task1").mkdir(parents=True)
        (input_dir / "eval" / "task1").mkdir(parents=True)

        # Create report to establish mapping - filename is just {run_id}
        report = {
            "metadata": {"team_id": "myteam", "run_id": "myrun", "topic_id": "t1"},
            "responses": [{"text": "Test response"}],
        }
        (input_dir / "runs" / "task1" / "myrun").write_text(
            json.dumps(report) + "\n"
        )

        # Create eval file with run_id in content
        eval_content = "run_id\ttopic_id\tmetric\tvalue\nmyrun\t1\tndcg\t0.5\n"
        (input_dir / "eval" / "task1" / "results.tsv").write_text(eval_content)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Read output and verify anonymization
        output_file = output_dir / "eval" / "task1" / "results.tsv"
        content = output_file.read_text()

        # Should not contain original run_id
        assert "myrun" not in content
        lines = content.strip().split("\n")
        assert len(lines) == 2  # header + data


class TestEvalFilenameAnonymization:
    """Test eval filename anonymization with {run_id}.{judge} format."""

    def test_trec_eval_format_files_copied_with_anon_filename(self, temp_dirs):
        """trec_eval format files (no run_id column) should be copied with anonymized filename."""
        input_dir, output_dir, db_path = temp_dirs

        # Create structure
        (input_dir / "runs" / "task1").mkdir(parents=True)
        (input_dir / "eval" / "task1").mkdir(parents=True)

        # Create report to establish mapping
        # Note: run file name is just {run_id}, not {team}-{run_id}
        report = {
            "metadata": {"team_id": "myteam", "run_id": "myrun", "topic_id": "t1"},
            "responses": [{"text": "Test response"}],
        }
        (input_dir / "runs" / "task1" / "myrun").write_text(
            json.dumps(report) + "\n"
        )

        # Create eval file in trec_eval format (3 columns, no run_id)
        # Filename follows {run_id}.{judge} pattern
        eval_content = "1\tndcg\t0.5\n2\tndcg\t0.6\n"
        (input_dir / "eval" / "task1" / "myrun.nist-edit").write_text(eval_content)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Verify task directory exists
        assert (output_dir / "eval" / "task1").exists()

        # Verify original filename doesn't exist
        assert not (output_dir / "eval" / "task1" / "myrun.nist-edit").exists()

        # Find the anonymized file
        eval_files = list((output_dir / "eval" / "task1").iterdir())
        assert len(eval_files) == 1

        # Verify anonymized filename pattern
        anon_filename = eval_files[0].name
        assert "myrun" not in anon_filename
        assert ".nist-edit" in anon_filename  # Judge extension preserved

        # Verify content is unchanged (trec_eval format has no run_id to anonymize)
        content = eval_files[0].read_text()
        assert "1\tndcg\t0.5" in content

    def test_multiple_eval_tasks_with_trec_eval_format(self, temp_dirs):
        """Both task directories should appear even when using trec_eval format."""
        input_dir, output_dir, db_path = temp_dirs

        # Create structure with two tasks
        (input_dir / "runs" / "task1").mkdir(parents=True)
        (input_dir / "runs" / "task2").mkdir(parents=True)
        (input_dir / "eval" / "task1").mkdir(parents=True)
        (input_dir / "eval" / "task2").mkdir(parents=True)

        # Create reports - filename is just {run_id}
        report1 = {
            "metadata": {"team_id": "team1", "run_id": "run1", "topic_id": "t1"},
            "responses": [{"text": "Response 1"}],
        }
        report2 = {
            "metadata": {"team_id": "team2", "run_id": "run2", "topic_id": "t2"},
            "responses": [{"text": "Response 2"}],
        }
        (input_dir / "runs" / "task1" / "run1").write_text(
            json.dumps(report1) + "\n"
        )
        (input_dir / "runs" / "task2" / "run2").write_text(
            json.dumps(report2) + "\n"
        )

        # Create eval files in trec_eval format (no run_id column)
        # Filename is {run_id}.{judge}
        eval_content = "1\tndcg\t0.5\n"
        (input_dir / "eval" / "task1" / "run1.judge").write_text(eval_content)
        (input_dir / "eval" / "task2" / "run2.judge").write_text(eval_content)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Verify both task directories exist
        assert (output_dir / "eval" / "task1").exists()
        assert (output_dir / "eval" / "task2").exists()

        # Verify files exist in both (with anonymized names)
        task1_files = list((output_dir / "eval" / "task1").iterdir())
        task2_files = list((output_dir / "eval" / "task2").iterdir())
        assert len(task1_files) == 1
        assert len(task2_files) == 1


class TestFingerprintIntegration:
    """Test fingerprint storage during anonymization."""

    def test_fingerprints_stored_during_anonymization(self, temp_dirs):
        """Fingerprints should be stored in mapping.db during anonymization."""
        input_dir, output_dir, db_path = temp_dirs

        # Create structure
        (input_dir / "runs" / "task1").mkdir(parents=True)

        # Create report - filename is just {run_id}
        report = {
            "metadata": {"team_id": "team1", "run_id": "run1", "topic_id": "topic1"},
            "responses": [{"text": "This is the response text."}],
        }
        (input_dir / "runs" / "task1" / "run1").write_text(
            json.dumps(report) + "\n"
        )

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Check fingerprint was stored
        with MappingStore(db_path) as store:
            stats = store.get_stats()
            assert stats["fingerprints"] >= 1


class TestTrecEvalRunidAnonymization:
    """Test trec_eval format 'runid' metric line anonymization."""

    def test_runid_metric_line_anonymized_tab_separated(self, temp_dirs):
        """trec_eval 'runid' metric line should be anonymized (tab-separated)."""
        input_dir, output_dir, db_path = temp_dirs

        # Create structure
        (input_dir / "runs" / "task1").mkdir(parents=True)
        (input_dir / "eval" / "task1").mkdir(parents=True)

        # Create report to establish mapping
        report = {
            "metadata": {"team_id": "myteam", "run_id": "myrun", "topic_id": "t1"},
            "responses": [{"text": "Test response"}],
        }
        (input_dir / "runs" / "task1" / "myrun").write_text(
            json.dumps(report) + "\n"
        )

        # Create eval file in trec_eval format with runid metric line
        eval_content = "ndcg\t1\t0.5\nrunid\tall\tmyrun\nmap\tall\t0.3\n"
        (input_dir / "eval" / "task1" / "myrun.eval").write_text(eval_content)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Find the anonymized file
        eval_files = list((output_dir / "eval" / "task1").iterdir())
        assert len(eval_files) == 1

        content = eval_files[0].read_text()
        # Original run_id should not appear
        assert "myrun" not in content
        # runid metric line should exist with anonymized value
        assert "runid\tall\t" in content

    def test_runid_metric_line_anonymized_space_separated(self, temp_dirs):
        """trec_eval 'runid' metric line should be anonymized (space-separated)."""
        input_dir, output_dir, db_path = temp_dirs

        # Create structure
        (input_dir / "runs" / "task1").mkdir(parents=True)
        (input_dir / "eval" / "task1").mkdir(parents=True)

        # Create report to establish mapping
        report = {
            "metadata": {"team_id": "myteam", "run_id": "myrun", "topic_id": "t1"},
            "responses": [{"text": "Test response"}],
        }
        (input_dir / "runs" / "task1" / "myrun").write_text(
            json.dumps(report) + "\n"
        )

        # Create eval file with space-separated content (like real trec_eval output)
        eval_content = "ndcg                  1       0.5\nrunid                 all     myrun\nmap                   all     0.3\n"
        (input_dir / "eval" / "task1" / "myrun.eval").write_text(eval_content)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Find the anonymized file
        eval_files = list((output_dir / "eval" / "task1").iterdir())
        assert len(eval_files) == 1

        content = eval_files[0].read_text()
        # Original run_id should not appear
        assert "myrun" not in content
        # runid metric line should exist
        assert "runid" in content


class TestEvalContentReplacement:
    """Test that eval file content run_id is replaced with filename's anonymized run_id."""

    def test_eval_ranking_content_replaced_with_filename_run_id(self, temp_dirs):
        """Eval ranking file content should use filename's run_id, not content's."""
        input_dir, output_dir, db_path = temp_dirs

        # Create structure
        (input_dir / "runs" / "task1").mkdir(parents=True)
        (input_dir / "eval" / "task1").mkdir(parents=True)

        # Create report to establish mapping for "correct_run"
        report = {
            "metadata": {"team_id": "myteam", "run_id": "correct_run", "topic_id": "t1"},
            "responses": [{"text": "Test response"}],
        }
        (input_dir / "runs" / "task1" / "correct_run").write_text(
            json.dumps(report) + "\n"
        )

        # Create eval file with WRONG run_id in content (filename is source of truth)
        # Format: {topic} Q0 {doc_id} {rank} {score} {run_id}
        eval_content = "1\tQ0\tdoc1\t1\t0.9\twrong_run\n2\tQ0\tdoc2\t1\t0.8\twrong_run\n"
        (input_dir / "eval" / "task1" / "correct_run.judge").write_text(eval_content)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Find the anonymized file
        eval_files = list((output_dir / "eval" / "task1").iterdir())
        assert len(eval_files) == 1

        content = eval_files[0].read_text()
        # Neither original run_id should appear
        assert "correct_run" not in content
        assert "wrong_run" not in content
        # Content should have anonymized run_id (plantimal name)
        lines = content.strip().split("\n")
        for line in lines:
            parts = line.split("\t")
            assert len(parts) == 6
            # run_id column should be a plantimal name (alphabetic, lowercase)
            assert parts[5].isalpha() and parts[5].islower()


class TestRunsContentReplacement:
    """Test that runs file content run_id is replaced with filename's anonymized run_id."""

    def test_runs_ranking_content_replaced_with_filename_run_id(self, temp_dirs):
        """Runs ranking file content should use filename's run_id, not content's."""
        input_dir, output_dir, db_path = temp_dirs

        # Create structure
        (input_dir / "runs" / "task1").mkdir(parents=True)

        # Create ranking file with WRONG run_id in content (filename is source of truth)
        # Format: {topic} Q0 {doc_id} {rank} {score} {run_id}
        # Filename is "correct_run", but content has "wrong_run"
        ranking_content = "1\tQ0\tdoc1\t1\t0.9\twrong_run\n2\tQ0\tdoc2\t1\t0.8\twrong_run\n"
        (input_dir / "runs" / "task1" / "correct_run").write_text(ranking_content)

        # Run anonymization
        config = PipelineConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            mapping_db=db_path,
            interactive=False,
        )
        pipeline = AnonymizationPipeline(config)
        pipeline.run()
        pipeline.close()

        # Find the anonymized file
        run_files = list((output_dir / "runs" / "task1").iterdir())
        assert len(run_files) == 1

        content = run_files[0].read_text()
        # Neither original run_id should appear
        assert "correct_run" not in content
        assert "wrong_run" not in content
        # Content should have anonymized run_id (plantimal name)
        lines = content.strip().split("\n")
        for line in lines:
            parts = line.split("\t")
            assert len(parts) == 6
            # run_id column should be a plantimal name (alphabetic, lowercase)
            assert parts[5].isalpha() and parts[5].islower()
