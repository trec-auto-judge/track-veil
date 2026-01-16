"""Tests for TSV format detection."""

import pytest
from trec_anon_lib.anonymizer.transformers import detect_tsv_format, TsvFormat


class TestHeaderDetection:
    """Test header row detection in TSV files."""

    def test_real_header_with_run_id(self):
        """Header row with known column names should be detected."""
        lines = [
            "run_id request_id metric value",
            "team1 1 ndcg 0.5",
            "team2 2 ndcg 0.6",
        ]
        hint = detect_tsv_format(lines)
        assert hint.confidence == "high"
        assert hint.run_id_columns == [0]
        assert "Header detected" in hint.reason

    def test_real_header_with_tabs(self):
        """Header row with tabs should be detected."""
        lines = [
            "run_id\trequest_id\tmetric\tvalue",
            "team1\t1\tndcg\t0.5",
        ]
        hint = detect_tsv_format(lines)
        assert hint.run_id_columns == [0]

    def test_data_row_with_numeric_not_header(self):
        """Row with numeric values (like 0.1) should NOT be detected as header."""
        lines = [
            "team1-run1 topic measure 0.1",
            "team1-run1 topic measure 0.2",
        ]
        hint = detect_tsv_format(lines)
        # Should fall back to column-count heuristics, not header detection
        assert "Header detected" not in hint.reason
        assert hint.likely_format == TsvFormat.TOT
        assert hint.run_id_columns == [0]

    def test_data_row_without_header_names(self):
        """Row without known header names should use heuristics."""
        lines = [
            "team1-run1 topicA ndcg best",
            "team2-run2 topicB map worst",
        ]
        hint = detect_tsv_format(lines)
        assert "Header detected" not in hint.reason

    def test_header_with_metric_and_topic_no_runid(self):
        """Header with topic and metric but no run_id = trec_eval format."""
        lines = [
            "topic_id metric value",
            "1 ndcg 0.5",
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.TREC_EVAL
        assert hint.run_id_columns == []


class TestColumnCountHeuristics:
    """Test column-count based format detection."""

    def test_three_columns_trec_eval(self):
        """3 columns should be detected as trec_eval format."""
        lines = [
            "1 ndcg 0.5",
            "2 ndcg 0.6",
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.TREC_EVAL
        assert hint.run_id_columns == []

    def test_four_columns_with_topic_in_col1(self):
        """4 columns with topic-like value in col 1 = ir_measures."""
        lines = [
            "run1 1 ndcg 0.5",
            "run1 2 map 0.4",
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.IR_MEASURES
        assert hint.run_id_columns == [0]

    def test_four_columns_with_topic_in_col2(self):
        """4 columns with topic-like value in col 2 = tot format."""
        lines = [
            "run1 ndcg 1 0.5",
            "run1 map 2 0.4",
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.TOT
        assert hint.run_id_columns == [0]

    def test_six_columns_with_q0_ranking(self):
        """6 columns with Q0 and float score = ranking format."""
        lines = [
            "1 Q0 doc1 1 0.95 run1",
            "1 Q0 doc2 2 0.85 run1",
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.RANKING
        assert hint.run_id_columns == [5]

    def test_six_columns_with_q0_int_values(self):
        """6 columns with Q0 and int values = ranking format (unified)."""
        lines = [
            "1 Q0 doc1 1 1 run1",
            "1 Q0 doc2 2 0 run1",
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.RANKING
        assert hint.run_id_columns == [5]


class TestEdgeCases:
    """Test edge cases in format detection."""

    def test_empty_file(self):
        """Empty file should return unknown format."""
        hint = detect_tsv_format([])
        assert hint.likely_format == TsvFormat.UNKNOWN
        assert hint.confidence == "low"

    def test_only_comments(self):
        """File with only comments should return unknown."""
        lines = [
            "# This is a comment",
            "# Another comment",
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.UNKNOWN

    def test_unusual_column_count(self):
        """Unusual column count should return unknown."""
        lines = [
            "a b c d e f g h",  # 8 columns
        ]
        hint = detect_tsv_format(lines)
        assert hint.likely_format == TsvFormat.UNKNOWN
