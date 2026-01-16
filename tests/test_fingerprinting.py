"""Tests for fingerprint-based mapping recovery."""

import pytest
import tempfile
from pathlib import Path

from trec_anon_lib.anonymizer.mapping import (
    MappingStore,
    compute_report_fingerprint,
)


class TestFingerprintComputation:
    """Test fingerprint computation."""

    def test_fingerprint_deterministic(self):
        """Same input should produce same fingerprint."""
        fp1 = compute_report_fingerprint("topic1", "This is the report text.")
        fp2 = compute_report_fingerprint("topic1", "This is the report text.")
        assert fp1 == fp2

    def test_fingerprint_differs_by_topic(self):
        """Different topic_id should produce different fingerprint."""
        fp1 = compute_report_fingerprint("topic1", "Same text")
        fp2 = compute_report_fingerprint("topic2", "Same text")
        assert fp1 != fp2

    def test_fingerprint_differs_by_text(self):
        """Different text should produce different fingerprint."""
        fp1 = compute_report_fingerprint("topic1", "Text A")
        fp2 = compute_report_fingerprint("topic1", "Text B")
        assert fp1 != fp2

    def test_fingerprint_is_sha256_hex(self):
        """Fingerprint should be 64-character hex string (SHA256)."""
        fp = compute_report_fingerprint("topic", "text")
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)


class TestFingerprintStorage:
    """Test fingerprint storage and lookup in MappingStore."""

    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)
        yield db_path
        db_path.unlink(missing_ok=True)

    def test_store_and_lookup_fingerprint(self, temp_db):
        """Stored fingerprint should be retrievable."""
        with MappingStore(temp_db, seed=42) as store:
            fp = compute_report_fingerprint("topic1", "Report text here")
            store.store_fingerprint(
                fingerprint=fp,
                original_team="team1",
                original_run="run1",
                topic_id="topic1",
                anon_team="Abc",
                anon_run="001",
            )

            result = store.lookup_fingerprint(fp)

        assert result is not None
        assert result["original_team"] == "team1"
        assert result["original_run"] == "run1"
        assert result["topic_id"] == "topic1"
        assert result["anon_team"] == "Abc"
        assert result["anon_run"] == "001"

    def test_lookup_nonexistent_fingerprint(self, temp_db):
        """Lookup of non-existent fingerprint should return None."""
        with MappingStore(temp_db, seed=42) as store:
            result = store.lookup_fingerprint("nonexistent" * 4)
        assert result is None

    def test_duplicate_fingerprint_ignored(self, temp_db):
        """Storing same fingerprint twice should not raise error."""
        with MappingStore(temp_db, seed=42) as store:
            fp = compute_report_fingerprint("topic1", "Text")

            # Store twice
            store.store_fingerprint(
                fingerprint=fp,
                original_team="team1",
                original_run="run1",
                topic_id="topic1",
                anon_team="Abc",
                anon_run="001",
            )
            store.store_fingerprint(
                fingerprint=fp,
                original_team="team1",
                original_run="run1",
                topic_id="topic1",
                anon_team="Abc",
                anon_run="001",
            )

            # Should still be retrievable
            result = store.lookup_fingerprint(fp)
            assert result is not None

    def test_fingerprint_stats(self, temp_db):
        """Stats should include fingerprint count."""
        with MappingStore(temp_db, seed=42) as store:
            fp1 = compute_report_fingerprint("topic1", "Text 1")
            fp2 = compute_report_fingerprint("topic2", "Text 2")

            store.store_fingerprint(
                fingerprint=fp1,
                original_team="team1",
                original_run="run1",
                topic_id="topic1",
                anon_team="Abc",
                anon_run="001",
            )
            store.store_fingerprint(
                fingerprint=fp2,
                original_team="team2",
                original_run="run2",
                topic_id="topic2",
                anon_team="Def",
                anon_run="002",
            )

            stats = store.get_stats()
            assert stats["fingerprints"] == 2
