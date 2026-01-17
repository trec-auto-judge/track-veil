"""SQLite-backed persistent mapping store for anonymization.

Stores:
- team → anon_team mappings
- run_id → anon_run mappings
- Pool state (indices) for reproducibility
- Metadata (seed, creation time)
"""

import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .pseudonyms import PseudonymPool


def compute_report_fingerprint(topic_id: str, report_text: str) -> str:
    """Compute SHA256 fingerprint from topic_id and report text.

    Uses null separator to prevent collisions from concatenation.
    """
    content = f"{topic_id}\0{report_text}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class MappingStore:
    """Persistent SQLite store for anonymization mappings.

    Usage:
        store = MappingStore("mapping.db", seed=42)
        anon_team = store.get_or_create_team("team1")  # "Fez"
        anon_run = store.get_or_create_run("run1")     # "07"

        # Later, reopen existing mapping:
        store = MappingStore("mapping.db")  # loads existing seed & state
    """

    def __init__(self, db_path: Path | str, seed: Optional[int] = None):
        self._db_path = Path(db_path)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

        # Load or initialize seed
        stored_seed = self._get_metadata("seed")
        if stored_seed is not None:
            if seed is not None and seed != int(stored_seed):
                raise ValueError(
                    f"Seed mismatch: DB has seed={stored_seed}, but seed={seed} was provided. "
                    "Use existing DB seed or create new DB."
                )
            self._seed = int(stored_seed)
        else:
            self._seed = seed if seed is not None else self._generate_seed()
            self._set_metadata("seed", str(self._seed))
            self._set_metadata("created_at", datetime.now().isoformat())

        # Initialize pool with stored state
        self._pool = PseudonymPool(seed=self._seed)
        team_idx = self._get_metadata("team_pool_index")
        run_idx = self._get_metadata("run_pool_index")
        if team_idx is not None and run_idx is not None:
            self._pool.set_indices(int(team_idx), int(run_idx))

        # Track run_id → original_team for cross-source consistency checks
        self._run_to_team: Dict[str, str] = {}

    def _generate_seed(self) -> int:
        """Generate a random seed for new databases."""
        import secrets
        return secrets.randbelow(2**31)

    def _init_schema(self):
        cur = self._conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS team_mappings (
                original TEXT PRIMARY KEY,
                anonymized TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS run_mappings (
                original TEXT PRIMARY KEY,
                anonymized TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS report_fingerprints (
                fingerprint TEXT PRIMARY KEY,
                original_team TEXT NOT NULL,
                original_run TEXT NOT NULL,
                topic_id TEXT NOT NULL,
                anon_team TEXT NOT NULL,
                anon_run TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS invalidated_names (
                name_type TEXT NOT NULL,
                name TEXT NOT NULL,
                invalidated_at TEXT NOT NULL,
                PRIMARY KEY (name_type, name)
            );
            """
        )
        self._conn.commit()

    def _get_metadata(self, key: str) -> Optional[str]:
        cur = self._conn.cursor()
        cur.execute("SELECT value FROM metadata WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def _set_metadata(self, key: str, value: str):
        cur = self._conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (key, value),
        )
        self._conn.commit()

    def _save_pool_state(self):
        """Persist current pool indices."""
        self._set_metadata("team_pool_index", str(self._pool._team_index))
        self._set_metadata("run_pool_index", str(self._pool._run_index))

    def _record_invalidated_name(self, name_type: str, name: str):
        """Record a name that was skipped due to collision."""
        cur = self._conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO invalidated_names (name_type, name, invalidated_at) VALUES (?, ?, ?)",
            (name_type, name, datetime.now().isoformat()),
        )
        self._conn.commit()

    def get_or_create_team(self, original: str) -> str:
        """Get anonymized team name, creating mapping if needed."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT anonymized FROM team_mappings WHERE original = ?",
            (original,),
        )
        row = cur.fetchone()
        if row:
            return row["anonymized"]

        # Create new mapping, retry if collision
        while True:
            anon = self._pool.get_team_pseudonym()
            try:
                cur.execute(
                    "INSERT INTO team_mappings (original, anonymized, created_at) VALUES (?, ?, ?)",
                    (original, anon, datetime.now().isoformat()),
                )
                self._conn.commit()
                self._save_pool_state()
                return anon
            except sqlite3.IntegrityError:
                # Name already used - record as invalidated and try next
                self._record_invalidated_name("team", anon)
                continue

    def get_or_create_run(self, original: str) -> str:
        """Get anonymized run ID, creating mapping if needed."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT anonymized FROM run_mappings WHERE original = ?",
            (original,),
        )
        row = cur.fetchone()
        if row:
            return row["anonymized"]

        # Create new mapping, retry if collision
        while True:
            anon = self._pool.get_run_pseudonym()
            try:
                cur.execute(
                    "INSERT INTO run_mappings (original, anonymized, created_at) VALUES (?, ?, ?)",
                    (original, anon, datetime.now().isoformat()),
                )
                self._conn.commit()
                self._save_pool_state()
                return anon
            except sqlite3.IntegrityError:
                # Name already used - record as invalidated and try next
                self._record_invalidated_name("run", anon)
                continue

    def get_team(self, original: str) -> Optional[str]:
        """Get anonymized team name if it exists."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT anonymized FROM team_mappings WHERE original = ?",
            (original,),
        )
        row = cur.fetchone()
        return row["anonymized"] if row else None

    def get_run(self, original: str) -> Optional[str]:
        """Get anonymized run ID if it exists."""
        cur = self._conn.cursor()
        cur.execute(
            "SELECT anonymized FROM run_mappings WHERE original = ?",
            (original,),
        )
        row = cur.fetchone()
        return row["anonymized"] if row else None

    def store_run_team(self, run_id: str, team: str):
        """Store the association between a run_id and its original team.

        Called when processing runs/ to track which team submitted each run.
        Used later to detect mismatches when metadata has different team name.
        """
        self._run_to_team[run_id] = team

    def get_run_team(self, run_id: str) -> Optional[str]:
        """Get the original team associated with a run_id."""
        return self._run_to_team.get(run_id)

    def get_all_team_mappings(self) -> Dict[str, str]:
        """Return all team mappings as {original: anonymized}."""
        cur = self._conn.cursor()
        cur.execute("SELECT original, anonymized FROM team_mappings")
        return {row["original"]: row["anonymized"] for row in cur.fetchall()}

    def get_all_run_mappings(self) -> Dict[str, str]:
        """Return all run mappings as {original: anonymized}."""
        cur = self._conn.cursor()
        cur.execute("SELECT original, anonymized FROM run_mappings")
        return {row["original"]: row["anonymized"] for row in cur.fetchall()}

    def get_stats(self) -> Dict[str, int]:
        """Return mapping statistics."""
        cur = self._conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM team_mappings")
        team_count = cur.fetchone()["count"]
        cur.execute("SELECT COUNT(*) as count FROM run_mappings")
        run_count = cur.fetchone()["count"]
        cur.execute("SELECT COUNT(*) as count FROM report_fingerprints")
        fingerprint_count = cur.fetchone()["count"]
        return {
            "teams": team_count,
            "runs": run_count,
            "fingerprints": fingerprint_count,
            "teams_remaining": self._pool.teams_remaining,
            "runs_remaining": self._pool.runs_remaining,
        }

    def store_fingerprint(
        self,
        fingerprint: str,
        original_team: str,
        original_run: str,
        topic_id: str,
        anon_team: str,
        anon_run: str,
    ) -> None:
        """Store a report fingerprint with its mapping.

        Uses INSERT OR IGNORE to handle duplicate fingerprints (same report
        appearing multiple times).
        """
        cur = self._conn.cursor()
        cur.execute(
            """INSERT OR IGNORE INTO report_fingerprints
               (fingerprint, original_team, original_run, topic_id,
                anon_team, anon_run, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                fingerprint,
                original_team,
                original_run,
                topic_id,
                anon_team,
                anon_run,
                datetime.now().isoformat(),
            ),
        )
        self._conn.commit()

    def lookup_fingerprint(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        """Look up original identifiers by fingerprint.

        Returns dict with keys: original_team, original_run, topic_id,
                               anon_team, anon_run
        Returns None if not found.
        """
        cur = self._conn.cursor()
        cur.execute(
            """SELECT original_team, original_run, topic_id, anon_team, anon_run
               FROM report_fingerprints WHERE fingerprint = ?""",
            (fingerprint,),
        )
        row = cur.fetchone()
        if row:
            return {
                "original_team": row["original_team"],
                "original_run": row["original_run"],
                "topic_id": row["topic_id"],
                "anon_team": row["anon_team"],
                "anon_run": row["anon_run"],
            }
        return None

    @property
    def seed(self) -> int:
        return self._seed

    def close(self):
        self._conn.close()

    def __enter__(self) -> "MappingStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
