"""Pattern-based repair rules for malformed JSONL records.

Stores repair rules in SQLite so fixes can be remembered and reapplied.
"""

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


class RepairAction(str, Enum):
    """Types of repair actions."""
    EXTRACT_KEY = "extract_key"      # Extract value from dict by key
    TO_STRING = "to_string"          # Convert to JSON string
    SKIP_RECORD = "skip_record"      # Skip the entire record
    DROP_FIELD = "drop_field"        # Remove the field entirely
    CUSTOM = "custom"                # Custom Python expression


@dataclass
class RepairRule:
    """A rule for repairing a specific field pattern."""
    field_path: str           # e.g., "metadata.narrative"
    original_type: str        # e.g., "dict", "list", "int"
    expected_type: str        # e.g., "str"
    action: RepairAction
    params: Dict[str, Any]    # Action-specific parameters
    pattern_hash: str         # Hash of (field_path, original_type, sample_structure)
    team_id: Optional[str] = None  # If set, rule only applies to this team

    def apply(self, value: Any) -> Tuple[Any, bool]:
        """Apply repair rule to value. Returns (repaired_value, should_skip_record)."""
        if self.action == RepairAction.SKIP_RECORD:
            return None, True

        if self.action == RepairAction.DROP_FIELD:
            return None, False

        if self.action == RepairAction.EXTRACT_KEY:
            key = self.params.get("key")
            if isinstance(value, dict) and key in value:
                return value[key], False
            raise ValueError(f"Cannot extract key '{key}' from {type(value)}")

        if self.action == RepairAction.TO_STRING:
            return json.dumps(value), False

        if self.action == RepairAction.CUSTOM:
            expr = self.params.get("expression")
            # Limited eval with only the value available
            return eval(expr, {"__builtins__": {}}, {"value": value}), False

        raise ValueError(f"Unknown repair action: {self.action}")


def compute_pattern_hash(field_path: str, value: Any) -> str:
    """Compute hash representing the error pattern.

    Uses field path + type + structure (keys for dicts, length hint for lists).
    """
    type_name = type(value).__name__

    if isinstance(value, dict):
        # Include sorted keys to identify structure
        structure = f"dict:{sorted(value.keys())}"
    elif isinstance(value, list) and value:
        # Include type of first element
        structure = f"list:{type(value[0]).__name__}"
    else:
        structure = type_name

    pattern_str = f"{field_path}|{structure}"
    return hashlib.sha256(pattern_str.encode()).hexdigest()[:16]


class RepairStore:
    """SQLite store for repair rules.

    Usage:
        store = RepairStore("mapping.db")
        rule = store.get_rule("metadata.narrative", malformed_value)
        if rule:
            repaired = rule.apply(malformed_value)
        else:
            # Ask user, then save:
            store.save_rule(new_rule)
    """

    def __init__(self, db_path: Path | str):
        self._db_path = Path(db_path)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS repair_rules (
                pattern_hash TEXT NOT NULL,
                field_path TEXT NOT NULL,
                original_type TEXT NOT NULL,
                expected_type TEXT NOT NULL,
                action TEXT NOT NULL,
                params TEXT NOT NULL,
                sample_value TEXT,
                team_id TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (pattern_hash, team_id)
            )
            """
        )
        self._conn.commit()

    def get_rule(
        self, field_path: str, value: Any, team_id: Optional[str] = None
    ) -> Optional[RepairRule]:
        """Look up a repair rule for this field and value pattern.

        Checks team-specific rules first, then falls back to global rules.
        """
        pattern_hash = compute_pattern_hash(field_path, value)
        cur = self._conn.cursor()

        # First check for team-specific rule
        if team_id:
            cur.execute(
                "SELECT * FROM repair_rules WHERE pattern_hash = ? AND team_id = ?",
                (pattern_hash, team_id),
            )
            row = cur.fetchone()
            if row:
                return self._row_to_rule(row)

        # Fall back to global rule (team_id IS NULL)
        cur.execute(
            "SELECT * FROM repair_rules WHERE pattern_hash = ? AND team_id IS NULL",
            (pattern_hash,),
        )
        row = cur.fetchone()
        if not row:
            return None

        return self._row_to_rule(row)

    def _row_to_rule(self, row: sqlite3.Row) -> RepairRule:
        """Convert a database row to a RepairRule."""
        return RepairRule(
            field_path=row["field_path"],
            original_type=row["original_type"],
            expected_type=row["expected_type"],
            action=RepairAction(row["action"]),
            params=json.loads(row["params"]),
            pattern_hash=row["pattern_hash"],
            team_id=row["team_id"],
        )

    def save_rule(self, rule: RepairRule, sample_value: Any = None):
        """Save a repair rule for future use."""
        cur = self._conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO repair_rules
            (pattern_hash, field_path, original_type, expected_type, action, params, sample_value, team_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule.pattern_hash,
                rule.field_path,
                rule.original_type,
                rule.expected_type,
                rule.action.value,
                json.dumps(rule.params),
                json.dumps(sample_value)[:1000] if sample_value else None,
                rule.team_id,
                datetime.now().isoformat(),
            ),
        )
        self._conn.commit()

    def get_all_rules(self) -> List[RepairRule]:
        """Return all stored repair rules."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM repair_rules")
        return [self._row_to_rule(row) for row in cur.fetchall()]

    def close(self):
        self._conn.close()

    def __enter__(self) -> "RepairStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def suggest_repair_options(
    field_path: str, value: Any, expected_type: str
) -> List[Tuple[str, RepairRule]]:
    """Suggest repair options for a malformed value.

    Returns list of (description, RepairRule) tuples.
    """
    pattern_hash = compute_pattern_hash(field_path, value)
    original_type = type(value).__name__
    options = []

    # Option: Extract key from dict
    if isinstance(value, dict):
        # Suggest extracting common keys that might contain the expected value
        for key in value.keys():
            if isinstance(value[key], str) and expected_type == "str":
                preview = value[key][:50] + "..." if len(value[key]) > 50 else value[key]
                options.append((
                    f"Extract '{key}' field: \"{preview}\"",
                    RepairRule(
                        field_path=field_path,
                        original_type=original_type,
                        expected_type=expected_type,
                        action=RepairAction.EXTRACT_KEY,
                        params={"key": key},
                        pattern_hash=pattern_hash,
                    ),
                ))

    # Option: Convert to JSON string
    options.append((
        f"Convert {original_type} to JSON string",
        RepairRule(
            field_path=field_path,
            original_type=original_type,
            expected_type=expected_type,
            action=RepairAction.TO_STRING,
            params={},
            pattern_hash=pattern_hash,
        ),
    ))

    # Option: Drop the field
    options.append((
        f"Drop field '{field_path}' entirely",
        RepairRule(
            field_path=field_path,
            original_type=original_type,
            expected_type=expected_type,
            action=RepairAction.DROP_FIELD,
            params={},
            pattern_hash=pattern_hash,
        ),
    ))

    # Option: Skip the record
    options.append((
        "Skip this record entirely",
        RepairRule(
            field_path=field_path,
            original_type=original_type,
            expected_type=expected_type,
            action=RepairAction.SKIP_RECORD,
            params={},
            pattern_hash=pattern_hash,
        ),
    ))

    return options
