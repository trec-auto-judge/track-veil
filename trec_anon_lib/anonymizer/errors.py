"""Error and warning collection for anonymization pipeline.

Collects:
- Malformed records that couldn't be repaired
- Email address warnings
- Other data quality issues
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class IssueType(str, Enum):
    """Types of data issues."""
    MALFORMED_FIELD = "malformed_field"
    EMAIL_FOUND = "email_found"
    PARSE_ERROR = "parse_error"
    UNKNOWN_FORMAT = "unknown_format"
    SKIPPED_RECORD = "skipped_record"


@dataclass
class DataIssue:
    """A single data issue found during processing."""
    issue_type: IssueType
    file_path: str
    line_number: Optional[int]
    field_path: Optional[str]
    message: str
    original_value: Optional[Any] = None
    context: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issue_type": self.issue_type.value,
            "file_path": self.file_path,
            "line_number": self.line_number,
            "field_path": self.field_path,
            "message": self.message,
            "original_value": _safe_serialize(self.original_value),
            "context": self.context,
            "timestamp": self.timestamp,
        }


def _safe_serialize(value: Any, max_len: int = 500) -> Any:
    """Safely serialize a value, truncating if needed."""
    if value is None:
        return None
    try:
        s = json.dumps(value)
        if len(s) > max_len:
            return s[:max_len] + "..."
        return value
    except (TypeError, ValueError):
        s = str(value)
        if len(s) > max_len:
            return s[:max_len] + "..."
        return s


class ErrorCollector:
    """Collects data issues during processing.

    Usage:
        collector = ErrorCollector()
        collector.add_warning(IssueType.EMAIL_FOUND, "file.jsonl", 5, "metadata.email", "Found email")
        collector.write_report("errors.jsonl")
    """

    def __init__(self):
        self._issues: List[DataIssue] = []
        self._email_addresses: List[Dict[str, Any]] = []

    def add_issue(
        self,
        issue_type: IssueType,
        file_path: str | Path,
        line_number: Optional[int],
        field_path: Optional[str],
        message: str,
        original_value: Any = None,
        **context,
    ):
        """Add a data issue."""
        self._issues.append(
            DataIssue(
                issue_type=issue_type,
                file_path=str(file_path),
                line_number=line_number,
                field_path=field_path,
                message=message,
                original_value=original_value,
                context=context,
            )
        )

    def add_email_warning(
        self,
        file_path: str | Path,
        line_number: Optional[int],
        field_path: str,
        email: str,
    ):
        """Record an email address found in data."""
        self._email_addresses.append({
            "file_path": str(file_path),
            "line_number": line_number,
            "field_path": field_path,
            "email": email,
        })
        self.add_issue(
            IssueType.EMAIL_FOUND,
            file_path,
            line_number,
            field_path,
            f"Email address found: {email[:20]}***",
            original_value=email,
        )

    def add_skipped_record(
        self,
        file_path: str | Path,
        line_number: int,
        reason: str,
        record: Any = None,
    ):
        """Record a skipped record."""
        self.add_issue(
            IssueType.SKIPPED_RECORD,
            file_path,
            line_number,
            None,
            reason,
            original_value=record,
        )

    @property
    def issues(self) -> List[DataIssue]:
        return self._issues

    @property
    def email_addresses(self) -> List[Dict[str, Any]]:
        return self._email_addresses

    def has_errors(self) -> bool:
        """Check if any errors (not just warnings) were collected."""
        error_types = {IssueType.MALFORMED_FIELD, IssueType.PARSE_ERROR}
        return any(i.issue_type in error_types for i in self._issues)

    def get_summary(self) -> Dict[str, int]:
        """Get count of issues by type."""
        summary: Dict[str, int] = {}
        for issue in self._issues:
            key = issue.issue_type.value
            summary[key] = summary.get(key, 0) + 1
        return summary

    def write_report(self, output_path: Path | str):
        """Write issues to a JSONL file."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            for issue in self._issues:
                f.write(json.dumps(issue.to_dict()) + "\n")

    def print_summary(self):
        """Print a summary of collected issues."""
        summary = self.get_summary()
        if not summary:
            print("No issues found.")
            return

        print(f"\nData issues summary ({len(self._issues)} total):")
        for issue_type, count in sorted(summary.items()):
            print(f"  {issue_type}: {count}")

        if self._email_addresses:
            print(f"\nEmail addresses found: {len(self._email_addresses)}")
            for e in self._email_addresses[:5]:
                print(f"  - {e['file_path']}:{e['line_number']} ({e['field_path']})")
            if len(self._email_addresses) > 5:
                print(f"  ... and {len(self._email_addresses) - 5} more")
