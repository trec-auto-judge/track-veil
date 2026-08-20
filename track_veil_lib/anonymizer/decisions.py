"""Save and load anonymization decisions for reproducible runs.

Decisions file format (YAML):
    version: 1
    runs_formats:
      task_name:
        file_type: "tsv"
        format: "ranking"
        run_id_cols: [5]
        team_cols: []
        has_header: false
    eval_formats:
      task_name:
        file_type: "tsv"
        format: "trec_eval"
        run_id_cols: []
        team_cols: []
        has_header: false
        filename_pattern: ".qrel_eval"
      jsonl_task_name:
        file_type: "jsonl"        # no TSV column layout; `format` is omitted
        filename_pattern: ".official.eval.jsonl"

All files in one task directory share a file_type, so it is detected once per
directory and replayed from here on the next run.
    manual_run_ids:
      task_name:
        "filename.txt": "run_id_value"
    email_policies:
      task_name:
        "metadata.email": "redact_all"
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from .errors import EmailAction
from .transformers import TsvFormat


DECISIONS_VERSION = 1


@dataclass
class FormatDecision:
    """Stored decision for one task directory's file format."""
    format: Optional[str] = None  # TsvFormat value; None when the directory is not TSV
    run_id_cols: List[int] = field(default_factory=list)
    team_cols: List[int] = field(default_factory=list)
    has_header: bool = False
    filename_pattern: Optional[str] = None  # For eval files only
    file_type: str = "tsv"  # "tsv" or "jsonl", detected once per directory

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"file_type": self.file_type}
        if self.format is not None:
            d["format"] = self.format
            d["run_id_cols"] = self.run_id_cols
            d["team_cols"] = self.team_cols
            d["has_header"] = self.has_header
        if self.filename_pattern is not None:
            d["filename_pattern"] = self.filename_pattern
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FormatDecision":
        return cls(
            format=d.get("format"),
            run_id_cols=d.get("run_id_cols", []),
            team_cols=d.get("team_cols", []),
            has_header=d.get("has_header", False),
            filename_pattern=d.get("filename_pattern"),
            file_type=d.get("file_type", "tsv"),
        )


@dataclass
class DecisionsStore:
    """Container for all anonymization decisions."""

    # Runs format decisions: task_name -> FormatDecision
    runs_formats: Dict[str, FormatDecision] = field(default_factory=dict)

    # Eval format decisions: task_name -> FormatDecision
    eval_formats: Dict[str, FormatDecision] = field(default_factory=dict)

    # Manual run ID mappings: task_name -> {filename: run_id}
    manual_run_ids: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # Email policies: task_name -> {field_path: action}
    email_policies: Dict[str, Dict[str, str]] = field(default_factory=dict)

    def save(self, path: Path) -> None:
        """Save decisions to YAML file."""
        data = {
            "version": DECISIONS_VERSION,
            "runs_formats": {
                k: v.to_dict() for k, v in self.runs_formats.items()
            },
            "eval_formats": {
                k: v.to_dict() for k, v in self.eval_formats.items()
            },
            "manual_run_ids": self.manual_run_ids,
            "email_policies": self.email_policies,
        }

        # Remove empty sections
        data = {k: v for k, v in data.items() if v or k == "version"}

        with open(path, "wt", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    @classmethod
    def load(cls, path: Path) -> "DecisionsStore":
        """Load decisions from YAML file."""
        with open(path, mode="rt", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        version = data.get("version", 1)
        if version != DECISIONS_VERSION:
            raise ValueError(
                f"Unsupported decisions file version: {version} "
                f"(expected {DECISIONS_VERSION})"
            )

        store = cls()

        for task, fmt_dict in data.get("runs_formats", {}).items():
            store.runs_formats[task] = FormatDecision.from_dict(fmt_dict)

        for task, fmt_dict in data.get("eval_formats", {}).items():
            store.eval_formats[task] = FormatDecision.from_dict(fmt_dict)

        store.manual_run_ids = data.get("manual_run_ids", {})
        store.email_policies = data.get("email_policies", {})

        return store

    def set_runs_format(
        self,
        task_name: str,
        fmt: Optional[TsvFormat],
        run_id_cols: List[int],
        team_cols: List[int],
        has_header: bool,
        file_type: str = "tsv",
    ) -> None:
        """Record a runs format decision. `fmt` is None for non-TSV directories."""
        self.runs_formats[task_name] = FormatDecision(
            format=fmt.value if fmt is not None else None,
            run_id_cols=run_id_cols,
            team_cols=team_cols,
            has_header=has_header,
            file_type=file_type,
        )

    def get_runs_format(
        self, task_name: str
    ) -> Optional[Tuple[TsvFormat, List[int], List[int], bool]]:
        """Get stored runs TSV format decision, or None if there is no TSV layout."""
        if task_name not in self.runs_formats:
            return None
        fd = self.runs_formats[task_name]
        if fd.format is None:
            return None
        return (
            TsvFormat(fd.format),
            fd.run_id_cols,
            fd.team_cols,
            fd.has_header,
        )

    def set_eval_format(
        self,
        task_name: str,
        fmt: Optional[TsvFormat],
        run_id_cols: List[int],
        team_cols: List[int],
        has_header: bool,
        filename_pattern: Optional[str] = None,
        file_type: str = "tsv",
    ) -> None:
        """Record an eval format decision. `fmt` is None for non-TSV directories."""
        self.eval_formats[task_name] = FormatDecision(
            format=fmt.value if fmt is not None else None,
            run_id_cols=run_id_cols,
            team_cols=team_cols,
            has_header=has_header,
            filename_pattern=filename_pattern,
            file_type=file_type,
        )

    def get_eval_format(
        self, task_name: str
    ) -> Optional[Tuple[TsvFormat, List[int], List[int], bool, Optional[str]]]:
        """Get stored eval TSV format decision, or None if there is no TSV layout."""
        if task_name not in self.eval_formats:
            return None
        fd = self.eval_formats[task_name]
        if fd.format is None:
            return None
        return (
            TsvFormat(fd.format),
            fd.run_id_cols,
            fd.team_cols,
            fd.has_header,
            fd.filename_pattern,
        )

    def get_eval_filename_pattern(self, task_name: str) -> Optional[str]:
        """Get the stored eval filename pattern, independent of the TSV layout.

        Kept separate from get_eval_format so JSONL directories -- which have no
        TSV layout -- still replay their filename pattern.
        """
        fd = self.eval_formats.get(task_name)
        return fd.filename_pattern if fd else None

    def get_eval_file_type(self, task_name: str) -> Optional[str]:
        """Get the stored file type ("tsv"/"jsonl") for an eval task directory."""
        fd = self.eval_formats.get(task_name)
        return fd.file_type if fd else None

    def get_runs_file_type(self, task_name: str) -> Optional[str]:
        """Get the stored file type ("tsv"/"jsonl") for a runs task directory."""
        fd = self.runs_formats.get(task_name)
        return fd.file_type if fd else None

    def set_manual_run_id(self, task_name: str, filename: str, run_id: str) -> None:
        """Record a manual run_id extraction."""
        if task_name not in self.manual_run_ids:
            self.manual_run_ids[task_name] = {}
        self.manual_run_ids[task_name][filename] = run_id

    def get_manual_run_id(self, task_name: str, filename: str) -> Optional[str]:
        """Get stored manual run_id."""
        return self.manual_run_ids.get(task_name, {}).get(filename)

    def set_email_policy(
        self, task_name: str, field_path: str, action: EmailAction
    ) -> None:
        """Record an email handling policy."""
        if task_name not in self.email_policies:
            self.email_policies[task_name] = {}
        self.email_policies[task_name][field_path] = action.value

    def get_email_policy(
        self, task_name: str, field_path: str
    ) -> Optional[EmailAction]:
        """Get stored email policy."""
        action_str = self.email_policies.get(task_name, {}).get(field_path)
        if action_str is None:
            return None
        return EmailAction(action_str)
