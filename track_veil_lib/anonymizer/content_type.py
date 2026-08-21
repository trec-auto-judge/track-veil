"""Identify which kind of record a JSONL run file holds.

A runs/ directory may hold Reports or nugget banks, and both are JSONL, so the
JSONL-vs-TSV sniff in the pipeline cannot tell them apart. Each format does have a
distinguishing requirement, though, so a trial validation discriminates them:

| content type              | required                                            |
|---------------------------|-----------------------------------------------------|
| ``report``                | ``responses`` or ``answer`` (either may be empty)   |
| ``ragtime26_nugget_bank`` | ``nugget_bank`` (may be empty) + ``metadata.topic_id`` |
| ``ragtime25_nugget_bank`` | nothing - every field is optional                   |

Because ragtime25 requires nothing, a Report also validates as an empty ragtime25
bank; candidates are therefore tried most-specific-first and ragtime25 is only
reported when nothing more specific fits. That makes the guess a suggestion, not a
verdict - the pipeline shows it with its evidence and asks the user to confirm.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class ContentType:
    """Record types a runs/ JSONL directory can hold."""

    REPORT = "report"
    RAGTIME26_NUGGET_BANK = "ragtime26_nugget_bank"
    RAGTIME25_NUGGET_BANK = "ragtime25_nugget_bank"
    RAW = "raw"
    UNKNOWN = "unknown"

    #: Most specific first - see the module docstring.
    ORDERED = (REPORT, RAGTIME26_NUGGET_BANK, RAGTIME25_NUGGET_BANK)

    LABELS = {
        REPORT: "Report (responses/answer with citations)",
        RAGTIME26_NUGGET_BANK: "Nugget bank, ragtime26 (metadata + nugget_bank list)",
        RAGTIME25_NUGGET_BANK: "Nugget bank, ragtime25 / v3 (query_id + keyed nugget_bank)",
        RAW: "Raw JSONL - anonymize recognized metadata fields only (see warning)",
        UNKNOWN: "Skip this task entirely",
    }


@dataclass
class ContentTypeGuess:
    """What the data looks like, and why."""

    guess: str = ContentType.UNKNOWN
    #: content types that accepted the record, most specific first
    accepted: List[str] = field(default_factory=list)
    #: content type -> why it was rejected
    rejected: Dict[str, str] = field(default_factory=dict)
    #: top-level keys of the sampled record, for display
    keys: List[str] = field(default_factory=list)
    #: format_version the record declared, if any - decisive when recognized
    declared_version: Optional[str] = None

    @property
    def is_confident(self) -> bool:
        """True when the record declared its format, or one candidate accepted it."""
        return self.declared_version is not None or len(self.accepted) == 1


def _validates_as_report(record: Dict[str, Any]) -> None:
    from autojudge_base.report import Report

    Report.model_validate(record)


def _validates_as_ragtime26(record: Dict[str, Any]) -> None:
    from autojudge_base.nugget_data import Ragtime26NuggetBank

    Ragtime26NuggetBank.model_validate(record)


def _validates_as_ragtime25(record: Dict[str, Any]) -> None:
    from autojudge_base.nugget_data import NuggetBank

    NuggetBank.model_validate(record)


_VALIDATORS = {
    ContentType.REPORT: _validates_as_report,
    ContentType.RAGTIME26_NUGGET_BANK: _validates_as_ragtime26,
    ContentType.RAGTIME25_NUGGET_BANK: _validates_as_ragtime25,
}


#: format_version values a written bank carries. Raw participant submissions have
#: none, but anything this pipeline (or a judge) wrote does, and it is decisive.
FORMAT_VERSIONS = {
    "v3": ContentType.RAGTIME25_NUGGET_BANK,
    "RAGTIME26": ContentType.RAGTIME26_NUGGET_BANK,
    "v4": None,  # NuggetizerNuggetBank - recognized, but not handled here
}


def guess_content_type(record: Dict[str, Any]) -> ContentTypeGuess:
    """Guess what a single parsed JSONL record is.

    Prefers a declared ``format_version``, which settles it outright; otherwise
    tries to validate the record against each candidate. Returns the guess plus the
    evidence behind it, so a caller can show the user what was tried rather than an
    unexplained answer.
    """
    result = ContentTypeGuess(keys=sorted(record) if isinstance(record, dict) else [])

    if not isinstance(record, dict):
        result.rejected[ContentType.UNKNOWN] = f"record is {type(record).__name__}, not an object"
        return result

    declared = record.get("format_version")
    if declared is not None:
        result.declared_version = str(declared)
        known = FORMAT_VERSIONS.get(result.declared_version, "unrecognized")
        if known is None:
            result.rejected[ContentType.UNKNOWN] = (
                f'format_version "{declared}" is the nuggetizer format, not handled here'
            )
            return result
        if known != "unrecognized":
            result.guess = known
            result.accepted = [known]
            return result

    for content_type in ContentType.ORDERED:
        try:
            _VALIDATORS[content_type](record)
            result.accepted.append(content_type)
        except Exception as e:
            result.rejected[content_type] = _short_reason(e)

    result.guess = result.accepted[0] if result.accepted else ContentType.UNKNOWN
    return result


def _short_reason(error: Exception) -> str:
    """One line explaining why a candidate did not fit."""
    from pydantic import ValidationError

    if isinstance(error, ValidationError):
        parts = []
        for err in error.errors()[:3]:
            location = ".".join(str(p) for p in err["loc"]) or "<root>"
            parts.append(f"{location}: {err['msg']}")
        return "; ".join(parts)

    message = str(error).splitlines()[0] if str(error) else type(error).__name__
    return message[:160]


def describe_guess(guess: ContentTypeGuess) -> List[str]:
    """Lines describing the guess and its evidence, for an interactive prompt."""
    lines = [f"  Record keys: {', '.join(guess.keys) or '(none)'}"]

    if guess.declared_version is not None:
        lines.append(f'  Declares:    format_version "{guess.declared_version}"')

    if guess.accepted:
        lines.append(f"  Parses as:   {', '.join(guess.accepted)}")
    else:
        lines.append("  Parses as:   nothing recognized")

    for content_type in ContentType.ORDERED:
        if content_type in guess.rejected:
            lines.append(f"    not {content_type}: {guess.rejected[content_type]}")

    if guess.accepted and not guess.is_confident:
        lines.append(
            "  More than one format fits; the most specific is suggested "
            "(ragtime25 accepts almost anything, so it fits by default)."
        )
    return lines
