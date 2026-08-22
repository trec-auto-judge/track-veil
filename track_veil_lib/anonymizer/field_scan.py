"""Walk a JSON tree and act on fields whose names look identifying.

One traversal and one set of field lists, shared by everything that needs them:

- the raw JSONL fallback, which has no schema to go by
- ``metadata.extra`` on a Report, which is an open bag of fields
- the second-pass scan over already-anonymized output, where a hit means the
  schema-driven pass missed something

Keys are matched case-insensitively at every depth, inside dicts and through lists.
Nothing else is touched: a key the lists do not name is left exactly as it was.

Anonymize each value exactly once
---------------------------------

``MappingStore.get_or_create_team``/``get_or_create_run`` are **not idempotent over
their own output**: handed a pseudonym they treat it as a fresh original and mint a
pseudonym *of the pseudonym*, storing a mapping (``T873 -> T412``) that nothing else
knows about. The output still looks anonymized, so the damage is silent.

That makes "walk everything" and "handle these fields by hand" dangerous to combine,
and both are needed: a schema-driven transformer must handle ``run_id`` itself,
because only it knows the filename-is-source-of-truth rule and has to record the
run -> team link; while the open ``Dict[str, Any]`` bags around it (``metadata.extra``,
per-sentence ``metadata``, unknown keys allowed by ``extra="allow"``) have no schema
and need the walk.

The rule this module enforces:

- a schema-driven caller hands this module only its *generic* fields -- the open
  ``Dict[str, Any]`` bags -- and handles its declared fields itself. The two sets
  never overlap, so no value is anonymized twice.
- the second pass over already-anonymized output uses ``scan_fields`` /
  ``scan_for_missed_fields``, which only report. Re-running ``anonymize_fields``
  there would double-map every field the first pass handled.
- ``MappingStore`` has a backstop that spots a pseudonym being anonymized again and
  warns/asks, but that is a safety net for operator error (re-running over an output
  directory), not a licence to double-process here: reaching it from our own code
  means noisy warnings and a prompt for a bug of ours.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple

REDACTED = "[REDACTED]"


class FieldCategory(str, Enum):
    """What an identifying field name means, and so what is done with it."""

    TEAM = "team"        # value replaced with the team's pseudonym
    RUN = "run"          # value replaced with the run's pseudonym
    REDACT = "redact"    # free text that may name the team or its system
    DROP = "drop"        # identifying blob removed outright


#: The one source of truth for which key names are identifying.
FIELDS_BY_CATEGORY: Dict[FieldCategory, Tuple[str, ...]] = {
    FieldCategory.TEAM: ("team_id", "team"),
    FieldCategory.RUN: ("run_id", "runid", "runtag"),
    # Free text in which a team describes its own system. Redacted whole: a
    # description identifies its author in ways no name substitution reaches.
    # Metadata records carry a family of these; add each one here by name.
    FieldCategory.REDACT: ("run_desc", "description", "std-desc", "rag-top-k"),
    # Email-bearing fields are deliberately NOT dropped here: addresses are found
    # by value (EMAIL_PATTERN) and handled through the email policy, which prompts
    # and remembers the answer. Dropping them by name would bypass that.
    FieldCategory.DROP: ("creator", "evaldata"),
}

_CATEGORY_BY_KEY: Dict[str, FieldCategory] = {
    key: category
    for category, keys in FIELDS_BY_CATEGORY.items()
    for key in keys
}


@dataclass
class FieldAction:
    """One identifying field the scan found, and what was done with it."""

    path: str
    key: str
    category: FieldCategory
    original: Any = None


def iter_json_fields(node: Any, path: str = "") -> Iterator[Tuple[str, Dict[str, Any], str]]:
    """Yield (path, containing_dict, key) for every key in a JSON tree.

    Descends through dicts and lists to any depth. The containing dict is yielded
    so a caller can modify or delete the entry, but callers must collect first and
    mutate afterwards - mutating during traversal is undefined.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            child_path = f"{path}.{key}" if path else key
            yield child_path, node, key
            yield from iter_json_fields(value, child_path)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            yield from iter_json_fields(item, f"{path}[{index}]")


def _category(key: str) -> Optional[FieldCategory]:
    """Which category a key name belongs to, if any (case-insensitive)."""
    return _CATEGORY_BY_KEY.get(key.lower())


def scan_fields(node: Any) -> List[FieldAction]:
    """Report identifying fields found anywhere in the tree, without changing it."""
    return [
        FieldAction(path=path, key=key, category=category, original=container.get(key))
        for path, container, key in iter_json_fields(node)
        if (category := _category(key)) is not None
    ]


def scan_for_missed_fields(node: Any, handled_prefixes: Tuple[str, ...]) -> List[FieldAction]:
    """Identifying fields sitting outside the paths a schema-driven pass owns.

    The second pass over an already-anonymized record: whatever this returns is
    something the format's own handling did not cover, so it belongs in the error
    report rather than being quietly fixed.
    """
    return [
        action for action in scan_fields(node)
        if not any(
            action.path == prefix or action.path.startswith(prefix + ".")
            for prefix in handled_prefixes
        )
    ]


def anonymize_fields(
    node: Any,
    mapping,
    expected_run_id: Optional[str] = None,
) -> List[FieldAction]:
    """Anonymize every identifying field in the tree, at any depth, in place.

    Teams and runs are replaced with their pseudonyms, free-text descriptions are
    redacted, identifying blobs are dropped. Returns what was acted on, so a caller
    can report it -- a hit in a second pass means an earlier schema-driven pass has
    a gap.

    ``expected_run_id`` keeps the filename-is-source-of-truth rule: when given, run
    fields map through it rather than through whatever the content claimed.

    Call this on the *generic* parts of a record only -- ``metadata.extra``, a
    per-sentence ``metadata``, an ``extra="allow"`` bag. A schema-driven caller
    handles its declared fields itself and must not pass them here as well, or the
    value would be anonymized twice (see "anonymize each value exactly once").
    """
    targets = [
        (path, container, key, _category(key))
        for path, container, key in iter_json_fields(node)
    ]

    # The original team, if any, so run->team can be recorded
    original_team = next(
        (container[key] for _p, container, key, category in targets
         if category is FieldCategory.TEAM
         and isinstance(container.get(key), str) and container[key]),
        "",
    )

    actions: List[FieldAction] = []
    for path, container, key, category in targets:
        if category is None or key not in container:
            continue  # already dropped as part of a parent, or not identifying
        value = container[key]

        if category is FieldCategory.DROP:
            actions.append(FieldAction(path, key, category, value))
            del container[key]

        elif category is FieldCategory.REDACT:
            if value:
                actions.append(FieldAction(path, key, category, value))
                container[key] = REDACTED

        elif category is FieldCategory.TEAM and isinstance(value, str) and value:
            container[key] = mapping.get_or_create_team(value)
            actions.append(FieldAction(path, key, category, value))

        elif category is FieldCategory.RUN and isinstance(value, str) and value:
            original_run = expected_run_id or value
            container[key] = mapping.get_or_create_run(original_run)
            actions.append(FieldAction(path, key, category, value))
            if original_team:
                mapping.store_run_team(original_run, original_team)

    return actions
