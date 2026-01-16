"""TREC data anonymization package."""

from .mapping import MappingStore, compute_report_fingerprint
from .pseudonyms import PseudonymPool
from .pipeline import AnonymizationPipeline
from .errors import EmailAction

__all__ = [
    "MappingStore",
    "compute_report_fingerprint",
    "PseudonymPool",
    "AnonymizationPipeline",
    "EmailAction",
]
