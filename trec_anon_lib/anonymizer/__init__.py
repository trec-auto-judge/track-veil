"""TREC data anonymization package."""

from .mapping import MappingStore
from .pseudonyms import PseudonymPool
from .pipeline import AnonymizationPipeline
from .errors import EmailAction

__all__ = ["MappingStore", "PseudonymPool", "AnonymizationPipeline", "EmailAction"]
