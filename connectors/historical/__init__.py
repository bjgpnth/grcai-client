"""
Historical log collection infrastructure.

This package provides:
- HistoricalLogCollectionMixin: Reusable mixin for historical log collection
- TimestampParserRegistry: Pluggable timestamp parser registry
- Log time filtering utilities
"""

from connectors.historical.historical_collection_mixin import HistoricalLogCollectionMixin
from connectors.historical.timestamp_parsers import TimestampParserRegistry

__all__ = [
    "HistoricalLogCollectionMixin",
    "TimestampParserRegistry",
]
