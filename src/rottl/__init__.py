"""
rottl: A library for rotating data structures with approximate TTL expiry.

Provides memory-efficient sets and Bloom filters that automatically evict
old data based on time-to-live (TTL) stages.
"""

from .bloom import RotatingTTLBloom
from .set import RotatingTTLSet

__all__ = [
    "RotatingTTLBloom",
    "RotatingTTLSet",
]
