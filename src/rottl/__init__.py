"""
rottl: A library for rotating data structures with TTL expiry.

Provides memory-efficient sets and Bloom filters that automatically
evict old data based on time-to-live (TTL) stages.
"""

from .set import RotatingTTLSet
from .bloom import RotatingTTLBloom

__all__ = [
    "RotatingTTLSet",
    "RotatingTTLBloom",
]
