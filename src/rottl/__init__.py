"""
rottl: A library for rotating data structures with approximate TTL expiry.

Provides memory-efficient rotating dicts, sets and Bloom filters that automatically
evict old data based on time-to-live (TTL) stages.
"""

from .bloom import RotatingTTLBloom
from .dict import RotatingTTLDict
from .set import RotatingTTLSet

__all__ = [
    "RotatingTTLBloom",
    "RotatingTTLDict",
    "RotatingTTLSet",
]
