import time
import typing

from ._base import _Bucket
from ._base import _RotatingTTLFastRejectBase


class RotatingTTLSet(_RotatingTTLFastRejectBase):
    """A rotating set with approximate time-based eviction.

    Manages a deque of buckets to provide approximate time-based eviction.
    Items are retained for a maximum of `ttl` seconds. Under normal volume,
    items live for at least `ttl - (ttl / num_buckets)` seconds, but may be
    evicted earlier if high insertion volume forces automatic capacity-based
    rotations.
    """

    __slots__ = ()

    _buckets: typing.Deque[_Bucket[set]]

    def add(self, item: typing.Any) -> None:
        """Adds an item to the active bucket, rotating by time or capacity if necessary.

        Args:
            item: The element to add to the structure.
        """
        now = time.monotonic()

        if (
            now - self._buckets[0].created_at >= self._bucket_ttl
            or len(self._buckets[0].impl) >= self._bucket_capacity
        ):
            self._rotate(now)

        self._buckets[0].impl.add(item)

    def _make_bucket_impl(self) -> set:
        """Returns a native Python set for the new bucket."""
        return set()

    def __repr__(self) -> str:
        return (
            f"<RotatingTTLSet("
            f"ttl={self._ttl}, "
            f"num_buckets={len(self._buckets)}/{self._num_buckets}, "
            f"bucket_capacity={self._bucket_capacity})>"
        )
