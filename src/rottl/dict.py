import time
import typing

from ._base import _RotationReason
from ._base import _RotatingTTLCollectionBase


class RotatingTTLDict(_RotatingTTLCollectionBase[dict]):
    """A rotating dict with approximate time-based eviction.

    Manages a deque of buckets to provide approximate time-based eviction.
    Items are retained for a maximum of `ttl` seconds. Under normal volume,
    items live for at least `ttl - (ttl / num_buckets)` seconds, but may be
    evicted earlier if high insertion volume forces automatic capacity-based
    rotations.
    """

    __slots__ = ()

    def get(self, key: typing.Any, default: typing.Any = None) -> typing.Any:
        """Returns the value for key if key is in the dictionary, else default."""
        idx = self._find_bucket_index(key)

        if idx is None:
            return default

        return self._buckets[idx].impl[key]

    def __getitem__(self, key: typing.Any) -> typing.Any:
        """Retrieves the latest value for the key across all valid buckets.

        Args:
            key: The key to search for.

        Returns:
            The value associated with the most recently added instance of the key.

        Raises:
            KeyError: If the key is not found.
        """
        idx = self._find_bucket_index(key)

        if idx is None:
            raise KeyError(key)

        return self._buckets[idx].impl[key]

    def __setitem__(self, key: typing.Any, value: typing.Any) -> None:
        """Sets the item in the active bucket, rotating by time or capacity if necessary."""
        now = time.monotonic()

        # 1. Time-based rotation check
        if now - self._buckets[0].created_at >= self._bucket_ttl:
            self._rotate(now, _RotationReason.TTL)

        # 2. Capacity-based rotation check
        elif len(self._buckets[0].impl) >= self._bucket_capacity:
            self._rotate(now, _RotationReason.CAPACITY)

        self._buckets[0].impl[key] = value

    def _make_bucket_impl(self) -> dict:
        """Returns a native Python dict for the new bucket."""
        return dict()

    @classmethod
    def _get_bucket_impl_item_count(cls, impl: dict):
        """Returns the exact item count of the dict bucket impl."""
        return len(impl)

    def __repr__(self) -> str:
        return (
            f"<RotatingTTLDict("
            f"ttl={self._ttl}, "
            f"num_buckets={len(self._buckets)}/{self._num_buckets}, "
            f"bucket_capacity={self._bucket_capacity})>"
        )
