import rbloom
import time
import typing

from ._base import _Bucket
from ._base import _RotationReason
from ._base import _RotatingTTLBase


class RotatingTTLBloom(_RotatingTTLBase):
    """A rotating Bloom filter with approximate time-based eviction.

    Manages a deque of buckets to provide approximate time-based eviction.
    Items are retained for a maximum of `ttl` seconds. Under normal volume,
    items live for at least `ttl - (ttl / num_buckets)` seconds, but may be
    evicted earlier if high insertion volume forces capacity-based rotations.

    Capacity enforcement is managed manually via `maybe_rotate_by_saturation`
    to maintain high performance during additions.
    """

    __slots__ = ("_bucket_fpr",)

    _buckets: typing.Deque[_Bucket[rbloom.Bloom]]

    def __init__(
        self,
        ttl: float,
        num_buckets: int,
        bucket_capacity: int,
        bucket_fpr: float,
    ):
        """Initializes the rotating TTL Bloom filter.

        Args:
            ttl: Total time-to-live for data in seconds.
            num_buckets: The number of rotation stages used to divide the total TTL.
            bucket_capacity: Max unique items per bucket to maintain the target false
                positive rate.
            bucket_fpr: Target false positive rate per bucket.
        """
        if not 0.0 < bucket_fpr < 1.0:
            raise ValueError("bucket_fpr must be between 0 and 1.")

        self._bucket_fpr = bucket_fpr
        super().__init__(ttl, num_buckets, bucket_capacity)

    @property
    def bucket_fpr(self):
        return self._bucket_fpr

    def add(self, item: typing.Any) -> None:
        """Adds an item to the active bucket, rotating by time if necessary.

        Args:
            item: The element to add to the structure.
        """
        now = time.monotonic()

        if now - self._buckets[0].created_at >= self._bucket_ttl:
            self._rotate(now, _RotationReason.TTL)

        self._buckets[0].impl.add(item)

    def get_active_bucket_approx_items(self):
        """Calculates the approximate number of items in the active bucket.

        If the active bucket has expired based on the total TTL, returns 0 to reflect
        its effective state.

        Note:
            Calculating this value requires counting all set bits in the underlying
            Bloom filter, which is an O(M) operation (where M is the filter size in bits).

        Returns:
            The approximate number of items currently in the active bucket.
        """
        if time.monotonic() - self._buckets[0].created_at > self._ttl:
            return 0

        return self._buckets[0].impl.approx_items

    def maybe_rotate_by_saturation(self) -> bool:
        """Checks bucket saturation and rotates if capacity is exceeded.

        Note:
            Checking saturation requires calculating the approximate items, which
            counts all set bits in the underlying Bloom filter (O(M) where M is the
            filter size in bits).
            While fast, it is not free. Call this periodically rather than on every
            add to preserve high write performance.

        Returns:
            True if a rotation occurred, False otherwise.
        """
        if self.get_active_bucket_approx_items() >= self._bucket_capacity:
            self._rotate(time.monotonic(), _RotationReason.CAPACITY)
            return True

        return False

    def _make_bucket_impl(self) -> rbloom.Bloom:
        """Returns an rbloom.Bloom filter for the new bucket."""
        return rbloom.Bloom(
            expected_items=self._bucket_capacity,
            false_positive_rate=self._bucket_fpr,
        )

    def __repr__(self) -> str:
        return (
            f"<RotatingTTLBloom("
            f"ttl={self._ttl}, "
            f"num_buckets={len(self._buckets)}/{self._num_buckets}, "
            f"bucket_capacity={self._bucket_capacity}, "
            f"bucket_fpr={self._bucket_fpr})>"
        )
