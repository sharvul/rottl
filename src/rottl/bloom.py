import rbloom
import time
import typing

from ._base import _RotationReason
from ._base import _RotatingTTLBase


class RotatingTTLBloom(_RotatingTTLBase[rbloom.Bloom]):
    """A rotating Bloom filter with approximate time-based eviction.

    Manages a deque of buckets to provide approximate time-based eviction.
    Items are retained for a maximum of `ttl` seconds. Under normal volume,
    items live for at least `ttl - (ttl / num_buckets)` seconds, but may be
    evicted earlier if high insertion volume forces automatic capacity-based
    rotations.
    """

    __slots__ = (
        "_bucket_fpr",
        "_inserts_until_saturation_check",
    )

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
        self._inserts_until_saturation_check = bucket_capacity

        super().__init__(ttl, num_buckets, bucket_capacity)

    @property
    def bucket_fpr(self):
        return self._bucket_fpr

    def add(self, item: typing.Any) -> None:
        """Adds an item to the active bucket, rotating by time or capacity if necessary.

        Args:
            item: The element to add to the structure.
        """
        now = time.monotonic()

        # 1. Time-based rotation check
        if now - self._buckets[0].created_at >= self._bucket_ttl:
            self._rotate(now, _RotationReason.TTL)

        # 2. Capacity-based rotation check (amortized)
        elif self._inserts_until_saturation_check <= 0:
            # O(M) operation: count set bits to estimate unique items
            approx_items = self._buckets[0].impl.approx_items

            if approx_items >= self._bucket_capacity:
                self._rotate(now, _RotationReason.CAPACITY)
            else:
                # Reset countdown using remaining capacity; use a 1% minimum floor
                # to prevent O(M) check thrashing near the saturation limit.
                self._inserts_until_saturation_check = max(
                    self._bucket_capacity - approx_items,  # remaining capacity
                    int(self._bucket_capacity * 0.01),  # 1% safety margin
                    1,  # minimal allowed interval
                )

        self._buckets[0].impl.add(item)
        self._inserts_until_saturation_check -= 1

    def _rotate(
        self,
        now: float,
        reason: typing.Optional[_RotationReason] = None,
    ) -> None:
        """Prepends a new bucket and resets the capacity check countdown."""
        super()._rotate(now, reason)
        self._inserts_until_saturation_check = self._bucket_capacity

    def _make_bucket_impl(self) -> rbloom.Bloom:
        """Returns an rbloom.Bloom filter for the new bucket."""
        return rbloom.Bloom(
            expected_items=self._bucket_capacity,
            false_positive_rate=self._bucket_fpr,
        )

    @classmethod
    def _get_bucket_impl_item_count(cls, bucket_impl: rbloom.Bloom):
        """Returns the approximate item count of the bloom bucket impl.

        Note:
            Calculating this value requires counting all set bits in the underlying
            Bloom filter, which is an O(M) operation (where M is the filter size in bits).
        """
        return bucket_impl.approx_items

    def __repr__(self) -> str:
        return (
            f"<RotatingTTLBloom("
            f"ttl={self._ttl}, "
            f"num_buckets={len(self._buckets)}/{self._num_buckets}, "
            f"bucket_capacity={self._bucket_capacity}, "
            f"bucket_fpr={self._bucket_fpr})>"
        )
