import rbloom
import time

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
            num_buckets: Number of buckets to divide the TTL into. Must be at least 2.
            bucket_capacity: Max unique items per bucket to maintain the target false 
                positive rate.
            bucket_fpr: Target false positive rate per bucket.
        """
        if not 0.0 < bucket_fpr < 1.0:
            raise ValueError("bucket_fpr must be between 0 and 1.")

        self._bucket_fpr = bucket_fpr
        super().__init__(ttl, num_buckets, bucket_capacity)

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
        if self._buckets[0].impl.approx_items >= self._bucket_capacity:
            self._rotate(time.monotonic())
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
