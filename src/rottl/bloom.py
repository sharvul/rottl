import rbloom
import time

from ._base import _RotatingTTLBase


class RotatingTTLBloom(_RotatingTTLBase):
    """A rotating Bloom filter with time-based eviction.

    Capacity enforcement is managed manually via `maybe_rotate_by_capacity`
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
            ttl: Time-to-live for items in seconds.
            num_buckets: Number of internal rotation stages.
            bucket_capacity: Max unique items per bucket to maintain the
                target false positive rate.
            bucket_fpr: Target false positive rate per bucket.
        """
        super().__init__(ttl, num_buckets, bucket_capacity)

        self._bucket_fpr = bucket_fpr

    def maybe_rotate_by_capacity(self) -> bool:
        """Checks bucket saturation and rotates if capacity is exceeded.

        Note:
            This is an O(N) operation. It is not called automatically to
            preserve 'add' performance.
            Call this periodically if you need to enforce that the
            bucket FPR is strictly maintained.

        Returns:
            True if a rotation occurred, False otherwise.
        """
        if self._buckets[0].impl.approx_items >= self._bucket_capacity:
            self._rotate(time.monotonic())
            return True

        return False

    def _make_bucket_impl(self) -> rbloom.Bloom:
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
