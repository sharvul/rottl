import rbloom
import time
import typing

from ._base import _RotatingTTLBase


class RotatingTTLSet(_RotatingTTLBase):
    """A rotating set with approximate time-based eviction.
    
    Manages a deque of buckets to provide approximate time-based eviction. 
    Items are retained for a maximum of `ttl` seconds. Under normal volume, 
    items live for at least `ttl - (ttl / num_buckets)` seconds, but may be 
    evicted earlier if high insertion volume forces automatic capacity-based 
    rotations.
    """

    __slots__ = (
        "_enable_history_fast_reject",
        "_history_rejection_filter_fpr",
        "_history_rejection_filter",
    )

    def __init__(
        self,
        ttl: float,
        num_buckets: int,
        bucket_capacity: int,
        enable_history_fast_reject: bool = False,
        history_rejection_filter_fpr: float = 0.001,
    ):
        """Initializes the rotating TTL set.

        Args:
            ttl: Total time-to-live for data in seconds.
            num_buckets: Number of internal rotation stages.
            bucket_capacity: Max items per bucket before auto-rotation.
            enable_history_fast_reject: If True, maintains an aggregate Bloom filter
                of all items in non-active buckets. This allows __contains__ to reject
                misses faster (when num_buckets isn't very small), at the cost of
                bucket rotation becoming an O(N) operation instead of O(1), and
                additional memory overhead.
            history_rejection_filter_fpr: The false positive rate for the history
                rejection Bloom filter.
        """
        if not 0.0 < history_rejection_filter_fpr < 1.0:
            raise ValueError("history_rejection_filter_fpr must be between 0 and 1.")

        self._enable_history_fast_reject = enable_history_fast_reject
        self._history_rejection_filter_fpr = history_rejection_filter_fpr
        self._history_rejection_filter = None

        super().__init__(ttl, num_buckets, bucket_capacity)

    @property
    def enable_history_fast_reject(self):
        return self._enable_history_fast_reject

    @property
    def history_rejection_filter_fpr(self):
        return self._history_rejection_filter_fpr

    def add(self, item) -> None:
        """Adds an item to the active bucket, rotating first if necessary.

        If history fast reject is enabled and a rotation occurred - rebuilds the
        history rejection filter.

        Args:
            item: The element to add to the structure.
        """
        now = time.monotonic()
        rotated = self._maybe_rotate_by_time(now)

        if not rotated and len(self._buckets[0].impl) >= self._bucket_capacity:
            self._rotate(now)
            rotated = True

        if rotated and self._enable_history_fast_reject:
            self._rebuild_history_rejection_filter(now)

        self._buckets[0].impl.add(item)

    def _make_bucket_impl(self) -> set:
        """Returns a native Python set for the new bucket."""
        return set()

    def _rebuild_history_rejection_filter(self, now: float) -> None:
        """Builds a new history rejection Bloom filter to replace the current one."""
        buckets_iter = iter(self._buckets)

        # Skip active bucket, iter on history buckets
        next(buckets_iter)

        self._history_rejection_filter = rbloom.Bloom(
            expected_items=(self._num_buckets - 1) * self._bucket_capacity,
            false_positive_rate=self._history_rejection_filter_fpr,
        )

        for bucket in buckets_iter:
            if now - bucket.created_at > self._ttl:
                break

            self._history_rejection_filter.update(bucket.impl)

    def __contains__(self, item: typing.Any) -> bool:
        """Checks membership across all valid buckets.

        The active bucket is checked first. If history fast reject is enabled, the
        history rejection filter is used to quickly reject misses before scanning the
        rest of the queue.

        Args:
            item: The element to search for.

        Returns:
            True if the item exists in any non-expired bucket.
        """
        now = time.monotonic()

        # All buckets are expired
        if now - self._buckets[0].created_at > self._ttl:
            return False

        # Check if item is in active bucket
        if item in self._buckets[0].impl:
            return True

        # Use fast history reject if enabled
        if self._enable_history_fast_reject and self._history_rejection_filter:
            if item not in self._history_rejection_filter:
                return False

        # Check if item is in any of the non-expired history buckets
        buckets_iter = iter(self._buckets)
        next(buckets_iter)  # Skip active bucket, already checked

        for bucket in buckets_iter:
            if now - bucket.created_at > self._ttl:
                break

            if item in bucket.impl:
                return True

        return False

    def __repr__(self) -> str:
        return (
            f"<RotatingTTLSet("
            f"ttl={self._ttl}, "
            f"num_buckets={len(self._buckets)}/{self._num_buckets}, "
            f"bucket_capacity={self._bucket_capacity})>"
        )
