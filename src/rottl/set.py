import rbloom
import time

from ._base import _RotatingTTLBase


class RotatingTTLSet(_RotatingTTLBase):
    """A rotating set with time-based eviction and automatic capacity rotation."""

    __slots__ = (
        "_use_history_proxy_bloom",
        "_history_proxy_bloom_fpr",
        "_history_proxy_bloom",
    )

    def __init__(
        self,
        ttl: float,
        num_buckets: int,
        bucket_capacity: int,
        use_history_proxy_bloom: bool = False,
        history_proxy_bloom_fpr: float = 0.001,
    ):
        """Initializes the rotating TTL set.

        Args:
            ttl: Time-to-live for items in seconds.
            num_buckets: Number of internal rotation stages.
            bucket_capacity: Optional max items per bucket before auto-rotation.
            use_history_proxy_bloom: If True, uses a proxy Bloom filter to
                speed up misses.
            history_proxy_bloom_fpr: The false positive rate for the history
                proxy Bloom filter.
        """
        super().__init__(ttl, num_buckets, bucket_capacity)

        self._use_history_proxy_bloom = use_history_proxy_bloom
        self._history_proxy_bloom_fpr = history_proxy_bloom_fpr
        self._history_proxy_bloom = None

    def add(self, item):
        now = time.monotonic()

        if not self._maybe_rotate(now):
            if len(self._buckets[0].impl) >= self._bucket_capacity:
                self._rotate(now)

        self._buckets[0].impl.add(item)

    def _rotate(self, now):
        super()._rotate(now)

        if self._use_history_proxy_bloom:
            self._update_history_proxy_bloom(now)

    def _make_bucket_impl(self) -> set:
        return set()

    def _make_history_proxy_bloom(self) -> rbloom.Bloom:
        return rbloom.Bloom(
            expected_items=(self._num_buckets - 1) * self._bucket_capacity,
            false_positive_rate=self._history_proxy_bloom_fpr,
        )

    def _update_history_proxy_bloom(self, now: float):
        buckets_iter = iter(self._buckets)

        # Skip active bucket, iter on history buckets
        next(buckets_iter)

        self._history_proxy_bloom = self._make_history_proxy_bloom()

        for bucket in buckets_iter:
            if now - bucket.created_at > self._ttl:
                break

            self._history_proxy_bloom.update(bucket.impl)

    def __contains__(self, item) -> bool:
        """Checks membership across all valid buckets.

        If history proxy optimization is enabled, utilizes an aggregated
        Bloom filter to quickly reject cache misses in historical buckets
        before iterating.

        Args:
            item: The element to search for.

        Returns:
            True if the item exists in any non-expired bucket.
        """
        now = time.monotonic()

        if not self._maybe_rotate(now) and item in self._buckets[0].impl:
            return True

        if self._use_history_proxy_bloom and self._history_proxy_bloom:
            if item not in self._history_proxy_bloom:
                return False

        while self._buckets and now - self._buckets[-1].created_at > self._ttl:
            self._buckets.pop()

        for idx in range(1, len(self._buckets)):
            if item in self._buckets[idx].impl:
                return True

        return False

    def __repr__(self) -> str:
        return (
            f"<RotatingTTLSet("
            f"ttl={self._ttl}, "
            f"num_buckets={len(self._buckets)}/{self._num_buckets}, "
            f"bucket_capacity={self._bucket_capacity})>"
        )
