import abc
import collections
import enum
import time
import typing
import rbloom

T = typing.TypeVar("T")


class _Bucket(typing.Generic[T]):
    """Internal container for a data structure and its creation timestamp.

    Attributes:
        impl: The underlying data structure instance (e.g., set or Bloom).
        created_at: Monotonic timestamp of when this bucket was initialized.
    """

    __slots__ = (
        "impl",
        "created_at",
    )

    def __init__(self, impl: T, created_at: float) -> None:
        self.impl = impl
        self.created_at = created_at


class _RotationReason(enum.Enum):
    TTL = enum.auto()
    CAPACITY = enum.auto()


class _RotatingTTLBase(abc.ABC, typing.Generic[T]):
    """Internal abstract base class for rotating TTL structures.

    Manages a deque of buckets to provide approximate time-based eviction.
    Items are retained for a maximum of `ttl` seconds. Under normal volume,
    items live for at least `ttl - (ttl / num_buckets)` seconds, but may be
    evicted earlier if high insertion volume forces capacity-based rotations.
    """

    __slots__ = (
        "_ttl",
        "_num_buckets",
        "_bucket_capacity",
        "_bucket_ttl",
        "_buckets",
        "_on_rotate_callbacks",
        "_rotations_by_ttl_count",
        "_rotations_by_capacity_count",
    )

    def __init__(
        self,
        ttl: float,
        num_buckets: int,
        bucket_capacity: int,
    ):
        """Initializes the rotating TTL structure.

        Args:
            ttl: Total time-to-live for data in seconds.
            num_buckets: The number of rotation stages used to divide the total TTL.
            bucket_capacity: Max expected items per bucket.
        """

        if num_buckets < 2:
            raise ValueError("num_buckets must be at least 2 for rotation logic.")
        if ttl <= 0.0:
            raise ValueError("ttl must be strictly positive.")
        if bucket_capacity < 1:
            raise ValueError("bucket_capacity must be at least 1.")

        self._ttl = ttl
        self._num_buckets = num_buckets
        self._bucket_capacity = bucket_capacity
        self._bucket_ttl = ttl / num_buckets

        self._buckets: typing.Deque[_Bucket[T]] = collections.deque(maxlen=num_buckets)
        self._on_rotate_callbacks: typing.List[typing.Callable[[], None]] = []

        self._rotations_by_ttl_count = 0
        self._rotations_by_capacity_count = 0

        # Invariant: _buckets is never empty after initialization
        self._rotate(time.monotonic())

    @property
    def ttl(self):
        return self._ttl

    @property
    def num_buckets(self):
        return self._num_buckets

    @property
    def bucket_capacity(self):
        return self._bucket_capacity

    @property
    def rotations_by_ttl(self):
        return self._rotations_by_ttl_count

    @property
    def rotations_by_capacity(self):
        return self._rotations_by_capacity_count

    def clear(self):
        """Removes all elements from the structure by purging all buckets."""
        self._buckets.clear()

        # Push a new bucket to keep _buckets non-empty
        self._rotate(time.monotonic())

    def add_on_rotate_callback(self, callback: typing.Callable[[], None]) -> None:
        """Registers a callback to be executed whenever a bucket rotation occurs.

        Args:
            callback: A callable to be invoked during the rotation process.
        """
        self._on_rotate_callbacks.append(callback)

    def clear_on_rotate_callbacks(self) -> None:
        """Removes all registered rotation callbacks."""
        self._on_rotate_callbacks.clear()

    def get_active_bucket_item_count(self) -> int:
        """Calculates the number of items (or approximate items) in the active bucket.

        If the active bucket has expired based on the total TTL, returns 0 to reflect
        its effective state.
        """
        if time.monotonic() - self._buckets[0].created_at >= self._ttl:
            return 0

        return self._get_bucket_impl_item_count(self._buckets[0].impl)

    def _rotate(
        self,
        now: float,
        reason: typing.Optional[_RotationReason] = None,
    ) -> None:
        """Initializes and prepends a new bucket to the sequence."""
        self._buckets.appendleft(self._make_bucket(now))

        if reason is _RotationReason.TTL:
            self._rotations_by_ttl_count += 1
        elif reason is _RotationReason.CAPACITY:
            self._rotations_by_capacity_count += 1

        for callback in self._on_rotate_callbacks:
            callback()

    def _make_bucket(self, now: float) -> _Bucket[T]:
        """Wraps a new subclass implementation into a _Bucket container."""
        return _Bucket(impl=self._make_bucket_impl(), created_at=now)

    @abc.abstractmethod
    def _make_bucket_impl(self) -> typing.Any:
        """Returns the raw internal structure for a new bucket."""
        ...

    @classmethod
    @abc.abstractmethod
    def _get_bucket_impl_item_count(cls, impl: T) -> int:
        """Returns the item count of the underlying bucket implementation.

        Note:
            The count may be exact or approximate, depending on the bucket implementation.
        """
        ...

    def __contains__(self, item: typing.Any) -> bool:
        """Checks membership across all valid buckets.

        Args:
            item: The element to search for.

        Returns:
            True if the item exists in any non-expired bucket.
        """
        now = time.monotonic()

        for bucket in self._buckets:
            if now - bucket.created_at >= self._ttl:
                break

            if item in bucket.impl:
                return True

        return False


class _RotatingTTLCollectionBase(_RotatingTTLBase[T], typing.Generic[T]):
    """Intermediate base class for structures supporting fast-reject history filters.

    Provides joint logic for structures that store exact items (like sets and dicts)
    and can optionally maintain an aggregate Bloom filter of historical buckets to
    speed up `__contains__` misses.
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
        """Initializes the rotating TTL structure.

        Args:
            ttl: Total time-to-live for data in seconds.
            num_buckets: The number of rotation stages used to divide the total TTL.
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

    def _rotate(
        self,
        now: float,
        reason: typing.Optional[_RotationReason] = None,
    ) -> None:
        """Initializes and prepends a new bucket to the sequence.

        If history fast reject is enabled - rebuilds the history rejection filter.
        """
        super()._rotate(now, reason)

        if self._enable_history_fast_reject:
            self._rebuild_history_rejection_filter(now)

    def _find_bucket_index(self, item: typing.Any) -> typing.Optional[int]:
        """
        Locates the index of the latest (most recent) bucket containing the item.

        Scanning starts from the active bucket (index 0) and proceeds through
        the history. If fast-reject is enabled, the history is skipped entirely
        on a Bloom filter miss.

        Returns:
            The index [0 to num_buckets-1] of the bucket, or None if not found.
        """
        now = time.monotonic()

        # Check if all buckets are expired
        if now - self._buckets[0].created_at >= self._ttl:
            return None

        # Check if item is in active bucket
        if item in self._buckets[0].impl:
            return 0

        # Use history fast reject if enabled
        if (
            self._enable_history_fast_reject
            and self._history_rejection_filter is not None
        ):
            if item not in self._history_rejection_filter:
                return None

        # Check if item is in any of the non-expired history buckets
        for idx in range(1, len(self._buckets)):
            if now - self._buckets[idx].created_at >= self._ttl:
                break

            if item in self._buckets[idx].impl:
                return idx

        return None

    def _rebuild_history_rejection_filter(self, now: float) -> None:
        """Builds a new history rejection Bloom filter to replace the current one."""
        self._history_rejection_filter = rbloom.Bloom(
            expected_items=(self._num_buckets - 1) * self._bucket_capacity,
            false_positive_rate=self._history_rejection_filter_fpr,
        )

        for idx in range(1, len(self._buckets)):
            if now - self._buckets[idx].created_at >= self._ttl:
                break

            # This works for both sets (iterates items) and dicts (iterates keys)
            self._history_rejection_filter.update(self._buckets[idx].impl)

    def __contains__(self, item: typing.Any) -> bool:
        """Checks membership across all valid buckets."""
        return self._find_bucket_index(item) is not None
