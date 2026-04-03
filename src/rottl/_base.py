import time
import collections
import abc
import typing


class _Bucket(typing.NamedTuple):
    """Internal container for a data structure and its creation timestamp.

    Attributes:
        impl: The underlying data structure instance (e.g., set or Bloom).
        created_at: Monotonic timestamp of when this bucket was initialized.
    """

    impl: typing.Any
    created_at: float


class _RotatingTTLBase(abc.ABC):
    """Internal abstract base class for rotating TTL structures.

    Manages a deque of buckets to provide time-based eviction. Items live for
    an approximate duration between `ttl - (ttl / num_buckets)` and `ttl`.
    When a bucket's TTL is reached, a new bucket is prepended and the
    oldest is evicted.
    """

    __slots__ = (
        "_ttl",
        "_num_buckets",
        "_bucket_capacity",
        "_bucket_ttl",
        "_buckets",
        "_last_rotation",
    )

    def __init__(
        self,
        ttl: float,
        num_buckets: int,
        bucket_capacity: int,
    ):
        """Initializes the base rotating structure.

        Args:
            ttl: Total time-to-live for data in seconds.
            num_buckets: Number of buckets to divide the TTL into. Must be at least 2.
            bucket_capacity: Maximum capacity per bucket.

        Raises:
            ValueError: If num_buckets is less than 2.
        """
        if num_buckets < 2:
            raise ValueError("num_buckets must be at least 2 for rotation logic.")

        self._ttl = ttl
        self._num_buckets = num_buckets
        self._bucket_capacity = bucket_capacity
        self._bucket_ttl = ttl / num_buckets

        self._buckets: typing.Deque[_Bucket] = collections.deque(maxlen=num_buckets)
        self._last_rotation = 0.0

    def add(self, item: typing.Any) -> None:
        """Adds an item to the active bucket, rotating first if necessary.

        Args:
            item: The element to add to the structure.
        """
        self._maybe_rotate(time.monotonic())
        self._buckets[0].impl.add(item)

    def _maybe_rotate(self, now: float) -> bool:
        """Refreshes buckets based on age and capacity.

        Appends a fresh bucket if the active one has expired, and prunes
        all buckets that have outlived the total TTL.

        Returns:
            bool: True if buckets were added or evicted.
        """
        if not self._buckets or now - self._last_rotation >= self._bucket_ttl:
            self._rotate(now)
            return True

        return False

    def _rotate(self, now: float):
        """Initializes and prepends a new bucket to the sequence."""
        self._last_rotation = now
        self._buckets.appendleft(self._make_bucket(now))

    def _make_bucket(self, now: float) -> _Bucket:
        """Wraps a new subclass implementation into a _Bucket container."""
        return _Bucket(impl=self._make_bucket_impl(), created_at=now)

    @abc.abstractmethod
    def _make_bucket_impl(self) -> typing.Any:
        """Returns the raw internal structure for a new bucket."""
        ...

    def __contains__(self, item: typing.Any) -> bool:
        """Checks membership across all valid buckets (after pruning
        expired buckets).

        Args:
            item: The element to search for.

        Returns:
            True if the item exists in any non-expired bucket.
        """
        now = time.monotonic()
        self._maybe_rotate(now)

        while self._buckets and now - self._buckets[-1].created_at > self._ttl:
            self._buckets.pop()

        for bucket in self._buckets:
            if item in bucket.impl:
                return True

        return False
