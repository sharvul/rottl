import abc
import collections
import time
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
        if ttl <= 0.0:
            raise ValueError("ttl must be strictly positive.")
        if bucket_capacity < 1:
            raise ValueError("bucket_capacity must be at least 1.")

        self._ttl = ttl
        self._num_buckets = num_buckets
        self._bucket_capacity = bucket_capacity
        self._bucket_ttl = ttl / num_buckets

        self._buckets: typing.Deque[_Bucket] = collections.deque(maxlen=num_buckets)

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

    def add(self, item: typing.Any) -> None:
        """Adds an item to the active bucket, rotating by time if necessary.

        Args:
            item: The element to add to the structure.
        """
        now = time.monotonic()

        if now - self._buckets[0].created_at >= self._bucket_ttl:
            self._rotate(now)

        self._buckets[0].impl.add(item)

    def _rotate(self, now: float) -> None:
        """Initializes and prepends a new bucket to the sequence."""
        self._buckets.appendleft(self._make_bucket(now))

    def _make_bucket(self, now: float) -> _Bucket:
        """Wraps a new subclass implementation into a _Bucket container."""
        return _Bucket(impl=self._make_bucket_impl(), created_at=now)

    @abc.abstractmethod
    def _make_bucket_impl(self) -> typing.Any:
        """Returns the raw internal structure for a new bucket."""
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
            if now - bucket.created_at > self._ttl:
                break

            if item in bucket.impl:
                return True

        return False
