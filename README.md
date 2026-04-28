# RoTTL — Rotating TTL Data Structures

RoTTL provides memory-efficient rotating sets, dicts, and Bloom filters with approximate TTL-based eviction.

Rather than maintaining per-item expiry timestamps, the TTL window is divided into a fixed number of rotating buckets — expired buckets are evicted as a whole, trading per-item precision for significantly lower memory usage and faster writes, at the cost of sequential bucket scans.

## Installation

```bash
pip install rottl
```

The only dependency is [`rbloom`](https://github.com/KenanHanke/rbloom). If you encounter installation issues, refer to their documentation.

## Quickstart

- **`RotatingTTLSet`** — exact membership tracking backed by native Python `set` buckets.

```python
from rottl import RotatingTTLSet

seen_ids = RotatingTTLSet(
    ttl=24 * 60 * 60, num_buckets=6, bucket_capacity=1_000_000
)
seen_ids.add("user_42")
print("user_42" in seen_ids)  # True
```

- **`RotatingTTLDict`** — key-value storage backed by native Python `dict` buckets.

```python
from rottl import RotatingTTLDict

response_cache = RotatingTTLDict(
    ttl=60 * 60, num_buckets=4, bucket_capacity=10_000
)
response_cache["GET /api/status"] = {"ok": True}
print(response_cache.get("GET /api/status"))  # {'ok': True}
```

- **`RotatingTTLBloom`** — probabilistic membership tracking backed by [`rbloom.Bloom`](https://github.com/KenanHanke/rbloom) buckets.

```python
from rottl import RotatingTTLBloom

visited_urls = RotatingTTLBloom(
    ttl=7 * 24 * 60 * 60, num_buckets=7, bucket_capacity=10_000_000, bucket_fpr=0.001
)
visited_urls.add("https://example.com")
print("https://example.com" in visited_urls)  # True
```

All three structures rotate automatically based on time and capacity, though capacity tracking differs between implementations:

* **`RotatingTTLSet` and `RotatingTTLDict`**: Capacity is checked inline on every insertion via an $O(1)$ `len()` call.
* **`RotatingTTLBloom`**: Estimating the number of unique inserted items requires inspecting filter occupancy by counting set bits — an $O(M)$ operation. To keep the hot path $O(1)$ for the vast majority of insertions, capacity is managed via an **amortized countdown** that defers the check.

`RotatingTTLSet` and `RotatingTTLDict` support an optional **history fast-reject** mode, which maintains an auxiliary Bloom filter over all non-expired historical buckets. This allows most negative lookups to be rejected without scanning the full bucket deque, at the cost of filter rebuild on each rotation.

## When to use RoTTL

- **Approximate TTL is acceptable.** Expiry happens at bucket boundaries, not per item. Under normal load (no capacity-based eviction), items live between `ttl - (ttl / num_buckets)` and `ttl` seconds. Capacity pressure can cause earlier eviction.
- **Memory-constrained environments.** RoTTL's bucket-level eviction avoids per-item bookkeeping, keeping structure overhead proportional to bucket count rather than item count.
  - `RotatingTTLDict` uses roughly 3–4× less memory than `cachetools.TTLCache`.
- **Write-heavy workloads.** RoTTL's write path is lightweight — no per-item expiry metadata is maintained on insertion.
  - `RotatingTTLDict` is 6–15× faster than `cachetools.TTLCache` on insertions (varies by fast-reject usage and rotation pressure).
- **Lookup performance scales with configuration.** Lookups scan up to `num_buckets` buckets, so with a small bucket count the overhead is negligible. With a large bucket count, hit cost depends on which bucket the item is found in, and miss cost grows linearly. `RotatingTTLSet` and `RotatingTTLDict`'s history fast-reject option makes most miss latency independent of `num_buckets`, at the cost of slower rotations.
