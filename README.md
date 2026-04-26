# RoTTL — Rotating TTL Data Structures
 
RoTTL provides memory-efficient rotating sets, dicts, and Bloom filters with approximate TTL-based eviction.
 
Rather than maintaining per-item expiry timestamps, the TTL window is divided into a fixed number of rotating buckets — expired buckets are evicted as a whole, trading per-item precision for significantly lower memory usage and faster writes, at the cost of sequentially scanned lookups.
 
The library exposes three structures:
 
- **`RotatingTTLSet`** — a rotating, TTL-based set backed by native Python `set` buckets.
- **`RotatingTTLDict`** — a rotating, TTL-based dict backed by native Python `dict` buckets.
- **`RotatingTTLBloom`** — a rotating, TTL-based Bloom filter backed by [`rbloom`](https://github.com/KenanHanke/rbloom).
 
All three structures rotate automatically on both time and capacity. However, they use different mechanisms to maintain performance:
 
* **`RotatingTTLSet` and `RotatingTTLDict`**: Capacity is checked inline on every insertion via an $O(1)$ `len()` call.
* **`RotatingTTLBloom`**: Estimating unique items requires counting all set bits in the filter — an $O(M)$ operation. To keep the hot path $O(1)$ for the vast majority of insertions, capacity is managed via an **amortized countdown** that defers the check.
 
`RotatingTTLSet` and `RotatingTTLDict` support an optional **history fast-reject** mode, which maintains an auxiliary Bloom filter over all non-expired historical buckets. This allows most membership misses to be short-circuited without scanning the full bucket deque, at the cost of filter rebuild on each rotation.
 
---
 
## When to use RoTTL
 
- **Approximate TTL is acceptable.** Expiry happens at bucket boundaries, not per item. Under normal load (no capacity-based eviction), items live between `ttl - (ttl / num_buckets)` and `ttl` seconds. Capacity pressure can cause earlier eviction.
- **Memory-constrained environments.** `RotatingTTLSet` and `RotatingTTLDict` use roughly 3–4× less memory than `cachetools.TTLCache` at all tested capacities. `RotatingTTLBloom` can reduce memory even further, depending on the configured `bucket_fpr` and `num_buckets`.
- **Write-heavy workloads.** RoTTL's write path avoids the expensive per-item bookkeeping required for exact TTL, making it 6–15x faster than `cachetools.TTLCache` (varies by usage of fast-reject and rotation pressure).
- **Lookup performance scales with configuration.** Lookups scan up to `num_buckets` buckets, so with a small bucket count the overhead is negligible. With a large bucket count, hit cost depends on which bucket the item is found in, and miss cost grows linearly. `RotatingTTLSet` and `RotatingTTLDict`'s history fast-reject option makes most miss latency independent of `num_buckets`, at the cost of slower rotations.
 