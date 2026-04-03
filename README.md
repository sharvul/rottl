# rottl

**rottl** is a high-performance Python library providing bucket-based rotating data structures with approximate TTL eviction and strict memory constraints.

Unlike standard TTL caches that track expiration for every individual item, `rottl` uses a staged bucket approach to offer constant-time operations and lower memory footprint.
