# Workload guidance

Satchel now exposes ext4, so applications receive normal Linux filesystem behavior. Databases no longer run through a second filesystem implemented as SQLite rows.

The default `durability=remote` mode publishes dirty blocks to S3 before an ext4 flush succeeds. Databases that use `fsync` correctly do not lose acknowledged transactions when a node fails. S3 latency becomes part of commit latency, so a database's own replication is still the better choice when it needs low-latency synchronous commits or automatic multi-node failover.

`durability=async` keeps flushes local and publishes on the configured sync interval. It is faster for flush-heavy workloads, but a host failure may lose the last interval.

Good fits include application state, uploads, caches, development databases, and services that accept a small recovery-point window.

Satchel still permits only one writer. Use a shared filesystem or database replication when several nodes must modify the same data concurrently.

Restore downloads every segment needed by the latest checkpoint and delta chain before mounting. Large volumes with little allocated data remain cheap because the local image is sparse, but Satchel does not yet fetch blocks lazily.
