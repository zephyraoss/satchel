# Workload guidance

Satchel now exposes ext4, so applications receive normal Linux filesystem behavior. Databases no longer run through a second filesystem implemented as SQLite rows.

The default `durability=remote` mode publishes dirty blocks to S3 before an ext4 flush succeeds. Databases that use `fsync` correctly do not lose acknowledged transactions when a node fails. S3 latency becomes part of commit latency, so a database's own replication is still the better choice when it needs low-latency synchronous commits or automatic multi-node failover.

`durability=async` acknowledges filesystem flushes without syncing the disposable local image and publishes on the configured interval. An application crash retains writes while Satchel keeps the volume mounted. A Satchel crash, forced restart, or host failure can lose unpublished generations because restart rebuilds from S3. The dirty-data limit bounds unpublished bytes and pauses writers if S3 cannot keep up.

Good fits include application state, uploads, caches, development databases, and services that accept a small recovery-point window.

Satchel still permits only one writer. Use a shared filesystem or database replication when several nodes must modify the same data concurrently.

Writable mount time depends on manifest count rather than allocated volume size. Satchel fetches a segment when ext4 first reads one of its blocks, then caches every live block from that segment in the local sparse image. Cold random reads pay S3 latency. Working-set reads stay local after their first access. Read-only mounts materialize their snapshot before mounting so garbage collection cannot remove unread snapshot data.
