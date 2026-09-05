# Workload guidance

Satchel now exposes ext4, so applications receive normal Linux filesystem behavior. Databases no longer run through a second filesystem implemented as SQLite rows.

The default `durability=local` mode writes dirty blocks to Satchel's local journal before an ext4 flush succeeds. S3 replication runs on the configured interval. A database that uses `fsync` correctly recovers acknowledged transactions after an application crash, Satchel crash, or same-node reboot as long as the state disk survives.

`durability=remote` publishes dirty blocks to S3 before an ext4 flush succeeds. It preserves acknowledged transactions after loss of the node or its disk. S3 latency becomes part of commit latency, so a database's own replication is still the better choice when it needs low-latency synchronous commits or automatic multi-node failover.

Local durability does not make unpublished writes portable. If another node advances the remote history, Satchel preserves the old node's journal and refuses to replay it automatically. The dirty-data limit bounds unpublished bytes and pauses writers if S3 cannot keep up.

Good fits include application state, uploads, caches, development databases, and services that accept a small recovery-point window.

Satchel still permits only one writer. Use a shared filesystem or database replication when several nodes must modify the same data concurrently.

Writable mount time depends on manifest count rather than allocated volume size. Satchel fetches a segment when ext4 first reads one of its blocks, then caches every live block from that segment in the local sparse image. Cold random reads pay S3 latency. Working-set reads stay local after their first access. Read-only mounts materialize their snapshot before mounting so garbage collection cannot remove unread snapshot data.
