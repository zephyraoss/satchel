# Block replication format

Satchel stores each volume under `volumes/<name>/`:

```text
state.json
manifests/<sha256>.json
manifest-bundles/<sha256>.json
segments/e<epoch>-g<generation>-<sha256>.seg.gz
```

Segment, manifest, and manifest-bundle objects are immutable. Their names contain a SHA-256 digest. Segment names also contain the writer epoch and generation, which prevents a garbage collector from racing with a later writer that happens to produce identical block data. `state.json` is the only mutable volume object. Its format identifier is `satchel-block-v2`. Volumes published with the append head store the same state document under `heads/` instead of `state.json`; see [Append head](#append-head).

## Local journal

With `durability=local`, Satchel stores a checksummed append-only journal under `--journal-dir`, which defaults to `<state-dir>/journals`. A record contains the complete 4 KiB value of every changed block in one generation. Satchel acknowledges a filesystem flush only after the record reaches stable local storage.

After a restart, Satchel validates the journal's volume identity, base manifest, writer epoch, and node identity against the remote head. It restores the remote image, discards an incomplete final journal record, and replays complete records. A checksum failure is treated as corruption. If a different writer advanced the remote history, Satchel preserves the journal and refuses the mount because block-level histories cannot be merged safely.

The replication worker marks journal records reclaimable only after the conditional remote head update succeeds. It compacts reclaimed data in batches instead of rewriting the journal after every S3 generation. Compaction uses a synced temporary file, atomic rename, and parent-directory sync. Recovery may replay already-published records after a crash between publication and compaction; replay is idempotent.

## State publication

`state.json` contains the volume identity, block geometry, current generation, current manifest, persistent lease epoch, and active lease. It also holds up to 64 KiB of recent manifest data and one pointer to the newest manifest bundle. Satchel reads the state's ETag and updates it with `If-Match` for lease renewal, generation publication, release, and takeover.

A generation becomes durable after Satchel uploads any external segment objects and advances `state.json`. If the new manifest fits within the 64 KiB inline budget, the same conditional state update includes its body. A small database commit therefore needs one S3 PUT rather than a manifest PUT followed by a state PUT.

Once the inline area reaches 16 KiB, Satchel starts packing it into one immutable object in the background. A later conditional state update installs the bundle pointer and removes the copied inline entries. If new manifests fill the 64 KiB hard limit before that upload finishes, the next publication waits for a bundle. Bundles form a linked history, while `state.json` keeps only the newest bundle pointer. Checkpoints pack pending inline history and store their own manifest at an immutable key. Large manifests retain the same two-phase path. Uploads that do not reach the final conditional update are unreachable and safe to ignore.

The lease and volume head share one conditional object. Once another node takes the lease, every publication attempt using the old ETag fails. This protects the committed history even if the old node has not unmounted yet.

A generation object created with `If-None-Match: *` cannot replace this conditional head. That condition only proves that the generation key was unused. It does not prove that the caller still owns the lease, so a stale writer could otherwise acknowledge a generation after another node took over.

## Append head

Some backends, notably Garage, accept `If-Match` and `If-None-Match` headers but ignore them. `--s3-head=append` keeps the same state document but publishes it as an append-only log of immutable versions under `heads/`:

```text
heads/<inverted epoch>_<lease token>_<inverted seq>_<claim ms>.json
```

The epoch and sequence fields are zero-padded and inverted, so a plain sorted listing puts the newest version first. Every version is written with an unconditional PUT to a key that no other writer will ever use, so writers never overwrite each other; they only ever disagree about which key is the head. The head of a volume is the first listed version with a non-zero sequence.

Taking or creating a lease writes a claim with sequence zero, waits for the settle window, and lists the prefix again. The claim stands only if no other claim or version with the same or a newer epoch appeared, and if the previous holder's head did not advance in the meantime. The claimant then writes sequence one and confirms it the same way. A claim whose PUT took longer than half the settle window is deleted and retried rather than trusted, which bounds the effect of a slow request landing after a competitor already checked. Claims older than the lease TTL that never reached sequence one are reaped by the next claimant.

Publication, renewal, release, and break write the next sequence and then list the prefix. The write is committed only if it is the newest version and no newer epoch exists; a newer epoch means another node took over and the writer reports lease loss exactly as the conditional head does. The lease fence therefore depends on every node seeing its own writes in a listing immediately, which Garage provides, and on clock skew between nodes staying well under the settle window. Break and delete bump the epoch so a stale holder's next write lands in an old epoch and fails.

Confirmed versions prune everything from older epochs and all but the two most recent sequences of the current one, so a volume keeps a handful of head objects rather than a growing log. Commit latency is one PUT plus one LIST instead of one conditional PUT. A volume is bound to the head mode it was created with; opening it in the other mode fails with an error naming the required flag.

## Segments

A segment is gzip-compressed binary data. Its uncompressed form contains:

```text
8 bytes   magic "SATSEG01"
4 bytes   block size, big endian
4 bytes   extent count, big endian
repeated:
  8 bytes first block number, big endian
  4 bytes byte length, big endian
  N bytes contiguous block data
```

The current format uses 4 KiB blocks and joins up to 256 adjacent blocks into one encoded extent. A generation is split into segments of at most 1024 blocks, or 4 MiB before compression. Satchel uploads large segment objects with up to four concurrent requests. It embeds compressed segments of 64 KiB or less in the immutable manifest, which removes one S3 request from the common database commit. A checkpoint references the source manifest instead of copying the inline bytes. A segment digest always covers the compressed bytes, and restore verifies it before writing any block.

## Manifests

A delta manifest references one or more segments and its parent manifest. Each reference records the block extents present in the segment and which extents contain zeros. This metadata lets a reader find a block without downloading unrelated segment bodies.

A manifest bundle stores a sorted batch of recent manifest bodies and points to the preceding bundle. The state records only the newest bundle and its generation range. Recovery follows that chain when a requested manifest is not still inline or stored as an individual object. Checkpoints record the bundle that owns any inline segment source they reuse, so lazy reads can fetch one bundle and serve every reference from it.

A checkpoint manifest reuses existing segment objects. Its references contain disjoint extents and select the newest non-zero source for each block. Creating one reads the bounded manifest chain, packs pending inline history, and publishes the checkpoint manifest plus `state.json`; it does not read the local image or upload block data. Its parent preserves older history, but a current restore stops at the checkpoint. Satchel acknowledges the triggering database flush after its delta generation becomes durable, then starts checkpoint work in the replication worker. A closely following flush may queue behind that metadata operation.

A writable mount holds the volume lease, reads the manifests, creates an empty sparse image, and starts NBD. The first read of a remote-backed block downloads and verifies its segment, then writes every still-relevant block from that segment into the local image. Full-block overwrites skip the download. Partial-block writes fetch the old block first so bytes outside the write remain intact. Read-only mounts and `satchel vol restore` materialize their fixed snapshot so retention cannot remove a cold segment during a long-lived mount. Materialized restores download up to four segments concurrently.

For point-in-time recovery, Satchel walks parent links across checkpoints, selects an exact generation or the latest generation at or before an RFC3339 timestamp, then restores from that point's nearest checkpoint.

## Failure rules

- Losing a lease prevents further state publication.
- An S3 error leaves the generation queued locally and applies backpressure when queued blocks reach the dirty limit.
- With `durability=remote`, a successful filesystem flush has advanced `state.json`, including its inline manifest when needed. Unflushed writes can still be lost after a host failure.
- With `durability=local`, a successful filesystem flush survives a Satchel crash or same-node reboot while the state disk remains intact. Loss of that disk can lose generations that did not advance the remote head.
- A local journal whose remote history was advanced by another writer requires operator recovery. Satchel does not replay it automatically.
- A corrupt segment or manifest fails the affected read or materialized restore. Satchel never returns unverified remote data.
- S3 object listing is not part of restore correctness. Restore follows exact keys from `state.json`.

## Garbage collection

`satchel vol gc` runs a two-phase collector. It retains the current restore chain, all generations inside `--history-retention`, and the checkpoint needed to restore the oldest retained generation. It first writes immutable records under `gc/`. A later pass deletes listed objects only if they remain outside the retained history for the full `--gc-grace` period. The extra delay protects restores that read an older `state.json` just before a checkpoint was published.
