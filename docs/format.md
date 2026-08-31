# Block replication format

Satchel stores each volume under `volumes/<name>/`:

```text
state.json
manifests/<sha256>.json
segments/e<epoch>-g<generation>-<sha256>.seg.gz
```

Segments and manifests are immutable. Manifest names contain their SHA-256 digest. Segment names contain the writer epoch, generation, and SHA-256 digest, which prevents a garbage collector from racing with a later writer that happens to produce identical block data. `state.json` is the only mutable volume object.

## State publication

`state.json` contains the volume identity, block geometry, current generation, current manifest, persistent lease epoch, and active lease. Satchel reads its ETag and updates it with `If-Match` for lease renewal, generation publication, release, and takeover.

A generation becomes durable only after Satchel uploads its segment, uploads its manifest, and advances `state.json`. In the default `durability=remote` mode, filesystem flush and FUA requests wait for this sequence. Uploads that do not reach the final conditional update are unreachable and safe to ignore.

The lease and volume head share one conditional object. Once another node takes the lease, every publication attempt using the old ETag fails. This protects the committed history even if the old node has not unmounted yet.

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

The current format uses 4 KiB blocks and joins up to 256 adjacent blocks into one extent. A segment key contains the SHA-256 digest of the compressed bytes. Restore verifies that digest before writing anything from the segment.

## Manifests

A delta manifest references one segment and its parent manifest. A checkpoint manifest references enough segments to reconstruct every non-zero block in the image. Its parent preserves older history, but a current restore stops when it reaches the checkpoint. Satchel writes a checkpoint at a filesystem flush or clean unmount after the configured number of delta generations.

Restore starts at the manifest named by `state.json`, follows parents back to a checkpoint or generation one, and applies the segments in ascending generation order to a new sparse image.

For point-in-time recovery, Satchel walks parent links across checkpoints, selects an exact generation or the latest generation at or before an RFC3339 timestamp, then restores from that point's nearest checkpoint.

## Failure rules

- Losing a lease prevents further state publication.
- An S3 error leaves the generation queued locally and applies backpressure when queued blocks reach the dirty limit.
- With `durability=remote`, a successful filesystem flush has advanced `state.json`. Unflushed writes can still be lost after a host failure.
- With `durability=async`, a process or host failure can lose generations that did not advance `state.json`.
- A corrupt segment or manifest stops restore. Satchel never skips corrupt data.
- S3 object listing is not part of restore correctness. Restore follows exact keys from `state.json`.

## Garbage collection

Clean unmounts run a two-phase collector. It retains the current restore chain, all generations inside `--history-retention`, and the checkpoint needed to restore the oldest retained generation. It first writes immutable records under `gc/`. A later pass deletes listed objects only if they remain outside the retained history for the full `--gc-grace` period. The extra delay protects restores that read an older `state.json` just before a checkpoint was published.
