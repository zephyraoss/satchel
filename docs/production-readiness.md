# Production readiness

This is the plan to take Satchel Block from a strong alpha to a backend you can
run under any database that holds irreplaceable data. The core replication and
fencing design is settled. What remains is destructive testing, provider
coverage, a few resilience changes the testing has already started to surface,
and the operational scaffolding an on-call team needs. The work is grouped into
tracks that can run in parallel and gated behind explicit exit criteria, because
"production ready" is a claim you earn against evidence, not a date.

## What "any database" adds beyond PostgreSQL

Nothing in the block backend is PostgreSQL-specific: it replicates an ext4
filesystem underneath whatever writes to it. But "works for any database" is a
stronger claim than "passed a pgbench run", because engines differ in the two
things that stress this design.

- **Flush discipline.** Satchel's durability contract is an ext4 flush or FUA
  write. An engine that fsyncs its journal on every commit (PostgreSQL with
  `synchronous_commit=on`, MySQL/InnoDB with
  `innodb_flush_log_at_trx_commit=1`) exercises the remote-durability path on
  the commit critical path. An engine that batches or defers fsync (MongoDB's
  journal at 100 ms, MySQL with the flush setting relaxed) shifts the exposure
  window. Each supported engine needs its own crash-consistency and durability
  proof, not an inherited one.
- **Working-set shape.** pgbench writes small scattered 4 KiB pages. An engine
  with a large sequential WAL, big compactions (RocksDB/LSM engines like MongoDB
  WiredTiger, Cassandra), or 16 KiB+ pages produces different generation sizes,
  compaction pressure, and checkpoint manifests. The extent planner and dirty
  limit behave differently under each.

So the plan validates a representative matrix — PostgreSQL, MySQL/InnoDB, and
one LSM engine (MongoDB or RocksDB) — rather than declaring engine-independence
from the filesystem abstraction alone.

## Finding from initial fault injection: S3 outage crashes the writer

The fault-injection harness (`test/fault/crashloop.sh`) already surfaced a
load-bearing behavior that belongs in every deployment decision.

The original behavior under `durability=remote` was that the very first failed
publish during any S3 blip returned `EIO` to the database's flush — and for a
WAL fsync, that makes PostgreSQL PANIC. A separate, later mechanism (the lease
heartbeat) also self-fenced the volume once the lease expired. Either way, **a
brief S3 interruption crashed the database**, even with no second node anywhere
near taking over.

This is now fixed (see Track D, done). A remote flush that can't reach S3 stalls
and retries with the filesystem flush held open, so the database waits instead of
crashing. Publication only succeeds once the conditional state update confirms
this node still holds the lease, so stalling is safe; a real takeover surfaces as
`ErrLeaseLost` and returns `EIO` immediately. The lease TTL remains the hard
backstop: an outage past the TTL still fences, because at that point another node
could legitimately take over. Verified on the bench host — a 15-second outage
(under the 30-second TTL) now leaves the database running with zero I/O errors in
its log and 405 acknowledged commits intact, where the same outage previously
crashed it.

Recovery in the fence case is also clean: after S3 returns and the volume
remounts, every acknowledged commit is present, pgbench's balance/history
invariant holds, and `pg_amcheck` is clean. No acknowledged commit is lost,
because in remote mode a commit isn't acknowledged until its state update reaches
S3; commits in flight during the outage simply fail rather than lying about
durability.

What remains: S3 availability is still coupled to database availability past the
lease TTL, which is inherent to single-writer fencing. Operators raise
`SATCHEL_LEASE_TTL` to widen the tolerance, trading failover speed. The residual
lost-ack edge case (a state PUT that commits but loses its acknowledgement) is
also fixed — see Track D.

## Finding from provider testing: publishes could commit manifests that were stored nowhere

Running the fault campaign against Cloudflare R2 surfaced the most serious bug
found so far — and it is not an R2 bug. `publishPrepared` decides where a
manifest body lives: embedded in `state.json`, archived as its own object, or
(implicitly) in the attached bundle. When a manifest small enough to embed
arrived while a ready background bundle was attached and the remaining inline
area still exceeded the 64 KiB state limit, no placement branch fired: the
commit advanced the head with a manifest key whose body existed **nowhere**.
`TestPublishNeverCommitsUnstoredManifestBody` reproduces it deterministically
on the in-memory store with zero fault injection — an ordinary publish under
ordinary load-shape conditions.

The damage is silent. Head restores keep working as long as newer writes shadow
the dangling references, so the harness's per-cycle verification passed for
hundreds of cycles while history rotted underneath: the R2 campaign volume lost
generation 75's manifest (and with it a 123-block inline segment still
referenced by two later checkpoints, which is what finally broke a remount),
and a metadata scan of the local MinIO longhaul volume found **1,105 lost
manifests across 20,437 generations**. Higher S3 latency widens the window
(the background bundler falls behind the commit rate, growing the inline area),
which is why R2 hit it in one cycle — but the mechanism is provider-independent.

The fix is a placement invariant: if a manifest is neither embedded nor covered
by the attached bundle, it is archived as its own object, unconditionally. Data
already lost to the bug is not recoverable (the bodies were never written);
affected volumes need to be rebuilt from a source of truth. Two lessons feed the
roadmap: restore-path errors of the form "read inline segment manifest … not
found" indicate this damage, and campaign verification must include full
metadata reachability, not just head restores — hence `vol verify` below.

Two smaller fixes came out of the same session: restore now resolves
inline-segment sources through the current inline area and the whole bundle
chain rather than only the location recorded at publish time
(`manifestSourceLoader.resolve`, covered by
`TestRestoreResolvesInlineSourceRolledIntoBundle` and
`TestRestoreResolvesBundleOnlySourceWithoutBundleRef`), and garbage collection
keeps the bundle-chain path down to any bundle that holds a referenced
inline-segment source, so a sweep cannot strand such references.

## Track A — Destructive fault injection

Goal: a repeatable campaign that kills every component at every point and
proves, each time, that acknowledged commits survive and the filesystem is
consistent. `test/fault/crashloop.sh` is the seed. It already rotates through
four faults with per-cycle verification (acked-commit ledger, pgbench
balance/history invariant, `pg_amcheck`):

- `kill -9` of PostgreSQL, then crash recovery on the still-mounted volume.
- `kill -9` of Satchel, lease broken via `vol lease break --yes`, takeover mount
  on fresh state.
- `kill -9` of Satchel, lease left to expire by TTL, takeover mount.
- S3 outage exceeding the lease window, self-fence, recover after S3 returns.

The acked-commit ledger is the important assertion: a dedicated writer records a
sequence number only after the commit returns success, and recovery must find
every recorded number and no gaps below the high-water mark. This is what turns
"it came back up" into "it did not lose durable data".

Exit criteria:

- A multi-day continuous campaign (thousands of cycles) with zero acked-commit
  loss and zero consistency violations.
- Fault points extended to the dangerous seams. Done and in the default
  rotation: `satchel-checkpoint` (kill -9 with checkpoint interval 1 so a
  checkpoint publish is almost certainly torn), `satchel-upload` (kill -9 during
  a direct-I/O random-data burst so large segment uploads are in flight), and
  `gc-kill` (kill -9 the collector mid-delete with zero grace, then prove the
  volume mounts, verifies, and a rerun collects to completion). The lost-ack
  path (a state PUT that commits but drops its acknowledgement) is covered at
  the unit level by the Track D fix, and end-to-end by the `s3-chaos` fault:
  `test/fault/chaosproxy` is a TCP proxy between Satchel and S3 that drops
  connections mid-body in both directions (a torn request, and the nastier
  torn response where the PUT committed but the acknowledgement was lost);
  validation cycles survived without fencing — commits stalled and resumed —
  with every check green. A real host reboot is covered by
  `test/fault/reboot.sh`: phase one runs load and triggers a sysrq-b hard reset
  with an fsynced acked-commit ledger on the host disk, phase two verifies
  after boot. The first real cycle recorded 143 fsynced acked commits before
  the reset and found all 143 after remount, with consistency and `pg_amcheck`
  clean (note the bench host's MinIO also survives the reset; against real S3
  the provider's durability is not a variable). Still to add: clock skew
  across takeover.
- The `gc-kill` seam found its first real bug. An interrupted sweep deletes
  point-in-time manifests in unordered fashion, and the history walk only
  tolerated a missing manifest directly under a checkpoint (the invariant a
  completed sweep maintains) — so a killed GC could leave a gap behind
  surviving expired deltas that wedged every future GC with "read manifest ...
  object not found". Data was never at risk: restore uses only the chain from
  the head to the newest checkpoint, and mount/verify passed throughout. Fixed
  by making the tolerance sticky in `loadHistory` (`internal/replica/remote.go`):
  once the walk has passed any checkpoint, a missing manifest ends the history
  instead of failing, and the next sweep reclaims the orphans. A missing
  manifest above the newest checkpoint — real restorable-chain corruption —
  still fails the walk. Covered by
  `TestHistoryToleratesManifestsRemovedByInterruptedGC` (fails against the
  pre-fix code) and `TestHistoryFailsOnMissingManifestAboveNewestCheckpoint`;
  the wedged bench volume was recovered in place by the fixed binary (127
  orphans marked, then deleted on the following pass).
- Two-node concurrent contention — both takeover paths now have integration
  tests in `test/e2e` with real NBD mounts and live heartbeats. The operator
  path (`TestConcurrentWriterIsFencedAfterTakeover`): node A publishes, an
  operator breaks its lease, node B takes over and restores A's data, and A's
  next durable write fences its mount. The automatic path
  (`TestPartitionedWriterSelfFencesAndYields`): node A is partitioned from S3,
  its in-flight commit stalls, it self-fences when the lease TTL expires, the
  stalled fsync returns `EIO` instead of a false ack, and node B takes over
  after expiry. In both, the durable state afterward contains only the
  legitimate writes and the superseded node's post-partition write is absent.
  The concurrent mount race is covered by
  `TestSimultaneousMountRaceAdmitsExactlyOneWriter`: two nodes race the
  volume-creating mount (the conditional create admits exactly one; the loser's
  retry is refused while the lease is held) and, after a clean release, two
  nodes race takeover with the same exactly-one outcome.
- Every failure preserved with its goroutine dump, satchel log, and postgres
  log for triage.

## Track B — Multi-day soak

Goal: prove nothing degrades over time — no memory growth, no goroutine leak,
no manifest-chain pathology, no unbounded object accumulation.

- Sustained mixed read/write load for at least 72 hours per engine, with the
  metrics endpoint scraped into a time series.
- Watch: RSS and goroutine count (leak detection), unpublished-bytes and
  backpressure-event rate (queue health), replication-stage p50/p99 drift,
  checkpoint duration as history grows, and S3 object count vs. GC reclamation
  (the checkpoint GC is new and under-exercised on long histories).
- Restore-from-scratch at the end: mount a fresh node against the months-of-
  history bucket and confirm restore time and correctness haven't degraded.

Exit criteria: flat memory and goroutine profiles over the full run, GC keeping
object count bounded, checkpoint and restore times stable, zero consistency
violations.

## Track C — Real provider coverage

Cloudflare R2 coverage started 2026-09-02 against a bucket ~13 ms from the
bench host (~135 Mbps up). Done so far: `VerifyConditionalWrites` passes on R2
(`If-Match`/`If-None-Match` semantics hold — the fence works), and the full
four-test e2e suite passes against R2, including both takeover paths and the
mount race. The e2e suite's S3 target is now configurable
(`SATCHEL_E2E_S3_BUCKET`/`REGION`/`ACCESS_KEY`/`SECRET_KEY`). R2's higher
latency immediately earned its keep: it widened a race that exposed the
unstored-manifest bug above, and exposed a test brittleness (the operator-break
e2e now quiesces the holder's trailing checkpoint before breaking, matching the
documented operator procedure). A fault campaign and a remote-durability
pgbench comparison (same host, MinIO vs R2) run with the fixed binary.

- Garage (v2.2.0) covered 2026-09-03: it accepts and ignores `If-Match` and
  `If-None-Match`, so the conditional probe correctly refuses it. The new
  append head (`--s3-head=append`, `internal/replica/head.go`) publishes the
  state document as an append-only version log with a settle-window takeover
  and a list-after-write fence; the full e2e suite runs against a local Garage
  with `SATCHEL_E2E_S3_HEAD=append`.
- Still to run: real AWS S3 (same region as compute) with the same battery, to
  shake out a third implementation's conditional-write and latency behavior.
- Confirm `If-Match`/`If-None-Match` behave identically — the fence depends on
  it, and providers vary in strong-consistency and conditional-write guarantees.
- Record p50/p95/**p99** commit latency per provider; size the product against
  p99, since the tail (currently 396 ms at 32 clients on loopback) is what an
  interactive workload feels.

Exit criteria: a published latency table per provider, conditional-write
semantics verified on each, and a fault campaign passed against real S3.

## Track D — Resilience changes

Testing is already pointing at code changes, not just documentation.

- **Soften S3-outage coupling — done.** A remote flush now stalls and retries
  through transient publish failures instead of returning `EIO` on the first
  one, so an S3 blip shorter than the lease TTL stalls the database rather than
  crashing it (`publishWithRetry` in `internal/replica/syncer.go`, with unit
  coverage in `TestRemoteFlushStallsThroughTransientPublishFailures` and
  `TestRemoteFlushFailsImmediatelyOnLeaseTakeover`). The lease TTL
  (`SATCHEL_LEASE_TTL`) is the hard backstop and is already configurable. The
  split-brain guarantee is intact: publication only acknowledges through the
  conditional state update, and a real takeover still fences immediately.
- **Lost-ack idempotency on the state PUT — done.** A state update that commits
  but loses its acknowledgement (connection dies mid-response) used to
  false-fence on retry, because the rebuilt body carried a fresh manifest
  timestamp and renewed lease expiry and no longer matched the committed bytes.
  The lease now remembers the exact marshaled body of an ambiguously-failed
  commit; every later commit attempt first fetches `state.json`, and if the
  current bytes equal the remembered attempt, adopts it as committed instead of
  rebuilding (`internal/replica/remote.go`). This covers publish, checkpoint,
  renew, and release. A genuine takeover still surfaces `ErrLeaseLost`
  unchanged, covered by `TestPublishRetryAdoptsCommitAfterLostAck`,
  `TestPublishRetryAfterLostAckDetectsTakeover`, and
  `TestRemoteFlushRecoversFromLostPublishAck`, all of which fail against the
  pre-fix code.
- **Performance across real storage.** The current numbers come from one slow
  SSD-class virtual disk (native one-client 55 TPS). Characterize the local
  block path on genuine NVMe to separate Satchel overhead from disk, and confirm
  the serial NBD path is still the right call there.
- **Format versioning.** Before external users store months of data, pin an
  on-disk/on-S3 format version with an explicit forward/backward-compatibility
  and upgrade procedure, so a Satchel upgrade can't strand an existing bucket.

Exit criteria: outage coupling has a tested mitigation with the split-brain
guarantee intact, an NVMe performance baseline exists, and the format carries a
version with a written upgrade path.

## Track E — Operational readiness

A backend for irreplaceable data is only as good as the runbook around it.

- **Monitoring thresholds.** Turn the existing metrics into alerts with real
  numbers: unpublished-bytes approaching the dirty limit, backpressure events,
  lease-fenced events, replication p99 regression, restore duration.
- **Disaster-recovery drills.** Documented, rehearsed procedures for: total node
  loss with takeover, restore-to-point-in-time from PITR history, and recovery
  from a corrupted or partially-deleted bucket.
- **Credential rotation** for S3 access without downtime, and a tested GC
  schedule that reclaims storage without racing a live writer.
- **Runbooks** for the failure modes the campaign exercises, especially the
  S3-outage/self-fence behavior: what an operator sees, what to check, and when
  to break a lease.

Exit criteria: alerts defined with thresholds, DR drills rehearsed on the bench
host, and a runbook per known failure mode.

## Deployment gates

The tracks map directly onto how far you can trust the backend:

- **Development and staging** — ready now.
- **Non-critical canary with tested independent backups** — ready now.
- **Production with independent backups and a deliberately limited canary** —
  after Track A (multi-day, extended fault points, two-node contention) and the
  Track D outage-coupling decision.
- **Sole copy of production data** — not until Tracks A through E are all green
  against a real provider, and specifically not while an S3 outage can crash the
  writer without an operator understanding and accepting that coupling.

## Running the harness

The fault campaign runs on a dedicated bench host (Linux with the `nbd` module,
PostgreSQL client tools, and a local or remote S3 endpoint):

```sh
SATCHEL_BIN=./satchel ./test/fault/crashloop.sh 20
```

It defaults to loopback MinIO managed by systemd (`SATCHEL_FAULT_S3_STOP` /
`SATCHEL_FAULT_S3_START` control the outage); point `SATCHEL_S3_ENDPOINT` at a
real provider for Track C. Results, logs, and any preserved failure artifacts
land under `/var/lib/satchel-fault/<run-id>/`.
