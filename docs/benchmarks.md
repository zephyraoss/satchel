# Benchmarks

The SQLite and Litestream benchmark results were removed with that backend. Run the current fio suite against a real NBD mount and S3 endpoint:

```sh
sudo modprobe nbd nbds_max=16
./test/bench/run.sh
```

The script defaults to `SATCHEL_DURABILITY=async` so it measures the block path rather than S3 request latency. Run it again with `SATCHEL_DURABILITY=remote` to measure the durability mode used by default for database volumes.

Record the durability mode, local disk type, S3 implementation, network placement, sync interval, dirty limit, kernel version, and volume allocation when publishing results. The benchmark script covers sequential writes, random 4 KiB writes, mixed random I/O, reads, and small-file creation.

Useful comparisons are application throughput, bytes uploaded per logical byte written, peak unpublished bytes, final-sync time, checkpoint time, and restore time.
