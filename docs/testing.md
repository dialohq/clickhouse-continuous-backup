# Test plan

## Fast checks

```bash
nix flake check
```

The flake builds the Kotlin Kafka Streams application, runs its stateful
topology tests, compiles the Rust recovery utility, runs its focused invariant
tests, packages and strictly lints the Helm chart, checks negative safety
validation, and renders the chart through Nixidy.

The Rust backup suite exercises:

- full/incremental chain planning, rollover, disabled incrementals, and corrupt
  chain heads;
- exact KeeperMap-derived offsets when Kafka Connect lags;
- refusal of Connect-ahead, unfinished KeeperMap, duplicate, out-of-range,
  overflow, and wrong-topic states, plus zero for never-ingested partitions;
- manifest tampering across backup shape, connector identity, topic, partition,
  observed offsets, exact offsets, and checkpoint identity;
- exhaustive lag/equality/ahead boundaries across representative offsets,
  independent unordered partitions, self-referential chains, and generation
  overflow;
- MergeTree-family acceptance and refusal of append-only engines or a missing
  KeeperMap table when incrementals are enabled.

## RKE2 end to end

Run on a disposable Linux host with root access and enough disk for RKE2 and
container images:

```bash
sudo nix develop -c bash e2e/run-rke2.sh
```

On failure, retain the cluster for inspection:

```bash
sudo env DURABLE_SINK_E2E_KEEP_ON_FAILURE=true \
  nix develop -c bash e2e/run-rke2.sh
```

The test creates a dedicated RKE2 namespace and installs Redpanda, source and
restore ClickHouse/Keeper instances, and MinIO. It then verifies:

1. repeated byte-identical event IDs yield one ClickHouse row;
2. a reused ID with a different value is quarantined;
3. three hard failures of both managed processing layers do not lose or double
   records;
4. ClickHouse has equal total-row and unique-ID counts;
5. an operator-paused connector makes the backup fail and remains paused;
6. invalid ClickHouse backup credentials make the Job fail without leaving the
   connector paused or creating a backup;
7. one full plus two incrementals are created, followed by a new full at the
   configured limit;
8. the incremental event-data tip and independent full KeeperMap checkpoint are
   visible to and restore on a clean ClickHouse/Keeper server;
9. the compacted recovery record contains exact KeeperMap-derived offsets and
   the chain dependencies;
10. the recovery utility refuses to patch until restored KeeperMap equals the
    manifest; rejects offset `-1`/`+1`, duplicate partitions, a self-consistent
    but unrestored Keeper state, the wrong event backup, and a running
    connector without changing Connect offsets; then patches while stopped and
    verifies exact read-back;
11. records written after the recovery point first reach the original target,
    then replay exactly once into the restored target after cutover;
12. the former target stops advancing after cutover.

MinIO is test-only. Production uses an externally managed S3-compatible store,
such as Ceph RGW, through the same ClickHouse named-collection interface.

## Production backup drill

Schedule a restore drill at least as often as the recovery objective demands.
Use a clean ClickHouse and isolated Keeper, validate the newest and an older
retained backup, measure restore duration, and publish a canary after an
offset-preserving cutover. Alert on failed or missing CronJobs, backup age,
canonical-topic retention headroom, Kafka consumer lag, connector task state,
and conflict-topic growth.
