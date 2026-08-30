# Test plan

## Fast checks

```bash
nix flake check
```

The flake compiles the Rust backup and recovery utility, runs its invariant
tests, packages and strictly lints the Helm chart, checks negative safety
validation, checks the E2E program, and renders the chart through Nixidy.

The Rust suite covers full/incremental chain planning and rollover; exact
KeeperMap-derived offsets; Connect-ahead, unfinished, duplicate, out-of-range,
overflow, and wrong-topic states; manifest tampering; independent unordered
partitions; Kafka truncation, topic rollback, and partition drift; recovery
catalog partition invariants; chain-scope mismatch; and MergeTree engine and
deduplication-setting validation.

## RKE2 end to end

Run on a disposable Linux host with root access:

```bash
sudo nix develop -c bash e2e/run-rke2.sh
```

To retain a failed cluster:

```bash
sudo env DURABLE_SINK_E2E_KEEP_ON_FAILURE=true \
  nix develop -c bash e2e/run-rke2.sh
```

The isolated test installs Redpanda, source and restore ClickHouse/Keeper
instances, MinIO, and this chart. It verifies:

1. three hard Connect failures during ingestion lose and duplicate no records;
2. total-row and unique-ID counts remain equal;
3. an operator-paused connector makes backup fail and remains paused;
4. bad backup credentials fail without leaving the connector paused or
   creating a backup;
5. one full plus two incrementals are created, then the configured limit rolls
   over to a new full;
6. target data and the independent full KeeperMap checkpoint restore on clean
   ClickHouse and Keeper instances;
7. the compacted recovery record contains exact KeeperMap-derived offsets and
   chain dependencies;
8. recovery rejects missing Keeper state, offset `-1`/`+1`, duplicate
   partitions, a restored-state mismatch, the wrong backup, and a running
   connector without changing offsets;
9. a valid stopped recovery patches exact offsets and verifies their read-back;
10. post-checkpoint records replay once into the restored target after cutover,
    while the former target stops advancing.

MinIO is test-only. Production uses an externally managed S3-compatible store,
such as Ceph RGW, through the same ClickHouse named-collection interface.

## Production backup drill

Regularly restore both the newest and an older retained backup into clean
ClickHouse and Keeper instances. Measure duration, validate exact offsets, and
publish a canary after an isolated cutover. Alert on failed or missing backup
Jobs, backup age, input-topic retention headroom, consumer lag, and connector
task state.
