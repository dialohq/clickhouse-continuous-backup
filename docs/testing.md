# Test plan

## Fast checks

```bash
nix flake check
```

The flake compiles the Rust backup and recovery utility, runs its invariant
tests, packages and strictly lints the Helm chart, checks negative safety
validation, checks the E2E program, and renders the chart through Nixidy.

The Rust suite covers physical-table snapshot grouping; full/incremental chain
planning and rollover; exact KeeperMap-derived offsets; idempotent KeeperMap
rehydration; Connect-ahead, unfinished, duplicate, out-of-range,
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
6. ingestion resumes while the full archive is still uploading, and records
   written after resume are absent from that immutable recovery snapshot;
7. target data restores into a pre-created production-engine table and
   KeeperMap is rehydrated from the manifest on a clean Keeper instance;
8. the compacted recovery record contains exact KeeperMap-derived offsets and
   chain dependencies;
9. recovery rejects conflicting Keeper state, offset `-1`/`+1`, duplicate
   partitions, the wrong backup, and a running
   connector without changing offsets;
10. partial KeeperMap rehydration is completed and repeated recovery is
    idempotent;
11. a valid stopped recovery patches exact offsets and verifies their read-back;
12. post-snapshot records replay once into the restored target after cutover,
    while the former target stops advancing.

MinIO is test-only. Production uses an externally managed S3-compatible store,
such as Ceph RGW, through the same ClickHouse named-collection interface.

## Production backup drill

Regularly restore both the newest and an older retained backup into clean
ClickHouse and Keeper instances. Rehydrate KeeperMap from the selected
manifest, measure duration, validate exact offsets, and
publish a canary after an isolated cutover. Alert on failed or missing backup
Jobs, backup age, input-topic retention headroom, consumer lag, and connector
task state.
