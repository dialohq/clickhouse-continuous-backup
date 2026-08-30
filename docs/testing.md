# Test plan

## Fast checks

```bash
nix flake check
```

The flake compiles the Rust backup utility, runs its invariant
tests, packages and strictly lints the Helm chart, checks negative safety
validation, checks the E2E program, and renders the chart through Nixidy.

The Rust suite covers physical-table snapshot grouping; full/incremental chain
planning and rollover; exact KeeperMap-derived offsets; Connect-ahead,
unfinished, duplicate, out-of-range,
overflow, and wrong-topic states; manifest tampering; independent unordered
partitions; Kafka truncation, topic rollback, and partition drift; backup
catalog partition invariants; chain-scope mismatch; and MergeTree engine and
deduplication-setting validation.
The recovery tests additionally cover immutable CRD generation; complete
exclusive target vectors; missing, duplicate, early, future, and unrelated
partitions; source-retention and log-truncation boundaries; complete bounded
topic retention; stable recovery identities; Kafka Connect's injected name;
monotonic live KeeperMap progress; safe ClickHouse identifiers; and
exclusive-offset KeeperMap encoding.

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

The isolated test installs Redpanda, ClickHouse/Keeper, MinIO, and this chart.
It verifies:

1. three hard Connect failures during ingestion lose and duplicate no records;
2. total-row and unique-ID counts remain equal;
3. an operator-paused connector makes backup fail and remains paused;
4. bad backup credentials fail without leaving the connector paused or
   creating a backup;
5. one full plus two incrementals are created, then the configured limit rolls
   over to a new full;
6. ingestion resumes while the full archive is still uploading, and records
   written after resume are absent from that immutable snapshot;
7. the base archive can be restored into an isolated probe table with the
   expected row count;
8. the compacted backup manifest contains exact KeeperMap-derived offsets and
   chain dependencies;
9. declarative bounded PITR restores a table and transactionally replays to an
    exact multi-partition vector, including a forced controller restart;
10. malformed and beyond-log-end vectors fail before changing their empty
    destinations;
11. an omitted vector starts isolated live-follow connectors and ingests new
    records once, including reconciliation of a pre-created stopped connector
    whose offset patch was interrupted before KeeperMap hydration; an explicit
    recovery leaves its follow connector stopped at the requested point;
12. a non-empty destination whose restore acknowledgement was lost fails closed
    and remains untouched.

MinIO is test-only. Production uses an externally managed S3-compatible store,
such as Ceph RGW, through the same ClickHouse named-collection interface.

## Production archive validation

Regularly restore both the newest and an older retained backup into temporary,
isolated ClickHouse tables. Measure duration and validate row counts,
aggregates, the manifest offsets, and the incremental dependency chain. Alert
on failed or missing backup Jobs, backup age, input-topic retention headroom,
consumer lag, and connector task state.
