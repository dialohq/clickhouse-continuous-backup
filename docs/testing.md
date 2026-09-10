# Test plan

## Fast checks

```bash
nix flake check
```

The flake compiles the Rust backup utility, runs its invariant
tests, packages and strictly lints the Helm chart, checks negative safety
validation, compiles the Rust E2E tests, and renders the chart through Nixidy.

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

## Rust tests

From the development shell, run the E2E suite with up to five concurrent tests:

```bash
cargo test --manifest-path backup/Cargo.toml --test e2e -- --test-threads=5 --nocapture
```

The scenarios and environment helpers live in `backup/tests/e2e/` and compile
into one test binary. Each service scenario starts its own dnvr environment with a
separate state directory and ports. There are six service scenarios; the command
above runs at most five environments concurrently. It requires a dnvr version supporting
state-preserving restarts.

Concurrency also depends on host resources. Each Redpanda instance uses Linux
AIO capacity shared with other brokers on the host. If its log reports minimum
AIO requirements are not met, reduce `--test-threads` or increase the host's
`fs.aio-max-nr` limit according to that error.

The dnvr suite covers ingestion, crash recovery, paused-connector and
invalid-credential refusals, missing-parent manifest rejection after catalog prefix
deletion, and the full/incremental backup sequence. That sequence checks ingestion
during upload, restores the immutable base
snapshot, verifies exact checkpoint offsets and the committed Kafka catalog
manifest, and checks rollover to a new full backup after two incrementals.
Dependent backup steps share one environment and run in order within one test;
the tests themselves can run concurrently. Kafka administration, producing,
and catalog reads use the Rust client, and backups call the Rust library directly.

`--nocapture` shows each test's tmux attach command and log directory immediately.
Run the printed command in another terminal to watch that environment while the
test runs. Each operation prints its elapsed time; a per-test total and breakdown
print when the test ends, including on error. Service waits run concurrently and
their durations overlap, so adding individual timings does not give the total.
Startup timeout errors include the last 40 lines of the relevant process log.
During startup the backend also polls dnvr's process API every 200 ms. If a
server exits, or a setup job exits unsuccessfully, startup fails immediately with
the process name, exit code, and log tail instead of waiting the full 120 seconds.
Successful one-shot setup jobs are allowed to exit. Process API requests have a
two-second timeout; API errors fail startup rather than silently disabling this check.

All scenarios preserve logs on failure (including startup errors and assertion
panics) under `backup/target/e2e-logs/<test>-<unique-id>/`. The failure output prints
that directory; it contains `logs/tmux-default-up/*.log` and `timings.txt`.
Successful tests remove their logs. Services are stopped and temporary service
data is removed in either case. A hard kill can bypass cleanup and timing output,
but logs already written to the persistent directory remain there.

To run only unit tests, or both unit tests and E2E tests:

```bash
cargo test --manifest-path backup/Cargo.toml --lib
cargo test --manifest-path backup/Cargo.toml -- --test-threads=5
```

The Nix package check runs unit and binary tests only; E2E tests run separately
in the development shell.

MinIO is test-only. Production uses an externally managed S3-compatible store,
such as Ceph RGW, through the same ClickHouse named-collection interface.

## Production archive validation

Regularly restore both the newest and an older retained backup into temporary,
isolated ClickHouse tables. Measure duration and validate row counts,
aggregates, the manifest offsets, and the incremental dependency chain. Alert
on failed or missing backup Jobs, backup age, input-topic retention headroom,
consumer lag, and connector task state.
