# Test plan

## Fast checks

```bash
nix flake check
```

The flake builds the Kotlin application, runs its stateful topology tests,
packages and strictly lints the Helm chart, checks negative safety validation,
and renders the chart through Nixidy.

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
5. a direct-S3 backup is visible to an independent ClickHouse server;
6. the database and KeeperMap checkpoint restore successfully;
7. preserving the Kafka Connect identity during cutover delivers later records
   exactly once to the restored target;
8. the former target stops advancing after cutover.

MinIO is test-only. Production uses an externally managed S3-compatible store,
such as Ceph RGW, through the same ClickHouse named-collection interface.

## Production backup drill

Schedule a restore drill at least as often as the recovery objective demands.
Use a clean ClickHouse and isolated Keeper, validate the newest and an older
retained backup, measure restore duration, and publish a canary after an
offset-preserving cutover. Alert on failed or missing CronJobs, backup age,
canonical-topic retention headroom, Kafka consumer lag, connector task state,
and conflict-topic growth.
