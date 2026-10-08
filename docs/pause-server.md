# Pause server

The pause server pauses Kafka Connect ingestion on request, returns the exact
input offsets at which it stopped, and resumes it again. The backup Job uses it
for its short per-table barriers; other tools can use it the same way.

It runs as `durable-clickhouse-backup pause-server <config>`, deployed by the
chart as `<release>-durable-clickhouse-sink-pause-server`.

## API

All requests and responses are JSON. `/pause`, `/resume`, and `/renew` run one
at a time.

| Endpoint | Body | Result |
| --- | --- | --- |
| `POST /pause` | exactly one of `{"topic": ...}`, `{"connector": ...}`, `{"table": ...}` | `200` with `{"token", "ttl_seconds", "watermark"}` |
| `POST /resume` | `{"token": ...}` | `200` once the token's connectors are `RUNNING` again |
| `POST /renew` | `{"token": ...}` | `200`; the token lasts another `ttl_seconds` |
| `GET /health` | | `200` |

`watermark` has one entry per matched pipeline: `connector`, the exact
`offsets` derived from its KeeperMap rows, and for reference the
`connect_offsets` and `keeper_rows` they came from. The exact offset of
partition `p` is `KeeperMap["<topic>-<p>"].maxOffset + 1`, or zero without a
row. Unfinished, duplicated, or out-of-range rows and Connect offsets ahead of
KeeperMap fail the request.

Error statuses:

| Status | Meaning |
| --- | --- |
| `404` | No pipeline matches, or the token is unknown (already resumed or expired). |
| `409` | A connector or task is `FAILED`, or a connector is not running and was not paused by this server. |
| `422`/`400` | The body has an unknown key, more than one key, or the wrong type. |
| `500` | The tokens file could not be written. |
| `502` | Kafka Connect or ClickHouse failed; anything this request paused is resumed best-effort. |

## Tokens

Every successful `/pause` returns a new token that holds the matched
connectors paused. Several tokens may hold the same connector; it is resumed
only when the last of them is released. A connector that someone paused
outside the server is never taken over: `/pause` refuses it with `409`, so a
later resume cannot undo another operator's decision.

`/resume` resumes the connectors that no other token holds, waits for them to
be `RUNNING`, and only then removes the token. A failed resume keeps the token,
so it can be retried.

## Expiry

A token expires `ttlSeconds` after `/pause` or the last `/renew`. On expiry the
server resumes its connectors exactly like `/resume` and logs a warning. The
backup client renews each of its tokens every `ttlSeconds / 3` until it
resumes it.

Expiry is what guarantees ingestion comes back when a caller cannot: a backup
killed by `SIGKILL`, node loss, a client that disconnects in the middle of
`/pause`, or a caller that never resumes. A caller whose work outlasts its
token without renewing loses the pause midway; the backup detects that through
its post-snapshot checks and fails instead of recording wrong offsets.

## Persistence and replicas

Tokens and their expiry times are written atomically to a JSON file
(`pausesFile`) before any connector is touched. On startup the server reloads
the file and expires tokens whose time has passed. The chart keeps the file on
a `ReadWriteOnce` PersistentVolumeClaim, so a rescheduled pod still resumes
pauses that the previous pod handed out.

The server takes an exclusive lock on `<pausesFile>.lock` at startup and exits
if another process holds it. Run exactly one replica; the chart's Deployment
uses `strategy: Recreate` so the old pod releases the volume and the lock
before the new one starts.

## Configuration

Chart values under `pauseServer`:

| Value | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Required when `backup.enabled` is true. |
| `port` | `8080` | Listen port and Service port. |
| `ttlSeconds` | `30` | Token lifetime without renewal. |
| `pauseTimeoutSeconds` | `120` | How long the server waits for connectors to pause or resume. |
| `logLevel` | `off,durable_clickhouse_backup=info` | `RUST_LOG` for the pod. |
| `credentialsFile` / `credentialsSecret` | | ClickHouse credentials; exactly one while enabled. See [authentication](authentication.md). |
| `persistence.size` / `persistence.storageClass` | `64Mi` / cluster default | Volume for the tokens file. |

The backup's client gives up on a request after `backup.pauseTimeoutSeconds +
timeouts.connectRequestSeconds`. Keep `backup.pauseTimeoutSeconds` at least
`pauseServer.pauseTimeoutSeconds`, so the client does not give up while the
server is still waiting for connectors.

The server has no authentication: anything that can reach its Service can
pause ingestion. Restrict access with the cluster's network policies if
needed.
