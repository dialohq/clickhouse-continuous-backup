# Pause server

The pause server pauses Kafka Connect ingestion on request, returns the exact
input offsets at which it stopped, and resumes it again. The backup Job uses it
for its short per-table barriers; other tools can use it the same way.

It runs as `durable-clickhouse-backup pause-server <config>`, deployed by the
chart as `<release>-durable-clickhouse-sink-pause-server`. Its replicas elect
one leader through ClickHouse Keeper, which also holds the tokens; the others
stand by and take over when the leader goes.

## API

All requests and responses are JSON. `/pause`, `/resume`, and `/renew` run one
at a time, and only on the leader.

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
| `500` | Keeper could not be read or written. |
| `502` | Kafka Connect or ClickHouse failed; anything this request paused is resumed best-effort. |
| `503` | This replica is not the leader, or stopped leading during the request. Retry. |

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

A token expires `ttlSeconds` after `/pause` or the last `/renew`. Once a
second, and when a replica starts leading, the leader sweeps: it resumes the
connectors of every expired token exactly like `/resume`, logs a warning, and
deletes the token. A connector still held by a token that hasn't expired stays
paused. If resuming fails, the token stays and the next sweep retries it.
Requests leave expired tokens to the sweep, except that `/renew` refuses one
with `404`. The backup client renews each of its tokens every
`ttlSeconds / 3` until it resumes it.

Expiry is what guarantees ingestion comes back when a caller cannot: a backup
killed by `SIGKILL`, node loss, a client that disconnects in the middle of
`/pause`, or a caller that never resumes. A caller whose work outlasts its
token without renewing loses the pause midway; the backup detects that through
its post-snapshot checks and fails instead of recording wrong offsets.

## Storage

Each token is a node in Keeper, `<keeperPath>/tokens/<token>`, holding its
connectors and expiry as JSON. The chart sets `keeperPath` to
`/durable-clickhouse-sink/pause-server/<stateNamespace>`. Nothing about tokens
lives in the replicas, so a new leader carries on with every token the previous
one handed out. The leader writes through its lock: once another replica leads,
a former leader's updates and deletes fail. Only creating a token can't be
guarded, because ClickHouse Keeper rejects the request that would need; a token
a former leader creates just expires.

## Leadership and routing

Every replica contends for a lock under `<keeperPath>/leader`, using Apache
Curator's leader latch: each creates an ephemeral sequential node, the lowest
leads, and every other waits on the one just before its own. A standby is ready
and answers `/health`, but refuses the other endpoints with `503`.

The leader labels its pod `durable-clickhouse.dialo.ai/pause-server-role:
active`, removes that label from the release's other pause server pods, and
repeats this every 10 seconds. The Service selects on the label, so it routes
only to the leader. The chart creates a ServiceAccount, Role, and RoleBinding
that allow the pause server to get, list, and patch pods in its namespace.

Failover:

- On `SIGTERM`, e.g. during a rollout, the leader deletes its lock node and a
  standby takes over at once.
- If the leader dies, Keeper deletes its node once its session times out (about
  10 seconds with ClickHouse Keeper's default minimum).
- A leader that loses its Keeper connection stops serving immediately, since
  another replica may lead before it learns that its session expired. It leads
  again if the connection returns within the session, and stands by again
  otherwise.

The backup client retries `503`s and connection failures for 30 seconds, which
covers a failover. Keep `ttlSeconds` comfortably longer than that, so tokens
survive it.

## Configuration

Chart values under `pauseServer`:

| Value | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Required when `backup.enabled` is true. |
| `replicas` | `2` | One leads, the rest stand by. |
| `keeperAddress` | | Required. ClickHouse Keeper or ZooKeeper, e.g. `clickhouse-keeper:9181`; comma-separate several servers. |
| `port` | `8080` | Listen port and Service port. |
| `ttlSeconds` | `30` | Token lifetime without renewal. |
| `pauseTimeoutSeconds` | `120` | How long the server waits for connectors to pause or resume. |
| `logLevel` | `off,durable_clickhouse_backup=info` | `RUST_LOG` for the pods. |
| `serviceAccountName` | | Existing account to run as instead of the one the chart creates; the chart still binds the pod permissions to it. |
| `credentialsFile` / `credentialsSecret` | | ClickHouse credentials; exactly one while enabled. See [authentication](authentication.md). |

The backup's client gives up on a request after `backup.pauseTimeoutSeconds +
timeouts.connectRequestSeconds`. Keep `backup.pauseTimeoutSeconds` at least
`pauseServer.pauseTimeoutSeconds`, so the client does not give up while the
server is still waiting for connectors.

The server has no authentication, and neither do its Keeper nodes: anything
that can reach its Service can pause ingestion, and anything that can reach
Keeper can change tokens. Restrict access with the cluster's network policies
if needed.
