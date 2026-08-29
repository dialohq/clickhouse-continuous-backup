# Schema and event contract

The chart is schema-tool-neutral. It does not create target tables, install a
schema registry, or own schema migrations.

## Record envelope

The Kafka key is the immutable event ID. The serialized value is passed through
the deduplicator without decoding and is interpreted only by the configured
Kafka Connect converter.

For durable append-only events, prefer a wide target table with stable IDs and
typed fields used by common filters. Mutable display names belong in reference
tables or dictionaries so they can be renamed without rewriting event history.
Source-specific or sparse attributes belong in a JSON column.

A typical logical shape is:

```text
id                         stable event ID
occurred_at                source event timestamp
ingested_at                ingestion timestamp
event_type                 stable type identifier
tenant_id                  stable tenant identifier
source_id                  stable source-system identifier
external_connection_id     source connection identifier
conversation_id            nullable stable conversation identifier
attempt_id                 nullable stable attempt identifier
voice_platform_id          nullable stable platform identifier
metadata                   source-specific JSON
```

Do not copy campaign, tenant, agent, or platform names into the durable fact row
when an ID can be joined to the current name.

## Serialization and schema registry

JSON without an embedded Connect schema is the default because it needs no
registry. Avro, Protobuf, and JSON Schema are supported by supplying the matching
Kafka Connect converter and its configuration in a custom Connect image. The
deduplicator remains independent of that choice because it compares serialized
bytes.

Schema compatibility policy and subject naming belong to the chosen registry.
Target ClickHouse evolution belongs to the caller's migration tool. Deploy a
backward-compatible target change before producing records that require it.

Changing serialization can make semantically identical values byte-different.
Use a new topic or complete the transition outside the active deduplication
horizon rather than silently switching encoding in place.
