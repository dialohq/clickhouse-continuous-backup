# Schema contract

The chart is schema-tool-neutral. It does not create target tables, install a
schema registry, prescribe columns, or own schema migrations.

The configured Kafka Connect converter interprets each serialized value. JSON
without an embedded Connect schema is the default. Avro, Protobuf, and JSON
Schema are supported by supplying matching converters and configuration in a
custom Connect image.

The target ClickHouse table must exist before connector registration and its
columns must be compatible with the converter output. Table engines must also
satisfy the delivery invariants documented in
[Guarantees](guarantees.md). Beyond those requirements, record shape, keys,
partitioning, ordering keys, codecs, TTLs, and secondary indexes belong to the
chart user.

Schema compatibility policy and subject naming belong to the selected registry.
Target ClickHouse evolution belongs to the caller's migration tool. Deploy a
backward-compatible target change before producing records that require it.
