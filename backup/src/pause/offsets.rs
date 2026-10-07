use std::collections::HashMap;

use anyhow::{Context, Result, bail};

use super::api::PipelineOffsets;
use crate::{
    clickhouse::ClickHouse,
    connect::{Connect, validate_offsets},
    model::{KafkaOffset, KafkaOffsetValue, KafkaPartition, KeeperRow, Pipeline},
};

pub(super) async fn read_offsets(
    connect: &Connect,
    clickhouse: &ClickHouse,
    pipeline: &Pipeline,
) -> Result<PipelineOffsets> {
    let connect_offsets = connect.offsets(&pipeline.connector).await?;
    let keeper_rows = clickhouse
        .keeper_rows(&pipeline.database, &pipeline.state_table)
        .await
        .with_context(|| format!("failed to read KeeperMap state: {}", pipeline.connector))?;
    let offsets = exact_offsets(pipeline, &connect_offsets, &keeper_rows)
        .with_context(|| format!("inconsistent offsets: {}", pipeline.connector))?;
    Ok(PipelineOffsets {
        connector: pipeline.connector.clone(),
        offsets,
        connect_offsets,
        keeper_rows,
    })
}

/// Derives exact offsets from KeeperMap and cross-checks them against Connect's offsets.
fn exact_offsets(
    pipeline: &Pipeline,
    observed: &[KafkaOffset],
    rows: &[KeeperRow],
) -> Result<Vec<KafkaOffset>> {
    validate_offsets(observed)?;
    let prefix = format!("{}-", pipeline.topic);
    if rows.iter().any(|row| !row.key.starts_with(&prefix)) {
        bail!("KeeperMap state contains an unexpected topic")
    }
    if rows.iter().any(|row| row.min_offset > row.max_offset) {
        bail!("KeeperMap state contains an invalid offset range")
    }
    if rows.iter().any(|row| row.max_offset > i64::MAX as u64) {
        bail!("KeeperMap state exceeds the connector's signed offset range")
    }
    let relevant = rows
        .iter()
        .filter_map(|row| {
            row.key
                .strip_prefix(&prefix)
                .map(|partition| (partition, row))
        })
        .map(|(partition, row)| {
            let partition = partition
                .parse::<u32>()
                .context("KeeperMap state key has an invalid partition")?;
            if partition >= pipeline.partitions {
                bail!("KeeperMap state contains an out-of-range partition")
            }
            if row.state != "AFTER_PROCESSING" {
                bail!("KeeperMap state is not safely committed: {}", row.key)
            }
            Ok((partition, row))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    if relevant.len()
        != rows
            .iter()
            .filter(|row| row.key.starts_with(&prefix))
            .count()
    {
        bail!("KeeperMap state contains duplicate partitions")
    }

    let mut offsets = Vec::with_capacity(pipeline.partitions as usize);
    for partition in 0..pipeline.partitions {
        let exact = relevant
            .get(&partition)
            .map(|row| {
                row.max_offset
                    .checked_add(1)
                    .context("KeeperMap offset overflow")
            })
            .transpose()?
            .unwrap_or(0);
        if let Some(committed) = observed.iter().find(|offset| {
            offset.partition.kafka_topic == pipeline.topic
                && offset.partition.kafka_partition == partition
        }) && committed.offset.kafka_offset > exact
        {
            bail!(
                "Kafka Connect offset is ahead of ClickHouse KeeperMap state: {}-{partition}",
                pipeline.topic
            )
        }
        offsets.push(KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: pipeline.topic.clone(),
                kafka_partition: partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: exact,
            },
        });
    }
    if observed.iter().any(|offset| {
        offset.partition.kafka_topic != pipeline.topic
            || offset.partition.kafka_partition >= pipeline.partitions
    }) {
        bail!("connector returned an unexpected topic partition")
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn pipeline() -> Pipeline {
        Pipeline {
            connector: "records".to_owned(),
            database: "records".to_owned(),
            state_table: "records_state".to_owned(),
            keeper_path: "/durable-clickhouse-sink/default/records".to_owned(),
            table: "records".to_owned(),
            topic: "records.input".to_owned(),
            partitions: 3,
        }
    }

    fn offset(partition: u32, value: u64) -> KafkaOffset {
        KafkaOffset {
            partition: KafkaPartition {
                kafka_topic: "records.input".to_owned(),
                kafka_partition: partition,
            },
            offset: KafkaOffsetValue {
                kafka_offset: value,
            },
        }
    }

    fn row(partition: u32, max: u64, state: &str) -> KeeperRow {
        KeeperRow {
            key: format!("records.input-{partition}"),
            min_offset: max,
            max_offset: max,
            state: state.to_owned(),
        }
    }

    #[test]
    fn keeper_offsets_are_exact_even_when_connect_lags() {
        let offsets = exact_offsets(
            &pipeline(),
            &[offset(0, 8)],
            &[row(0, 9, "AFTER_PROCESSING")],
        )
        .unwrap();
        assert_eq!(offsets, vec![offset(0, 10), offset(1, 0), offset(2, 0)]);
    }

    #[test]
    fn connect_may_lag_keeper_by_any_amount_but_never_lead() {
        for max_offset in [0, 1, 2, 31, 1024, u32::MAX as u64] {
            let exact = max_offset + 1;
            for observed in [0, 1, exact / 2, exact] {
                let offsets = exact_offsets(
                    &pipeline(),
                    &[offset(0, observed)],
                    &[row(0, max_offset, "AFTER_PROCESSING")],
                )
                .unwrap();
                assert_eq!(offsets[0], offset(0, exact));
            }
            assert!(
                exact_offsets(
                    &pipeline(),
                    &[offset(0, exact + 1)],
                    &[row(0, max_offset, "AFTER_PROCESSING")],
                )
                .is_err()
            );
        }
    }

    #[test]
    fn partitions_are_derived_independently_from_unordered_state() {
        let offsets = exact_offsets(
            &pipeline(),
            &[offset(2, 90), offset(0, 10)],
            &[
                row(2, 99, "AFTER_PROCESSING"),
                row(0, 10, "AFTER_PROCESSING"),
            ],
        )
        .unwrap();
        assert_eq!(offsets, vec![offset(0, 11), offset(1, 0), offset(2, 100)]);
    }

    #[test]
    fn rejects_connect_ahead_of_clickhouse() {
        let error = exact_offsets(
            &pipeline(),
            &[offset(0, 11)],
            &[row(0, 9, "AFTER_PROCESSING")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("ahead"));
    }

    #[test]
    fn rejects_unfinished_keeper_state() {
        let error = exact_offsets(
            &pipeline(),
            &[offset(0, 9)],
            &[row(0, 9, "BEFORE_PROCESSING")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("not safely committed"));
    }

    #[test]
    fn rejects_out_of_range_and_duplicate_keeper_partitions() {
        assert!(exact_offsets(&pipeline(), &[], &[row(3, 9, "AFTER_PROCESSING")]).is_err());
        assert!(
            exact_offsets(
                &pipeline(),
                &[],
                &[
                    row(0, 9, "AFTER_PROCESSING"),
                    row(0, 10, "AFTER_PROCESSING")
                ]
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_keeper_rows_for_another_topic() {
        let mut unexpected = row(0, 9, "AFTER_PROCESSING");
        unexpected.key = "other-0".to_owned();
        assert!(exact_offsets(&pipeline(), &[], &[unexpected]).is_err());
    }

    #[test]
    fn rejects_unexpected_connect_topic_and_offset_overflow() {
        let mut unexpected = offset(0, 1);
        unexpected.partition.kafka_topic = "other".to_owned();
        assert!(exact_offsets(&pipeline(), &[unexpected], &[]).is_err());
        assert!(exact_offsets(&pipeline(), &[offset(3, 1)], &[]).is_err());
        assert!(exact_offsets(&pipeline(), &[], &[row(0, u64::MAX, "AFTER_PROCESSING")]).is_err());
    }

    #[test]
    fn rejects_invalid_keeper_offset_range() {
        let mut invalid = row(0, 9, "AFTER_PROCESSING");
        invalid.min_offset = 10;
        assert!(exact_offsets(&pipeline(), &[], &[invalid]).is_err());
    }
}
