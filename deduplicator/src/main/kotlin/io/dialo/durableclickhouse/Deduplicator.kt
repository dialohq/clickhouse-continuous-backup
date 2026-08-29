package io.dialo.durableclickhouse

import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.WindowStore
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant

object Deduplicator {
    const val STORE = "record-ids"
    const val SOURCE = "raw-source"
    const val PROCESSOR = "deduplicate"
    const val CANONICAL = "canonical-sink"
    const val CONFLICT = "conflict-sink"
    const val ERROR_HEADER = "durable-clickhouse-error"

    fun topology(config: PipelineConfig): Topology =
        Topology().apply {
            addSource(
                SOURCE,
                StringDeserializer(),
                ByteArrayDeserializer(),
                config.inputTopic,
            )
            addProcessor(
                PROCESSOR,
                ProcessorSupplier { ProcessorImpl(config.retention) },
                SOURCE,
            )
            addStateStore(
                Stores.windowStoreBuilder(
                    Stores.persistentWindowStore(
                        STORE,
                        config.retention,
                        config.retention,
                        false,
                    ),
                    Serdes.String(),
                    Serdes.ByteArray(),
                ),
                PROCESSOR,
            )
            addSink(
                CANONICAL,
                config.outputTopic,
                StringSerializer(),
                ByteArraySerializer(),
                PROCESSOR,
            )
            addSink(
                CONFLICT,
                config.conflictTopic,
                StringSerializer(),
                ByteArraySerializer(),
                PROCESSOR,
            )
        }

    private class ProcessorImpl(
        private val retention: Duration,
    ) : Processor<String, ByteArray, String, ByteArray> {
        private lateinit var context: ProcessorContext<String, ByteArray>
        private lateinit var seen: WindowStore<String, ByteArray>

        override fun init(context: ProcessorContext<String, ByteArray>) {
            this.context = context
            seen = context.getStateStore(STORE)
        }

        override fun process(record: Record<String, ByteArray>) {
            val key = record.key()
            if (key.isNullOrBlank()) return conflict(record, "missing-record-id")
            val value = record.value() ?: return conflict(record, "null-value")

            val timestamp = record.timestamp()
            val fingerprint = MessageDigest.getInstance("SHA-256").digest(value)
            val previous = seen.fetch(
                key,
                Instant.ofEpochMilli((timestamp - retention.toMillis()).coerceAtLeast(0)),
                Instant.ofEpochMilli(timestamp),
            ).use { values -> if (values.hasNext()) values.next().value else null }

            when {
                previous == null -> {
                    seen.put(key, fingerprint, timestamp)
                    context.forward(record, CANONICAL)
                }
                previous.contentEquals(fingerprint) -> Unit
                else -> conflict(record, "record-id-content-mismatch")
            }
        }

        private fun conflict(record: Record<String, ByteArray>, reason: String) {
            val headers = RecordHeaders(record.headers()).add(RecordHeader(ERROR_HEADER, reason.encodeToByteArray()))
            context.forward(record.withHeaders(headers), CONFLICT)
        }
    }
}
