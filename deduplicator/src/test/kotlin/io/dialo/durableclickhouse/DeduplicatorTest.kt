package io.dialo.durableclickhouse

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.Properties

object DeduplicatorTest {
    private val retention = Duration.ofDays(30)
    private val config = PipelineConfig("raw", "canonical", "conflicts", retention)

    @JvmStatic
    fun main(args: Array<String>) {
        val stateDirectory = Path.of(args.single())
        driver(stateDirectory).use { driver ->
            val input = input(driver)
            val canonical = output(driver, config.outputTopic)
            val conflicts = output(driver, config.conflictTopic)

            input.pipeInput("event-1", "first".encodeToByteArray(), Instant.EPOCH)
            input.pipeInput("event-1", "first".encodeToByteArray(), Instant.EPOCH)
            check(canonical.readValuesToList().map(::String) == listOf("first"))
            check(conflicts.isEmpty)

            input.pipeInput("event-1", "changed".encodeToByteArray(), Instant.EPOCH)
            val conflict = conflicts.readRecord()
            check(String(conflict.value) == "changed")
            check(conflict.headers.lastHeader(Deduplicator.ERROR_HEADER).text() == "record-id-content-mismatch")

            input.pipeInput(null, "missing".encodeToByteArray(), Instant.EPOCH)
            check(conflicts.readRecord().headers.lastHeader(Deduplicator.ERROR_HEADER).text() == "missing-record-id")

            input.pipeInput("event-1", "first".encodeToByteArray(), Instant.EPOCH.plus(retention).plusMillis(1))
            check(canonical.readValue().contentEquals("first".encodeToByteArray()))
        }
    }

    private fun driver(stateDirectory: Path) =
        TopologyTestDriver(
            Deduplicator.topology(config),
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplicator-test")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092")
                put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString())
            },
            Instant.EPOCH,
        )

    private fun input(driver: TopologyTestDriver): TestInputTopic<String, ByteArray> =
        driver.createInputTopic(config.inputTopic, StringSerializer(), org.apache.kafka.common.serialization.ByteArraySerializer())

    private fun output(driver: TopologyTestDriver, topic: String): TestOutputTopic<String, ByteArray> =
        driver.createOutputTopic(topic, StringDeserializer(), ByteArrayDeserializer())

    private fun Header?.text() = requireNotNull(this).value().decodeToString()
}
