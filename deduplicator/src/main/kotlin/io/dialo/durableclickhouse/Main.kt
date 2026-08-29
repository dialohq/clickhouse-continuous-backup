package io.dialo.durableclickhouse

import com.sun.net.httpserver.HttpServer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.Properties

data class PipelineConfig(
    val inputTopic: String,
    val outputTopic: String,
    val conflictTopic: String,
    val retention: Duration,
)

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val environment = System.getenv()
        val config = pipelineConfig(environment)
        val streams = KafkaStreams(Deduplicator.topology(config), streamsProperties(environment))
        streams.setUncaughtExceptionHandler {
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT
        }

        val health = healthServer(streams, environment.getOrDefault("HEALTH_PORT", "8080").toInt())
        Runtime.getRuntime().addShutdownHook(Thread {
            health.stop(0)
            streams.close(Duration.ofSeconds(30))
        })
        health.start()
        streams.start()
    }

    fun pipelineConfig(environment: Map<String, String>) =
        PipelineConfig(
            inputTopic = environment.required("INPUT_TOPIC"),
            outputTopic = environment.required("OUTPUT_TOPIC"),
            conflictTopic = environment.required("CONFLICT_TOPIC"),
            retention = Duration.ofMillis(environment.required("DEDUPLICATION_RETENTION_MS").toLong()),
        ).also { require(!it.retention.isZero && !it.retention.isNegative) }

    fun streamsProperties(environment: Map<String, String>) =
        Properties().apply {
            environment["KAFKA_PROPERTIES_FILE"]?.let { path ->
                Files.newBufferedReader(Path.of(path)).use(::load)
            }
            put(StreamsConfig.APPLICATION_ID_CONFIG, environment.required("APPLICATION_ID"))
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, environment.required("KAFKA_BOOTSTRAP_SERVERS"))
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
            put(StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed")
            put(StreamsConfig.STATE_DIR_CONFIG, environment.getOrDefault("STATE_DIR", "/var/lib/deduplicator"))
            put(StreamsConfig.REPLICATION_FACTOR_CONFIG, environment.getOrDefault("REPLICATION_FACTOR", "3"))
            put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, environment.getOrDefault("STANDBY_REPLICAS", "1"))
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, environment.getOrDefault("COMMIT_INTERVAL_MS", "100"))
        }

    private fun healthServer(
        streams: KafkaStreams,
        port: Int,
    ) = HttpServer.create(InetSocketAddress(port), 0).apply {
        createContext("/health") { exchange ->
            val healthy = streams.state() !in setOf(KafkaStreams.State.ERROR, KafkaStreams.State.NOT_RUNNING)
            exchange.sendResponseHeaders(if (healthy) 200 else 503, -1)
            exchange.close()
        }
        createContext("/ready") { exchange ->
            exchange.sendResponseHeaders(if (streams.state() == KafkaStreams.State.RUNNING) 200 else 503, -1)
            exchange.close()
        }
    }

    private fun Map<String, String>.required(name: String) =
        requireNotNull(this[name]?.takeUnless(String::isBlank)) { "$name is required" }
}
