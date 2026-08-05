package com.upbit.stream.history.integration

import com.upbit.stream.common.avro.CandleEventAvroMapper
import com.upbit.stream.common.event.CandleEvent
import com.upbit.stream.common.kafka.KafkaTopics
import io.apicurio.registry.resolver.config.SchemaResolverConfig
import io.apicurio.registry.serde.avro.AvroKafkaSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.servlet.client.RestTestClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.QuestDBContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration

/**
 * Stands in for stream-processor (which normally produces candle.1m) by Avro-encoding a
 * [CandleEvent] itself via Apicurio's serializer, then verifies [com.upbit.stream.history.kafka.CandleWriter]
 * writes it to QuestDB and [com.upbit.stream.history.api.CandleQueryController] can read it back —
 * also exercises [com.upbit.stream.history.questdb.QuestDbSchemaInitializer]'s DEDUP table creation
 * against a real QuestDB instance.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CandleHistoryIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:4.2.1"))

        @Container
        @JvmStatic
        val apicurio: GenericContainer<*> =
            GenericContainer(DockerImageName.parse("apicurio/apicurio-registry:3.3.1"))
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200))

        @Container
        @JvmStatic
        val questdb: QuestDBContainer = QuestDBContainer(DockerImageName.parse("questdb/questdb:9.4.3"))

        private fun registryUrl() = "http://${apicurio.host}:${apicurio.getMappedPort(8080)}/apis/registry/v3"

        @DynamicPropertySource
        @JvmStatic
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            // history-api's own consumer (CandleWriter) is Spring Boot's autoconfigured one,
            // reading the registry URL from spring.kafka.properties.apicurio.registry.url
            // (see application.yml) — unlike stream-processor/api-server, which read a
            // standalone apicurio.registry.url property in their own @Configuration classes.
            registry.add("spring.kafka.properties.apicurio.registry.url") { registryUrl() }
            registry.add("spring.datasource.url") { questdb.jdbcUrl }
            registry.add("spring.datasource.username") { questdb.username }
            registry.add("spring.datasource.password") { questdb.password }
            // QuestDBContainer.getIlpUrl() points at the legacy raw-TCP ILP port, but
            // QuestDbSenderConfig builds its Sender with Sender.Transport.HTTP (ILP-over-HTTP,
            // same port as the REST/web-console port, 9000) — matching docker-compose's own
            // QUESTDB_ILP_ADDRESS=questdb:9000. Using getIlpUrl() here fails with "Failed to
            // detect server line protocol version: peer disconnect" since HTTP framing sent to
            // the raw TCP ILP port isn't understood by that port.
            registry.add("questdb.ilp-address") { "${questdb.host}:${questdb.getMappedPort(9000)}" }
        }
    }

    @LocalServerPort
    private var port: Int = 0

    @Test
    fun `avro candle published to kafka lands in questdb and is queryable`() {
        val market = "KRW-XRP"
        val windowStart = 1_700_000_000_000L
        val windowEnd = windowStart + 60_000L
        val candle = CandleEvent(
            market = market,
            windowStartEpochMillis = windowStart,
            windowEndEpochMillis = windowEnd,
            open = 700.0,
            high = 720.0,
            low = 690.0,
            close = 710.0,
            volume = 12345.6,
        )

        val serializer = AvroKafkaSerializer<GenericRecord>().apply {
            configure(
                mapOf(
                    SchemaResolverConfig.REGISTRY_URL to registryUrl(),
                    SchemaResolverConfig.AUTO_REGISTER_ARTIFACT to true,
                ),
                false,
            )
        }

        KafkaProducer<String, GenericRecord>(
            mapOf(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers),
            StringSerializer(),
            serializer,
        ).use { producer ->
            producer.send(
                ProducerRecord(KafkaTopics.CANDLE_1M, market, CandleEventAvroMapper.toGenericRecord(candle)),
            ).get()
        }

        // Fetched as a raw JSON string (rather than deserialized into CandleEvent) since this
        // module has no jackson-module-kotlin dependency for Kotlin data class construction —
        // the server's own serialization (getter-based) works fine without it, so this is
        // enough to prove the write-then-read round trip without adding a new main dependency.
        val client = RestTestClient.bindToServer().baseUrl("http://localhost:$port").build()
        val uri = "/api/candles?market=$market&from=$windowStart&to=$windowEnd"

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            val body = client.get().uri(uri).exchange()
                .expectStatus().isOk()
                .expectBody(String::class.java)
                .returnResult()
                .responseBody

            assertThat(body).contains("\"market\":\"$market\"")
            assertThat(body).contains("\"close\":710.0")
            assertThat(body).contains("\"volume\":12345.6")
        }
    }
}
