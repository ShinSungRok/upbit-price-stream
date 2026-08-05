package com.upbit.stream.api.integration

import com.redis.testcontainers.RedisContainer
import com.upbit.stream.common.event.TickerEvent
import com.upbit.stream.common.kafka.KafkaTopics
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.web.reactive.server.WebTestClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration

/**
 * Verifies the real Kafka -> [com.upbit.stream.api.cache.LatestPriceCache] -> Valkey -> `/api/latest/{market}`
 * path against real containers. `AvroConsumerConfig`'s listener container factory also needs a
 * reachable Apicurio registry to construct (even though this test never publishes to candle.1m).
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LatestPriceIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:4.2.1"))

        @Container
        @JvmStatic
        @ServiceConnection(name = "redis")
        val redis: RedisContainer =
            RedisContainer(DockerImageName.parse("valkey/valkey:8.1.9").asCompatibleSubstituteFor("redis"))

        @Container
        @JvmStatic
        val apicurio: GenericContainer<*> =
            GenericContainer(DockerImageName.parse("apicurio/apicurio-registry:3.3.1"))
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forStatusCode(200))

        @DynamicPropertySource
        @JvmStatic
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("apicurio.registry.url") {
                "http://${apicurio.host}:${apicurio.getMappedPort(8080)}/apis/registry/v3"
            }
        }
    }

    @LocalServerPort
    private var port: Int = 0

    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun `ticker published to kafka is readable via latest price endpoint`() {
        val market = "KRW-ETH"
        val ticker = TickerEvent(
            type = "ticker",
            market = market,
            openingPrice = 4_000_000.0,
            highPrice = 4_100_000.0,
            lowPrice = 3_950_000.0,
            tradePrice = 4_050_000.0,
            prevClosingPrice = 3_990_000.0,
            change = "RISE",
            changePrice = 60_000.0,
            signedChangePrice = 60_000.0,
            changeRate = 0.015,
            signedChangeRate = 0.015,
            tradeVolume = 0.5,
            accTradeVolume = 200.0,
            accTradePrice = 800_000_000.0,
            tradeTimestamp = System.currentTimeMillis(),
            timestamp = System.currentTimeMillis(),
        )
        val payload = json.encodeToString(TickerEvent.serializer(), ticker)

        KafkaProducer<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ),
        ).use { producer ->
            producer.send(ProducerRecord(KafkaTopics.UPBIT_TICKER_RAW, market, payload)).get()
        }

        val client = WebTestClient.bindToServer().baseUrl("http://localhost:$port").build()

        await().atMost(Duration.ofSeconds(30)).untilAsserted {
            val body = client.get().uri("/api/latest/$market").exchange()
                .expectStatus().isOk
                .returnResult(String::class.java)
                .responseBody
                .blockFirst(Duration.ofSeconds(5))
            assertThat(body).isEqualTo(payload)
        }
    }
}
