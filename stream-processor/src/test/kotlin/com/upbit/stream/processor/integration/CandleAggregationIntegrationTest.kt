package com.upbit.stream.processor.integration

import com.upbit.stream.common.avro.CandleEventAvroMapper
import com.upbit.stream.common.event.TickerEvent
import com.upbit.stream.common.kafka.KafkaTopics
import io.apicurio.registry.resolver.config.SchemaResolverConfig
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer
import kotlinx.serialization.json.Json
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.UUID

/**
 * Exercises the real Spring context (including [com.upbit.stream.processor.config.CandleAggregationTopologyConfig]
 * and its `@EnableKafkaStreams` topology) against real Kafka + Apicurio containers — unlike
 * [com.upbit.stream.processor.topology.CandleTopologyTest], which drives the topology in isolation
 * with [org.apache.kafka.streams.TopologyTestDriver] and a registry-free serde. This test is the
 * one that actually proves the Apicurio schema register/resolve round-trip works end to end.
 */
@Testcontainers
@SpringBootTest
class CandleAggregationIntegrationTest {

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

        private fun registryUrl() = "http://${apicurio.host}:${apicurio.getMappedPort(8080)}/apis/registry/v3"

        @DynamicPropertySource
        @JvmStatic
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
            registry.add("apicurio.registry.url") { registryUrl() }
        }
    }

    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun `raw ticker produces a decodable candle on candle-1m`() {
        val market = "KRW-BTC"
        val tradePrice = 100_500_000.0
        val ticker = TickerEvent(
            type = "ticker",
            market = market,
            openingPrice = 100_000_000.0,
            highPrice = 101_000_000.0,
            lowPrice = 99_000_000.0,
            tradePrice = tradePrice,
            prevClosingPrice = 99_800_000.0,
            change = "RISE",
            changePrice = 700_000.0,
            signedChangePrice = 700_000.0,
            changeRate = 0.007,
            signedChangeRate = 0.007,
            tradeVolume = 0.01,
            accTradeVolume = 123.45,
            accTradePrice = 12_345_000_000.0,
            tradeTimestamp = System.currentTimeMillis(),
            timestamp = System.currentTimeMillis(),
        )

        KafkaProducer<String, String>(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ),
        ).use { producer ->
            producer.send(
                ProducerRecord(
                    KafkaTopics.UPBIT_TICKER_RAW,
                    market,
                    json.encodeToString(TickerEvent.serializer(), ticker),
                ),
            ).get()
        }

        // Configured explicitly (matching AvroRegistrySerde's pattern) rather than relying on
        // KafkaConsumer's constructor to call configure() on our behalf — leaving that to the
        // constructor left the deserializer's schemaResolver uninitialized, causing a NPE from
        // Apicurio's close() during consumer.close() below.
        val candleDeserializer = AvroKafkaDeserializer<GenericRecord>().apply {
            configure(mapOf(SchemaResolverConfig.REGISTRY_URL to registryUrl()), false)
        }
        val consumer = KafkaConsumer<String, GenericRecord>(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "test-${UUID.randomUUID()}",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ),
            StringDeserializer(),
            candleDeserializer,
        )
        consumer.subscribe(listOf(KafkaTopics.CANDLE_1M))

        try {
            val received = mutableListOf<GenericRecord>()
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                consumer.poll(Duration.ofMillis(500)).forEach { received += it.value() }
                assertThat(received).isNotEmpty()
            }

            val candle = CandleEventAvroMapper.fromGenericRecord(received.first())
            assertThat(candle.market).isEqualTo(market)
            assertThat(candle.close).isEqualTo(tradePrice)
            assertThat(candle.high).isEqualTo(tradePrice)
            assertThat(candle.low).isEqualTo(tradePrice)
        } finally {
            consumer.close()
        }
    }
}
