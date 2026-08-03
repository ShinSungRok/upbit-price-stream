package com.upbit.stream.processor.topology

import com.upbit.stream.common.event.CandleEvent
import com.upbit.stream.common.kafka.KafkaTopics
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.Properties

class CandleTopologyTest {

    private val json = Json { ignoreUnknownKeys = true }
    private lateinit var driver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<String, String>
    private lateinit var outputTopic: TestOutputTopic<String, String>

    @BeforeEach
    fun setUp() {
        val builder = StreamsBuilder()
        CandleTopology.build(builder)

        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "candle-topology-test")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        }

        driver = TopologyTestDriver(builder.build(), props)
        inputTopic = driver.createInputTopic(
            KafkaTopics.UPBIT_TICKER_RAW,
            Serdes.String().serializer(),
            Serdes.String().serializer(),
        )
        outputTopic = driver.createOutputTopic(
            KafkaTopics.CANDLE_1M,
            Serdes.String().deserializer(),
            Serdes.String().deserializer(),
        )
    }

    @AfterEach
    fun tearDown() {
        driver.close()
    }

    private fun tickerJson(market: String, tradePrice: Double, tradeVolume: Double, timestamp: Long): String =
        """
        {
          "type": "ticker",
          "code": "$market",
          "opening_price": $tradePrice,
          "high_price": $tradePrice,
          "low_price": $tradePrice,
          "trade_price": $tradePrice,
          "prev_closing_price": $tradePrice,
          "change": "EVEN",
          "change_price": 0.0,
          "signed_change_price": 0.0,
          "change_rate": 0.0,
          "signed_change_rate": 0.0,
          "trade_volume": $tradeVolume,
          "acc_trade_volume": $tradeVolume,
          "acc_trade_price": 0.0,
          "trade_timestamp": $timestamp,
          "timestamp": $timestamp
        }
        """.trimIndent()

    @Test
    fun `aggregates ticks within the same window into a single OHLCV candle`() {
        val windowStart = Instant.parse("2026-07-27T10:00:00Z")

        inputTopic.pipeInput("KRW-BTC", tickerJson("KRW-BTC", 100_000_000.0, 0.01, windowStart.toEpochMilli()), windowStart)
        inputTopic.pipeInput(
            "KRW-BTC",
            tickerJson("KRW-BTC", 101_500_000.0, 0.02, windowStart.plusSeconds(10).toEpochMilli()),
            windowStart.plusSeconds(10),
        )
        inputTopic.pipeInput(
            "KRW-BTC",
            tickerJson("KRW-BTC", 99_800_000.0, 0.03, windowStart.plusSeconds(20).toEpochMilli()),
            windowStart.plusSeconds(20),
        )

        val outputs = outputTopic.readKeyValuesToList()
        assertThat(outputs).hasSize(3)

        val finalCandle = json.decodeFromString(CandleEvent.serializer(), outputs.last().value)

        assertThat(finalCandle.market).isEqualTo("KRW-BTC")
        assertThat(finalCandle.open).isEqualTo(100_000_000.0)
        assertThat(finalCandle.high).isEqualTo(101_500_000.0)
        assertThat(finalCandle.low).isEqualTo(99_800_000.0)
        assertThat(finalCandle.close).isEqualTo(99_800_000.0)
        assertThat(finalCandle.volume).isEqualTo(0.06)
    }

    @Test
    fun `ticks in different windows produce separate candles`() {
        val windowStart = Instant.parse("2026-07-27T10:00:00Z")

        inputTopic.pipeInput("KRW-ETH", tickerJson("KRW-ETH", 5_000_000.0, 1.0, windowStart.toEpochMilli()), windowStart)
        inputTopic.pipeInput(
            "KRW-ETH",
            tickerJson("KRW-ETH", 5_100_000.0, 1.0, windowStart.plusSeconds(90).toEpochMilli()),
            windowStart.plusSeconds(90),
        )

        val outputs = outputTopic.readKeyValuesToList()
        val candles = outputs.map { json.decodeFromString(CandleEvent.serializer(), it.value) }

        assertThat(candles.map { it.windowStartEpochMillis }.distinct()).hasSize(2)
        assertThat(candles[0].volume).isEqualTo(1.0)
        assertThat(candles[1].volume).isEqualTo(1.0)
    }

    @Test
    fun `malformed ticker JSON is dropped instead of failing the stream`() {
        val windowStart = Instant.parse("2026-07-27T10:00:00Z")

        inputTopic.pipeInput("KRW-XRP", "not valid json", windowStart)
        inputTopic.pipeInput("KRW-XRP", tickerJson("KRW-XRP", 800.0, 5.0, windowStart.toEpochMilli()), windowStart)

        val outputs = outputTopic.readKeyValuesToList()
        assertThat(outputs).hasSize(1)

        val candle = json.decodeFromString(CandleEvent.serializer(), outputs.single().value)
        assertThat(candle.open).isEqualTo(800.0)
        assertThat(candle.volume).isEqualTo(5.0)
    }
}
