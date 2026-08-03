package com.upbit.stream.processor.topology

import com.upbit.stream.common.event.CandleEvent
import com.upbit.stream.common.event.TickerEvent
import com.upbit.stream.common.kafka.KafkaTopics
import com.upbit.stream.processor.candle.CandleAccumulator
import com.upbit.stream.processor.serde.KotlinxJsonSerde
import kotlinx.serialization.json.Json
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import java.time.Duration

/**
 * Pure (Spring-free) topology definition so it can be exercised directly with
 * [org.apache.kafka.streams.TopologyTestDriver] in unit tests, independent of the
 * Spring context that wires it up in [com.upbit.stream.processor.config.CandleAggregationTopologyConfig].
 */
object CandleTopology {

    private val json = Json { ignoreUnknownKeys = true }
    private val tickerSerde = KotlinxJsonSerde(TickerEvent.serializer(), json)
    private val candleAccumulatorSerde = KotlinxJsonSerde(CandleAccumulator.serializer(), json)

    fun build(streamsBuilder: StreamsBuilder): KStream<String, String> {
        val rawTicks: KStream<String, String> =
            streamsBuilder.stream(KafkaTopics.UPBIT_TICKER_RAW, Consumed.with(Serdes.String(), Serdes.String()))

        val validTicks: KStream<String, TickerEvent> = rawTicks
            .mapValues { raw -> runCatching { json.decodeFromString(TickerEvent.serializer(), raw) }.getOrNull() }
            .filter { _, ticker -> ticker != null }
            .mapValues { ticker -> ticker!! }

        val windowedCandles = validTicks
            .groupByKey(Grouped.with(Serdes.String(), tickerSerde))
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .aggregate(
                { CandleAccumulator() },
                { _, ticker, acc -> acc.accumulate(ticker) },
                Materialized.with(Serdes.String(), candleAccumulatorSerde),
            )

        val candleStream: KStream<String, String> = windowedCandles
            .toStream()
            .map { windowedKey, acc ->
                val candle = acc.toCandleEvent(
                    market = windowedKey.key(),
                    windowStart = windowedKey.window().start(),
                    windowEnd = windowedKey.window().end(),
                )
                KeyValue(windowedKey.key(), json.encodeToString(CandleEvent.serializer(), candle))
            }

        candleStream.to(KafkaTopics.CANDLE_1M, Produced.with(Serdes.String(), Serdes.String()))

        return candleStream
    }
}
